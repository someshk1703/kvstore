# KVStore — Redis-inspired Distributed Key-Value Store

A Redis-inspired distributed key-value store built from scratch in Java — no Redis libraries, no Spring Data. Every component (storage engine, eviction policy, persistence layer, replication protocol) is hand-rolled to deeply understand what production systems do under the hood.

> **Resume line:** "Built a Redis-inspired distributed key-value store in Java from scratch — LRU eviction, TTL, AOF persistence, primary-replica replication with partial resync, and consistent hashing cluster mode. Benchmarked at 120k ops/sec on a single node."

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│  Client Layer  (TCP sockets / HTTP via thin wrapper) │
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────┐
│  Protocol Layer  (CommandParser / ResponseSerializer) │
│  Text-based RESP-lite: +OK  $bulk  :int  *array      │
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────┐
│  Engine Layer  (KVStore — ConcurrentHashMap)          │
│  All reads & writes; notifies AOF + Replication       │
└────────┬─────────────┬──────────────────────────────┘
         │             │
┌────────▼──────┐  ┌───▼──────────────────────────────┐
│ Memory Mgmt   │  │  Persistence Layer                │
│ LRUCache      │  │  AOFWriter  SnapshotManager        │
│ ExpiryManager │  │  CrashRecovery (startup replay)    │
└───────────────┘  └───────────────────────────────────┘
         │
┌────────▼──────────────────────────────────────────┐
│  Replication Layer  (ReplicationManager)           │
│  Primary → Replicas  ·  RingBuffer backlog         │
│  Partial resync via offset  ·  WAIT command        │
└────────────────────────────────────────────────────┘
         │
┌────────▼──────────────────────────────────────────┐
│  Cluster Layer (Week 6 bonus)                      │
│  ConsistentHashRing  ·  150 virtual nodes          │
└────────────────────────────────────────────────────┘
```

## Package Structure

```
src/main/java/com/somesh/kvstore/
├── engine/
│   ├── KVStore.java           # core storage, main API
│   ├── ValueEntry.java        # value + expiry metadata
│   └── CommandExecutor.java   # routes commands to engine
├── memory/
│   ├── LRUCache.java          # O(1) doubly linked list + hashmap
│   └── ExpiryManager.java     # background expiry sweep (100ms)
├── persistence/
│   ├── AOFWriter.java         # append-only file log (WAL)
│   ├── SnapshotManager.java   # periodic full serialisation
│   └── CrashRecovery.java     # replay AOF/snapshot on startup
├── protocol/
│   ├── CommandParser.java     # raw bytes → Command
│   ├── ResponseSerializer.java
│   └── Command.java           # enum + args
├── server/
│   ├── TcpServer.java         # ServerSocket + thread pool
│   └── ClientHandler.java     # per-client read/write loop
├── replication/
│   ├── ReplicationManager.java
│   ├── ReplicaConnection.java
│   └── RingBuffer.java        # circular command backlog
└── cluster/
    ├── ConsistentHashRing.java  # ring: ConcurrentSkipListMap + 150 vnodes + MD5
    ├── NodeInfo.java            # immutable record (id, host, port)
    ├── ClusterRouter.java       # routes commands; MOVED response factory
    ├── ClusterManager.java      # membership + failover coordinator
    └── HealthMonitor.java       # periodic PING; declares down after N missed beats
```

---

## Command Reference

| Command | Response | Notes |
|---|---|---|
| `SET key value [EX seconds]` | `+OK` | Set a key with optional TTL |
| `GET key` | `$value` or `$-1` | Returns value or null |
| `DEL key [key ...]` | `:N` | Returns count of deleted keys |
| `EXISTS key` | `:0` or `:1` | Key existence check |
| `EXPIRE key seconds` | `:0` or `:1` | Set TTL on existing key |
| `TTL key` | `:seconds` / `:-1` / `:-2` | `-1`=no expiry, `-2`=missing |
| `PERSIST key` | `:0` or `:1` | Remove TTL from key |
| `INCR key` | `:newvalue` | Atomic increment (fails if non-integer) |
| `MSET k1 v1 k2 v2 ...` | `+OK` | Atomic multi-set |
| `MGET k1 k2 ...` | `*N array` | Multi-get |
| `KEYS pattern` | `*N array` | Pattern match — O(N), avoid in prod |
| `FLUSHALL` | `+OK` | Wipe everything — admin only |
| `INFO` | `$stats` | Memory, ops/sec, replication status |
| `REPLCONF` | `+OK` | Replica handshake |
| `WAIT numreplicas timeout` | `:N` | Block until N replicas acknowledge |

### HTTP API (Week 5)

```
GET    /api/keys/{key}           → {"key":"x","value":"y","ttl":30}
POST   /api/keys/{key}           body: {"value":"y","ttlSeconds":30}
DELETE /api/keys/{key}           → {"deleted":true}
GET    /api/keys?pattern=user:*  → {"keys":["user:1","user:2"]}
GET    /api/info                 → {"ops_per_sec":12043,"mem_mb":84,...}
GET    /api/health               → {"status":"UP"}
POST   /api/admin/flush          → {"flushed":true}
```

---

## Build & Run

### Prerequisites

- Java 21+
- Maven 3.9+
- Docker + docker-compose (for cluster mode)

### Single node (TCP only)

```bash
cd kvstore && mvn clean package -q
java -jar target/kvstore-0.1.0-SNAPSHOT.jar
```

### Single node with HTTP API (TCP + HTTP)

```bash
java -jar target/kvstore-0.1.0-SNAPSHOT.jar --http
# TCP  → localhost:6379
# HTTP → localhost:8080
```

### CLI client

```bash
# Single-shot
java -jar target/kvstore-0.1.0-SNAPSHOT.jar --cli SET foo bar
java -jar target/kvstore-0.1.0-SNAPSHOT.jar --cli GET foo
java -jar target/kvstore-0.1.0-SNAPSHOT.jar --cli KEYS 'user:*'

# Interactive REPL
java -jar target/kvstore-0.1.0-SNAPSHOT.jar --cli
kvstore> SET session:1 alice EX 60
+OK
kvstore> GET session:1
"alice"
kvstore> quit
```

### Benchmark

```bash
# Server must be running first
java -jar target/kvstore-0.1.0-SNAPSHOT.jar --benchmark
```

### Primary + replica (docker-compose)

```bash
cd kvstore
docker-compose up          # start primary (:6379 / :8080) + replica (:6381)
docker-compose down        # tear down
```

### Tests

```bash
cd kvstore && mvn test
```

---

## Deploy (Railway)

The repo includes a `railway.toml` at the root that configures Railway to use the multi-stage Dockerfile automatically — no manual dashboard settings needed.

```toml
# railway.toml (already committed)
[build]
builder = "DOCKERFILE"
dockerfilePath = "kvstore/Dockerfile"

[deploy]
healthcheckPath = "/api/health"
healthcheckTimeout = 30
```

**Steps:**
1. Push the repo to GitHub.
2. In Railway → **New Project → Deploy from GitHub repo** → select this repo.
3. Under **Settings → Networking** → **Generate Domain** to get a public HTTPS URL.
4. Add any env vars under **Variables** (e.g. `AOF_ENABLED=true`).
5. Railway injects a `PORT` env var — the HTTP layer reads it automatically (falls back to `8080` locally).

**Two-service setup (primary + replica):**  
Create two Railway services pointing at the same repo. Set the replica service's start command to:
```
--http --replicaof <primary-private-host> 6380
```
Keep replication traffic on Railway's private network — do **not** expose port 6380 publicly.

**Note:** The TCP protocol port (6379) requires Railway's TCP proxy feature (paid tier). The HTTP API works on the free tier via the generated public domain.

---

## Build Plan (Week-by-Week)

| Week | Module | Goal | Status |
|---|---|---|---|
| 1 | Core engine | `GET` / `SET` / `DEL` in memory over TCP | ✅ Complete |
| 2 | TTL + LRU eviction | Keys expire; LRU eviction when over memory limit | ✅ Complete |
| 3 | Persistence | AOF + snapshot; crash-safe restart | ✅ Complete |
| 4 | Replication | Primary-replica sync with partial resync + `WAIT` | ✅ Complete |
| 5 | HTTP API + CLI | Spring Boot wrapper, CLI, benchmark, Docker | ✅ Complete |
| 6 | Cluster (bonus) | Consistent hashing, virtual nodes, automatic failover | ✅ Complete |

### Week 1 — What was built

- **`KVStore`** — `ConcurrentHashMap`-backed store; thread-safe `GET`, `SET`, `DEL`, `EXISTS`, `PING`
- **`CommandParser`** — parses inline and multi-bulk RESP wire format into typed `Command` objects
- **`ResponseSerializer`** — emits `+OK`, `$bulk`, `:integer`, `-ERR` per the RESP-lite spec
- **`TcpServer`** — `ServerSocket` accept loop dispatching each client connection to a fixed thread pool (10 threads)
- **`ClientHandler`** — per-client read/write loop; reads until disconnect, writes serialized response
- **`ServerConfig`** — single constants file (port 6379, pool size, backlog, CRLF)
- **53 unit tests** across engine, parser, and serializer — all green

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Java 21 |
| Concurrency | `ConcurrentHashMap`, `ExecutorService`, CAS |
| Transport | Raw `ServerSocket` / TCP |
| HTTP wrapper | Spring Boot (Week 5 only) |
| Testing | JUnit 5, AssertJ, Mockito |
| Logging | SLF4J + Logback |
| Packaging | Maven, Docker |

---

## Key Design Decisions

**ConcurrentHashMap over synchronized HashMap** — CAS on individual bins; reads are lock-free; near-linear read scaling with CPU cores.

**Lazy expiry** — Background sweep every 100 ms checks a bucket of keys rather than all keys. Same strategy Redis uses. Avoids O(N) scan on every operation.

**AOF + Snapshot together** — Snapshot for fast restore, then replay only AOF commands written after the snapshot timestamp. Best of both durability models.

**Ring buffer backlog** — Fixed-size circular buffer of recent write commands enables partial resync for replicas that briefly disconnect, avoiding a full snapshot retransfer.

**150 virtual nodes per physical node** — Empirically measured: adding a 5th node to a 4-node cluster redistributed 19.7% of keys (theoretical 20%), and distribution stayed within ±11% of even. With 1 vnode the variance would be ~50%; 150 vnodes reduces it to ~10%.

**Failover without consensus** — Automatic failover (primary down → promote replica) is fast but carries a documented split-brain risk. A production system would use Raft/etcd; this implementation deliberately surfaces the trade-off rather than hiding it.

---

## Interview Talking Points

- **LRU eviction** — doubly linked list + hashmap gives O(1) get and put; same as LeetCode 146 but in production
- **Replication lag** — `WAIT` command implements synchronous replication; async vs sync trade-off
- **AOF vs snapshot** — WAL pattern used by every database; fsync strategy controls durability vs throughput
- **Consistent hashing** — adding a node moves only 1/N keys; virtual nodes prevent hotspots

---

## Project Status

- [x] Week 1 — Core engine complete
  - `KVStore` — `ConcurrentHashMap` store with `set` / `get` / `del` / `exists` / `pttl`
  - `ValueEntry` — immutable value + TTL with lazy expiry
  - `CommandResult` — typed RESP result (OK / STRING / INTEGER / ERROR)
  - `CommandExecutor` — dispatch table routing SET / GET / DEL / EXISTS / PTTL / PING
  - `CommandType` + `Command` — parsed command value objects
  - `CommandParser` — inline text → `Command`
  - `ResponseSerializer` — `CommandResult` → RESP-lite wire bytes
  - `TcpServer` — `ServerSocket` + cached thread pool
  - `ClientHandler` — per-client read/parse/execute/write loop
  - `Main` — wires store + server, binds port 6379
- [x] Week 2 — TTL + LRU eviction (`EXPIRE`, `TTL`, `PERSIST`, `ExpiryManager`, `LRUCache`)
- [x] Week 3 — AOF + Snapshot persistence (`AOFWriter`, `SnapshotManager`, `CrashRecovery`)
- [x] Week 4 — Primary-replica replication (`ReplicationManager`, `ReplicaClient`, `RingBuffer`, `WAIT`)
- [x] Week 5 — HTTP API + CLI + Benchmarks + Docker
  - `HttpApiApplication` — Spring Boot 3.2 REST layer on port 8080
  - `KeyValueController` — GET/POST/DELETE/KEYS/INFO endpoints
  - `KvCli` — interactive REPL + single-shot TCP client
  - `BenchmarkRunner` — ConcurrentHashMap vs synchronized HashMap comparison
  - `Dockerfile` — multi-stage build (Maven build + JRE runtime)
  - `docker-compose.yml` — primary (TCP+HTTP) + replica
  - `KVStore#keys(pattern)` — glob pattern matching for KEYS command
- [x] Week 6 — Consistent hashing cluster (bonus)
  - `ConsistentHashRing` — `ConcurrentSkipListMap<Long,String>` ring; 150 vnodes per node via MD5; O(log n) key lookup with clockwise wraparound
  - `NodeInfo` — immutable record `(id, host, port)`
  - `ClusterRouter` — routes commands to owning node; static `movedResponse()` factory for MOVED redirect protocol
  - `ClusterManager` — membership registry; failover: removes down primary, promotes replica in ring
  - `HealthMonitor` — periodic PING probes; declares node DOWN after 3 consecutive missed beats
  - **Measured redistribution: 19.7% of keys moved when adding a 5th node (theoretical 20%)**
  - **Measured distribution: within ±11% of even across 10,000 keys with 3 nodes**
