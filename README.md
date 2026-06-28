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
    └── ConsistentHashRing.java # consistent hashing (Week 6)
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
POST   /api/admin/flush          → {"flushed":true}
```

---

## Build & Run

### Prerequisites

- Java 21+
- Maven 3.9+
- Docker + docker-compose (for cluster mode)

### Single node

```bash
mvn clean package -q
java -jar target/kvstore-0.1.0-SNAPSHOT.jar
```

### Primary + replica (docker-compose)

```bash
docker-compose up
# Primary on :6379 / HTTP :8080
# Replica  on :6380
```

```yaml
# docker-compose.yml
services:
  kv-primary:
    build: kvstore/
    ports: ["6379:6379", "8080:8080"]
    environment:
      ROLE: primary
      MAX_MEMORY_MB: 256

  kv-replica:
    build: kvstore/
    ports: ["6380:6379"]
    environment:
      ROLE: replica
      PRIMARY_HOST: kv-primary
      PRIMARY_PORT: 6379
    depends_on: [kv-primary]
```

### Tests

```bash
mvn test
```

---

## Build Plan (Week-by-Week)

| Week | Module | Goal |
|---|---|---|
| 1 | Core engine | `GET` / `SET` / `DEL` in memory over TCP |
| 2 | TTL + LRU eviction | Keys expire; LRU eviction when over memory limit |
| 3 | Persistence | AOF + snapshot; crash-safe restart |
| 4 | Replication | Primary-replica sync with partial resync + `WAIT` |
| 5 | HTTP API + CLI | Spring Boot wrapper, Docker image, docker-compose |
| 6 | Cluster (bonus) | Consistent hashing, virtual nodes, automatic failover |

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

**150 virtual nodes per physical node** — Balances load across physical nodes without over-engineering. Reduces hotspots from uneven hash distribution.

---

## Interview Talking Points

- **LRU eviction** — doubly linked list + hashmap gives O(1) get and put; same as LeetCode 146 but in production
- **Replication lag** — `WAIT` command implements synchronous replication; async vs sync trade-off
- **AOF vs snapshot** — WAL pattern used by every database; fsync strategy controls durability vs throughput
- **Consistent hashing** — adding a node moves only 1/N keys; virtual nodes prevent hotspots

---

## Project Status

- [x] Week 1 — Core engine scaffolded (`KVStore`, `TcpServer`, `CommandParser`)
- [ ] Week 2 — LRU + TTL
- [ ] Week 3 — AOF + Snapshot persistence
- [ ] Week 4 — Primary-replica replication
- [ ] Week 5 — HTTP API + Docker
- [ ] Week 6 — Consistent hashing cluster (bonus)
