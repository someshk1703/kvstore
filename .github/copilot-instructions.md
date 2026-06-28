<!-- SPECKIT START -->
## Project: KVStore — Redis-inspired Distributed Key-Value Store

**Language / runtime:** Java 21, Maven 3.9+  
**Maven project root:** `kvstore/` (all `mvn` commands run from there)  
**Main class:** `com.somesh.kvstore.Main`  
**Artifact:** `kvstore/target/kvstore-0.1.0-SNAPSHOT.jar`

### Build commands
```bash
# compile
cd kvstore && mvn compile

# run all tests
cd kvstore && mvn test

# package fat jar
cd kvstore && mvn clean package -q

# run
java -jar kvstore/target/kvstore-0.1.0-SNAPSHOT.jar
```

### Package layout
```
com.somesh.kvstore
├── engine/       – KVStore (ConcurrentHashMap), ValueEntry, CommandExecutor
├── memory/       – LRUCache (O(1) doubly-linked-list), ExpiryManager
├── persistence/  – AOFWriter (WAL), SnapshotManager, CrashRecovery
├── protocol/     – CommandParser, ResponseSerializer, Command enum
├── server/       – TcpServer (ServerSocket + thread pool), ClientHandler
├── replication/  – ReplicationManager, ReplicaConnection, RingBuffer
└── cluster/      – ConsistentHashRing (Week 6 bonus)
```

### Key design constraints
- No Redis client libraries; every subsystem is hand-rolled
- TCP protocol is RESP-lite: `+OK`, `$bulk`, `:integer`, `*array`
- Persistence: AOF (every write) + periodic snapshot; both replayed on startup
- Replication: async primary→replica propagation; `WAIT` for synchronous ack
- Eviction: lazy expiry sweep every 100 ms; LRU eviction when over memory cap
- Cluster (Week 6): consistent hashing with 150 virtual nodes per physical node

### Week-by-week milestones
| Week | Focus | Status |
|---|---|---|
| 1 | Core engine — GET/SET/DEL over TCP | scaffolded |
| 2 | TTL + LRU eviction | pending |
| 3 | AOF + Snapshot persistence | pending |
| 4 | Primary-replica replication | pending |
| 5 | HTTP API (Spring Boot) + Docker | pending |
| 6 | Consistent hashing cluster (bonus) | pending |

For the full spec see `plan/kv_store_spec.html`.  
For the implementation plan see `plan/Build.md`.
<!-- SPECKIT END -->
