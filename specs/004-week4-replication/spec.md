# Feature Specification: Week 4 — Primary-Replica Replication

**Feature Branch**: `004-week4-replication`

**Created**: 2026-07-17

**Status**: Completed ✅

---

## User Scenarios & Testing

### User Story 1 — Fresh Replica Sync (Priority: P1)

A fresh replica connects to a running primary. The primary sends a full snapshot of its current keyspace. The replica applies every entry. After sync, both stores are byte-for-byte identical.

**Why this priority**: Without initial sync, a replica that starts empty is useless.

**Independent Test**: Start primary, `SET name somesh`, `SET city pune`. Start replica with `--replicaof localhost 6380`. `GET name` on replica returns `somesh`.

**Acceptance Scenarios**:

1. **Given** primary has keys `{name=somesh, city=pune}`, **When** fresh replica connects, **Then** replica contains `name=somesh` and `city=pune` after sync
2. **Given** primary has a key with `EX 30`, **When** replica syncs, **Then** the key has a positive TTL on the replica (less than the primary's TTL due to propagation lag)
3. **Given** primary has an expired key, **When** replica syncs, **Then** that expired key is NOT present on the replica

---

### User Story 2 — Live Write Propagation (Priority: P1)

After initial sync, every write command on the primary propagates to all connected replicas asynchronously. Replicas apply commands in offset order.

**Why this priority**: A replica that stops updating after sync is a snapshot, not a replica.

**Acceptance Scenarios**:

1. **Given** replica connected, **When** primary executes `SET k v`, **Then** replica applies `SET k v` and `GET k` returns `v` on replica
2. **Given** replica connected, **When** primary executes `DEL k`, **Then** replica removes `k` and `GET k` returns `$-1`
3. **Given** replica connected, **When** 100 writes are made on primary, **Then** replica receives all 100 in correct offset order

---

### User Story 3 — Short Disconnect: Partial Resync (Priority: P2)

A replica disconnects briefly (less than the backlog capacity of commands) and reconnects. The primary streams only the missed commands — no full snapshot transfer needed.

**Why this priority**: Avoiding full resyncs makes reconnects cheap — critical for stability under network hiccups.

**Acceptance Scenarios**:

1. **Given** replica at offset 50, **When** it reconnects and the backlog still contains offset 51, **Then** only commands from offset 51 onward are replayed
2. **Given** backlog capacity of 1024, **When** replica misses 100 commands, **Then** partial resync delivers exactly 100 commands
3. **Given** partial resync complete, **When** primary gets new writes, **Then** replica continues receiving live propagation normally

---

### User Story 4 — Long Disconnect: Full Resync Fallback (Priority: P2)

A replica falls too far behind — the missed commands have been rotated out of the ring-buffer backlog. The primary triggers a full resync: sends a snapshot, then switches to live streaming from the snapshot's offset forward.

**Acceptance Scenarios**:

1. **Given** backlog capacity of 4 and replica misses 6 writes, **When** it reconnects, **Then** `backlog.contains(lastOffset+1)` returns `false` and full resync is initiated
2. **Given** full resync initiated, **When** complete, **Then** replica's store exactly matches primary's current state
3. **Given** full resync in progress, **When** primary receives new writes, **Then** those writes are propagated after the resync stream completes

---

### User Story 5 — WAIT Command (Priority: P2)

A client can call `WAIT numreplicas timeoutMs` to block until at least N replicas have acknowledged the current write offset, or the timeout elapses. Returns the actual count of replicas that acked.

**Why this priority**: Without WAIT, async replication has no escape hatch for synchronous durability requirements.

**Independent Test**: `SET k v` → `WAIT 1 100` → returns `:1` if replica acks within 100ms; returns `:0` if replica is down.

**Acceptance Scenarios**:

1. **Given** 1 connected replica that is healthy, **When** `WAIT 1 100`, **Then** returns `:1` before the timeout
2. **Given** 0 connected replicas, **When** `WAIT 1 100`, **Then** returns `:0` at ~100ms
3. **Given** 2 replicas, **When** `WAIT 2 0` (non-blocking), **Then** returns current ack count immediately

---

### User Story 6 — Replica Mode: Read-Only Enforcement (Priority: P1)

While in replica mode, write commands from external clients are rejected with a `READONLY` error. Read commands (`GET`, `EXISTS`, `TTL`, `PING`) succeed normally.

**Acceptance Scenarios**:

1. **Given** server in replica mode, **When** client sends `SET k v`, **Then** returns `-READONLY You can't write against a read only replica.`
2. **Given** server in replica mode, **When** client sends `GET k`, **Then** returns the current value
3. **Given** server in replica mode, **When** replication stream applies `SET k v`, **Then** the write succeeds (replication path bypasses READONLY check)

---

### Edge Cases

- What if the primary crashes *after* acknowledging a write but *before* propagating it? → That write is lost on replicas. Use `WAIT 1 0` for synchronous durability guarantees.
- What if a replica's value for a key with TTL drifts from the primary's? → The TTL is propagated as `PX <remainingMs>` computed at propagation time; small drift is expected and acceptable.
- What if two replicas reconnect simultaneously? → Each is handled independently by the replication manager; no inter-replica dependencies.
- What if `WAIT` is called on a standalone server with no replication manager? → Returns `0` immediately (no replicas exist).

---

## Functional Requirements

| ID | Requirement |
|----|-------------|
| FR-4.1 | `RingBuffer<T>` implements a fixed-capacity circular backlog with monotonic offsets |
| FR-4.2 | `RingBuffer.append(T)` returns a monotonically increasing offset; thread-safe |
| FR-4.3 | `RingBuffer.contains(offset)` returns `true` iff item has not been overwritten |
| FR-4.4 | `RingBuffer.getRange(fromOffset)` returns items from `fromOffset` to latest, or empty if evicted |
| FR-4.5 | Every successful write command is propagated to all connected replicas asynchronously |
| FR-4.6 | Fresh replica (offset=-1) triggers full resync: snapshot stream + live streaming |
| FR-4.7 | Reconnecting replica whose last offset is in backlog triggers partial resync |
| FR-4.8 | Reconnecting replica whose last offset is evicted triggers full resync |
| FR-4.9 | `WAIT numreplicas timeoutMs` blocks until N replicas ack or timeout elapses |
| FR-4.10 | `REPLINFO` returns current replication role, master offset, and connected replica count |
| FR-4.11 | Replica mode rejects `SET`, `DEL`, `EXPIRE`, `PERSIST` from external clients |
| FR-4.12 | Replica mode allows read commands (`GET`, `EXISTS`, `TTL`, `PTTL`, `PING`) |
| FR-4.13 | `--replicaof host port` flag starts the server in replica mode |
| FR-4.14 | `--with-replication` flag starts primary with replication listener on port 6380 |

---

## Success Criteria

- `mvn test` exits `BUILD SUCCESS` with zero failures
- All 32 replication tests (`RingBufferTest`, `ReplicationManagerTest`, `ReplicationIntegrationTest`) pass
- A fresh replica receives all primary data after connecting
- Partial resync delivers only the missed commands, not the full history
- `WAIT` returns correctly under both "replica keeps up" and "replica down" scenarios
- Write commands are rejected on replicas with `READONLY` error

---

## Architecture

```
Primary:
  TcpServer (port 6379) ← external clients
  ReplicationManager
    ├── RingBuffer<String> backlog (capacity=1024)
    ├── CopyOnWriteArrayList<ReplicaConnection> replicas
    └── Replica listener (port 6380)

Replica:
  TcpServer (port 6381) ← external clients (read-only)
  ReplicaClient ← connects to primary:6380
    └── applies *REPL <offset> <command> to CommandExecutor
```

## Key Design Decisions

### Propagation timing: after local write, not before
Writes are applied to the local store first, then propagated. This means:
- Primary acknowledgement does not require replica acknowledgement (async by default)
- Use `WAIT` to synchronize when durability requires it
- A primary crash between local write and propagation loses the write on replicas (acceptable trade-off)

### CopyOnWriteArrayList for replica registry
The replica list is read on every write (hot path) and written only when replicas connect or disconnect (cold path). COW is optimal for this read-heavy ratio.

### Ring buffer vs unbounded list
A ring buffer bounds memory at `O(capacity)` items regardless of write volume. When a replica falls too far behind, it pays for a full resync — the correct trade-off between memory and network.

### Full resync protocol
1. Primary sends `FULLRESYNC <masterOffset>`
2. Primary streams current store as `*REPL <offset> SET key value [PX ttlMs]` lines
3. Primary sends `FULLRESYNC_END`
4. Replica switches to live streaming mode

### Partial resync protocol
1. Replica sends `REPLCONF <lastOffset>`
2. Primary checks `backlog.contains(lastOffset + 1)`
3. If yes: streams `backlog.getRange(lastOffset + 1)` then registers for live propagation
4. Replica continues sending `*ACK <offset>` for each applied command

---

## Assumptions

- Replication is asynchronous by default; `WAIT` provides synchronous semantics on demand
- The ring buffer tracks command count (not bytes); `REPLICATION_BACKLOG_SIZE = 1024` commands
- Values containing spaces are safe because the parser tokenizes correctly
- Replica-to-primary failover (promotion) is out of scope for Week 4
