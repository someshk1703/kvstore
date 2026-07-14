# Feature Specification: Week 3 — AOF + Snapshot Persistence

**Feature Branch**: `003-week3-aof-snapshot`

**Created**: 2026-07-14

**Status**: Completed ✅ (2026-07-14)

---

## User Scenarios & Testing

### User Story 1 — Survive a Restart via AOF Replay (Priority: P1)

A developer stores data, kills the JVM process (simulating a crash), restarts the server, and all previously written data is fully restored. The Append-Only File is the write-ahead log that makes this possible.

**Why this priority**: Without durability, the store is just an in-memory cache. Persistence is what makes it a database.

**Independent Test**: `SET persist_me hello` → kill JVM (`kill -9`) → restart → `GET persist_me` → `$hello`

**Acceptance Scenarios**:

1. **Given** 3 SET commands executed, **When** AOF file is inspected, **Then** it contains exactly 3 command lines in order
2. **Given** JVM killed mid-run, **When** server restarts and replays AOF, **Then** all written keys are restored
3. **Given** AOF file with a truncated last line, **When** replay is attempted, **Then** the malformed line is skipped and recovery continues (no crash)
4. **Given** a key is SET then DEL'd in that order in AOF, **When** replayed, **Then** the key is absent after recovery

---

### User Story 2 — Snapshot for Fast Startup (Priority: P2)

The server periodically writes a full point-in-time snapshot of the keyspace so that cold restarts do not need to replay the entire AOF history — reducing startup time significantly after long operation.

**Why this priority**: AOF can grow unbounded; snapshots provide a compact baseline.

**Independent Test**: Start server, write 10 keys, wait 65 seconds, stop server — `data/kvstore.rdb` exists and all 10 keys load from it.

**Acceptance Scenarios**:

1. **Given** server running, **When** 60 seconds elapse, **Then** snapshot file exists on disk
2. **Given** snapshot file exists, **When** loaded into a fresh store, **Then** all non-expired keys match exactly
3. **Given** a key had `EX 1` and it has since expired, **When** snapshot is loaded, **Then** that key is NOT present

---

### User Story 3 — Combined Recovery: Snapshot + AOF Delta (Priority: P2)

When both a snapshot and an AOF exist, the server loads the snapshot (fast baseline) and replays only the AOF entries written after the snapshot — avoiding double-application of already-captured commands.

**Acceptance Scenarios**:

1. **Given** snapshot at T=100 and AOF with entries before and after T=100, **When** recovery runs, **Then** only post-T=100 entries are replayed
2. **Given** a key SET before snapshot and DEL'd after snapshot, **When** recovery runs, **Then** the key is absent
3. **Given** only AOF exists (no snapshot), **When** recovery runs, **Then** full AOF is replayed
4. **Given** only snapshot exists (no AOF), **When** recovery runs, **Then** snapshot is loaded successfully
5. **Given** neither file exists, **When** server starts, **Then** starts with empty store

---

### User Story 4 — AOF Compaction (Priority: P3)

After the store has been running for a while, the AOF grows with redundant entries (multiple SETs to the same key). An AOF rewrite compacts this down to one SET per live key, atomically swapping in the new file.

**Acceptance Scenarios**:

1. **Given** key SET 100 times with different values, **When** AOF rewrite runs, **Then** new AOF has exactly 1 SET for that key
2. **Given** AOF rewrite in progress, **When** new writes come in concurrently, **Then** no writes are lost

---

### Edge Cases

- What if a snapshot write fails mid-way? → Temp file is cleaned up; old snapshot is preserved
- What happens if both snapshot and AOF are corrupted? → Server starts empty (logs a warning)
- Can the snapshot thread block client writes? → No; snapshot serializes current store state and writes in a background thread; client writes are not blocked

---

## Functional Requirements

| ID | Requirement |
|----|-------------|
| FR-3.1 | Every write command (SET, DEL, EXPIRE, PERSIST) is appended to the AOF immediately after in-memory store update |
| FR-3.2 | AOF fsync mode is configurable: `ALWAYS` (flush per write) or `EVERYSEC` (background flush) |
| FR-3.3 | AOF replay reads line-by-line, parses each command, re-executes against a fresh store |
| FR-3.4 | A malformed or truncated last line in the AOF is skipped; recovery does not abort |
| FR-3.5 | Snapshot is triggered every 60 seconds OR every 10,000 writes (whichever comes first) |
| FR-3.6 | Snapshot write is atomic: write to `.tmp` file, then rename to `.rdb` |
| FR-3.7 | Snapshot format: one entry per line as `<key>\t<value>\t<expiresAt_epoch_ms>` |
| FR-3.8 | After each snapshot, a `# SNAPSHOT <timestamp_ms>` marker is appended to the AOF |
| FR-3.9 | On startup, `CrashRecovery` detects which files exist and applies the correct recovery path |
| FR-3.10 | When both snapshot and AOF exist, snapshot is loaded first; only AOF entries after the `# SNAPSHOT` marker are replayed |
| FR-3.11 | Expired keys are not written to snapshots; expired keys replayed from AOF do not appear if their TTL has elapsed |

---

## Success Criteria

- `mvn test` exits `BUILD SUCCESS` with zero failures
- Three SET commands produce exactly three lines in the AOF file
- A JVM kill followed by restart restores all written keys (smoke-tested manually)
- Snapshot file appears on disk within 65 seconds of server start
- Combined recovery with both snapshot and AOF correctly restores final state
- A DEL command in AOF that post-dates the snapshot removes the key on recovery
- Concurrent writes during a background snapshot are not lost and the snapshot is not corrupted

---

## Assumptions

- AOF format is simple text (one command per line); values with spaces are not supported in Week 3 (protocol limitation)
- Snapshot uses a tab-separated text format (not binary) — simpler to debug, sufficient for Week 3
- `ATOMIC_MOVE` is used for snapshot swap; fallback to `REPLACE_EXISTING` if filesystem doesn't support it
- Default fsync mode is `ALWAYS` for strongest durability guarantee
- AOF and snapshot files live in `data/` directory relative to working directory
