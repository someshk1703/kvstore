# Feature Specification: Week 2 — TTL + LRU Eviction

**Feature Branch**: `002-week2-ttl-lru`

**Created**: 2026-07-07

**Status**: Completed ✅

---

## User Scenarios & Testing

### User Story 1 — Key Expiry via TTL (Priority: P1)

A developer can set keys with a time-to-live so that they automatically become inaccessible after the TTL elapses — without requiring the client to explicitly delete them.

**Why this priority**: TTL is a fundamental Redis feature used in caching, session management, and rate limiting.

**Independent Test**: `SET session:abc token123 EX 2` → wait 3 seconds → `GET session:abc` → `$-1`

**Acceptance Scenarios**:

1. **Given** `SET k v EX 1`, **When** GET is called within 1 s, **Then** returns `v`
2. **Given** `SET k v EX 1`, **When** GET is called after 1 s elapses, **Then** returns `$-1`
3. **Given** key `k` with TTL, **When** `TTL k`, **Then** returns positive integer (seconds remaining)
4. **Given** key `k` with no TTL, **When** `TTL k`, **Then** returns `:-1`
5. **Given** key `k` doesn't exist, **When** `TTL k`, **Then** returns `:-2`

---

### User Story 2 — TTL Management Commands (Priority: P2)

A developer can update or remove a TTL on an existing key without replacing its value.

**Acceptance Scenarios**:

1. **Given** `SET k v`, **When** `EXPIRE k 10`, **Then** returns `:1` and `TTL k` returns positive
2. **Given** `SET k v EX 10`, **When** `PERSIST k`, **Then** `TTL k` returns `:-1`
3. **Given** key not set, **When** `EXPIRE nosuchkey 10`, **Then** returns `:0`

---

### User Story 3 — LRU Eviction under Memory Pressure (Priority: P3)

When the store exceeds its memory cap, the least-recently-used key is evicted first, ensuring the most-accessed data stays hot.

**Acceptance Scenarios**:

1. **Given** LRU capacity of 3 and keys A, B, C stored, **When** key D is stored, **Then** the least-recently-used of A/B/C is evicted
2. **Given** keys A, B, C and then A accessed, **When** D is stored, **Then** B is evicted (A was promoted to MRU)

---

### Edge Cases

- What if `EXPIRE` is called on an already-expired key? → Returns `:0` (key not found)
- What if a key's TTL expires between SET and the background sweep finding it? → Lazy expiry on GET returns `$-1` correctly
- Can the background sweep fall behind if many keys expire at once? → Sweep repeats if >25% of sampled keys were expired

---

## Functional Requirements

| ID | Requirement |
|----|-------------|
| FR-2.1 | `SET key value EX <seconds>` stores with TTL |
| FR-2.2 | `EXPIRE key <seconds>` sets TTL on existing key |
| FR-2.3 | `TTL key` returns remaining seconds; `-1` no expiry; `-2` missing |
| FR-2.4 | `PERSIST key` removes TTL from a key |
| FR-2.5 | Expired keys are removed lazily on GET |
| FR-2.6 | Background sweep runs every 100 ms to evict expired keys proactively |
| FR-2.7 | LRU eviction triggers when memory exceeds configurable cap |
| FR-2.8 | LRU implementation uses O(1) doubly-linked list (no `LinkedHashMap`) |

---

## Success Criteria

- `mvn test` exits `BUILD SUCCESS` with zero failures
- Key set with `EX 1` is inaccessible after 1 second
- Expired key removed by background sweep within 200 ms (without any GET)
- LRU eviction correctly removes least-recently-used entry when capacity is exceeded
- All TTL commands return semantically correct values

---

## Assumptions

- `EX` is in seconds; `PX` (milliseconds) is also supported as an extension
- Memory cap defaults to 128 MB; LRU capacity defaults to 100,000 keys
- Background sweep uses `ScheduledExecutorService` (daemon thread)
