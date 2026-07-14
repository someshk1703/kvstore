# Feature Specification: Week 1 — Core Engine: GET / SET / DEL over TCP

**Feature Branch**: `001-week1-core-engine`

**Created**: 2026-07-07

**Status**: Completed ✅

---

## User Scenarios & Testing

### User Story 1 — Basic Key-Value Storage (Priority: P1)

A developer connects to the store over TCP and can store and retrieve string values by key. This is the foundational contract: what you write, you can read back.

**Why this priority**: Without basic GET/SET/DEL, the store is unusable.

**Independent Test**: Connect via telnet/redis-cli, `SET username somesh`, then `GET username` returns `somesh`.

**Acceptance Scenarios**:

1. **Given** an empty store, **When** `SET foo bar`, **Then** returns `+OK`
2. **Given** `foo=bar` is stored, **When** `GET foo`, **Then** returns `$3\r\nbar\r\n`
3. **Given** key never set, **When** `GET nosuchkey`, **Then** returns `$-1`
4. **Given** `foo=bar` is stored, **When** `DEL foo`, **Then** returns `:1` and subsequent `GET foo` returns `$-1`
5. **Given** `foo=bar`, **When** `SET foo newval`, **Then** `GET foo` returns `newval`

---

### User Story 2 — Multiple Deletions (Priority: P2)

A developer can delete multiple keys in a single command, getting a count of how many were deleted.

**Acceptance Scenarios**:

1. **Given** keys `a`, `b`, `c` stored, **When** `DEL a b c`, **Then** returns `:3`
2. **Given** key `x` not stored, **When** `DEL x`, **Then** returns `:0`

---

### Edge Cases

- What happens when `GET` is called on a never-set key? → Returns `$-1` (nil bulk string)
- What happens when a client disconnects mid-request? → ClientHandler closes its resources cleanly
- What if an unknown command is sent? → Returns `-ERR unknown command ...`

---

## Functional Requirements

| ID | Requirement |
|----|-------------|
| FR-1.1 | `SET key value` stores the value and returns `+OK` |
| FR-1.2 | `GET key` returns the value as a bulk string or `$-1` if missing |
| FR-1.3 | `DEL key [key ...]` deletes keys and returns the count deleted |
| FR-1.4 | The server listens on port 6379 (configurable) |
| FR-1.5 | The server handles multiple concurrent clients via a fixed thread pool |
| FR-1.6 | RESP-lite protocol: `+OK`, `$<len>\r\n<val>\r\n`, `:N`, `-ERR ...` |

---

## Success Criteria

- `mvn test` exits `BUILD SUCCESS` with zero test failures
- A developer can store and retrieve values over TCP in under 1 ms round-trip (local loopback)
- Server handles at least 10 concurrent client connections without errors
- All four RESP-lite response types are correctly encoded

---

## Assumptions

- Values are UTF-8 strings without embedded newlines or spaces (protocol limitation in Week 1)
- Port 6379 matches Redis default so standard clients work without config
