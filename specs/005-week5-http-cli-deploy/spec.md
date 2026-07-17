# Feature Specification: Week 5 — HTTP API, CLI, Benchmarks & Deploy

**Feature Branch**: `005-week5-http-cli-deploy`

**Created**: 2026-07-17

**Status**: Completed ✅

---

## Overview

Week 5 wraps the existing TCP + engine internals into a demo-ready, deployable
system. The store already has the hard parts (TTL, LRU, AOF, replication) — this
week makes all of that visible to an interviewer without them needing to know the
RESP wire protocol.

---

## User Scenarios & Testing

### User Story 1 — HTTP GET/SET/DEL (Priority: P1)

A developer or interviewer can interact with the store using plain HTTP — no RESP
knowledge required.

**Acceptance Scenarios**:

1. **Given** an empty store, **When** `POST /api/keys/foo` with body `{"value":"bar"}`,
   **Then** returns `201 Created` and `GET /api/keys/foo` returns `{"key":"foo","value":"bar"}`
2. **Given** `foo=bar`, **When** `GET /api/keys/foo`, **Then** returns `200 OK` with `{"key":"foo","value":"bar"}`
3. **Given** key does not exist, **When** `GET /api/keys/missing`, **Then** returns `404 Not Found`
4. **Given** `foo=bar`, **When** `DELETE /api/keys/foo`, **Then** returns `204 No Content`
5. **Given** `POST /api/keys/ttlkey` with `{"value":"v","ttlSeconds":2}`, **When** 3 seconds pass, **Then** `GET /api/keys/ttlkey` returns `404`

### User Story 2 — Architecture Proof: Shared Engine (Priority: P1)

A value written via TCP is immediately readable via HTTP (and vice versa) — proving
both protocols sit on the same underlying store, not two divergent code paths.

**Acceptance Scenarios**:

1. **Given** `SET shared_key hello` via TCP, **When** `GET /api/keys/shared_key` via HTTP, **Then** returns `{"key":"shared_key","value":"hello"}`
2. **Given** `POST /api/keys/httpkey` via HTTP, **When** `GET httpkey` via TCP, **Then** returns `httpkey`'s value

### User Story 3 — KEYS pattern endpoint (Priority: P2)

An operator can list all keys matching a glob pattern via HTTP.

**Acceptance Scenarios**:

1. **Given** keys `user:1`, `user:2`, `session:abc`, **When** `GET /api/keys?pattern=user:*`, **Then** returns `["user:1","user:2"]`
2. **Given** no matching keys, **When** `GET /api/keys?pattern=nonexistent:*`, **Then** returns `[]`

### User Story 4 — INFO endpoint (Priority: P2)

A developer can get server metadata (key count, memory, uptime) via HTTP.

**Acceptance Scenarios**:

1. **Given** server running with 5 keys, **When** `GET /api/info`, **Then** response includes `totalKeys: 5`, `uptimeSeconds > 0`, `version`, `mode`

### User Story 5 — CLI client (Priority: P1)

A developer can interact with the server from the command line using `kv-cli`,
mirroring `redis-cli`'s feel.

**Acceptance Scenarios**:

1. **Given** server running, **When** `java -jar kvstore.jar --cli SET foo bar`, **Then** prints `+OK`
2. **Given** server running, **When** `java -jar kvstore.jar --cli GET foo`, **Then** prints `bar`
3. **Given** server not running, **When** attempting connection, **Then** prints clear error message instead of stack trace
4. **Given** interactive mode, **When** user types `PING`, **Then** prints `PONG`

### User Story 6 — Benchmarks (Priority: P2)

A developer can run a throughput benchmark and produce real numbers to quote in
interviews.

**Acceptance Scenarios**:

1. **Given** benchmark run, **When** single-threaded 100k SETs, **Then** ops/sec printed with warm-up excluded
2. **Given** benchmark run, **When** multi-threaded 8-thread 100k SETs, **Then** ops/sec printed and compared to single-threaded
3. **Given** benchmark run, **When** `ConcurrentHashMap` vs synchronized `HashMap` comparison, **Then** both numbers printed for the same load

### User Story 7 — Docker + Compose (Priority: P1)

A clean clone → `docker-compose up` brings up primary + replica with HTTP and TCP
both reachable.

**Acceptance Scenarios**:

1. **Given** `docker-compose up`, **When** primary starts, **Then** TCP port 6379 and HTTP port 8080 are reachable
2. **Given** `docker-compose up`, **When** replica starts, **Then** it syncs from primary
3. **Given** both containers running, **When** `SET x y` on primary via TCP, **Then** `GET x` on replica returns `y`

---

## Functional Requirements

| ID | Requirement |
|----|-------------|
| FR-5.1 | Spring Boot HTTP API on port 8080; TCP server on 6379; both share one `KVStore` instance |
| FR-5.2 | `GET /api/keys/{key}` → `200 {"key","value"}` or `404` |
| FR-5.3 | `POST /api/keys/{key}` with `{"value","ttlSeconds?}` → `201` |
| FR-5.4 | `DELETE /api/keys/{key}` → `204` or `404` |
| FR-5.5 | `GET /api/keys?pattern=<glob>` → `200 [array of keys]` |
| FR-5.6 | `GET /api/info` → server metadata JSON |
| FR-5.7 | `KvCli` supports single-shot mode (`--cli CMD args`) and interactive REPL mode |
| FR-5.8 | `BenchmarkRunner` produces ops/sec numbers for single-thread, multi-thread, and ConcurrentHashMap vs synchronized comparison |
| FR-5.9 | `Dockerfile` builds fat JAR from source; `docker-compose.yml` starts primary (port 6379/8080) + replica (port 6381) |
| FR-5.10 | `KEYS` glob pattern matching in `KVStore` (converts `?` and `*` to regex) |

---

## Design Decisions

| Decision | Chosen | Rejected | Reason |
|----------|--------|----------|--------|
| HTTP framework | Spring Boot 3.2 | Javalin, Jetty raw | Spring Boot is the industry standard; interviewers know it |
| HTTP starts how | Background daemon thread from Main | Spring takes over main | Keep TCP server on main thread; HTTP secondary |
| KVStore sharing | Static holder in HttpApiApplication | Second KVStore instance | Two stores would diverge — defeats the proof point |
| CLI protocol | Raw TCP + RESP-lite read | HTTP | CLI should exercise the same wire path as `redis-cli` |
| Benchmark design | In-process direct call + TCP client | JMH | Simpler to demo; JMH requires separate build setup |

---

## Non-Functional Requirements

| NFR | Target |
|-----|--------|
| HTTP startup time | < 5 seconds |
| Single-threaded benchmark | > 100k ops/sec (direct calls), > 20k ops/sec (TCP) |
| Docker image size | < 500 MB |

---

## Files Changed

| File | Change |
|------|--------|
| `pom.xml` | Added Spring Boot 3.2.5 BOM + starter-web + boot-maven-plugin |
| `KVStore.java` | Added `keys(String pattern)` method |
| `CommandExecutor.java` | Added `KEYS` command handler |
| `CommandType.java` | Added `KEYS` enum value |
| `HttpApiApplication.java` | Spring Boot app entry point with static KVStore holder |
| `KeyValueController.java` | REST endpoints |
| `KvCli.java` | CLI client (single-shot + interactive) |
| `BenchmarkRunner.java` | Throughput benchmark |
| `Main.java` | Added `--http` flag, `--cli` passthrough |
| `Dockerfile` | Multi-stage build |
| `docker-compose.yml` | Primary + replica compose stack |
| `README.md` | Architecture, design decisions, benchmark numbers |
