# Feature Specification: Week 6 — Consistent Hashing Cluster (Bonus)

**Feature Branch**: `006-week6-cluster`

**Created**: 2026-07-17

**Status**: Pending ⏳

---

## Overview

Week 6 adds horizontal sharding: a `ConsistentHashRing` distributes keys across
multiple physical nodes using virtual nodes (vnodes). A client-facing proxy or
the CLI routes commands to the correct shard automatically.

This is a bonus week — the interview value is knowing _why_ consistent hashing
solves the re-distribution problem (only `k/n` keys move when a node is added,
vs `k` keys with modular hashing).

---

## User Scenarios & Testing

### User Story 1 — Hash Ring Construction (Priority: P1)

A ring is constructed from N physical nodes, each represented by 150 virtual nodes
(vnodes). A key is consistently mapped to the same node as long as the ring does
not change.

**Acceptance Scenarios**:

1. **Given** ring with nodes `A`, `B`, `C`, **When** `getNode("user:123")`, **Then** always returns the same node for the same key
2. **Given** ring with nodes `A`, `B`, **When** `addNode("C")`, **Then** only the keys that hashed between `C`'s predecessor and `C` move; the rest stay

### User Story 2 — Key Routing (Priority: P1)

The CLI (or a proxy) transparently routes `GET`/`SET`/`DEL` to the correct shard
node.

**Acceptance Scenarios**:

1. **Given** 3 nodes running, **When** `SET user:1 alice` routed to shard 1, **Then** `GET user:1` also routes to shard 1 and returns `alice`
2. **Given** node 2 down, **When** routing a key owned by node 2, **Then** returns error, not silent data loss

### User Story 3 — Node Add/Remove (Priority: P2)

Adding or removing a node migrates only the affected key slice.

**Acceptance Scenarios**:

1. **Given** ring with nodes `A`, `B`, **When** add `C`, **Then** keys newly owned by `C` are migrated from their previous owner
2. **Given** ring with nodes `A`, `B`, `C`, **When** remove `C`, **Then** `C`'s keys are redistributed to `A` or `B` with no data loss

---

## Functional Requirements

| ID | Requirement |
|----|-------------|
| FR-6.1 | `ConsistentHashRing` places 150 virtual nodes per physical node on a SHA-1 ring |
| FR-6.2 | `getNode(key)` returns the physical node owning the key in O(log n) |
| FR-6.3 | `addNode` / `removeNode` updates the ring and returns the set of keys to migrate |
| FR-6.4 | CLI `--cluster` flag accepts a comma-separated node list and routes automatically |
| FR-6.5 | Unit tests verify ring balance (Gini coefficient of key distribution < 0.1) |

---

## Design Decisions

| Decision | Chosen | Rejected | Reason |
|----------|--------|----------|--------|
| Hash function | SHA-1 | MD5, MurmurHash | Widely known, sufficient for interview demo |
| Virtual nodes | 150 per node | Fewer/more | Redis uses 16k slots; 150 vnodes is a clean middle ground |
| Routing strategy | Client-side routing | Proxy layer | Simpler; proxy adds an extra hop |

---

## Files Planned

| File | Purpose |
|------|---------|
| `cluster/ConsistentHashRing.java` | Ring data structure with vnode mapping |
| `cluster/ShardRouter.java` | Routes commands to the correct node |
| `cluster/ClusterNode.java` | Metadata about a cluster member |
| `KvCli.java` | Extended with `--cluster` flag |
