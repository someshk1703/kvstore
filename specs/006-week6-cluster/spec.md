# Feature Specification: Week 6 — Consistent Hashing Cluster (Bonus)

**Feature Branch**: `006-week6-cluster`

**Created**: 2026-07-17

**Status**: Completed ✅

---

## Overview

Week 6 adds horizontal sharding: a `ConsistentHashRing` distributes keys across
multiple physical nodes using virtual nodes (vnodes). A `ClusterRouter` forwards
commands to the correct shard transparently, and a `ClusterManager` handles
failover when a primary goes down — promoting its replica automatically.

This is the interview-differentiator week. Key talking points:
- **Why consistent hashing?** — only `k/n` keys move when a node is added (vs `k` with modular hashing)
- **Why 150 vnodes?** — empirically measured: 19.7% redistribution on adding a 5th node to a 4-node cluster (theoretical 20%), distribution within ±11% of even
- **Split-brain awareness** — failover is automatic but the split-brain risk is known and documented

---

## User Scenarios & Testing

### User Story 1 — Hash Ring Construction (Priority: P1)

A ring is constructed from N physical nodes, each represented by 150 virtual nodes
(vnodes). A key is consistently mapped to the same node as long as the ring does
not change.

**Acceptance Scenarios**:

1. **Given** ring with nodes `A`, `B`, `C`, **When** `getNode("user:123")`, **Then** always returns the same node for the same key ✅
2. **Given** empty ring, **When** `getNode("foo")`, **Then** returns `null` ✅
3. **Given** single node, **When** any key, **Then** that node owns all keys ✅
4. **Given** key hash past the last ring position, **When** `getNode(k)`, **Then** wraps to first node (no null, no exception) ✅
5. **Given** 3 nodes and 10,000 sample keys, **When** distribution measured, **Then** each node within ±20% of ideal 3333 keys ✅ *(observed: ≤11% deviation)*

### User Story 2 — Node Add/Remove & Redistribution (Priority: P1)

Adding or removing a node migrates only the affected key slice.

**Acceptance Scenarios**:

1. **Given** 4-node ring, **When** `addNode("nodeE")`, **Then** ~20% of keys move (measured: **19.7%**) ✅
2. **Given** 3-node ring, **When** `removeNode("nodeA")`, **Then** only nodeA's keys move; nodeB and nodeC keys are undisturbed ✅
3. **Given** `addNode` called twice with same ID, **When** ring inspected, **Then** still only `VNODES` virtual entries for that node (idempotent) ✅

### User Story 3 — MOVED Protocol (Priority: P1)

When a client hits the wrong shard node directly, that node returns a MOVED error
identifying the correct owner — mirroring Redis Cluster's redirect behavior.

**Acceptance Scenarios**:

1. **Given** key owned by `node-2`, **When** `ClusterRouter.movedResponse("node-2", info)`, **Then** returns `-MOVED node-2 10.0.0.2:6381\r\n` ✅

### User Story 4 — Failover (Priority: P1)

When a primary node fails, its replica is promoted to own the shard in the ring.
Routing automatically redirects to the replica with no manual intervention.

**Acceptance Scenarios**:

1. **Given** `primary-1` registered with `replica-1`, **When** `handleNodeDown("primary-1")`, **Then** `primary-1` removed from ring and `replica-1` added ✅
2. **Given** failover completed, **When** all 1000 test keys checked, **Then** 100% route to `replica-1` ✅
3. **Given** node declared down twice, **When** ring inspected, **Then** idempotent — replica not added twice ✅
4. **Given** primary with no replica, **When** `handleNodeDown`, **Then** primary removed, shard marked unavailable ✅

---

## Functional Requirements

| ID | Requirement | Status |
|----|-------------|--------|
| FR-6.1 | `ConsistentHashRing` places `VNODES=150` virtual nodes per physical node via MD5 hash | ✅ |
| FR-6.2 | `getNode(key)` returns the physical node owning the key in O(log n) via `ConcurrentSkipListMap.ceilingEntry` | ✅ |
| FR-6.3 | Wraparound: key hash past the last ring position wraps to `firstEntry()` | ✅ |
| FR-6.4 | `addNode` / `removeNode` are idempotent | ✅ |
| FR-6.5 | `ClusterRouter.movedResponse` returns `-MOVED <nodeId> <host>:<port>\r\n` | ✅ |
| FR-6.6 | `ClusterManager` registers primary + replica pairs; only primary enters ring | ✅ |
| FR-6.7 | `ClusterManager.handleNodeDown` removes primary, adds replica, marks node down | ✅ |
| FR-6.8 | `HealthMonitor` pings each node every 1000 ms; declares DOWN after 3 consecutive missed beats | ✅ |
| FR-6.9 | Distribution with 3 nodes and 150 vnodes is within ±20% of even across 10,000 keys | ✅ |
| FR-6.10 | Adding a 5th node to a 4-node cluster redistributes 10–30% of keys (~1/N theoretical) | ✅ *(19.7% measured)* |

---

## Design Decisions

| Decision | Chosen | Rejected | Reason |
|----------|--------|----------|--------|
| Hash function | MD5 (first 8 bytes → long) | MurmurHash, SHA-256 | MD5 is in the JDK; MurmurHash needs a dependency; SHA-256 is slower with no ring-placement benefit |
| Ring data structure | `ConcurrentSkipListMap` | `TreeMap` + lock | Skip list gives lock-free reads — the hot path (every key lookup). Writes (node add/remove) are rare |
| Virtual nodes | 150 | 1, 10, 200 | 150 reduces distribution variance to <10%; 10 still shows ~15% hot spots; 200 gives diminishing returns |
| Routing strategy | Client-side router (`ClusterRouter`) | Proxy layer | Simpler; proxy adds an extra network hop on every request |
| Failover trigger | N=3 missed heartbeats | 1 missed | Single-missed-beat is too flappy (GC pause, brief packet loss) |
| Split-brain mitigation | Known limitation, documented | Raft/etcd | Out of scope for 6-week project; deliberately surfaced as a known trade-off |

---

## Files Implemented

| File | Purpose |
|------|---------|
| `cluster/ConsistentHashRing.java` | Ring with `TreeMap<Long,String>`, 150 vnodes, MD5 hashing, wraparound |
| `cluster/NodeInfo.java` | Immutable record: `(id, host, port)` |
| `cluster/ClusterRouter.java` | Routes commands to owning node; static `movedResponse()` factory |
| `cluster/ClusterManager.java` | Membership registry, failover: removes down primary, promotes replica |
| `cluster/HealthMonitor.java` | Periodic PING probe; fires callback after N missed beats |

## Tests Implemented

| Test class | Tests | All pass |
|------------|-------|----------|
| `cluster/ConsistentHashRingTest.java` | 11 | ✅ |
| `cluster/ClusterManagerTest.java` | 9 | ✅ |

**Key measured results** (printed during test run):
- Distribution: nodeA=2994 keys (−10.2%), nodeB=3309 (+0.7%), nodeC=3697 (+10.9%) — all within ±11%
- Redistribution: adding 5th node moved **19.7%** of keys (theoretical 20%)

---

## Split-Brain Note (Interview Preparation)

Automatic failover has a split-brain risk: if a primary is partitioned (slow network, not dead), both the original primary and the newly promoted replica may accept writes for the same shard simultaneously. Our implementation:

- **Makes it unlikely**: 3-missed-beat threshold requires ~3 seconds of consecutive failure to trigger promotion
- **Does not prevent it**: no distributed consensus (Raft/Paxos) or external coordinator (etcd/ZooKeeper) is used
- **Mitigates impact**: the `downNodes` set prevents double-promotion; a rejoin-as-replica policy avoids immediate re-promotion after recovery

Production answer: use a Raft-based coordinator or external lock service to make the promotion decision atomically.

