package com.somesh.kvstore.cluster;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for ConsistentHashRing.
 *
 * Key assertions:
 * 1. Empty ring returns null.
 * 2. Single node owns all keys.
 * 3. Same key always routes to the same node (determinism).
 * 4. Wraparound: key hash past last ring position routes to first node.
 * 5. With 3 nodes and 150 vnodes, 10,000 keys distribute within ±20% of ideal.
 * 6. Adding a 5th node to a 4-node cluster redistributes ~20% of keys.
 * 7. addNode is idempotent.
 * 8. removeNode removes all vnodes for that physical node.
 */
class ConsistentHashRingTest {

    private ConsistentHashRing ring;

    @BeforeEach
    void setUp() {
        ring = new ConsistentHashRing();
    }

    // ── Basic correctness ─────────────────────────────────────────────────────

    @Test
    void emptyRingReturnsNull() {
        assertNull(ring.getNode("anykey"), "Empty ring must return null");
    }

    @Test
    void singleNodeOwnsAllKeys() {
        ring.addNode("nodeA");
        assertEquals("nodeA", ring.getNode("foo"));
        assertEquals("nodeA", ring.getNode("bar"));
        assertEquals("nodeA", ring.getNode(""));
        assertEquals("nodeA", ring.getNode("key:12345"));
    }

    @Test
    void virtualNodeCountIsVnodesPerPhysicalNode() {
        ring.addNode("nodeA");
        assertEquals(ConsistentHashRing.VNODES, ring.virtualNodeCount());
        ring.addNode("nodeB");
        assertEquals(2 * ConsistentHashRing.VNODES, ring.virtualNodeCount());
    }

    @Test
    void addNodeIsIdempotent() {
        ring.addNode("nodeA");
        ring.addNode("nodeA");  // second add must be a no-op
        assertEquals(ConsistentHashRing.VNODES, ring.virtualNodeCount());
    }

    @Test
    void removeNodeDeletesAllVnodes() {
        ring.addNode("nodeA");
        ring.addNode("nodeB");
        ring.removeNode("nodeA");
        assertEquals(ConsistentHashRing.VNODES, ring.virtualNodeCount());
        assertFalse(ring.getNodes().contains("nodeA"));
        assertEquals("nodeB", ring.getNode("anykey"),
                "After removing nodeA, all keys must route to nodeB");
    }

    // ── Determinism ───────────────────────────────────────────────────────────

    @Test
    void sameKeyAlwaysRoutesToSameNode() {
        ring.addNode("nodeA");
        ring.addNode("nodeB");
        ring.addNode("nodeC");

        String target = ring.getNode("user:42");
        assertNotNull(target);
        for (int i = 0; i < 200; i++) {
            assertEquals(target, ring.getNode("user:42"),
                    "Key routing must be deterministic on call #" + i);
        }
    }

    // ── Wraparound ────────────────────────────────────────────────────────────

    /**
     * Wraparound: if a key's hash is larger than every position in the ring,
     * getNode must wrap to the first entry rather than returning null or throwing.
     *
     * We verify this by exhaustively checking that no key returns null and that
     * every key routes to one of the registered nodes — including keys whose
     * MD5-derived hash falls past the highest vnode position.
     */
    @Test
    void wrapAroundProducesValidNode() {
        ring.addNode("onlyNode");
        for (int i = 0; i < 500; i++) {
            String node = ring.getNode("wrap-test-" + i);
            assertNotNull(node, "getNode must not return null for key wrap-test-" + i);
            assertEquals("onlyNode", node);
        }
    }

    @Test
    void wrapAroundWithMultipleNodes() {
        ring.addNode("nodeA");
        ring.addNode("nodeB");
        ring.addNode("nodeC");

        for (int i = 0; i < 1000; i++) {
            String node = ring.getNode("k" + i);
            assertNotNull(node, "Wraparound must not return null for k" + i);
            assertTrue(ring.getNodes().contains(node),
                    "Routing must resolve to a registered node, got: " + node);
        }
    }

    // ── Distribution ──────────────────────────────────────────────────────────

    /**
     * With 3 nodes and 150 vnodes each, 10,000 sample keys should distribute
     * within ±20% of perfectly even (actual variance is typically ≤10%).
     *
     * This is the number to quote in interviews:
     *   "With 150 vnodes, distribution was within X% of ideal across 10k keys."
     */
    @Test
    void distributionIsEvenWith150Vnodes() {
        ring.addNode("nodeA");
        ring.addNode("nodeB");
        ring.addNode("nodeC");

        Map<String, Integer> counts = new HashMap<>();
        ring.getNodes().forEach(n -> counts.put(n, 0));

        int totalKeys = 10_000;
        for (int i = 0; i < totalKeys; i++) {
            String node = ring.getNode("key:" + i);
            counts.merge(node, 1, Integer::sum);
        }

        double expected = totalKeys / 3.0;
        for (Map.Entry<String, Integer> e : counts.entrySet()) {
            double deviationPct = Math.abs(e.getValue() - expected) / expected * 100.0;
            System.out.printf("  Node %-8s : %5d keys  (deviation %.1f%% from ideal %.0f)%n",
                    e.getKey(), e.getValue(), deviationPct, expected);
            assertTrue(deviationPct < 20.0,
                    String.format("Node %s deviation %.1f%% exceeds 20%% threshold", e.getKey(), deviationPct));
        }
    }

    // ── Redistribution ────────────────────────────────────────────────────────

    /**
     * Adding a 5th node to a 4-node cluster should move ~1/5 = 20% of keys.
     * This is the core "consistent hashing advantage" number — validate it empirically.
     *
     * Accepted range: 10–30% (generously allows for hash distribution variance).
     * Typical observed value: 18–22%.
     */
    @Test
    void addingNodeRedistributesApproximatelyOneNthOfKeys() {
        ring.addNode("nodeA");
        ring.addNode("nodeB");
        ring.addNode("nodeC");
        ring.addNode("nodeD");

        int totalKeys = 10_000;
        String[] before = new String[totalKeys];
        for (int i = 0; i < totalKeys; i++) {
            before[i] = ring.getNode("key:" + i);
        }

        ring.addNode("nodeE");  // 5th node added

        int moved = 0;
        for (int i = 0; i < totalKeys; i++) {
            if (!before[i].equals(ring.getNode("key:" + i))) moved++;
        }

        double movedPct = (double) moved / totalKeys * 100.0;
        System.out.printf("  Adding 5th node moved %.1f%% of keys (theoretical: ~20%%)%n", movedPct);
        assertTrue(movedPct > 10.0 && movedPct < 30.0,
                String.format("Expected ~20%% redistribution, got %.1f%%", movedPct));
    }

    /**
     * Removing a node should move its keys to its successor — no other node's
     * keys should be disturbed.
     */
    @Test
    void removingNodeOnlyMovesItsKeys() {
        ring.addNode("nodeA");
        ring.addNode("nodeB");
        ring.addNode("nodeC");

        int totalKeys = 10_000;
        Map<String, String> before = new HashMap<>();
        for (int i = 0; i < totalKeys; i++) {
            before.put("key:" + i, ring.getNode("key:" + i));
        }

        ring.removeNode("nodeA");

        int movedToNonA = 0;
        for (int i = 0; i < totalKeys; i++) {
            String key = "key:" + i;
            String prevOwner = before.get(key);
            String newOwner = ring.getNode(key);
            if ("nodeA".equals(prevOwner)) {
                // Keys previously on nodeA must now be on nodeB or nodeC
                assertNotEquals("nodeA", newOwner, "Removed node must not own any key");
            } else {
                // Keys NOT on nodeA must stay on the same node
                assertEquals(prevOwner, newOwner,
                        "Key " + key + " was on " + prevOwner + " but moved to " + newOwner
                        + " after nodeA removal — only nodeA's keys should move");
            }
        }
    }
}
