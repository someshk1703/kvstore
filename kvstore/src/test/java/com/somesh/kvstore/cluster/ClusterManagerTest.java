package com.somesh.kvstore.cluster;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

/**
 * Tests for ClusterManager — membership and failover coordination.
 */
class ClusterManagerTest {

    // ── Registration ──────────────────────────────────────────────────────────

    @Test
    void registerNodeAddsItToRing() {
        ConsistentHashRing ring = new ConsistentHashRing();
        ClusterManager manager = new ClusterManager(ring);

        NodeInfo primary = new NodeInfo("node-1", "localhost", 6379);
        manager.registerNode(primary, null);

        assertTrue(ring.getNodes().contains("node-1"));
        assertNotNull(ring.getNode("anykey"));
    }

    @Test
    void registerNodeWithReplicaTracksBoth() {
        ConsistentHashRing ring = new ConsistentHashRing();
        ClusterManager manager = new ClusterManager(ring);

        NodeInfo primary = new NodeInfo("primary-1", "localhost", 6379);
        NodeInfo replica  = new NodeInfo("replica-1",  "localhost", 6380);
        manager.registerNode(primary, replica);

        // Both are in the registry
        Map<String, NodeInfo> all = manager.getAllNodes();
        assertTrue(all.containsKey("primary-1"));
        assertTrue(all.containsKey("replica-1"));
        // Only the primary is in the ring
        assertTrue(ring.getNodes().contains("primary-1"));
        assertFalse(ring.getNodes().contains("replica-1"));
    }

    @Test
    void getOwnerReturnsCorrectNode() {
        ConsistentHashRing ring = new ConsistentHashRing();
        ClusterManager manager = new ClusterManager(ring);

        NodeInfo n1 = new NodeInfo("node-1", "localhost", 6379);
        NodeInfo n2 = new NodeInfo("node-2", "localhost", 6381);
        manager.registerNode(n1, null);
        manager.registerNode(n2, null);

        String key = "user:42";
        NodeInfo owner = manager.getOwner(key);
        assertNotNull(owner);
        assertTrue(owner.id().equals("node-1") || owner.id().equals("node-2"));
    }

    @Test
    void getOwnerReturnsNullWhenRingIsEmpty() {
        ClusterManager manager = new ClusterManager(new ConsistentHashRing());
        assertNull(manager.getOwner("somekey"));
    }

    // ── Failover ──────────────────────────────────────────────────────────────

    @Test
    void failoverPromotesReplicaToRing() {
        ConsistentHashRing ring = new ConsistentHashRing();
        ClusterManager manager = new ClusterManager(ring);

        NodeInfo primary = new NodeInfo("primary-1", "localhost", 6379);
        NodeInfo replica  = new NodeInfo("replica-1",  "localhost", 6380);
        manager.registerNode(primary, replica);

        // Simulate node-down event
        manager.handleNodeDown("primary-1");

        // Primary is removed from ring; replica takes over
        assertFalse(ring.getNodes().contains("primary-1"), "Failed primary must be removed from ring");
        assertTrue(ring.getNodes().contains("replica-1"),  "Replica must be added to ring on failover");
        assertTrue(manager.getDownNodes().contains("primary-1"));
    }

    @Test
    void failoverIsIdempotent() {
        ConsistentHashRing ring = new ConsistentHashRing();
        ClusterManager manager = new ClusterManager(ring);

        NodeInfo primary = new NodeInfo("primary-1", "localhost", 6379);
        NodeInfo replica  = new NodeInfo("replica-1",  "localhost", 6380);
        manager.registerNode(primary, replica);

        manager.handleNodeDown("primary-1");
        manager.handleNodeDown("primary-1");  // second call must be a no-op

        // Ring must contain replica exactly once (VNODES virtual nodes, not 2×)
        assertEquals(ConsistentHashRing.VNODES, ring.virtualNodeCount());
    }

    @Test
    void failoverWithNoReplicaRemovesNodeFromRing() {
        ConsistentHashRing ring = new ConsistentHashRing();
        ClusterManager manager = new ClusterManager(ring);

        NodeInfo primary = new NodeInfo("standalone", "localhost", 6379);
        manager.registerNode(primary, null);  // no replica

        manager.handleNodeDown("standalone");

        assertFalse(ring.getNodes().contains("standalone"));
        assertTrue(ring.isEmpty());
    }

    @Test
    void keysRerouteToReplicaAfterFailover() {
        ConsistentHashRing ring = new ConsistentHashRing();
        ClusterManager manager = new ClusterManager(ring);

        NodeInfo primary = new NodeInfo("primary-1", "localhost", 6379);
        NodeInfo replica  = new NodeInfo("replica-1",  "localhost", 6380);
        manager.registerNode(primary, replica);

        // Record which keys routed to primary-1
        int routedToPrimary = 0;
        int total = 1000;
        for (int i = 0; i < total; i++) {
            if ("primary-1".equals(ring.getNode("key:" + i))) routedToPrimary++;
        }
        assertTrue(routedToPrimary > 0, "Some keys should have been on primary-1");

        // Trigger failover
        manager.handleNodeDown("primary-1");

        // After failover, those keys must route to replica-1
        for (int i = 0; i < total; i++) {
            String owner = ring.getNode("key:" + i);
            assertEquals("replica-1", owner,
                    "After failover, all keys must route to replica-1, but key:" + i + " routes to " + owner);
        }
    }

    // ── MOVED response format ─────────────────────────────────────────────────

    @Test
    void movedResponseHasCorrectFormat() {
        NodeInfo nodeInfo = new NodeInfo("node-2", "10.0.0.2", 6381);
        String moved = ClusterRouter.movedResponse("node-2", nodeInfo);
        assertEquals("-MOVED node-2 10.0.0.2:6381\r\n", moved);
    }
}
