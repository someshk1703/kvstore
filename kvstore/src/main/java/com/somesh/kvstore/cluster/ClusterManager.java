package com.somesh.kvstore.cluster;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cluster membership and failover coordinator.
 *
 * <h2>Responsibilities</h2>
 * <ul>
 *   <li>Maintain the live registry of primary and replica nodes.</li>
 *   <li>Respond to node-down events from {@link HealthMonitor} by promoting
 *       the affected primary's replica to own that shard in the ring.</li>
 *   <li>Keep the {@link ConsistentHashRing} consistent: every membership change
 *       is immediately reflected so subsequent routing decisions are correct.</li>
 * </ul>
 *
 * <h2>Failover flow</h2>
 * <pre>
 *   HealthMonitor detects N missed beats
 *       → calls ClusterManager#handleNodeDown(primaryId)
 *           → remove primary from ring
 *           → add replica to ring (replica now owns the shard)
 *           → log promotion event
 * </pre>
 *
 * <h2>Split-brain caveat</h2>
 * Automatic failover carries a split-brain risk: if a primary is merely
 * partitioned (slow network, not dead), both the original primary and the
 * promoted replica may accept writes for the same key range simultaneously.
 * A production system resolves this with a distributed consensus protocol
 * (Raft/Paxos) or an external coordinator (etcd, ZooKeeper). This
 * implementation makes the risk explicit rather than hiding it — clients
 * should use {@code WAIT} semantics and operators should be aware of the
 * window between failure detection and promotion.
 *
 * <h2>Rejoin policy</h2>
 * When a failed primary comes back online it re-registers as a replica of
 * the promoted node. Manual operator action is required to promote it back
 * to primary — this prevents the automatic promotion from being immediately
 * undone if the node flaps.
 */
public class ClusterManager {

    private static final Logger log = LoggerFactory.getLogger(ClusterManager.class);

    private final ConsistentHashRing ring;

    /** Complete node registry: primary + replica nodes. Key = nodeId. */
    private final Map<String, NodeInfo> allNodes = new ConcurrentHashMap<>();

    /** primary nodeId → replica nodeId. Populated by {@link #registerNode}. */
    private final Map<String, String> primaryToReplica = new ConcurrentHashMap<>();

    /** Nodes currently marked as down (to avoid duplicate failover handling). */
    private final Set<String> downNodes = ConcurrentHashMap.newKeySet();

    private HealthMonitor healthMonitor;

    public ClusterManager(ConsistentHashRing ring) {
        this.ring = ring;
    }

    // ── Node registration ─────────────────────────────────────────────────────

    /**
     * Register a primary node and its replica.
     *
     * <p>Only the primary is added to the ring initially. The replica is tracked
     * internally and promoted if the primary fails.
     *
     * @param primary the primary shard node (must not be null)
     * @param replica the hot-standby replica (may be null if running without replication)
     */
    public void registerNode(NodeInfo primary, NodeInfo replica) {
        allNodes.put(primary.id(), primary);
        ring.addNode(primary.id());
        if (replica != null) {
            allNodes.put(replica.id(), replica);
            primaryToReplica.put(primary.id(), replica.id());
            log.info("Registered primary {} with replica {}", primary, replica);
        } else {
            log.info("Registered primary {} (no replica)", primary);
        }
    }

    // ── Health monitoring ─────────────────────────────────────────────────────

    /**
     * Start the periodic health monitor for all registered primary nodes.
     * Calls {@link #handleNodeDown(String)} when a primary is declared down.
     */
    public void startHealthMonitor() {
        Map<String, NodeInfo> primaries = new ConcurrentHashMap<>();
        for (String primaryId : primaryToReplica.keySet()) {
            NodeInfo n = allNodes.get(primaryId);
            if (n != null) primaries.put(primaryId, n);
        }
        // Also monitor primaries that have no replica configured
        for (Map.Entry<String, NodeInfo> e : allNodes.entrySet()) {
            if (!primaryToReplica.containsValue(e.getKey())) {
                primaries.putIfAbsent(e.getKey(), e.getValue());
            }
        }
        healthMonitor = new HealthMonitor(primaries, this::handleNodeDown);
        healthMonitor.start();
        log.info("Health monitor started for {} primary nodes", primaries.size());
    }

    /** Stop the health monitor (e.g. on graceful shutdown). */
    public void stopHealthMonitor() {
        if (healthMonitor != null) {
            healthMonitor.close();
            log.info("Health monitor stopped");
        }
    }

    // ── Failover ──────────────────────────────────────────────────────────────

    /**
     * Called when a primary node is declared down by the health monitor.
     *
     * <p>Package-private for direct invocation in tests.
     *
     * @param primaryId the node ID of the failed primary
     */
    void handleNodeDown(String primaryId) {
        if (!downNodes.add(primaryId)) {
            // Already being handled — avoid duplicate promotions
            return;
        }

        String replicaId = primaryToReplica.get(primaryId);
        if (replicaId == null) {
            log.error("Node {} is down but has no replica configured — shard unavailable", primaryId);
            ring.removeNode(primaryId);
            return;
        }

        log.warn("Initiating failover: primary {} is down, promoting replica {}", primaryId, replicaId);
        ring.removeNode(primaryId);
        ring.addNode(replicaId);
        log.info("Failover complete: replica {} now handles shard previously owned by {}",
                replicaId, primaryId);
    }

    // ── Query ─────────────────────────────────────────────────────────────────

    /**
     * Return the {@link NodeInfo} for the node currently owning the given key.
     * Consults the ring; returns {@code null} if the ring is empty.
     */
    public NodeInfo getOwner(String key) {
        String nodeId = ring.getNode(key);
        return nodeId != null ? allNodes.get(nodeId) : null;
    }

    /** Unmodifiable view of all registered nodes. */
    public Map<String, NodeInfo> getAllNodes() {
        return Collections.unmodifiableMap(allNodes);
    }

    /** Unmodifiable view of node IDs currently marked down. */
    public Set<String> getDownNodes() {
        return Collections.unmodifiableSet(downNodes);
    }

    /** primary → replica ID mapping (unmodifiable). */
    public Map<String, String> getPrimaryToReplicaMap() {
        return Collections.unmodifiableMap(primaryToReplica);
    }
}
