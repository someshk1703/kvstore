package com.somesh.kvstore.cluster;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Consistent hash ring for distributing keys across cluster nodes.
 *
 * <h2>Why virtual nodes?</h2>
 * With 1 virtual node per physical node, the ring has large gaps between
 * consecutive positions. Adding a single node redistributes ~50% of keys in
 * the worst case because a single large arc now gets split. With 150 vnodes,
 * each node occupies 150 small, uniformly-spread positions — so a new node
 * absorbs only ~1/N of keys (the theoretical minimum), and no single arc
 * dominates load.
 *
 * <h2>Why 150?</h2>
 * Empirically validated sweet spot: distribution variance drops to ~5–10%
 * vs ~50% with 1 vnode; 10 vnodes reduce variance to ~15% but still leave
 * perceptible hot spots. 150 consistently produces &lt;10% deviation from
 * perfectly even distribution while keeping memory overhead negligible.
 *
 * <h2>Hashing</h2>
 * MD5 is used for uniform distribution, not security. The first 8 bytes of
 * the digest become a 64-bit long ring position. MD5 is ~3× faster than
 * SHA-256 and has proven uniform distribution for ring placement.
 *
 * <h2>Thread safety</h2>
 * {@link ConcurrentSkipListMap} provides lock-free sorted reads with O(log n)
 * insertions/deletions. {@link CopyOnWriteArraySet} makes node membership reads
 * lock-free (membership changes are rare). Both are safe for concurrent access.
 *
 * <h2>Interview angles</h2>
 * <ul>
 *   <li>Why MD5 over MurmurHash? — MD5 is in the JDK; MurmurHash requires a
 *       dependency. For a from-scratch project, JDK-only is intentional.</li>
 *   <li>Why ConcurrentSkipListMap over TreeMap+lock? — Skip list gives lock-free
 *       reads, which is the hot path (every key lookup is a read). Writes are
 *       rare (only when nodes join/leave).</li>
 *   <li>What happens on hash collision? — ConcurrentSkipListMap is a map, so a
 *       collision overwrites the earlier entry. At VNODES=150 and a 64-bit key
 *       space, the probability of collision across a typical cluster is negligible
 *       (~10^-14 per node pair).</li>
 * </ul>
 */
public class ConsistentHashRing {

    /**
     * Virtual nodes per physical node.
     *
     * <p>150 is the empirically validated sweet spot — distribution within
     * 5–10% of perfectly even. Redis uses 16,384 hash slots total; we use
     * 150 vnodes per node, so a 10-node cluster has 1,500 virtual positions.
     */
    public static final int VNODES = 150;

    /**
     * Ring: hash position → physical node ID.
     * ConcurrentSkipListMap: lock-free sorted reads, O(log n) writes.
     */
    private final NavigableMap<Long, String> ring = new ConcurrentSkipListMap<>();

    /** Set of active physical node IDs for membership tracking. */
    private final Set<String> nodes = new CopyOnWriteArraySet<>();

    // ── Node management ───────────────────────────────────────────────────────

    /**
     * Add a physical node by placing {@link #VNODES} virtual entries on the ring.
     * Idempotent: adding the same node twice is a no-op.
     */
    public void addNode(String nodeId) {
        if (nodes.add(nodeId)) {
            for (int i = 0; i < VNODES; i++) {
                long pos = hash(nodeId + "#" + i);
                ring.put(pos, nodeId);
            }
        }
    }

    /**
     * Remove a physical node by clearing all its virtual entries.
     * Keys previously owned by this node will re-route to the next node clockwise.
     */
    public void removeNode(String nodeId) {
        if (nodes.remove(nodeId)) {
            for (int i = 0; i < VNODES; i++) {
                long pos = hash(nodeId + "#" + i);
                ring.remove(pos);
            }
        }
    }

    /**
     * Return the node that owns the given key.
     *
     * <p>Clockwise lookup: find the ceiling entry (first position ≥ key hash).
     * If no such entry exists (key hash is past the last ring position),
     * wrap around to the first entry. This guarantees every key maps to exactly
     * one node as long as the ring is non-empty.
     *
     * @param key the key to route
     * @return the owning node ID, or {@code null} if the ring is empty
     */
    public String getNode(String key) {
        if (ring.isEmpty()) return null;
        long keyHash = hash(key);
        Map.Entry<Long, String> entry = ring.ceilingEntry(keyHash);
        if (entry == null) {
            entry = ring.firstEntry();  // wrap around: key is past last ring position
        }
        return entry.getValue();
    }

    /** Unmodifiable view of physical node IDs currently in the ring. */
    public Set<String> getNodes() {
        return Collections.unmodifiableSet(nodes);
    }

    /** Total number of virtual positions currently in the ring. */
    public int virtualNodeCount() {
        return ring.size();
    }

    /** Whether the ring contains no nodes. */
    public boolean isEmpty() {
        return ring.isEmpty();
    }

    // ── Hashing ───────────────────────────────────────────────────────────────

    /**
     * Compute a deterministic 64-bit hash for a string using the first 8 bytes
     * of MD5. Used only for ring placement — not for security.
     *
     * <p>Package-private for direct access in tests (distribution measurement).
     */
    static long hash(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
            // Interpret first 8 bytes as a big-endian signed 64-bit integer.
            // Using signed long is fine — the ring spans the full long value space
            // and ConcurrentSkipListMap handles negative keys correctly.
            long h = 0;
            for (int i = 0; i < 8; i++) {
                h = (h << 8) | (digest[i] & 0xFF);
            }
            return h;
        } catch (NoSuchAlgorithmException e) {
            // MD5 is guaranteed to be present in every Java SE implementation
            throw new IllegalStateException("MD5 not available", e);
        }
    }
}
