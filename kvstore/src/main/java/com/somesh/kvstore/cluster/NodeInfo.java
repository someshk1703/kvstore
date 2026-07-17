package com.somesh.kvstore.cluster;

/**
 * Immutable descriptor for a single cluster node.
 *
 * <h2>Fields</h2>
 * <ul>
 *   <li>{@code id}   — logical identifier used as the key in the consistent hash ring
 *       and in all membership maps (e.g. "node-1", "primary-shard-3").</li>
 *   <li>{@code host} — TCP hostname or IP address clients connect to.</li>
 *   <li>{@code port} — TCP port the node's KVStore server listens on.</li>
 * </ul>
 *
 * Uses a Java 16+ {@code record} — zero-boilerplate, immutable, value-based equality.
 */
public record NodeInfo(String id, String host, int port) {

    /** Convenience: "{host}:{port}" — used in MOVED error responses. */
    public String address() {
        return host + ":" + port;
    }

    @Override
    public String toString() {
        return id + "@" + address();
    }
}
