package com.somesh.kvstore.replication;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.somesh.kvstore.engine.CommandExecutor;
import com.somesh.kvstore.engine.KVStore;

/**
 * Unit tests for {@link ReplicationManager}.
 *
 * <p>These tests use a small backlog size to make eviction easy to trigger without
 * needing real network connections. Network integration tests are in
 * {@link ReplicationIntegrationTest}.
 */
@DisplayName("ReplicationManager")
class ReplicationManagerTest {

    private KVStore            store;
    private ReplicationManager manager;

    @BeforeEach
    void setUp() {
        store   = new KVStore();
        manager = new ReplicationManager(store, null);
    }

    @AfterEach
    void tearDown() {
        manager.close();
    }

    // ── getMasterOffset ─────────────────────────────────────────────────────

    @Test
    @DisplayName("masterOffset is -1 before any propagation")
    void masterOffsetInitial() {
        assertThat(manager.getMasterOffset()).isEqualTo(-1);
    }

    @Test
    @DisplayName("masterOffset advances with each propagation")
    void masterOffsetAdvances() {
        manager.propagate("SET foo bar");
        assertThat(manager.getMasterOffset()).isEqualTo(0);

        manager.propagate("DEL foo");
        assertThat(manager.getMasterOffset()).isEqualTo(1);
    }

    // ── propagate ───────────────────────────────────────────────────────────

    @Test
    @DisplayName("propagate returns increasing offsets")
    void propagateReturnsOffsets() {
        long off0 = manager.propagate("SET a 1");
        long off1 = manager.propagate("SET b 2");
        long off2 = manager.propagate("DEL a");

        assertThat(off0).isEqualTo(0);
        assertThat(off1).isEqualTo(1);
        assertThat(off2).isEqualTo(2);
    }

    // ── waitForReplicas ─────────────────────────────────────────────────────

    @Test
    @DisplayName("waitForReplicas returns 0 when no replicas connected")
    void waitNoReplicas() {
        manager.propagate("SET k v");
        int acked = manager.waitForReplicas(1, 50);
        assertThat(acked).isEqualTo(0);
    }

    @Test
    @DisplayName("waitForReplicas returns 0 when masterOffset is -1")
    void waitNoWrites() {
        // No writes have happened → masterOffset = -1 → trivially done
        int acked = manager.waitForReplicas(0, 50);
        assertThat(acked).isEqualTo(0);  // 0 replicas, but call should not throw
    }

    @Test
    @DisplayName("waitForReplicas times out at ~timeoutMs when no replicas ack")
    void waitTimeout() {
        manager.propagate("SET x y");
        long start = System.currentTimeMillis();
        int acked = manager.waitForReplicas(1, 100);
        long elapsed = System.currentTimeMillis() - start;

        assertThat(acked).isEqualTo(0);
        // Should have taken at least 80ms but not much more than 200ms
        assertThat(elapsed).isGreaterThanOrEqualTo(80L);
        assertThat(elapsed).isLessThan(500L);
    }

    // ── getReplicaCount ─────────────────────────────────────────────────────

    @Test
    @DisplayName("getReplicaCount is 0 initially")
    void replicaCountInitial() {
        assertThat(manager.getReplicaCount()).isEqualTo(0);
    }

    // ── CommandExecutor integration ──────────────────────────────────────────

    @Test
    @DisplayName("propagate is called from CommandExecutor on write commands")
    void commandExecutorPropagates() {
        CommandExecutor executor = new CommandExecutor(store);
        executor.setReplicationManager(manager);

        com.somesh.kvstore.protocol.CommandParser parser = new com.somesh.kvstore.protocol.CommandParser();
        executor.execute(parser.parse("SET testkey testval"));

        // One write command was propagated → masterOffset = 0
        assertThat(manager.getMasterOffset()).isEqualTo(0);
    }

    @Test
    @DisplayName("non-write commands (GET, PING) do not advance masterOffset")
    void readCommandsNotPropagated() {
        store.set("k", "v");
        CommandExecutor executor = new CommandExecutor(store);
        executor.setReplicationManager(manager);
        com.somesh.kvstore.protocol.CommandParser parser = new com.somesh.kvstore.protocol.CommandParser();

        executor.execute(parser.parse("GET k"));
        executor.execute(parser.parse("PING"));

        assertThat(manager.getMasterOffset()).isEqualTo(-1);  // no writes
    }
}
