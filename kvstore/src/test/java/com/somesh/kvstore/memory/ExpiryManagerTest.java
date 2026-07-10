package com.somesh.kvstore.memory;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.somesh.kvstore.engine.KVStore;

/**
 * Unit tests for ExpiryManager — the background expiry sweep.
 *
 * Tests call {@link ExpiryManager#sweep()} directly (package-private visibility)
 * rather than waiting for the scheduler, making them fast and deterministic.
 *
 * Tests cover:
 *   - Expired keys are removed by a sweep
 *   - Non-expired keys are untouched by a sweep
 *   - Keys with no TTL are untouched
 *   - Mixed key set handled correctly
 */
class ExpiryManagerTest {

    private KVStore      store;
    private ExpiryManager manager;

    @BeforeEach
    void setUp() {
        store   = new KVStore();
        manager = new ExpiryManager(store);
        // Do NOT call manager.start() — tests call sweep() directly.
    }

    // ─── Sweep removes expired keys ───────────────────────────────────────────

    @Test
    @DisplayName("sweep removes a single expired key")
    void sweep_removesExpiredKey() throws InterruptedException {
        store.set("fleeting", "value", 50);   // 50 ms TTL

        Thread.sleep(80);   // let it expire

        manager.sweep();

        assertThat(store.getStore().containsKey("fleeting"))
            .as("expired key should be removed by sweep")
            .isFalse();
    }

    @Test
    @DisplayName("sweep removes all expired keys from a mixed set")
    void sweep_removesAllExpiredKeys_leavesLiveKeys() throws InterruptedException {
        // Three short-lived keys
        store.set("a", "1", 50);
        store.set("b", "2", 50);
        store.set("c", "3", 50);
        // Two permanent keys
        store.set("x", "alive");
        store.set("y", "alive");

        Thread.sleep(80);   // let a, b, c expire

        manager.sweep();

        assertThat(store.getStore()).doesNotContainKey("a");
        assertThat(store.getStore()).doesNotContainKey("b");
        assertThat(store.getStore()).doesNotContainKey("c");
        assertThat(store.getStore()).containsKey("x");
        assertThat(store.getStore()).containsKey("y");
    }

    // ─── Sweep leaves live and no-TTL keys untouched ──────────────────────────

    @Test
    @DisplayName("sweep does not remove a key that has not yet expired")
    void sweep_doesNotRemoveLiveKey() throws InterruptedException {
        store.set("long-lived", "value", 60_000);   // 60-second TTL

        Thread.sleep(20);

        manager.sweep();

        assertThat(store.getStore().containsKey("long-lived"))
            .as("key with 60s TTL should not be removed by sweep")
            .isTrue();
    }

    @Test
    @DisplayName("sweep does not remove a key with no TTL")
    void sweep_doesNotRemoveKeyWithNoTtl() throws InterruptedException {
        store.set("permanent", "forever");   // no TTL

        Thread.sleep(20);

        manager.sweep();

        assertThat(store.getStore().containsKey("permanent"))
            .as("key with no TTL should survive sweep")
            .isTrue();
    }

    // ─── Expired keys removed without GET involvement ─────────────────────────

    @Test
    @DisplayName("expired key removed by sweep even if GET is never called")
    void sweep_removesExpiredKey_withoutGetBeingCalled() throws InterruptedException {
        store.set("never-read", "data", 50);

        Thread.sleep(80);

        // Verify it's expired (bypass lazy delete by checking the map directly)
        var entry = store.getStore().get("never-read");
        assertThat(entry).isNotNull();
        assertThat(entry.isExpired()).isTrue();

        manager.sweep();

        assertThat(store.getStore().containsKey("never-read"))
            .as("active sweep must evict expired key even with no GET calls")
            .isFalse();
    }

    // ─── Timing: sweep within 200 ms of expiry ────────────────────────────────

    @Test
    @DisplayName("expired key is removed within 200 ms of expiry when scheduler runs")
    void scheduledSweep_removesExpiredKeyWithin200ms() throws InterruptedException {
        store.set("scheduled-test", "value", 50);  // 50 ms TTL

        manager.start();   // real scheduler — fires every 100 ms

        try {
            Thread.sleep(250);   // 50 ms TTL + 100 ms sweep + 100 ms buffer

            assertThat(store.getStore().containsKey("scheduled-test"))
                .as("key should be gone within 200 ms of expiry when scheduler is running")
                .isFalse();
        } finally {
            manager.stop();
        }
    }
}
