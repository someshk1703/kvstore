package com.somesh.kvstore.memory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.somesh.kvstore.config.ServerConfig;
import com.somesh.kvstore.engine.KVStore;

/**
 * Background thread that actively evicts expired keys.
 *
 * <h2>Why active expiry at all?</h2>
 * Lazy expiry (checking {@code isExpired()} on every GET) only removes keys that
 * are actually read. A key written once and never read again will stay in memory
 * forever without this sweep. Over time that causes unbounded memory growth even
 * when every key has a TTL.
 *
 * <h2>The Redis algorithm</h2>
 * Redis samples 20 random keys from the set of keys with TTLs every 100ms.
 * If > 25% of the sampled keys were expired, it immediately repeats the cycle
 * (aggressively cleans up when expiry rate is high). We implement a simplified
 * version: sample up to {@link ServerConfig#MAX_KEYS_PER_SWEEP} random keys,
 * remove those that are expired, repeat if > 25% were expired.
 *
 * <h2>Thread model</h2>
 * A single daemon thread via {@link ScheduledExecutorService}. Daemon threads
 * don't prevent the JVM from shutting down — important for clean server restarts.
 */
public class ExpiryManager {

    private static final Logger log = LoggerFactory.getLogger(ExpiryManager.class);

    /** Fraction of sampled keys that triggers an immediate repeat cycle. */
    private static final double REPEAT_THRESHOLD = 0.25;

    private final KVStore store;
    private final ScheduledExecutorService scheduler;

    public ExpiryManager(KVStore store) {
        this.store     = store;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "expiry-sweep");
            t.setDaemon(true);   // don't block JVM shutdown
            return t;
        });
    }

    // ── Lifecycle ────────────────────────────────────────────────────────────

    /** Start the background sweep thread. Safe to call once. */
    public void start() {
        scheduler.scheduleAtFixedRate(
            this::sweep,
            ServerConfig.EXPIRY_SWEEP_INTERVAL_MS,
            ServerConfig.EXPIRY_SWEEP_INTERVAL_MS,
            TimeUnit.MILLISECONDS
        );
        log.info("ExpiryManager started — sweep every {} ms", ServerConfig.EXPIRY_SWEEP_INTERVAL_MS);
    }

    /** Stop the sweep thread. Called on server shutdown. */
    public void stop() {
        scheduler.shutdownNow();
        log.info("ExpiryManager stopped");
    }

    // ── Core sweep logic ─────────────────────────────────────────────────────

    /**
     * One sweep cycle: sample random keys, delete expired ones.
     * Repeats immediately if the expired fraction exceeds {@link #REPEAT_THRESHOLD}.
     *
     * Package-private for testing — tests call this directly instead of
     * waiting for the scheduler.
     */
    void sweep() {
        try {
            boolean highExpiryRate;
            do {
                highExpiryRate = sweepOnce();
            } while (highExpiryRate);
        } catch (Exception e) {
            // Never let an exception kill the scheduler thread.
            log.warn("Expiry sweep error (continuing): {}", e.getMessage(), e);
        }
    }

    /**
     * Samples up to {@link ServerConfig#MAX_KEYS_PER_SWEEP} keys and removes
     * expired ones.
     *
     * @return {@code true} if the expired fraction exceeded {@link #REPEAT_THRESHOLD}
     *         (signals the caller to run another cycle immediately)
     */
    private boolean sweepOnce() {
        // Snapshot the live key set — avoid ConcurrentModificationException.
        // This is a point-in-time view; new keys added concurrently are fine
        // to miss — they'll be caught next cycle or by lazy expiry on GET.
        List<String> keys = new ArrayList<>(store.getStore().keySet());

        if (keys.isEmpty()) return false;

        // Randomise so we don't always scan the same keys.
        Collections.shuffle(keys);

        int sampleSize  = Math.min(keys.size(), ServerConfig.MAX_KEYS_PER_SWEEP);
        int expiredCount = 0;

        for (int i = 0; i < sampleSize; i++) {
            String key   = keys.get(i);
            // getStore().get() returns the raw ValueEntry; we check it directly
            // so we don't trigger the KVStore.get() lazy-delete path unnecessarily.
            var entry = store.getStore().get(key);
            if (entry != null && entry.isExpired()) {
                // Conditional remove — only deletes if the map still holds this
                // exact entry object. Safe under concurrent SET-on-same-key.
                store.getStore().remove(key, entry);
                // Also evict from LRU so memory accounting stays consistent.
                store.evictFromLru(key);
                expiredCount++;
                log.debug("Expiry sweep evicted key '{}' (expired at {})", key, entry.expiresAt);
            }
        }

        double ratio = (double) expiredCount / sampleSize;
        log.trace("Sweep: sampled={}, expired={}, ratio={}", sampleSize, expiredCount, ratio);
        return ratio > REPEAT_THRESHOLD;
    }
}
