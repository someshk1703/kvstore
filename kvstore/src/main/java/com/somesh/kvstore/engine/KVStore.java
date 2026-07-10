package com.somesh.kvstore.engine;

import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.somesh.kvstore.config.ServerConfig;
import com.somesh.kvstore.memory.LRUCache;

/**
 * Core key-value storage engine.
 *
 * <h2>Week 2 additions</h2>
 * <ul>
 *   <li><b>LRUCache</b> — tracks access order for eviction. Every successful GET/SET
 *       promotes the key to MRU position. When memory exceeds
 *       {@link ServerConfig#MAX_MEMORY_BYTES}, LRU keys are evicted until usage
 *       drops below 80% of the threshold.</li>
 *   <li><b>EXPIRE / PERSIST</b> — update an existing entry's TTL without replacing
 *       the full value.</li>
 * </ul>
 *
 * <h2>Concurrency design</h2>
 * {@code ConcurrentHashMap} handles concurrent reads and writes to the main store.
 * {@code LRUCache} is NOT thread-safe on its own, so all LRU operations are guarded
 * by {@code synchronized(lruLock)}. This is a coarse lock, but it's only held for
 * O(1) operations (pointer swaps), so contention is minimal in practice.
 *
 * Interview question this answers: "Why not just use a synchronized LinkedHashMap?"
 * Answer: LinkedHashMap.get() and put() would need a global lock for both the map
 * and the eviction order, and LinkedHashMap's access-order mode isn't safe for
 * concurrent use. The ConcurrentHashMap + separate LRU design keeps the hot read
 * path lock-free for the map itself while only locking for LRU bookkeeping.
 */
public class KVStore {

    private static final Logger log = LoggerFactory.getLogger(KVStore.class);

    private final ConcurrentHashMap<String, ValueEntry> store =
            new ConcurrentHashMap<>(256);

    // LRU cache tracks access order. Capacity = max keys before eviction by count.
    // Lock object — every LRU read/write is wrapped in synchronized(lruLock).
    private final LRUCache lru;
    private final Object   lruLock = new Object();

    // ── Constructors ─────────────────────────────────────────────────────────

    public KVStore() {
        this(ServerConfig.LRU_CAPACITY);
    }

    /** Package-private constructor for tests — allows a small capacity. */
    KVStore(int lruCapacity) {
        this.lru = new LRUCache(lruCapacity);
    }

    // ── Write operations ──────────────────────────────────────────────────────

    public String set(String key, String value, long ttlMs) {
        ValueEntry newEntry  = new ValueEntry(value, ttlMs);
        ValueEntry previous  = store.put(key, newEntry);

        synchronized (lruLock) {
            lru.put(key, value);   // promote / insert in LRU
        }

        maybeEvict();   // enforce memory cap after every write
        return previous == null ? null : previous.value;
    }

    public String set(String key, String value) {
        return set(key, value, -1);
    }

    public int del(String... keys) {
        int deleted = 0;
        for (String key : keys) {
            if (store.remove(key) != null) {
                synchronized (lruLock) { lru.remove(key); }
                deleted++;
            }
        }
        return deleted;
    }

    /**
     * Sets or updates the TTL on an existing key without changing its value.
     *
     * @param key    the key
     * @param ttlMs  TTL in milliseconds; must be > 0
     * @return {@code 1} if the key exists and the TTL was set;
     *         {@code 0} if the key does not exist
     */
    public int expire(String key, long ttlMs) {
        // ConcurrentHashMap.compute is atomic — read-modify-write in one lock step.
        boolean[] updated = {false};
        store.computeIfPresent(key, (k, existing) -> {
            if (!existing.isExpired()) {
                updated[0] = true;
                return new ValueEntry(existing.value, ttlMs);
            }
            return existing;   // already expired — don't update
        });
        return updated[0] ? 1 : 0;
    }

    /**
     * Removes the TTL from an existing key, making it persistent.
     *
     * @return {@code 1} if the key existed and had a TTL that was removed;
     *         {@code 0} if the key doesn't exist or already had no TTL
     */
    public int persist(String key) {
        boolean[] updated = {false};
        store.computeIfPresent(key, (k, existing) -> {
            if (!existing.isExpired() && existing.hasTtl()) {
                updated[0] = true;
                return new ValueEntry(existing.value, -1);   // -1 = no expiry
            }
            return existing;
        });
        return updated[0] ? 1 : 0;
    }

    // ── Read operations ───────────────────────────────────────────────────────

    public String get(String key) {
        ValueEntry entry = store.get(key);
        if (entry == null) return null;
        if (entry.isExpired()) {
            store.remove(key, entry);   // conditional remove — safe under concurrency
            synchronized (lruLock) { lru.remove(key); }
            return null;
        }
        synchronized (lruLock) { lru.get(key); }   // promote to MRU
        return entry.value;
    }

    public boolean exists(String key) {
        return get(key) != null;
    }

    /**
     * Returns the remaining TTL in milliseconds.
     *
     * @return positive ms remaining; {@code -1} if no expiry; {@code -2} if key missing or expired
     */
    public long pttl(String key) {
        ValueEntry entry = store.get(key);
        if (entry == null) return -2;
        if (entry.isExpired()) {
            store.remove(key, entry);
            synchronized (lruLock) { lru.remove(key); }
            return -2;
        }
        return entry.remainingTtlMs();
    }

    /**
     * Remaining TTL in whole seconds, following Redis TTL semantics:
     * {@code -1} = no expiry, {@code -2} = key missing or expired, positive = seconds.
     */
    public long ttl(String key) {
        long ms = pttl(key);
        if (ms < 0) return ms;   // pass through -1 and -2 sentinel values
        return Math.max(1, ms / 1000);   // round up: 999ms → 1s (not 0)
    }

    // ── Meta ──────────────────────────────────────────────────────────────────

    public int size() {
        return store.size();
    }

    public ConcurrentHashMap<String, ValueEntry> getStore() {
        return store;
    }

    /**
     * Called by {@link com.somesh.kvstore.memory.ExpiryManager} when the sweep
     * evicts a key — removes it from the LRU tracking structure so memory
     * accounting stays consistent.
     */
    public void evictFromLru(String key) {
        synchronized (lruLock) { lru.remove(key); }
    }

    // ── Memory management ─────────────────────────────────────────────────────

    /**
     * Checks current JVM heap usage. If it exceeds {@link ServerConfig#MAX_MEMORY_BYTES},
     * evicts LRU keys until usage drops below 80% of the threshold.
     *
     * <p>Called synchronously on every write (mirroring Redis's eviction-on-write model).
     * The O(1) evict-per-call cost is negligible compared to the I/O of a write.
     *
     * <p>Interview note: this approach has one known race — two threads could both
     * detect the threshold breached and each evict. That results in slightly more
     * eviction than necessary, not less, which is safe (conservative).
     */
    private void maybeEvict() {
        Runtime rt = Runtime.getRuntime();
        long usedBytes = rt.totalMemory() - rt.freeMemory();

        if (usedBytes <= ServerConfig.MAX_MEMORY_BYTES) return;

        long targetBytes = (long) (ServerConfig.MAX_MEMORY_BYTES * ServerConfig.EVICTION_TARGET_RATIO);
        int evictions = 0;

        while (true) {
            usedBytes = rt.totalMemory() - rt.freeMemory();
            if (usedBytes <= targetBytes) break;

            String evictedKey;
            synchronized (lruLock) {
                evictedKey = lru.evictLeastRecentlyUsed();
            }
            if (evictedKey == null) break;   // LRU is empty — nothing left to evict

            store.remove(evictedKey);
            evictions++;
            log.debug("Memory eviction: removed key '{}' (evictions so far: {})", evictedKey, evictions);
        }

        if (evictions > 0) {
            log.info("Memory eviction complete — evicted {} keys", evictions);
        }
    }
}
