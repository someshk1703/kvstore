package com.somesh.kvstore.engine;

import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KVStore {

    private static final Logger log = LoggerFactory.getLogger(KVStore.class);

    private final ConcurrentHashMap<String, ValueEntry> store =
            new ConcurrentHashMap<>(256);

    // ── Write operations ────────────────────────────────────────────

    public String set(String key, String value, long ttlMs) {
        ValueEntry newEntry = new ValueEntry(value, ttlMs);
        ValueEntry previous = store.put(key, newEntry);
        log.debug("SET {} ttl={}ms (was={})", key, ttlMs,
                previous == null ? "null" : previous.value);
        return previous == null ? null : previous.value;
    }

    public String set(String key, String value) {
        return set(key, value, -1);
    }

    public int del(String... keys) {
        int deleted = 0;
        for (String key : keys) {
            if (store.remove(key) != null) deleted++;
        }
        return deleted;
    }

    // ── Read operations ─────────────────────────────────────────────

    public String get(String key) {
        ValueEntry entry = store.get(key);
        if (entry == null) return null;
        if (entry.isExpired()) {
            store.remove(key, entry);   // conditional remove — safe under concurrency
            return null;
        }
        return entry.value;
    }

    public boolean exists(String key) {
        return get(key) != null;
    }

    public long pttl(String key) {
        ValueEntry entry = store.get(key);
        if (entry == null) return -2;
        if (entry.isExpired()) {
            store.remove(key, entry);
            return -2;
        }
        return entry.remainingTtlMs();
    }

    // ── Meta ────────────────────────────────────────────────────────

    public int size() {
        return store.size();
    }

    /** For ExpiryManager sweep only — do not use for reads. */
    public ConcurrentHashMap<String, ValueEntry> getStore() {
        return store;
    }
}