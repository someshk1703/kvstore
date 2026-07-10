package com.somesh.kvstore.memory;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for LRUCache — the doubly linked list + HashMap implementation.
 *
 * Tests cover:
 *   - Basic get/put semantics
 *   - Eviction order (LRU is evicted first)
 *   - Promotion on access (get() moves to front)
 *   - Update in place (put() on existing key)
 *   - Remove
 *   - evictLeastRecentlyUsed() for memory-pressure eviction
 *   - Edge cases: empty cache, capacity 1, construction with invalid capacity
 */
class LRUCacheTest {

    private LRUCache cache;

    @BeforeEach
    void setUp() {
        cache = new LRUCache(3);   // small capacity for easy reasoning
    }

    // ─── Basic semantics ──────────────────────────────────────────────────────

    @Test
    @DisplayName("get on missing key returns null")
    void get_missingKey_returnsNull() {
        assertThat(cache.get("ghost")).isNull();
    }

    @Test
    @DisplayName("put then get returns value")
    void put_thenGet_returnsValue() {
        cache.put("k", "v");
        assertThat(cache.get("k")).isEqualTo("v");
    }

    @Test
    @DisplayName("put overwrites existing value")
    void put_existingKey_updatesValue() {
        cache.put("k", "old");
        cache.put("k", "new");
        assertThat(cache.get("k")).isEqualTo("new");
    }

    @Test
    @DisplayName("size reflects number of entries")
    void size_reflectsEntryCount() {
        assertThat(cache.size()).isEqualTo(0);
        cache.put("a", "1");
        cache.put("b", "2");
        assertThat(cache.size()).isEqualTo(2);
    }

    @Test
    @DisplayName("containsKey returns true for existing key")
    void containsKey_existingKey_returnsTrue() {
        cache.put("x", "y");
        assertThat(cache.containsKey("x")).isTrue();
    }

    @Test
    @DisplayName("containsKey returns false for missing key")
    void containsKey_missingKey_returnsFalse() {
        assertThat(cache.containsKey("nope")).isFalse();
    }

    @Test
    @DisplayName("remove returns value and key is gone")
    void remove_existingKey_returnsValueAndKeyGone() {
        cache.put("r", "removeme");
        String removed = cache.remove("r");
        assertThat(removed).isEqualTo("removeme");
        assertThat(cache.get("r")).isNull();
        assertThat(cache.size()).isEqualTo(0);
    }

    @Test
    @DisplayName("remove on missing key returns null")
    void remove_missingKey_returnsNull() {
        assertThat(cache.remove("ghost")).isNull();
    }

    // ─── Eviction order ───────────────────────────────────────────────────────

    @Test
    @DisplayName("inserting capacity+1 evicts the least recently used key")
    void put_overCapacity_evictsLruKey() {
        // Insert 3 keys (fills the capacity-3 cache): a → b → c (c is MRU)
        cache.put("a", "1");
        cache.put("b", "2");
        cache.put("c", "3");

        // Insert 4th — "a" is LRU and should be evicted
        String evicted = cache.put("d", "4");

        assertThat(evicted)
            .as("LRU key 'a' should be evicted")
            .isEqualTo("a");
        assertThat(cache.get("a"))
            .as("evicted key should no longer be in cache")
            .isNull();
        assertThat(cache.get("d")).isEqualTo("4");
    }

    @Test
    @DisplayName("get promotes the accessed key — it is no longer eviction candidate")
    void get_promotesKey_accessedKeyNotEvictedFirst() {
        cache.put("a", "1");
        cache.put("b", "2");
        cache.put("c", "3");

        // Access "a" — it becomes MRU; "b" is now LRU
        cache.get("a");

        // Insert 4th — "b" should be evicted (LRU after "a" was promoted)
        String evicted = cache.put("d", "4");

        assertThat(evicted)
            .as("'b' should be evicted because 'a' was recently accessed")
            .isEqualTo("b");
        assertThat(cache.get("a"))
            .as("promoted key 'a' should still be present")
            .isEqualTo("1");
    }

    @Test
    @DisplayName("put on existing key promotes it — it is not the next eviction candidate")
    void put_existingKey_promotesToMru() {
        cache.put("a", "1");
        cache.put("b", "2");
        cache.put("c", "3");

        // Update "a" — should promote it to MRU; "b" becomes LRU
        cache.put("a", "updated");

        String evicted = cache.put("d", "4");

        assertThat(evicted)
            .as("'b' should be evicted; 'a' was re-inserted and promoted")
            .isEqualTo("b");
        assertThat(cache.get("a")).isEqualTo("updated");
    }

    @Test
    @DisplayName("keysInOrder reflects MRU→LRU ordering")
    void keysInOrder_reflectsMruToLruOrder() {
        cache.put("a", "1");
        cache.put("b", "2");
        cache.put("c", "3");
        cache.get("a");   // promote "a" to MRU

        List<String> order = cache.keysInOrder();

        // After promoting "a": MRU=a, then c, then b=LRU
        assertThat(order).first().isEqualTo("a");
        assertThat(order).last().isEqualTo("b");
    }

    // ─── evictLeastRecentlyUsed ───────────────────────────────────────────────

    @Test
    @DisplayName("evictLeastRecentlyUsed removes and returns LRU key")
    void evictLeastRecentlyUsed_returnsLruKey() {
        cache.put("a", "1");
        cache.put("b", "2");
        cache.put("c", "3");

        String evicted = cache.evictLeastRecentlyUsed();

        assertThat(evicted).isEqualTo("a");
        assertThat(cache.get("a")).isNull();
        assertThat(cache.size()).isEqualTo(2);
    }

    @Test
    @DisplayName("evictLeastRecentlyUsed on empty cache returns null")
    void evictLeastRecentlyUsed_emptyCache_returnsNull() {
        assertThat(cache.evictLeastRecentlyUsed()).isNull();
    }

    @Test
    @DisplayName("sequential evictLeastRecentlyUsed empties the cache in LRU order")
    void evictLeastRecentlyUsed_sequentially_drainsCacheInOrder() {
        cache.put("a", "1");
        cache.put("b", "2");
        cache.put("c", "3");

        assertThat(cache.evictLeastRecentlyUsed()).isEqualTo("a");
        assertThat(cache.evictLeastRecentlyUsed()).isEqualTo("b");
        assertThat(cache.evictLeastRecentlyUsed()).isEqualTo("c");
        assertThat(cache.evictLeastRecentlyUsed()).isNull();
        assertThat(cache.size()).isEqualTo(0);
    }

    // ─── Edge cases ───────────────────────────────────────────────────────────

    @Test
    @DisplayName("capacity-1 cache evicts on second insertion")
    void capacityOne_secondInsertEvictsFirst() {
        LRUCache tiny = new LRUCache(1);
        tiny.put("first", "1");
        String evicted = tiny.put("second", "2");

        assertThat(evicted).isEqualTo("first");
        assertThat(tiny.get("first")).isNull();
        assertThat(tiny.get("second")).isEqualTo("2");
    }

    @Test
    @DisplayName("construction with capacity ≤ 0 throws IllegalArgumentException")
    void constructor_invalidCapacity_throwsIllegalArgument() {
        assertThatThrownBy(() -> new LRUCache(0))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new LRUCache(-1))
            .isInstanceOf(IllegalArgumentException.class);
    }
}
