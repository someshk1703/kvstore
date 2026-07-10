package com.somesh.kvstore.memory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * O(1) Least-Recently-Used cache backed by a doubly linked list + HashMap.
 *
 * <h2>Why both a list AND a map?</h2>
 * <ul>
 *   <li>The <b>HashMap</b> gives O(1) lookup: "does this key exist, and where is it?"</li>
 *   <li>The <b>doubly linked list</b> gives O(1) insertion at head and O(1) removal
 *       from any position — because each node holds direct prev/next pointers, so
 *       unlinking is a pointer swap, not a scan.</li>
 *   <li>Neither structure alone can do both: a HashMap has no inherent order,
 *       a singly linked list requires O(N) traversal to remove an arbitrary node.</li>
 * </ul>
 *
 * <h2>Eviction policy</h2>
 * On every {@link #get}, the accessed node is moved to the head (most-recently-used).
 * On every {@link #put}, if the cache is at capacity the node at the tail
 * (least-recently-used) is evicted before the new entry is inserted.
 *
 * <h2>Thread safety</h2>
 * NOT thread-safe on its own. The caller ({@link com.somesh.kvstore.engine.KVStore})
 * must synchronize access or use it from a single thread. {@code KVStore} wraps
 * accesses via {@code synchronized(lruLock)}.
 */
public class LRUCache {

    // ── Internal node ────────────────────────────────────────────────────────

    static class Node {
        String key;
        String value;
        Node   prev;
        Node   next;

        Node() {}   // sentinel head / tail

        Node(String key, String value) {
            this.key   = key;
            this.value = value;
        }
    }

    // ── State ────────────────────────────────────────────────────────────────

    private final int           capacity;
    private final Map<String, Node> map;

    // Sentinel nodes — never hold data; simplify edge cases at list boundaries.
    // head.next = most recently used
    // tail.prev = least recently used
    private final Node head = new Node();
    private final Node tail = new Node();

    // ── Constructor ──────────────────────────────────────────────────────────

    public LRUCache(int capacity) {
        if (capacity <= 0) throw new IllegalArgumentException("capacity must be > 0");
        this.capacity = capacity;
        this.map      = new HashMap<>(capacity * 2); // pre-size to avoid rehashing
        head.next = tail;
        tail.prev = head;
    }

    // ── Public API ───────────────────────────────────────────────────────────

    /**
     * Returns the value for {@code key}, or {@code null} if not present.
     * Accessing a key marks it as most-recently-used.
     */
    public String get(String key) {
        Node node = map.get(key);
        if (node == null) return null;
        moveToFront(node);
        return node.value;
    }

    /**
     * Inserts or updates {@code key → value}.
     * If at capacity, the least-recently-used key is evicted first.
     *
     * @return the evicted key, or {@code null} if no eviction happened
     */
    public String put(String key, String value) {
        Node existing = map.get(key);
        if (existing != null) {
            // Update in place and promote — no capacity change.
            existing.value = value;
            moveToFront(existing);
            return null;
        }

        String evictedKey = null;
        if (map.size() >= capacity) {
            evictedKey = evictLRU();
        }

        Node node = new Node(key, value);
        addToFront(node);
        map.put(key, node);
        return evictedKey;
    }

    /**
     * Removes {@code key} from the cache.
     *
     * @return the removed value, or {@code null} if not present
     */
    public String remove(String key) {
        Node node = map.remove(key);
        if (node == null) return null;
        unlink(node);
        return node.value;
    }

    /** Returns {@code true} if the cache contains {@code key}. */
    public boolean containsKey(String key) {
        return map.containsKey(key);
    }

    /** Number of entries currently in the cache. */
    public int size() {
        return map.size();
    }

    /** Maximum capacity set at construction time. */
    public int capacity() {
        return capacity;
    }

    /** Returns the keys ordered from most-recently-used to least-recently-used.
     * Primarily for testing / debugging — do not call in a hot path.
     */
    public List<String> keysInOrder() {
        List<String> result = new ArrayList<>(map.size());
        Node cur = head.next;
        while (cur != tail) {
            result.add(cur.key);
            cur = cur.next;
        }
        return result;
    }

    /**
     * Evicts and returns the least-recently-used key, regardless of capacity.
     * Returns {@code null} if the cache is empty.
     *
     * <p>Used by {@link com.somesh.kvstore.engine.KVStore} for memory-pressure
     * eviction independent of a new insertion.
     */
    public String evictLeastRecentlyUsed() {
        if (map.isEmpty()) return null;
        Node lruNode = tail.prev;
        if (lruNode == head) return null;
        unlink(lruNode);
        map.remove(lruNode.key);
        return lruNode.key;
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    /** Remove the tail.prev node (least recently used) and return its key. */
    private String evictLRU() {
        Node lru = tail.prev;
        if (lru == head) return null;   // empty list — should never happen if capacity > 0
        unlink(lru);
        map.remove(lru.key);
        return lru.key;
    }

    /** Detach {@code node} from its current position in the list. */
    private void unlink(Node node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
        node.prev = null;
        node.next = null;
    }

    /** Insert {@code node} immediately after the sentinel head. */
    private void addToFront(Node node) {
        node.next       = head.next;
        node.prev       = head;
        head.next.prev  = node;
        head.next       = node;
    }

    /** Move an already-linked {@code node} to the front. */
    private void moveToFront(Node node) {
        unlink(node);
        addToFront(node);
    }
}
