package com.somesh.kvstore.replication;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Fixed-capacity circular buffer with monotonically increasing offset tracking.
 *
 * <h2>Design</h2>
 * Every item appended is assigned a global offset starting at 0. The buffer
 * holds at most {@code capacity} items. When full, the oldest item is overwritten
 * and the base offset advances. This mirrors Redis's replication backlog (PSYNC
 * backlog buffer).
 *
 * <h2>Offset semantics</h2>
 * <ul>
 *   <li>{@code nextOffset} — the offset that will be assigned to the next append</li>
 *   <li>An offset is "in the buffer" when: {@code nextOffset - capacity <= offset < nextOffset}</li>
 *   <li>When a replica reconnects with {@code fromOffset}, if that offset is still in the
 *       buffer we can do a partial resync; otherwise a full resync is needed.</li>
 * </ul>
 *
 * <h2>Concurrency</h2>
 * All public methods are guarded by an internal {@link ReentrantLock}. This
 * is a coarse lock, but each operation is O(1), so contention is negligible.
 *
 * <h2>Interview angles</h2>
 * <ul>
 *   <li>Why a ring buffer instead of an unbounded list? → bounded memory, O(1) append</li>
 *   <li>What happens if the replica falls behind past buffer capacity? → full resync</li>
 *   <li>Why AtomicLong is NOT sufficient here? → contains() + getRange() need atomicity
 *       across multiple fields (nextOffset + buffer contents)</li>
 * </ul>
 *
 * @param <T> the type of items stored in the backlog
 */
public class RingBuffer<T> {

    private final int capacity;
    private final Object[] buffer;
    private final ReentrantLock lock = new ReentrantLock();

    /** Offset to assign to the NEXT appended item. */
    private long nextOffset = 0;

    /**
     * Create a ring buffer with the given capacity.
     *
     * @param capacity maximum number of items to retain in the backlog; must be > 0
     */
    public RingBuffer(int capacity) {
        if (capacity <= 0) throw new IllegalArgumentException("capacity must be > 0");
        this.capacity = capacity;
        this.buffer   = new Object[capacity];
    }

    // ── Public API ────────────────────────────────────────────────────────────

    /**
     * Append an item and return its assigned offset.
     *
     * <p>If the buffer is full, the oldest item is silently overwritten and the
     * base offset advances. The returned offset is the item's permanent address.
     *
     * @param item the item to store; must not be null
     * @return the monotonically increasing offset assigned to this item
     */
    public long append(T item) {
        lock.lock();
        try {
            long offset = nextOffset;
            buffer[(int) (offset % capacity)] = item;
            nextOffset++;
            return offset;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Check whether the given offset is still retained in the buffer.
     *
     * <p>Returns {@code true} iff the item has not been overwritten by newer appends.
     *
     * @param offset the offset to check
     * @return {@code true} if offset is in [nextOffset - capacity, nextOffset)
     */
    public boolean contains(long offset) {
        lock.lock();
        try {
            if (offset < 0 || offset >= nextOffset) return false;
            // oldest retained offset = nextOffset - capacity (or 0 if buffer not yet full)
            long oldest = Math.max(0, nextOffset - capacity);
            return offset >= oldest;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Return all items from {@code fromOffset} (inclusive) to the latest appended item.
     *
     * <p>The caller is responsible for checking {@link #contains(long)} first to
     * determine whether a partial resync is possible. If {@code fromOffset} is not
     * in the buffer, this method returns an empty list (full resync required).
     *
     * @param fromOffset the first offset to include
     * @return a snapshot list of items; may be empty but never null
     */
    @SuppressWarnings("unchecked")
    public List<T> getRange(long fromOffset) {
        lock.lock();
        try {
            if (!contains(fromOffset)) return List.of();
            List<T> result = new ArrayList<>();
            for (long off = fromOffset; off < nextOffset; off++) {
                result.add((T) buffer[(int) (off % capacity)]);
            }
            return result;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Return the offset that will be assigned to the NEXT appended item.
     * This is the "master offset" advertised to replicas.
     *
     * @return next offset (0 if nothing has been appended yet)
     */
    public long getNextOffset() {
        lock.lock();
        try {
            return nextOffset;
        } finally {
            lock.unlock();
        }
    }

    /**
     * The offset of the most recently appended item, or -1 if nothing was appended yet.
     */
    public long getLatestOffset() {
        lock.lock();
        try {
            return nextOffset - 1;
        } finally {
            lock.unlock();
        }
    }

    /** @return the maximum number of items this buffer retains */
    public int getCapacity() {
        return capacity;
    }
}
