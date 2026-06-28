package com.somesh.kvstore.engine;

/**
 * The atomic unit of storage in the KV store.
 *
 * <p>Every key in the store maps to exactly one {@code ValueEntry}. It bundles
 * the stored value with its expiry metadata so that a single map lookup gives
 * you everything you need to answer a GET request.
 *
 * <h2>Design decisions worth knowing for interviews</h2>
 *
 * <ul>
 *   <li><b>Immutable fields ({@code final})</b> — once a ValueEntry is created,
 *       neither its value nor its expiry can change. To "update" a key you replace
 *       the entire entry in the map. This makes the object safe to share across
 *       threads without synchronization — a reader that holds a reference to an
 *       entry sees a consistent snapshot even if another thread concurrently
 *       calls SET on the same key.</li>
 *
 *   <li><b>Sentinel value {@code -1} for "no expiry"</b> — using {@code 0} would
 *       be ambiguous (epoch time = Jan 1 1970, technically a valid — if ancient —
 *       expiry). {@code -1} is unambiguous. Same pattern as
 *       {@link String#indexOf(String)} returning -1 for "not found".</li>
 *
 *   <li><b>{@link System#currentTimeMillis()} vs {@link System#nanoTime()}</b> —
 *       currentTimeMillis() is wall-clock time. nanoTime() is a monotonic timer
 *       that is not affected by system clock changes (NTP adjustments, DST).
 *       For production TTL logic, nanoTime() is safer. We use currentTimeMillis()
 *       here for simplicity and because the difference only matters if the system
 *       clock jumps during a server's uptime.</li>
 *
 *   <li><b>String-only values for now</b> — Redis supports multiple value types
 *       (string, list, set, hash, zset). We'll stay with String in Week 1 and
 *       introduce a polymorphic {@code RedisValue} type in a later week.</li>
 * </ul>
 */
public final class ValueEntry {

    /** The stored value. Never null — use DEL to remove a key entirely. */
    public final String value;

    /**
     * Absolute expiry timestamp in milliseconds since epoch.
     * {@code -1} means the entry never expires.
     *
     * <p>Stored as absolute time (not relative TTL) so that the cost of the
     * TTL→timestamp conversion is paid once on write, not on every read.
     */
    public final long expiresAt;

    /**
     * Creates a new entry.
     *
     * @param value  the stored string value; must not be null
     * @param ttlMs  time-to-live in milliseconds; use {@code -1} for no expiry
     */
    public ValueEntry(String value, long ttlMs) {
        if (value == null) throw new IllegalArgumentException("value must not be null");
        this.value = value;
        this.expiresAt = ttlMs < 0
                ? -1
                : System.currentTimeMillis() + ttlMs;
    }

    /**
     * Returns {@code true} if this entry has an expiry set AND that expiry has passed.
     *
     * <p>Called on every GET path (lazy expiry). Also called by the background
     * {@code ExpiryManager} sweep (active expiry). Both paths delete the entry
     * if this returns true.
     */
    public boolean isExpired() {
        return expiresAt != -1 && System.currentTimeMillis() > expiresAt;
    }

    /**
     * Returns {@code true} if this entry has a TTL set (even if not yet expired).
     * Useful for the PERSIST and TTL commands you'll add later.
     */
    public boolean hasTtl() {
        return expiresAt != -1;
    }

    /**
     * Remaining time-to-live in milliseconds, or {@code -1} if no expiry is set,
     * or {@code -2} if already expired.
     *
     * <p>Maps to the semantics of Redis's {@code PTTL} command.
     */
    public long remainingTtlMs() {
        if (expiresAt == -1) return -1;
        long remaining = expiresAt - System.currentTimeMillis();
        return remaining <= 0 ? -2 : remaining;
    }

    @Override
    public String toString() {
        return "ValueEntry{value='" + value + "', expiresAt=" + expiresAt + "}";
    }
}