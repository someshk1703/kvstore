package com.somesh.kvstore.protocol;

/**
 * All commands the server understands.
 *
 * Week 1: SET, GET, DEL, EXISTS, PTTL, PING
 * Week 2: EXPIRE, TTL, PERSIST, INCR
 * Week 3: (no new commands — persistence is internal)
 * Week 4: REPLCONF, WAIT
 * Week 5: MSET, MGET, KEYS, FLUSHALL, INFO
 */
public enum CommandType {
    // ── Week 1 ──
    SET, GET, DEL, EXISTS, PTTL, PING,

    // ── Week 2 ──
    EXPIRE, TTL, PERSIST, INCR,

    // ── Week 4 ──
    REPLCONF, WAIT,

    // ── Week 5 ──
    MSET, MGET, KEYS, FLUSHALL, INFO,

    // Sentinel for unrecognised input — never dispatched
    UNKNOWN
}
