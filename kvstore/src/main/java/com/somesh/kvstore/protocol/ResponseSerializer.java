package com.somesh.kvstore.protocol;

import com.somesh.kvstore.config.ServerConfig;
import com.somesh.kvstore.engine.CommandResult;

/**
 * Converts a CommandResult into a RESP-format string for writing to the client socket.
 *
 * RESP quick reference:
 *   +OK\r\n                  simple string (status)
 *   -ERR message\r\n         error
 *   :42\r\n                  integer
 *   $5\r\nhello\r\n          bulk string (length-prefixed)
 *   $-1\r\n                  null bulk string (nil)
 *
 * Thread safety: fully stateless — one instance shared across all ClientHandler threads.
 *
 * Design note: this class knows about RESP format and CommandResult types.
 * It knows nothing about KVStore, Command parsing, or sockets.
 * ResponseSerializer → CommandResult only. No other dependencies.
 */
public class ResponseSerializer {

    // ── Public API ──────────────────────────────────────────────────

    /**
     * Serialize a CommandResult to its RESP wire representation.
     *
     * @param result must not be null
     * @return complete RESP string including \r\n terminator(s), ready to write to socket
     */
    public String serialize(CommandResult result) {
        return switch (result.type) {
            case OK      -> ok();
            case STRING  -> bulk(result.stringValue);
            case INTEGER -> integer(result.intValue);
            case ERROR   -> error(result.errorMessage);
        };
    }

    // ── RESP type builders — package-visible for tests ──────────────

    /**
     * +OK\r\n
     * Used for: SET, and any future write command that just confirms success.
     */
    String ok() {
        return "+" + "OK" + ServerConfig.CRLF;
    }

    /**
     * $len\r\nvalue\r\n   for non-null values
     * $-1\r\n             for null (nil) — key not found
     *
     * Used for: GET, PING (with or without message), any string-returning command.
     *
     * Why bulk and not simple string (+)?
     * Bulk strings are binary-safe — a stored value might contain \r\n.
     * Simple strings cannot. Always use bulk for user data.
     */
    String bulk(String value) {
        if (value == null) {
            return "$-1" + ServerConfig.CRLF;
        }
        // Length is byte count, not char count.
        // For ASCII-only values they're equal. For Unicode, they differ.
        // getBytes() with explicit charset is the correct approach.
        byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        return "$" + bytes.length + ServerConfig.CRLF
             + value + ServerConfig.CRLF;
    }

    /**
     * :n\r\n
     * Used for: DEL (count), EXISTS (count), PTTL (millis), any integer result.
     */
    String integer(long value) {
        return ":" + value + ServerConfig.CRLF;
    }

    /**
     * -ERR message\r\n
     *
     * Convention: message already contains the error code prefix (ERR, WRONGTYPE, etc.)
     * if the caller supplied it. We don't double-prefix here.
     *
     * Redis error format: "-{CODE} {message}\r\n"
     * We keep it simple: "-{message}\r\n"
     */
    String error(String message) {
        String safeMessage = (message == null) ? "ERR unknown error" : message;
        // Errors must be single-line — strip any embedded newlines
        String sanitized = safeMessage.replace("\r", "").replace("\n", " ");
        return "-" + sanitized + ServerConfig.CRLF;
    }

    /**
     * *n\r\n followed by n RESP elements.
     * Not used in Week 1 — here for Week 3 (KEYS, LRANGE, SMEMBERS).
     *
     * @param elements already-serialized RESP strings for each element
     */
    String array(String... elements) {
        if (elements == null) {
            return "*-1" + ServerConfig.CRLF;   // null array
        }
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(elements.length).append(ServerConfig.CRLF);
        for (String element : elements) {
            sb.append(element);   // each element already has its own \r\n
        }
        return sb.toString();
    }
}