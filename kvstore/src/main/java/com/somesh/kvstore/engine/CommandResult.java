package com.somesh.kvstore.engine;

/**
 * The typed outcome of executing a command against {@link KVStore}.
 *
 * <p>Mirrors the four RESP types so {@code ResponseSerializer} can pattern-match
 * without {@code instanceof} chains:
 * <pre>
 *   OK      → "+OK\r\n"
 *   STRING  → "$N\r\n<value>\r\n"  (or "$-1\r\n" for null)
 *   INTEGER → ":N\r\n"
 *   ERROR   → "-ERR message\r\n"
 * </pre>
 *
 * <p>Immutable and thread-safe — safe to pass across threads if needed.
 */
public final class CommandResult {

    public enum Type { OK, STRING, INTEGER, ERROR, ARRAY }

    public final Type         type;
    public final String       stringValue;   // non-null when type == STRING
    public final long         intValue;      // meaningful when type == INTEGER
    public final String       errorMessage;  // non-null when type == ERROR
    public final java.util.List<String> arrayValue;   // non-null when type == ARRAY

    private CommandResult(Type type, String stringValue, long intValue,
                          String errorMessage, java.util.List<String> arrayValue) {
        this.type         = type;
        this.stringValue  = stringValue;
        this.intValue     = intValue;
        this.errorMessage = errorMessage;
        this.arrayValue   = arrayValue;
    }

    // ── Static factories — one per RESP type ───────────────────────

    public static CommandResult ok() {
        return new CommandResult(Type.OK, null, 0, null, null);
    }

    /** Null-safe: pass {@code null} to produce a RESP nil bulk string ({@code $-1}). */
    public static CommandResult string(String value) {
        return new CommandResult(Type.STRING, value, 0, null, null);
    }

    public static CommandResult integer(long value) {
        return new CommandResult(Type.INTEGER, null, value, null, null);
    }

    public static CommandResult error(String message) {
        return new CommandResult(Type.ERROR, null, 0, message, null);
    }

    /** RESP array of bulk strings. Pass an empty list for an empty array. */
    public static CommandResult array(java.util.List<String> values) {
        return new CommandResult(Type.ARRAY, null, 0, null,
            java.util.List.copyOf(values));
    }

    @Override
    public String toString() {
        return "CommandResult{type=" + type +
               (stringValue  != null ? ", str='"   + stringValue  + "'" : "") +
               (type == Type.INTEGER ? ", int="    + intValue           : "") +
               (errorMessage != null ? ", err='"   + errorMessage + "'" : "") + "}";
    }
}
