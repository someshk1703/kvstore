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

    public enum Type { OK, STRING, INTEGER, ERROR }

    public final Type   type;
    public final String stringValue;   // non-null when type == STRING
    public final long   intValue;      // meaningful when type == INTEGER
    public final String errorMessage;  // non-null when type == ERROR

    private CommandResult(Type type, String stringValue, long intValue, String errorMessage) {
        this.type         = type;
        this.stringValue  = stringValue;
        this.intValue     = intValue;
        this.errorMessage = errorMessage;
    }

    // ── Static factories — one per RESP type ───────────────────────

    public static CommandResult ok() {
        return new CommandResult(Type.OK, null, 0, null);
    }

    /** Null-safe: pass {@code null} to produce a RESP nil bulk string ({@code $-1}). */
    public static CommandResult string(String value) {
        return new CommandResult(Type.STRING, value, 0, null);
    }

    public static CommandResult integer(long value) {
        return new CommandResult(Type.INTEGER, null, value, null);
    }

    public static CommandResult error(String message) {
        return new CommandResult(Type.ERROR, null, 0, message);
    }

    @Override
    public String toString() {
        return "CommandResult{type=" + type +
               (stringValue  != null ? ", str='"   + stringValue  + "'" : "") +
               (type == Type.INTEGER ? ", int="    + intValue           : "") +
               (errorMessage != null ? ", err='"   + errorMessage + "'" : "") + "}";
    }
}
