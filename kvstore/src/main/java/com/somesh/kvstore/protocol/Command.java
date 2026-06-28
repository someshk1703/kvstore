package com.somesh.kvstore.protocol;

/**
 * Immutable value object representing a parsed client command.
 *
 * <p>The {@code args} array does NOT include the command name itself:
 * <pre>
 *   "SET foo bar"   → type=SET,  args=["foo","bar"]
 *   "GET foo"       → type=GET,  args=["foo"]
 *   "DEL a b c"     → type=DEL,  args=["a","b","c"]
 *   "PING"          → type=PING, args=[]
 * </pre>
 */
public final class Command {

    private final CommandType type;
    private final String[]    args;

    public Command(CommandType type, String[] args) {
        this.type = type;
        this.args = args == null ? new String[0] : args.clone(); // defensive copy
    }

    public CommandType type()     { return type; }
    /** Returns a defensive copy — callers cannot mutate internal state. */
    public String[]    args()     { return args.clone(); }
    public int         argCount() { return args.length; }

    /**
     * Safe indexed access — returns {@code null} rather than throwing
     * {@link ArrayIndexOutOfBoundsException} so handlers can use it without
     * a length pre-check.
     */
    public String arg(int index) {
        return (index >= 0 && index < args.length) ? args[index] : null;
    }

    @Override
    public String toString() {
        return type + " " + String.join(" ", args);
    }
}
