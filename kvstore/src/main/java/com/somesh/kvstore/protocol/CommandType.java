package com.somesh.kvstore.protocol;

/**
 * All supported commands with their expected argument counts.
 *
 * minArgs / maxArgs exclude the command name itself.
 * -1 for maxArgs means "no upper limit".
 *
 * Adding a new command = one line here + one handler in CommandExecutor.
 * The parser reads these values — no switch needed.
 */
public enum CommandType {

    // Week 1 core
    GET   (1, 1),
    SET   (2, 4),   // SET key value [EX n | PX n]
    DEL   (1, -1),  // DEL key [key ...]

    // Useful from day one
    EXISTS(1, -1),  // EXISTS key [key ...]
    PTTL  (1, 1),
    PING  (0, 1),   // PING [message]

    // Sentinel for unknown commands
    UNKNOWN(0, -1);

    public final int minArgs;
    public final int maxArgs;   // -1 = unlimited

    CommandType(int minArgs, int maxArgs) {
        this.minArgs = minArgs;
        this.maxArgs = maxArgs;
    }

    /**
     * Case-insensitive lookup. Returns UNKNOWN rather than throwing
     * for unrecognized names — the parser handles the error response.
     */
    public static CommandType fromString(String name) {
        try {
            return CommandType.valueOf(name.toUpperCase());
        } catch (IllegalArgumentException e) {
            return UNKNOWN;
        }
    }
}