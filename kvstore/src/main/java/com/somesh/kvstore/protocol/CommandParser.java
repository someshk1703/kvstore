package com.somesh.kvstore.protocol;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parses a raw text line from a TCP client into a {@link Command}.
 *
 * <p>Protocol: RESP inline — tokens separated by whitespace, newline-terminated.
 * The first token is the command name; all remaining tokens are arguments.
 * Command names are case-insensitive.
 *
 * <pre>
 *   "SET foo bar"       → Command{SET, ["foo","bar"]}
 *   "GET foo"           → Command{GET, ["foo"]}
 *   "set FOO bar EX 10" → Command{SET, ["FOO","bar","EX","10"]}
 *   "PING"              → Command{PING, []}
 * </pre>
 */
public final class CommandParser {

    private static final Logger log = LoggerFactory.getLogger(CommandParser.class);

    private CommandParser() {}

    /**
     * Parse one line into a Command.
     *
     * @param line raw line from the client (newline already stripped by BufferedReader)
     * @return a Command — never null; unknown command names produce {@link CommandType#UNKNOWN}
     */
    public static Command parse(String line) {
        if (line == null || line.isBlank()) {
            return new Command(CommandType.UNKNOWN, new String[0]);
        }

        // Split on any whitespace run; limit=0 discards trailing empty strings
        String[] tokens = line.trim().split("\\s+");
        if (tokens.length == 0) {
            return new Command(CommandType.UNKNOWN, new String[0]);
        }

        CommandType type;
        try {
            type = CommandType.valueOf(tokens[0].toUpperCase());
        } catch (IllegalArgumentException e) {
            log.debug("Unrecognised command: {}", tokens[0]);
            type = CommandType.UNKNOWN;
        }

        // args = everything after the command name
        String[] args = new String[tokens.length - 1];
        System.arraycopy(tokens, 1, args, 0, args.length);

        return new Command(type, args);
    }
}
