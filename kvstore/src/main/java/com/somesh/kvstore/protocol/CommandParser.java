package com.somesh.kvstore.protocol;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Parses raw text lines from a client socket into Command objects.
 *
 * Protocol: inline text, one command per line.
 *   "SET foo bar"     → Command{SET,  ["foo","bar"]}
 *   "GET foo"         → Command{GET,  ["foo"]}
 *   "DEL a b c"       → Command{DEL,  ["a","b","c"]}
 *   "PING"            → Command{PING, []}
 *
 * Responsibilities:
 *   - Strip CRLF / leading / trailing whitespace
 *   - Tokenize on whitespace
 *   - Resolve command name to CommandType
 *   - Validate argument count
 *   - Return a Command (valid or error) — never throws
 *
 * Not responsible for:
 *   - Reading bytes from socket (ClientHandler does that)
 *   - Serializing responses (ResponseSerializer does that)
 *   - Business logic (CommandExecutor does that)
 *
 * Thread safety: stateless — one instance can be shared across all
 * ClientHandler threads with no synchronization.
 */
public class CommandParser {

    private static final Logger log = LoggerFactory.getLogger(CommandParser.class);

    // ── Public API ──────────────────────────────────────────────────

    /**
     * Parse one line from the client into a Command.
     *
     * @param line raw line from the socket, may include \r\n, may be null
     * @return a Command ready for CommandExecutor; never null.
     *         Returns Command{UNKNOWN} with an error message for all
     *         invalid inputs — never throws.
     */
    public Command parse(String line) {

        // ── Step 1: null guard ──────────────────────────────────────
        // null means the client closed the connection.
        // ClientHandler checks for this case separately, but we guard
        // here too so the parser is safe regardless of caller.
        if (line == null) {
            log.debug("Received null line — client likely disconnected");
            return errorCommand("ERR connection closed");
        }

        // ── Step 2: strip CRLF and surrounding whitespace ───────────
        // BufferedReader.readLine() strips \n but leaves \r on Windows.
        // Telnet sends \r\n. Strip both to be safe.
        String stripped = line.stripTrailing()   // remove trailing whitespace + \r
                              .strip();           // remove any remaining leading whitespace
        // Note: String.strip() is Unicode-aware (Java 11+).
        // String.trim() only handles ASCII whitespace ≤ \u0020.
        // strip() is the correct modern choice.

        // ── Step 3: empty line guard ────────────────────────────────
        // Empty lines are common — telnet sends one on Enter.
        // Ignore silently rather than returning an error.
        if (stripped.isEmpty()) {
            log.trace("Empty line received — ignoring");
            return emptyCommand();
        }

        // ── Step 4: tokenize ────────────────────────────────────────
        // split("\\s+") on an already-stripped string is safe.
        // "SET  foo  bar" → ["SET", "foo", "bar"]
        String[] tokens = stripped.split("\\s+");

        // ── Step 5: resolve command type ────────────────────────────
        String      rawName = tokens[0];
        CommandType type    = CommandType.fromString(rawName);

        if (type == CommandType.UNKNOWN) {
            log.debug("Unknown command: '{}'", rawName);
            return errorCommand(
                "ERR unknown command '" + rawName.toLowerCase() + "', " +
                "with args beginning with: " + formatArgs(tokens, 1));
        }

        // ── Step 6: extract args (everything after the command name) ─
        String[] args = tokens.length > 1
                ? Arrays.copyOfRange(tokens, 1, tokens.length)
                : new String[0];

        // ── Step 7: validate argument count ─────────────────────────
        String arityError = validateArity(type, args.length);
        if (arityError != null) {
            log.debug("Arity error for {}: expected {}-{} args, got {}",
                      type, type.minArgs, type.maxArgs, args.length);
            return errorCommand(arityError);
        }

        // ── Step 8: return valid command ─────────────────────────────
        log.debug("Parsed: {} {}", type, Arrays.toString(args));
        return new Command(type, args);
    }

    // ── Private helpers ─────────────────────────────────────────────

    /**
     * Validate argument count against the CommandType's declared arity.
     *
     * @return null if valid, error message string if invalid
     */
    private String validateArity(CommandType type, int argCount) {
        if (argCount < type.minArgs) {
            return "ERR wrong number of arguments for '"
                   + type.name().toLowerCase() + "' command";
        }
        if (type.maxArgs != -1 && argCount > type.maxArgs) {
            return "ERR wrong number of arguments for '"
                   + type.name().toLowerCase() + "' command";
        }
        return null;  // valid
    }

    /**
     * Build a Command that carries an error to be returned to the client.
     * CommandExecutor checks for UNKNOWN type and returns the error message
     * directly without touching KVStore.
     */
    private Command errorCommand(String errorMessage) {
        return new Command(CommandType.UNKNOWN, new String[]{ errorMessage });
    }

    /**
     * Sentinel for empty/whitespace-only lines.
     * CommandExecutor will see UNKNOWN with no args and produce no response.
     * ClientHandler just reads the next line.
     */
    private Command emptyCommand() {
        return new Command(CommandType.UNKNOWN, new String[0]);
    }

    /**
     * Format args[from..] for inclusion in an error message.
     * Matches Redis's error format: shows first few args with quotes.
     * "with args beginning with: 'foo' 'bar'"
     */
    private String formatArgs(String[] tokens, int from) {
        if (from >= tokens.length) return "(no args)";
        StringBuilder sb = new StringBuilder();
        int limit = Math.min(tokens.length, from + 3); // show up to 3 args
        for (int i = from; i < limit; i++) {
            if (sb.length() > 0) sb.append(" ");
            sb.append("'").append(tokens[i]).append("'");
        }
        if (tokens.length - from > 3) sb.append(" ...");
        return sb.toString();
    }
}