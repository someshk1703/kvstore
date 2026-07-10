package com.somesh.kvstore.engine;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.somesh.kvstore.protocol.Command;
import com.somesh.kvstore.protocol.CommandType;

/**
 * Routes parsed Command objects to the appropriate KVStore operation
 * and returns a CommandResult the protocol layer can serialize.
 *
 * Responsibilities:
 *   - Argument count validation
 *   - Argument type coercion (String → long for TTL)
 *   - Dispatching to KVStore
 *   - Returning typed CommandResult
 *
 * Not responsible for:
 *   - Parsing raw bytes (CommandParser does that)
 *   - Serializing responses (ResponseSerializer does that)
 *   - Storage mechanics (KVStore does that)
 */
public class CommandExecutor {

    private static final Logger log = LoggerFactory.getLogger(CommandExecutor.class);

    private final KVStore kvStore;

    // Dispatch table: CommandType → handler method
    private final Map<CommandType, Function<Command, CommandResult>> handlers;

    public CommandExecutor(KVStore kvStore) {
        this.kvStore = kvStore;
        this.handlers = buildHandlerMap();
    }

    // ── Public API ──────────────────────────────────────────────────

    /**
     * Execute a parsed command and return its result.
     * Never throws — all errors are returned as CommandResult.error().
     */
    public CommandResult execute(Command cmd) {
        Function<Command, CommandResult> handler = handlers.get(cmd.type());

        if (handler == null) {
            log.warn("Unknown command: {}", cmd.type());
            return CommandResult.error(
                "ERR unknown command '" + cmd.type().name().toLowerCase() + "'");
        }

        try {
            return handler.apply(cmd);
        } catch (Exception e) {
            // Defensive: handler bugs should not crash ClientHandler thread
            log.error("Unexpected error executing command {}: {}", cmd.type(), e.getMessage(), e);
            return CommandResult.error("ERR internal server error");
        }
    }

    // ── Handler map ─────────────────────────────────────────────────

    private Map<CommandType, Function<Command, CommandResult>> buildHandlerMap() {
        Map<CommandType, Function<Command, CommandResult>> map = new HashMap<>();
        map.put(CommandType.SET,    this::handleSet);
        map.put(CommandType.GET,    this::handleGet);
        map.put(CommandType.DEL,    this::handleDel);
        map.put(CommandType.EXISTS, this::handleExists);
        map.put(CommandType.PTTL,   this::handlePttl);
        map.put(CommandType.PING,   this::handlePing);
        // Week 2 — TTL management
        map.put(CommandType.EXPIRE,  this::handleExpire);
        map.put(CommandType.TTL,     this::handleTtl);
        map.put(CommandType.PERSIST, this::handlePersist);
        return Map.copyOf(map);   // immutable after construction
    }

    // ── Handlers ────────────────────────────────────────────────────

    /**
     * SET key value [EX seconds | PX milliseconds]
     *
     * Supported forms:
     *   SET key value          → no expiry
     *   SET key value EX 10    → expire in 10 seconds
     *   SET key value PX 5000  → expire in 5000 milliseconds
     */
    private CommandResult handleSet(Command cmd) {
        // Minimum: SET key value = 2 args after command name
        if (cmd.argCount() < 2) {
            return CommandResult.error(
                "ERR wrong number of arguments for 'set' command");
        }

        String key   = cmd.arg(0);
        String value = cmd.arg(1);
        long   ttlMs = -1;  // default: no expiry

        // Parse optional EX / PX modifier
        if (cmd.argCount() >= 4) {
            String modifier = cmd.arg(2).toUpperCase();
            String rawTtl   = cmd.arg(3);

            long parsed = parseLong(rawTtl);
            if (parsed < 0) {
                return CommandResult.error(
                    "ERR value is not an integer or out of range");
            }
            if (parsed == 0) {
                return CommandResult.error(
                    "ERR invalid expire time in 'set' command");
            }

            ttlMs = switch (modifier) {
                case "EX" -> parsed * 1000L;   // seconds → ms
                case "PX" -> parsed;            // already ms
                default   -> Long.MIN_VALUE;    // sentinel: unknown modifier
            };

            if (ttlMs == Long.MIN_VALUE) {
                return CommandResult.error(
                    "ERR syntax error — unknown option '" + cmd.arg(2) + "'");
            }
        }

        kvStore.set(key, value, ttlMs);
        return CommandResult.ok();
    }

    /**
     * GET key
     * Returns the value as a bulk string, or null bulk ($-1) if not found.
     */
    private CommandResult handleGet(Command cmd) {
        if (cmd.argCount() != 1) {
            return CommandResult.error(
                "ERR wrong number of arguments for 'get' command");
        }
        String value = kvStore.get(cmd.arg(0));
        return CommandResult.string(value);  // null → $-1 in ResponseSerializer
    }

    /**
     * DEL key [key ...]
     * Returns count of deleted keys as integer.
     */
    private CommandResult handleDel(Command cmd) {
        if (cmd.argCount() < 1) {
            return CommandResult.error(
                "ERR wrong number of arguments for 'del' command");
        }
        // Collect all key args into an array
        String[] keys = cmd.args();
        int deleted = kvStore.del(keys);
        return CommandResult.integer(deleted);
    }

    /**
     * EXISTS key [key ...]
     * Returns count of keys that exist (key listed twice counts twice).
     */
    private CommandResult handleExists(Command cmd) {
        if (cmd.argCount() < 1) {
            return CommandResult.error(
                "ERR wrong number of arguments for 'exists' command");
        }
        int count = 0;
        for (String key : cmd.args()) {
            if (kvStore.exists(key)) count++;
        }
        return CommandResult.integer(count);
    }

    /**
     * PTTL key
     * Returns remaining TTL in milliseconds.
     * -1 = no expiry, -2 = key not found or expired.
     */
    private CommandResult handlePttl(Command cmd) {
        if (cmd.argCount() != 1) {
            return CommandResult.error(
                "ERR wrong number of arguments for 'pttl' command");
        }
        return CommandResult.integer(kvStore.pttl(cmd.arg(0)));
    }

    /**
     * PING [message]
     * Returns PONG or the message echoed back. Used by health checks.
     */
    private CommandResult handlePing(Command cmd) {
        if (cmd.argCount() == 0) return CommandResult.string("PONG");
        return CommandResult.string(cmd.arg(0));
    }

    /**
     * EXPIRE key seconds
     * Sets a TTL on an existing key. Returns 1 if set, 0 if key not found.
     */
    private CommandResult handleExpire(Command cmd) {
        if (cmd.argCount() != 2) {
            return CommandResult.error(
                "ERR wrong number of arguments for 'expire' command");
        }
        long seconds = parseLong(cmd.arg(1));
        if (seconds < 0) {
            return CommandResult.error(
                "ERR value is not an integer or out of range");
        }
        if (seconds == 0) {
            return CommandResult.error(
                "ERR invalid expire time in 'expire' command");
        }
        return CommandResult.integer(kvStore.expire(cmd.arg(0), seconds * 1000L));
    }

    /**
     * TTL key
     * Returns remaining TTL in whole seconds.
     * -1 = no expiry, -2 = key not found or expired.
     */
    private CommandResult handleTtl(Command cmd) {
        if (cmd.argCount() != 1) {
            return CommandResult.error(
                "ERR wrong number of arguments for 'ttl' command");
        }
        return CommandResult.integer(kvStore.ttl(cmd.arg(0)));
    }

    /**
     * PERSIST key
     * Removes the TTL from a key, making it persistent.
     * Returns 1 if the TTL was removed, 0 if the key has no TTL or doesn't exist.
     */
    private CommandResult handlePersist(Command cmd) {
        if (cmd.argCount() != 1) {
            return CommandResult.error(
                "ERR wrong number of arguments for 'persist' command");
        }
        return CommandResult.integer(kvStore.persist(cmd.arg(0)));
    }

    // ── Helpers ─────────────────────────────────────────────────────

    /**
     * Parse a long from string, returning -1 on failure rather than throwing.
     * Keeps handler code clean — no try-catch at the call site.
     */
    private long parseLong(String s) {
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            return -1;
        }
    }
}