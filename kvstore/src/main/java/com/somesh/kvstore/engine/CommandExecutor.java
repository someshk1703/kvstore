package com.somesh.kvstore.engine;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.somesh.kvstore.config.ServerConfig;
import com.somesh.kvstore.persistence.AOFWriter;
import com.somesh.kvstore.persistence.SnapshotManager;
import com.somesh.kvstore.protocol.Command;
import com.somesh.kvstore.protocol.CommandType;
import com.somesh.kvstore.replication.ReplicationManager;

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
 *
 * <h2>Week 3 additions — persistence wiring</h2>
 * {@link AOFWriter} and {@link SnapshotManager} are optional collaborators set via
 * setters. When {@code aofWriter} is non-null, every successful write command is
 * appended to the AOF after the in-memory store is updated. When {@code null}
 * (replay mode), commands execute silently — preventing re-logging of replayed
 * commands.
 *
 * <h2>Week 4 additions — replication wiring</h2>
 * {@link ReplicationManager} is an optional collaborator. When set, every
 * successful write command is propagated to replicas after the local store
 * is updated. In replica mode ({@link #setReplicaMode(boolean)}), write
 * commands from external clients are rejected with an error.
 */
public class CommandExecutor {

    private static final Logger log = LoggerFactory.getLogger(CommandExecutor.class);

    /** Command types that mutate state and must be logged to the AOF. */
    private static final Set<CommandType> WRITE_COMMANDS =
        Set.of(CommandType.SET, CommandType.DEL, CommandType.EXPIRE, CommandType.PERSIST);

    private final KVStore kvStore;

    // Persistence collaborators — null in replay mode
    private volatile AOFWriter          aofWriter;
    private volatile SnapshotManager    snapshotManager;

    // Replication collaborator — null when not primary
    private volatile ReplicationManager replicationManager;

    // When true, reject write commands from external clients (we are a replica)
    private volatile boolean            replicaMode = false;

    // Dispatch table: CommandType → handler method
    private final Map<CommandType, Function<Command, CommandResult>> handlers;

    public CommandExecutor(KVStore kvStore) {
        this.kvStore  = kvStore;
        this.handlers = buildHandlerMap();
    }

    // ── Persistence wiring ──────────────────────────────────────────

    /**
     * Attach an {@link AOFWriter}. Set to {@code null} to enter replay mode
     * (commands execute without being logged back to the AOF).
     */
    public void setAofWriter(AOFWriter aofWriter) {
        this.aofWriter = aofWriter;
    }

    /**
     * Attach a {@link SnapshotManager} to track write counts and trigger
     * write-threshold-based snapshots. Set to {@code null} to disable.
     */
    public void setSnapshotManager(SnapshotManager snapshotManager) {
        this.snapshotManager = snapshotManager;
    }

    // ── Replication wiring ──────────────────────────────────────────

    /**
     * Attach a {@link ReplicationManager}. When set, every successful write
     * command is propagated to connected replicas after the local store update.
     * Set to {@code null} to disable (standalone mode, or replica mode).
     */
    public void setReplicationManager(ReplicationManager replicationManager) {
        this.replicationManager = replicationManager;
    }

    /**
     * Enable or disable replica mode.
     * In replica mode, write commands from external clients are rejected.
     * Replication-applied commands (via {@link #execute}) still succeed because
     * they arrive via the replication path, not client connections.
     *
     * @param replicaMode {@code true} to enter replica (read-only) mode
     */
    public void setReplicaMode(boolean replicaMode) {
        this.replicaMode = replicaMode;
    }

    // ── Public API ──────────────────────────────────────────────────

    /**
     * Execute a parsed command and return its result.
     * Never throws — all errors are returned as CommandResult.error().
     */
    public CommandResult execute(Command cmd) {
        // Replica mode: reject write commands from external clients
        if (replicaMode && WRITE_COMMANDS.contains(cmd.type())) {
            return CommandResult.error(
                "READONLY You can't write against a read only replica.");
        }

        Function<Command, CommandResult> handler = handlers.get(cmd.type());

        if (handler == null) {
            log.warn("Unknown command: {}", cmd.type());
            return CommandResult.error(
                "ERR unknown command '" + cmd.type().name().toLowerCase() + "'");
        }

        try {
            CommandResult result = handler.apply(cmd);
            // Log successful write commands to AOF (only in normal mode, not replay)
            if (result.type != CommandResult.Type.ERROR
                    && WRITE_COMMANDS.contains(cmd.type())) {
                logToAof(cmd);
                notifySnapshot();
                propagateToReplicas(cmd);
            }
            return result;
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
        // Week 4 — Replication
        map.put(CommandType.WAIT,     this::handleWait);
        map.put(CommandType.REPLCONF, this::handleReplconf);
        map.put(CommandType.REPLINFO, this::handleReplinfo);
        // Week 5 — KEYS + INFO
        map.put(CommandType.KEYS, this::handleKeys);
        map.put(CommandType.INFO, this::handleInfo);
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

    /**
     * Append the command to the AOF if an {@link AOFWriter} is wired in.
     *
     * <p>{@link Command#toString()} produces {@code "SET foo bar EX 30"} — exactly
     * the inline text format the AOF uses.
     */
    private void logToAof(Command cmd) {
        AOFWriter w = aofWriter;   // local copy for thread safety
        if (w == null) return;
        try {
            w.log(cmd.toString());
        } catch (IOException e) {
            // AOF write failure is serious but should not kill the client thread.
            // Log and continue — the key is in memory; durability is degraded.
            log.error("AOF write failed for command '{}': {}", cmd, e.getMessage(), e);
        }
    }

    /** Notify the snapshot manager that a write has occurred. */
    private void notifySnapshot() {
        SnapshotManager sm = snapshotManager;
        if (sm != null) sm.onWrite();
    }

    /**
     * Propagate a write command to replicas via the {@link ReplicationManager}.
     *
     * <p>Called after the local store is updated and AOF is written.
     * {@code cmd.toString()} produces the inline text format (e.g. "SET foo bar EX 30").
     */
    private void propagateToReplicas(Command cmd) {
        ReplicationManager rm = replicationManager;
        if (rm != null) {
            rm.propagate(cmd.toString());
        }
    }

    // ── Week 4 handlers — Replication ───────────────────────────────

    /**
     * WAIT numreplicas timeoutMs
     *
     * Blocks until at least {@code numreplicas} replicas have acknowledged the
     * current write offset, or {@code timeoutMs} elapses.
     * Returns the count of replicas that have acked.
     *
     * <p>If no {@link ReplicationManager} is wired, returns 0 immediately
     * (standalone mode — no replicas exist).
     *
     * <p>Example: {@code WAIT 1 100} → ":1" if one replica acks within 100ms.
     */
    private CommandResult handleWait(Command cmd) {
        if (cmd.argCount() != 2) {
            return CommandResult.error("ERR wrong number of arguments for 'wait' command");
        }
        long numReplicas = parseLong(cmd.arg(0));
        long timeoutMs   = parseLong(cmd.arg(1));
        if (numReplicas < 0 || timeoutMs < 0) {
            return CommandResult.error("ERR value is not an integer or out of range");
        }

        ReplicationManager rm = replicationManager;
        if (rm == null) return CommandResult.integer(0);

        int acked = rm.waitForReplicas((int) numReplicas, timeoutMs);
        return CommandResult.integer(acked);
    }

    /**
     * REPLCONF &lt;arg&gt; [arg ...]
     *
     * Used internally during the replication handshake.
     * On a primary, this command is handled at the server level (TcpServer
     * inspects the raw line before routing to CommandExecutor). Here we return
     * OK so that the parser does not reject it as UNKNOWN.
     */
    private CommandResult handleReplconf(Command cmd) {
        return CommandResult.ok();
    }

    /**
     * REPLINFO
     *
     * Returns a human-readable summary of the replication state:
     * role, master offset, connected replica count.
     */
    private CommandResult handleReplinfo(Command cmd) {
        ReplicationManager rm = replicationManager;
        if (rm == null) {
            return CommandResult.string("role:standalone\r\nmaster_offset:-1\r\nconnected_replicas:0");
        }
        String info = "role:primary\r\n" +
                      "master_offset:" + rm.getMasterOffset() + "\r\n" +
                      "connected_replicas:" + rm.getReplicaCount();
        return CommandResult.string(info);
    }

    // ── Week 5 handlers — KEYS / INFO ───────────────────────────────

    /**
     * KEYS pattern
     *
     * Returns all keys matching the glob pattern. Not recommended in production
     * (full scan), but useful for debugging and the HTTP API.
     *
     * Example: KEYS user:* → *2\r\n$6\r\nuser:1\r\n$6\r\nuser:2\r\n
     */
    private CommandResult handleKeys(Command cmd) {
        if (cmd.argCount() != 1) {
            return CommandResult.error("ERR wrong number of arguments for 'keys' command");
        }
        java.util.List<String> keys = new java.util.ArrayList<>(kvStore.keys(cmd.arg(0)));
        java.util.Collections.sort(keys);   // deterministic order for tests
        return CommandResult.array(keys);
    }

    /**
     * INFO
     *
     * Returns a human-readable summary of server metadata.
     * Mirrors Redis INFO's format — newline-separated key:value pairs.
     */
    private CommandResult handleInfo(Command cmd) {
        Runtime rt = Runtime.getRuntime();
        long usedBytes  = rt.totalMemory() - rt.freeMemory();
        int  totalKeys  = kvStore.size();
        ReplicationManager rm = replicationManager;
        String role = rm != null ? "primary" : (replicaMode ? "replica" : "standalone");

        String info = "# Server\r\n" +
                      "version:0.1.0\r\n" +
                      "mode:" + role + "\r\n" +
                      "tcp_port:" + ServerConfig.SERVER_PORT + "\r\n" +
                      "http_port:8080\r\n" +
                      "\r\n" +
                      "# Keyspace\r\n" +
                      "total_keys:" + totalKeys + "\r\n" +
                      "\r\n" +
                      "# Memory\r\n" +
                      "used_memory_bytes:" + usedBytes + "\r\n" +
                      "max_memory_bytes:" + ServerConfig.MAX_MEMORY_BYTES;
        return CommandResult.string(info);
    }
}