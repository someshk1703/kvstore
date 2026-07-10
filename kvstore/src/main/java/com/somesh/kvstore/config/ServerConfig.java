package com.somesh.kvstore.config;

/**
 * Single source of truth for every tuneable constant in the server.
 *
 * <p>Design decisions worth knowing for interviews:
 *
 * <ul>
 *   <li><b>No Spring @Value / @ConfigurationProperties</b> — this is intentional.
 *       You're building everything from scratch. A plain constants file is honest
 *       about what's happening.</li>
 *   <li><b>All fields are static final</b> — these are compile-time constants.
 *       The JVM inlines them at call sites, so there is zero runtime overhead
 *       vs. magic numbers scattered through the code.</li>
 *   <li><b>Private constructor</b> — prevents instantiation. This class is a
 *       namespace for constants, not a bean or a singleton. Same pattern as
 *       java.lang.Math.</li>
 *   <li><b>Everything in one place</b> — when an interviewer asks "how would
 *       you tune this for production?", you can point here and say "change this
 *       one file, redeploy". No hunting across 10 classes.</li>
 * </ul>
 *
 * <p>When you add persistence (Week 2) and replication (Week 4), add their
 * constants here too. Do not create separate Constants files per package.
 */
public final class ServerConfig {

    // ──────────────────────────────────────────────────────────────
    // Prevent instantiation — this is a utility class
    // ──────────────────────────────────────────────────────────────
    private ServerConfig() {
        throw new UnsupportedOperationException("ServerConfig is a constants class");
    }


    // ──────────────────────────────────────────────────────────────
    // TCP Server
    // ──────────────────────────────────────────────────────────────

    /**
     * Port the server listens on.
     *
     * <p>6379 is Redis's default. Using the same port means any Redis client
     * (redis-cli, Jedis, Lettuce) can point at your server without config changes.
     * You'll need to stop a real Redis instance first if one is running locally.
     */
    public static final int SERVER_PORT = 6379;

    /**
     * Number of threads in the client handler pool.
     *
     * <p><b>Why 10?</b> Rule of thumb for IO-bound workloads:
     * {@code threads ≈ 2 × CPU cores}. Most dev machines have 4–8 cores,
     * so 10 is a reasonable starting ceiling for local testing.
     *
     * <p>This is a {@link java.util.concurrent.Executors#newFixedThreadPool(int)}
     * pool. Each connected client gets its own thread from this pool. When the
     * pool is exhausted, new connections queue until a thread is free.
     *
     * <p>Interview question: "What's wrong with a fixed thread pool for a
     * high-concurrency server?" Answer: thread-per-connection doesn't scale
     * past ~10k connections (the C10K problem). The real solution is NIO with
     * a selector loop (Netty's model). You'd mention this as a known limitation
     * and a future upgrade path.
     */
    public static final int THREAD_POOL_SIZE = 10;

    /**
     * SO_BACKLOG — how many pending connection requests the OS will queue
     * before refusing new ones.
     *
     * <p>Passed to {@link java.net.ServerSocket#ServerSocket(int, int)} as the
     * second argument. Default in Java when omitted is 50. Keep at 50 for now.
     */
    public static final int SERVER_BACKLOG = 50;


    // ──────────────────────────────────────────────────────────────
    // Memory Management  (Week 2)
    // ──────────────────────────────────────────────────────────────

    /**
     * Maximum heap the key-value store is allowed to consume before LRU
     * eviction kicks in, in bytes.
     *
     * <p>Default: 128 MB. Deliberately conservative for local dev.
     * On a production box you'd tune this to ~70% of available heap.
     *
     * <p>Read via: {@link Runtime#getRuntime()} → {@link Runtime#totalMemory()}
     * and {@link Runtime#freeMemory()} to compute used memory, then compare
     * against this threshold.
     */
    public static final long MAX_MEMORY_BYTES = 128L * 1024 * 1024; // 128 MB

    /**
     * How often the background expiry sweep runs, in milliseconds.
     *
     * <p>The sweep proactively deletes expired keys even if nobody reads them
     * (lazy expiry alone misses keys that are written and never read again).
     * 100ms is Redis's approximate active expiry frequency.
     *
     * <p>Too low → too much CPU. Too high → stale memory builds up.
     * 100ms is a proven sweet spot for small-to-medium keyspaces.
     */
    public static final long EXPIRY_SWEEP_INTERVAL_MS = 100L;

    /**
     * Maximum number of keys to scan per expiry sweep cycle.
     *
     * <p>Redis scans 20 random keys per 100ms cycle and repeats if more than
     * 25% were expired. We'll start simpler: scan up to MAX_KEYS_PER_SWEEP
     * and move on. Prevents the sweep thread from hogging CPU on a large keyspace.
     */
    public static final int MAX_KEYS_PER_SWEEP = 20;

    /**
     * Maximum number of entries tracked by the LRU cache.
     *
     * <p>When the store exceeds this many keys, the LRU eviction logic
     * removes the least-recently-used entries until the count is back
     * within bounds. Setting to a large value effectively disables
     * eviction (relying on MAX_MEMORY_BYTES alone).
     */
    public static final int LRU_CAPACITY = 100_000;

    /**
     * Once eviction starts (memory > MAX_MEMORY_BYTES), keys are evicted
     * until memory drops below this fraction of MAX_MEMORY_BYTES.
     *
     * <p>0.80 = 80%. Mirrors Redis's "maxmemory-samples" approach of
     * targeting a safe headroom below the hard limit to avoid thrashing.
     */
    public static final double EVICTION_TARGET_RATIO = 0.80;


    // ──────────────────────────────────────────────────────────────
    // Persistence (Week 2–3)
    // ──────────────────────────────────────────────────────────────

    /** Path to the Append-Only File. Relative to the working directory. */
    public static final String AOF_FILE_PATH = "data/kvstore.aof";

    /** Path to the RDB-style snapshot file. */
    public static final String SNAPSHOT_FILE_PATH = "data/kvstore.rdb";

    /**
     * How many write commands trigger an automatic snapshot.
     *
     * <p>Mirrors Redis's {@code save 900 1 / save 300 10 / save 60 10000}
     * concept. We simplify to a single write-count threshold.
     */
    public static final int SNAPSHOT_WRITE_THRESHOLD = 10_000;

    /**
     * How often the snapshot runs regardless of write count, in seconds.
     *
     * <p>60 seconds is a reasonable default. Lower → less data loss on crash,
     * higher → less I/O overhead. The tradeoff is RTO vs RPO — something
     * to talk through in a system design interview.
     */
    public static final long SNAPSHOT_INTERVAL_SECONDS = 60L;


    // ──────────────────────────────────────────────────────────────
    // Replication (Week 4)
    // ──────────────────────────────────────────────────────────────

    /**
     * Size of the replication backlog ring buffer, in number of commands.
     *
     * <p>When a replica reconnects after a short outage, the primary checks
     * if the missed commands are still in this buffer. If yes → partial resync
     * (cheap). If no → full snapshot transfer (expensive).
     *
     * <p>Redis default is 1 MB of bytes. We'll track command count instead
     * for simplicity. 1024 is a reasonable starting size.
     */
    public static final int REPLICATION_BACKLOG_SIZE = 1024;

    /**
     * Port replicas connect to for replication traffic.
     *
     * <p>Keeping replication on a separate port from client traffic (6379)
     * lets you apply different network policies (firewall, QoS) to each.
     */
    public static final int REPLICATION_PORT = 6380;


    // ──────────────────────────────────────────────────────────────
    // Protocol
    // ──────────────────────────────────────────────────────────────

    /**
     * Line terminator for the RESP-like protocol.
     *
     * <p>Real RESP uses {@code \r\n}. We use the same. Never hardcode
     * "\r\n" as a string literal in multiple places — always reference
     * this constant so a single change propagates everywhere.
     */
    public static final String CRLF = "\r\n";

    /**
     * Charset for encoding/decoding bytes to/from the socket.
     *
     * <p>UTF-8 for everything. Do not use the platform default charset
     * ({@link java.nio.charset.Charset#defaultCharset()}) — it varies
     * by OS and breaks cross-platform compatibility.
     */
    public static final String CHARSET = "UTF-8";

}
