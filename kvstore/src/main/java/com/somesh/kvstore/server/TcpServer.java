package com.somesh.kvstore.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.somesh.kvstore.config.ServerConfig;
import com.somesh.kvstore.engine.CommandExecutor;
import com.somesh.kvstore.engine.KVStore;
import com.somesh.kvstore.protocol.CommandParser;
import com.somesh.kvstore.protocol.ResponseSerializer;
import com.somesh.kvstore.replication.ReplicaClient;
import com.somesh.kvstore.replication.ReplicationManager;

/**
 * TCP server — listens on a port, accepts client connections, delegates each
 * to a ClientHandler on a thread pool.
 *
 * Lifecycle:
 *   start()  → binds port, enters accept loop (blocks calling thread)
 *   stop()   → closes ServerSocket, shuts down pool, waits for in-flight requests
 *
 * Thread model:
 *   - One thread runs the accept loop (the thread that calls start())
 *   - N threads in the pool handle client connections concurrently
 *   - One shared KVStore instance — thread-safe via ConcurrentHashMap
 *
 * The accept loop is intentionally minimal: accept → submit → repeat.
 * No processing happens on the accept thread.
 */
public class TcpServer {

    private static final Logger log = LoggerFactory.getLogger(TcpServer.class);

    // ── Core components — shared, constructed once ──────────────────

    private final KVStore           kvStore;
    private final CommandExecutor   executor;
    private final CommandParser     parser;
    private final ResponseSerializer serializer;

    // ── Server state ────────────────────────────────────────────────

    private final int               port;
    private ServerSocket            serverSocket;
    private ExecutorService         threadPool;

    // AtomicBoolean so stop() can signal the accept loop from another thread
    // without synchronization overhead on the hot path.
    private final AtomicBoolean     running = new AtomicBoolean(false);

    // ── Replication state (Week 4) ───────────────────────────────────

    /** Primary-mode: manages the backlog and propagation to replicas. */
    private ReplicationManager      replicationManager;

    /** Replica-mode: client that connects to the primary and applies its stream. */
    private ReplicaClient           replicaClient;

    /** Replica listener: listens for replica connections on the replication port. */
    private Thread                  replicaListenerThread;

    // ── Constructor ─────────────────────────────────────────────────

    public TcpServer(int port, KVStore store) {
        this.port       = port;
        this.kvStore    = store;
        this.executor   = new CommandExecutor(kvStore);
        this.parser     = new CommandParser();
        this.serializer = new ResponseSerializer();
    }

    public TcpServer(int port) {
        this(port, new KVStore());
    }

    // Convenience: use the configured default port
    public TcpServer() {
        this(ServerConfig.SERVER_PORT);
    }

    // ── Lifecycle ───────────────────────────────────────────────────

    /**
     * Bind the port and enter the accept loop.
     *
     * This method BLOCKS until stop() is called from another thread
     * (e.g., a shutdown hook). Typically called from main().
     */
    public void start() throws IOException {
        // Build a bounded thread pool with explicit rejection handling.
        // In Week 1 we use a simpler fixed pool — see comment below on
        // why we'll upgrade this in Week 3.
        this.threadPool = buildThreadPool();

        // SO_REUSEADDR is set by default in Java's ServerSocket —
        // allows rebinding the port immediately after a restart
        // without waiting for TIME_WAIT to expire (important during dev).
        this.serverSocket = new ServerSocket(
            port,
            ServerConfig.SERVER_BACKLOG
        );

        running.set(true);
        log.info("KVStore server started on port {}", port);
        log.info("Thread pool size: {}", ServerConfig.THREAD_POOL_SIZE);

        // Register a JVM shutdown hook so Ctrl+C triggers clean shutdown.
        // This runs in a separate thread when the JVM begins termination.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown hook triggered — stopping server");
            stop();
        }, "shutdown-hook"));

        acceptLoop();
    }

    /**
     * The accept loop — the single most important method in this class.
     *
     * Contract: do as little work as possible here. Accept the connection,
     * create a handler, submit to pool, repeat. Any blocking work here
     * delays accepting the next connection.
     */
    private void acceptLoop() {
        while (running.get()) {
            try {
                Socket clientSocket = serverSocket.accept();     // blocks until connection

                // Configure socket-level timeouts.
                // SO_TIMEOUT: if the client sends nothing for 5 minutes,
                // close the connection. Prevents idle sockets consuming
                // thread pool slots forever.
                clientSocket.setSoTimeout(300_000);  // 5 minutes in ms

                // Disable Nagle's algorithm — we want responses sent immediately,
                // not buffered waiting for more data. A KV store sends small
                // responses; Nagle adds latency for no benefit here.
                clientSocket.setTcpNoDelay(true);

                ClientHandler handler = new ClientHandler(
                    clientSocket, kvStore, executor, parser, serializer
                );

                threadPool.submit(handler);
                log.debug("Accepted connection from {}",
                          clientSocket.getRemoteSocketAddress());

            } catch (IOException e) {
                // When stop() closes serverSocket, accept() throws an IOException.
                // Distinguish intentional shutdown from real errors.
                if (!running.get()) {
                    log.info("Accept loop exiting — server is stopping");
                } else {
                    log.error("Unexpected error in accept loop: {}", e.getMessage(), e);
                }
                // Either way, exit the loop
                break;
            }
        }
    }

    /**
     * Graceful shutdown — three phases:
     *
     * Phase 1: Stop accepting new connections (close ServerSocket).
     *          accept() unblocks and throws IOException, exiting the loop.
     *
     * Phase 2: Tell the thread pool to finish in-flight tasks and reject new ones.
     *          shutdown() is non-blocking — it signals intent, doesn't wait.
     *
     * Phase 3: Wait up to 30s for in-flight tasks to complete.
     *          If they don't finish, force-terminate with shutdownNow().
     *          shutdownNow() interrupts running threads — handlers must
     *          check Thread.interrupted() or catch InterruptedException.
     */
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;  // already stopped — idempotent
        }

        log.info("Stopping server...");

        // Phase 1: close ServerSocket → unblocks accept()
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException e) {
            log.warn("Error closing server socket: {}", e.getMessage());
        }

        // Phase 2: signal pool shutdown
        if (threadPool != null) {
            threadPool.shutdown();

            // Phase 3: wait for in-flight requests
            try {
                boolean terminated = threadPool.awaitTermination(30, TimeUnit.SECONDS);
                if (!terminated) {
                    log.warn("Thread pool did not terminate in 30s — forcing shutdown");
                    threadPool.shutdownNow();  // interrupt remaining threads

                    // Give interrupted threads a moment to clean up
                    boolean forcedTerminated = threadPool.awaitTermination(5, TimeUnit.SECONDS);
                    if (!forcedTerminated) {
                        log.error("Thread pool did not respond to forced shutdown");
                    }
                }
            } catch (InterruptedException e) {
                threadPool.shutdownNow();
                Thread.currentThread().interrupt();  // restore interrupt flag
            }
        }

        // Week 3: aofWriter.flush(); aofWriter.close();
        // Week 4: clean up replication resources
        if (replicationManager != null) {
            replicationManager.close();
        }
        if (replicaClient != null) {
            replicaClient.close();
        }
        if (replicaListenerThread != null) {
            replicaListenerThread.interrupt();
        }

        log.info("Server stopped cleanly");
    }

    // ── Thread pool construction ─────────────────────────────────────

    /**
     * Build the client handler thread pool.
     *
     * Week 1: Fixed thread pool — simple, predictable.
     * Week 3 upgrade: Switch to bounded queue + CallerRunsPolicy for
     *   backpressure. When pool is saturated, the accept loop slows
     *   down naturally rather than queueing work unboundedly.
     *
     *   new ThreadPoolExecutor(
     *       ServerConfig.THREAD_POOL_SIZE,      // core
     *       ServerConfig.THREAD_POOL_SIZE,      // max (same = fixed)
     *       60L, TimeUnit.SECONDS,
     *       new LinkedBlockingQueue<>(500),     // bounded queue
     *       namedThreadFactory(),
     *       new ThreadPoolExecutor.CallerRunsPolicy()
     *   );
     *
     * Java 21 upgrade: newVirtualThreadPerTaskExecutor() — one virtual
     *   thread per client, JVM-scheduled, ~1KB stack. Handles C10K cleanly.
     *   Drop-in replacement — no other code changes needed.
     */
    private ExecutorService buildThreadPool() {
        return Executors.newFixedThreadPool(
            ServerConfig.THREAD_POOL_SIZE,
            namedThreadFactory()
        );
    }

    /**
     * Thread factory that gives threads meaningful names.
     * Default thread names are "pool-1-thread-1" — useless in stack traces.
     * "kvstore-worker-3" tells you immediately which subsystem is involved.
     */
    private ThreadFactory namedThreadFactory() {
        return new ThreadFactory() {
            private final java.util.concurrent.atomic.AtomicInteger count =
                new java.util.concurrent.atomic.AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "kvstore-worker-" + count.incrementAndGet());
                // Daemon threads don't prevent JVM shutdown.
                // Worker threads should be non-daemon so in-flight requests
                // complete before the JVM exits. The shutdown hook handles
                // the wait explicitly.
                t.setDaemon(false);
                return t;
            }
        };
    }

    // ── Accessors for testing ────────────────────────────────────────

    public boolean isRunning()         { return running.get(); }
    public int     getPort()           { return port; }
    public KVStore getKvStore()        { return kvStore; }
    public CommandExecutor getExecutor() { return executor; }

    // ── Week 4: Replication API ──────────────────────────────────────

    /**
     * Enable primary-side replication.
     *
     * <p>Sets up a {@link ReplicationManager} and starts a listener on
     * {@link ServerConfig#REPLICATION_PORT} that accepts replica connections.
     * Call before {@link #start()} so the replication port is open when
     * the server begins accepting client connections.
     *
     * <p>Requires a {@link com.somesh.kvstore.persistence.SnapshotManager}
     * for the full-resync path; pass {@code null} to skip full-resync support.
     *
     * @param snapshotManager snapshot manager for full-resync; may be null
     */
    public void enableReplication(com.somesh.kvstore.persistence.SnapshotManager snapshotManager) {
        this.replicationManager = new ReplicationManager(kvStore, snapshotManager);
        executor.setReplicationManager(replicationManager);
        log.info("Replication enabled on port {}", ServerConfig.REPLICATION_PORT);

        // Start a listener thread that accepts replica connections
        replicaListenerThread = new Thread(() -> acceptReplicaConnections(), "repl-listener");
        replicaListenerThread.setDaemon(true);
        replicaListenerThread.start();
    }

    /**
     * Configure this server to run as a replica of the given primary.
     *
     * <p>Starts a {@link ReplicaClient} that connects to the primary, performs
     * the handshake, and continuously applies the replication stream.
     * The local store enters read-only mode: write commands from external clients
     * are rejected with {@code READONLY}.
     *
     * <p>Call before {@link #start()}.
     *
     * @param primaryHost primary's hostname or IP
     * @param primaryPort primary's replication port (usually 6380)
     */
    public void startAsReplica(String primaryHost, int primaryPort) {
        executor.setReplicaMode(true);
        this.replicaClient = new ReplicaClient(primaryHost, primaryPort, executor);
        Thread replicaThread = new Thread(replicaClient, "repl-client");
        replicaThread.setDaemon(true);
        replicaThread.start();
        log.info("Started in replica mode — primary={}:{}", primaryHost, primaryPort);
    }

    /**
     * Background thread body: listens for replica connections on the replication port.
     */
    private void acceptReplicaConnections() {
        try (ServerSocket replicationServerSocket =
                new ServerSocket(ServerConfig.REPLICATION_PORT)) {
            log.info("Replica listener started on port {}", ServerConfig.REPLICATION_PORT);
            while (!Thread.currentThread().isInterrupted()) {
                Socket replicaSocket = replicationServerSocket.accept();
                // Read the REPLCONF <lastOffset> line from the replica
                try {
                    java.io.BufferedReader replicaIn = new java.io.BufferedReader(
                        new java.io.InputStreamReader(
                            replicaSocket.getInputStream(),
                            java.nio.charset.StandardCharsets.UTF_8));
                    java.io.PrintWriter replicaOut = new java.io.PrintWriter(
                        new java.io.OutputStreamWriter(
                            replicaSocket.getOutputStream(),
                            java.nio.charset.StandardCharsets.UTF_8), false);

                    String handshake = replicaIn.readLine();
                    long fromOffset = -1;
                    if (handshake != null && handshake.startsWith("REPLCONF ")) {
                        try {
                            fromOffset = Long.parseLong(handshake.substring(9).trim());
                        } catch (NumberFormatException e) {
                            fromOffset = -1;
                        }
                    }
                    replicaOut.println("+OK");
                    replicaOut.flush();
                    // Hand off to ReplicationManager
                    if (replicationManager != null) {
                        replicationManager.registerReplica(replicaSocket, fromOffset);
                    }
                } catch (IOException e) {
                    log.warn("Error during replica handshake: {}", e.getMessage());
                }
            }
        } catch (IOException e) {
            if (!Thread.currentThread().isInterrupted()) {
                log.error("Replica listener error: {}", e.getMessage());
            }
        }
    }

    // ── Entry point ─────────────────────────────────────────────────

    public static void main(String[] args) throws IOException {
        TcpServer server = new TcpServer();
        server.start();   // blocks here until shutdown hook fires
    }
}