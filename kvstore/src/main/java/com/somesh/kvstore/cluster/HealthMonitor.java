package com.somesh.kvstore.cluster;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodic heartbeat monitor for cluster nodes.
 *
 * <h2>Failure detection</h2>
 * Sends a PING to each monitored node every {@link #HEARTBEAT_INTERVAL_MS} ms.
 * A missed beat increments a counter. Once the counter reaches
 * {@link #MISSED_BEATS_THRESHOLD} the node is declared DOWN and the supplied
 * {@code onNodeDown} callback fires.
 *
 * <h2>Anti-flapping</h2>
 * A single missed beat does NOT trigger failover — transient network hiccups
 * and GC pauses can cause one-off PING timeouts that immediately resolve.
 * Requiring {@link #MISSED_BEATS_THRESHOLD} consecutive failures gives the
 * node {@code THRESHOLD × INTERVAL_MS} to recover before a potentially
 * disruptive failover begins.
 *
 * <h2>Thread model</h2>
 * One single-threaded {@link ScheduledExecutorService} runs the check loop.
 * All PING sockets are opened synchronously in that thread. For a large cluster
 * this would need parallelisation; for a demo cluster of 3–5 nodes sequential
 * pings within the 1-second window are fine.
 *
 * <h2>Interview angle</h2>
 * "What if the monitor itself is slow?" — if checking N nodes takes longer than
 * the interval, beats will queue up. A production monitor would use async I/O
 * (Netty) or run probes in parallel. For this project we keep it simple: the
 * scheduled executor will naturally start the next check when the previous one
 * finishes (fixed-rate semantics).
 */
public class HealthMonitor implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(HealthMonitor.class);

    /**
     * Number of consecutive missed heartbeats before a node is declared down.
     *
     * <p>3 missed beats at 1-second intervals = 3 seconds to declare failure.
     * Low enough to detect real failures quickly; high enough to survive a
     * momentary GC pause or network glitch.
     */
    public static final int MISSED_BEATS_THRESHOLD = 3;

    /**
     * Heartbeat interval in milliseconds.
     *
     * <p>1 second matches typical production heartbeat cadence (e.g. etcd default).
     */
    public static final long HEARTBEAT_INTERVAL_MS = 1_000L;

    /**
     * Socket connect/read timeout for PING probes.
     * Short enough to detect failures within the heartbeat window.
     */
    private static final int PING_TIMEOUT_MS = 500;

    private final Map<String, NodeInfo> nodeRegistry;
    private final Consumer<String> onNodeDown;
    private final Map<String, AtomicInteger> missedBeats = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler;

    /**
     * @param nodeRegistry  nodes to monitor (nodeId → NodeInfo)
     * @param onNodeDown    callback invoked once when a node crosses the failure threshold
     */
    public HealthMonitor(Map<String, NodeInfo> nodeRegistry, Consumer<String> onNodeDown) {
        this.nodeRegistry = nodeRegistry;
        this.onNodeDown = onNodeDown;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "health-monitor");
            t.setDaemon(true);
            return t;
        });
    }

    /** Start the heartbeat loop. */
    public void start() {
        scheduler.scheduleAtFixedRate(
                this::checkAll,
                HEARTBEAT_INTERVAL_MS,      // initial delay — let nodes finish starting up
                HEARTBEAT_INTERVAL_MS,
                TimeUnit.MILLISECONDS);
        log.info("HealthMonitor started: {} nodes, {}ms interval, threshold={}",
                nodeRegistry.size(), HEARTBEAT_INTERVAL_MS, MISSED_BEATS_THRESHOLD);
    }

    // ── Check loop ────────────────────────────────────────────────────────────

    private void checkAll() {
        for (Map.Entry<String, NodeInfo> entry : nodeRegistry.entrySet()) {
            String nodeId = entry.getKey();
            NodeInfo info = entry.getValue();
            boolean alive = ping(info);

            AtomicInteger counter = missedBeats.computeIfAbsent(nodeId, id -> new AtomicInteger(0));
            if (alive) {
                counter.set(0);
            } else {
                int missed = counter.incrementAndGet();
                log.warn("Node {} missed heartbeat ({}/{})", nodeId, missed, MISSED_BEATS_THRESHOLD);
                if (missed >= MISSED_BEATS_THRESHOLD) {
                    log.error("Node {} declared DOWN after {} consecutive missed beats", nodeId, missed);
                    onNodeDown.accept(nodeId);
                    // Remove from monitoring so the callback fires only once
                    nodeRegistry.remove(nodeId);
                }
            }
        }
    }

    /**
     * Send a PING and expect a response containing "PONG".
     * Opens a fresh TCP connection for each probe — avoids false positives from
     * a stale persistent connection.
     */
    private boolean ping(NodeInfo nodeInfo) {
        try (Socket socket = new Socket()) {
            socket.connect(
                    new java.net.InetSocketAddress(nodeInfo.host(), nodeInfo.port()),
                    PING_TIMEOUT_MS);
            socket.setSoTimeout(PING_TIMEOUT_MS);
            try (PrintWriter out = new PrintWriter(
                         new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true);
                 BufferedReader in = new BufferedReader(
                         new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {
                out.println("PING");
                String response = in.readLine();
                return response != null && response.contains("PONG");
            }
        } catch (IOException e) {
            log.debug("PING to {} failed: {}", nodeInfo, e.getMessage());
            return false;
        }
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
        log.info("HealthMonitor stopped");
    }
}
