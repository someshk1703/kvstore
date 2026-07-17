package com.somesh.kvstore.replication;

import java.io.Closeable;
import java.io.IOException;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.somesh.kvstore.config.ServerConfig;
import com.somesh.kvstore.engine.KVStore;
import com.somesh.kvstore.persistence.SnapshotManager;

/**
 * Primary-side replication engine.
 *
 * <h2>What it does</h2>
 * <ol>
 *   <li>Maintains a {@link RingBuffer} backlog of serialized write commands.</li>
 *   <li>Tracks a monotonically increasing master offset.</li>
 *   <li>Propagates every write command asynchronously to all connected replicas.</li>
 *   <li>Handles replica handshakes: partial resync (backlog) or full resync (snapshot).</li>
 *   <li>Implements the {@code WAIT} command: blocks until N replicas have acked.</li>
 * </ol>
 *
 * <h2>Propagation ordering</h2>
 * Writes are applied to the local store first, then propagated. This means a
 * primary crash after a write but before propagation can lose the write on
 * replicas. {@code WAIT} closes this window for the caller: it blocks until the
 * desired ack count is reached.
 *
 * <h2>Thread model</h2>
 * <ul>
 *   <li>Propagation: called by any client-handler thread; uses
 *       {@code CopyOnWriteArrayList} so readers of the replica list are lock-free.</li>
 *   <li>ACK reader: one daemon thread per replica, started by
 *       {@link #registerReplica(Socket)}.</li>
 *   <li>WAIT: blocks the calling thread but does not hold any locks.</li>
 * </ul>
 *
 * <h2>Interview angles</h2>
 * <ul>
 *   <li>Why CopyOnWriteArrayList? — replica list is read on every write, written
 *       rarely (only when replicas join/leave). COW is optimal for that ratio.</li>
 *   <li>Why propagate after applying locally, not before? — "write to primary
 *       succeeds" must not depend on replica availability; async propagation
 *       keeps the write path fast. WAIT is the escape hatch for synchrony.</li>
 *   <li>What is the durability window without WAIT? — up to one replication
 *       lag window; use WAIT 1 0 for synchronous semantics.</li>
 * </ul>
 */
public class ReplicationManager implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ReplicationManager.class);

    // ── Backlog + offset ─────────────────────────────────────────────────────────

    /** Circular backlog of "<offset> <command text>" entries. */
    private final RingBuffer<String> backlog;

    /**
     * The offset of the last appended command.
     * {@code masterOffset.get() == backlog.getLatestOffset()}.
     * Exposed separately so callers can read it without buffer interaction.
     */
    private final AtomicLong masterOffset = new AtomicLong(-1);

    // ── Replica registry ──────────────────────────────────────────────────────

    /**
     * All currently connected replicas.
     * CopyOnWriteArrayList: reads on every propagate (fast), writes only on
     * replica connect/disconnect (rare). Perfect fit.
     */
    private final CopyOnWriteArrayList<ReplicaConnection> replicas =
        new CopyOnWriteArrayList<>();

    // ── Dependencies ────────────────────────────────────────────────────────────

    private final KVStore store;
    private final SnapshotManager snapshotManager;  // for full-resync snapshot generation

    /** Thread pool for ACK reader threads (one per replica). */
    private final ExecutorService ackReaderPool;

    // ── Constructor ────────────────────────────────────────────────────────────

    public ReplicationManager(KVStore store, SnapshotManager snapshotManager) {
        this.store           = store;
        this.snapshotManager = snapshotManager;
        this.backlog         = new RingBuffer<>(ServerConfig.REPLICATION_BACKLOG_SIZE);
        this.ackReaderPool   = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "repl-ack-reader");
            t.setDaemon(true);
            return t;
        });
    }

    // ── Propagation ────────────────────────────────────────────────────────────

    /**
     * Append a write command to the backlog and propagate to all replicas.
     *
     * <p>Called by {@link com.somesh.kvstore.engine.CommandExecutor} after every
     * successful write to the local store. This is a synchronous append to the
     * ring buffer followed by async sends to each replica.
     *
     * @param commandText the inline command text, e.g. "SET foo bar"
     * @return the offset assigned to this command
     */
    public long propagate(String commandText) {
        // Append to backlog and get the stable offset
        long offset = backlog.append(commandText);
        masterOffset.set(offset);

        // Send to all replicas. Disconnected replicas are pruned lazily.
        String frame = offset + " " + commandText;
        List<ReplicaConnection> current = replicas;  // COW snapshot
        for (ReplicaConnection replica : current) {
            if (replica.isConnected()) {
                replica.send(frame);
            } else {
                replicas.remove(replica);
                log.info("Pruned disconnected replica {}", replica.getReplicaId());
            }
        }
        return offset;
    }

    /**
     * Current master offset (-1 if no writes have been propagated yet).
     */
    public long getMasterOffset() {
        return masterOffset.get();
    }

    // ── Replica handshake ───────────────────────────────────────────────────────

    /**
     * Handle a new replica connecting with its last-known offset.
     *
     * <p>Decision tree:
     * <ol>
     *   <li>If {@code fromOffset} is in the backlog → partial resync: stream
     *       all missed commands from the backlog.</li>
     *   <li>If {@code fromOffset} is not in the backlog (too far behind or fresh
     *       replica with offset=0) → full resync: send a snapshot of the current
     *       store, then switch to live streaming.</li>
     * </ol>
     *
     * <p>After either path, the replica is registered so future propagations
     * reach it.
     *
     * @param socket     the accepted TCP socket to the replica
     * @param fromOffset the last offset the replica has applied (-1 for a fresh replica)
     */
    public void registerReplica(Socket socket, long fromOffset) throws IOException {
        ReplicaConnection conn = new ReplicaConnection(socket);
        log.info("Replica connecting from {} with offset={}", conn.getReplicaId(), fromOffset);

        if (fromOffset >= 0 && backlog.contains(fromOffset + 1)) {
            // Partial resync path
            log.info("Partial resync for {} from offset {}", conn.getReplicaId(), fromOffset + 1);
            List<String> missed = backlog.getRange(fromOffset + 1);
            for (String entry : missed) {
                // entry is already "<offset> <command>"
                conn.send(entry);
            }
        } else {
            // Full resync path
            log.info("Full resync for {} (fromOffset={}, backlogCapacity={})",
                conn.getReplicaId(), fromOffset, backlog.getCapacity());
            sendFullResync(conn);
        }

        // Register for future propagations
        replicas.add(conn);
        log.info("Replica {} registered; total replicas={}", conn.getReplicaId(), replicas.size());

        // Start ACK reader thread for this replica
        startAckReader(conn);
    }

    // ── WAIT command ────────────────────────────────────────────────────────────

    /**
     * Block until at least {@code numReplicas} replicas have acknowledged the
     * current master offset, or until {@code timeoutMs} elapses.
     *
     * <p>This implements Redis's {@code WAIT} semantics:
     * <ol>
     *   <li>Capture the current master offset.</li>
     *   <li>Request an immediate ACK from all replicas (REPLGETACK).</li>
     *   <li>Poll until the target ack count is reached or timeout fires.</li>
     *   <li>Return the actual count of replicas that acked in time.</li>
     * </ol>
     *
     * <p>A timeout of 0 means "return immediately with current ack state".
     *
     * @param numReplicas desired number of replicas to ack
     * @param timeoutMs   maximum wait in milliseconds; 0 for non-blocking
     * @return number of replicas that have acked &le; numReplicas
     */
    public int waitForReplicas(int numReplicas, long timeoutMs) {
        long targetOffset = masterOffset.get();

        // If there are no writes at all, everyone has trivially acked.
        if (targetOffset < 0) {
            return replicas.size();
        }

        // Request immediate ACK from each replica
        for (ReplicaConnection replica : replicas) {
            if (replica.isConnected()) {
                replica.send("REPLGETACK *");
            }
        }

        if (timeoutMs <= 0) {
            return countAckedReplicas(targetOffset);
        }

        // Poll every 10ms until timeout
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            int acked = countAckedReplicas(targetOffset);
            if (acked >= numReplicas) return acked;
            try {
                Thread.sleep(Math.min(10, deadline - System.currentTimeMillis()));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        return countAckedReplicas(targetOffset);
    }

    // ── Replica count ────────────────────────────────────────────────────────────

    /** @return number of currently connected replicas */
    public int getReplicaCount() {
        return replicas.size();
    }

    // ── Private helpers ───────────────────────────────────────────────────────────

    private int countAckedReplicas(long targetOffset) {
        int count = 0;
        for (ReplicaConnection r : replicas) {
            if (r.getAckedOffset() >= targetOffset) count++;
        }
        return count;
    }

    /**
     * Full resync: serialize the current store contents and send each entry
     * as a synthetic SET command, followed by a FULLRESYNC_END marker.
     * The replica loads these before accepting live stream commands.
     */
    private void sendFullResync(ReplicaConnection conn) throws IOException {
        conn.send("FULLRESYNC " + masterOffset.get());
        // Snapshot the store and stream as SET commands
        store.getStore().forEach((key, entry) -> {
            if (!entry.isExpired()) {
                long expiresAt = entry.expiresAt;
                String cmd;
                if (expiresAt < 0) {
                    cmd = "SET " + key + " " + entry.value;
                } else {
                    long remainingMs = expiresAt - System.currentTimeMillis();
                    if (remainingMs > 0) {
                        cmd = "SET " + key + " " + entry.value + " PX " + remainingMs;
                    } else {
                        return; // expired during iteration, skip
                    }
                }
                long offset = backlog.getNextOffset() - 1;
                conn.send(offset + " " + cmd);
            }
        });
        conn.send("FULLRESYNC_END");
    }

    /**
     * Start a daemon ACK-reader thread for the given replica.
     * Reads {@code *ACK <offset>} lines and calls {@link ReplicaConnection#updateAckedOffset}.
     */
    private void startAckReader(ReplicaConnection conn) {
        ackReaderPool.submit(() -> {
            log.debug("ACK reader started for {}", conn.getReplicaId());
            while (conn.isConnected()) {
                long acked = conn.readAck();
                if (acked >= 0) {
                    conn.updateAckedOffset(acked);
                    log.debug("Replica {} acked offset {}", conn.getReplicaId(), acked);
                } else {
                    // Connection closed or parse error
                    log.info("ACK reader exiting for {}", conn.getReplicaId());
                    replicas.remove(conn);
                    conn.close();
                    break;
                }
            }
        });
    }

    @Override
    public void close() {
        log.info("ReplicationManager shutting down ({} replicas)", replicas.size());
        for (ReplicaConnection r : replicas) {
            r.close();
        }
        replicas.clear();
        ackReaderPool.shutdownNow();
    }
}
