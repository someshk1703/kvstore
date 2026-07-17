package com.somesh.kvstore.replication;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents one connected replica on the primary's side.
 *
 * <h2>Responsibilities</h2>
 * <ul>
 *   <li>Hold the network connection to a single replica</li>
 *   <li>Send serialized write commands to the replica</li>
 *   <li>Track the highest replication offset the replica has acknowledged</li>
 *   <li>Clean up resources on close</li>
 * </ul>
 *
 * <h2>Thread model</h2>
 * {@link #send(String)} is called by the propagation path (could be any of the
 * client-handler threads). {@link #updateAckedOffset(long)} is called by the ACK
 * reader thread. Both use {@code volatile} / {@code AtomicLong} — no additional
 * synchronization needed.
 *
 * <h2>Protocol</h2>
 * Write commands are sent as one line each in the same inline text format the
 * parser understands, prefixed with the offset:
 * <pre>
 *   *REPL 42 SET foo bar
 *   *REPL 43 DEL baz
 * </pre>
 * The replica applies the command and responds:
 * <pre>
 *   *ACK 43
 * </pre>
 */
public class ReplicaConnection implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ReplicaConnection.class);

    private final Socket           socket;
    private final PrintWriter      out;
    private final BufferedReader   in;
    private final AtomicLong       ackedOffset = new AtomicLong(-1);
    private final String           replicaId;  // host:port for logging

    public ReplicaConnection(Socket socket) throws IOException {
        this.socket    = socket;
        this.replicaId = socket.getRemoteSocketAddress().toString();
        this.out = new PrintWriter(
            new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), false);
        this.in = new BufferedReader(
            new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
    }

    // ── Public API ────────────────────────────────────────────────────────────

    /**
     * Send a replication command line to this replica.
     *
     * <p>The line is framed as:
     * <pre>*REPL &lt;offset&gt; &lt;command&gt;</pre>
     *
     * @param offsetAndCommand the offset and command text, e.g. "42 SET foo bar"
     */
    public synchronized void send(String offsetAndCommand) {
        try {
            out.println("*REPL " + offsetAndCommand);
            out.flush();
        } catch (Exception e) {
            log.warn("Failed to send to replica {}: {}", replicaId, e.getMessage());
        }
    }

    /**
     * Read one ACK line from the replica. Blocks until data is available.
     * Returns the offset acked, or -1 on error / connection closed.
     */
    public long readAck() {
        try {
            String line = in.readLine();
            if (line != null && line.startsWith("*ACK ")) {
                return Long.parseLong(line.substring(5).trim());
            }
        } catch (IOException | NumberFormatException e) {
            log.debug("Replica {} ack read error: {}", replicaId, e.getMessage());
        }
        return -1;
    }

    /** Update the highest confirmed offset from this replica. */
    public void updateAckedOffset(long offset) {
        ackedOffset.updateAndGet(current -> Math.max(current, offset));
    }

    /** The highest replication offset this replica has confirmed. -1 = none yet. */
    public long getAckedOffset() {
        return ackedOffset.get();
    }

    /** The replica's remote address (host:port), used for logging. */
    public String getReplicaId() {
        return replicaId;
    }

    public boolean isConnected() {
        return !socket.isClosed();
    }

    @Override
    public void close() {
        try {
            socket.close();
        } catch (IOException e) {
            log.debug("Error closing replica socket {}: {}", replicaId, e.getMessage());
        }
    }
}
