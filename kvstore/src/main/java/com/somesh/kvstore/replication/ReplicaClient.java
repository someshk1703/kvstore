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

import com.somesh.kvstore.engine.CommandExecutor;
import com.somesh.kvstore.protocol.Command;
import com.somesh.kvstore.protocol.CommandParser;

/**
 * Replica-side replication client.
 *
 * <h2>What it does</h2>
 * <ol>
 *   <li>Connects to the primary at the configured host/port.</li>
 *   <li>Sends a {@code REPLCONF <lastOffset>} handshake:
 *       <ul>
 *         <li>{@code -1} for a fresh replica (full resync expected).</li>
 *         <li>The last applied offset if reconnecting (partial resync if possible).</li>
 *       </ul>
 *   </li>
 *   <li>Handles the primary's response:
 *       <ul>
 *         <li>{@code FULLRESYNC <masterOffset>}: receive a snapshot stream of SET
 *             commands until {@code FULLRESYNC_END}, then switch to live mode.</li>
 *         <li>{@code CONTINUE <fromOffset>}: receive only the missed commands.</li>
 *       </ul>
 *   </li>
 *   <li>In live mode: continuously reads {@code *REPL <offset> <command>} lines,
 *       applies them to the local {@link CommandExecutor}, and sends
 *       {@code *ACK <offset>} back to the primary.</li>
 * </ol>
 *
 * <h2>Read-only enforcement</h2>
 * While in replica mode the server rejects client write commands. This is
 * enforced by {@link com.somesh.kvstore.server.ClientHandler} via the
 * {@code replicaMode} flag on {@link com.somesh.kvstore.server.TcpServer}.
 *
 * <h2>Reconnect</h2>
 * If the connection to the primary drops, this client logs the error and stops.
 * A production system would implement exponential back-off reconnect; that is
 * left as a future enhancement to keep this class focused.
 *
 * <h2>Interview angles</h2>
 * <ul>
 *   <li>What is replication lag? The time between a write being applied on the
 *       primary and the replica applying the same command.</li>
 *   <li>Why does the replica send ACKs? So the primary can implement WAIT and
 *       track per-replica progress for partial-resync decisions.</li>
 *   <li>Why does the replica reject writes? A replica serving stale reads is
 *       acceptable; a replica accepting writes that never reach the primary
 *       would silently split-brain the cluster.</li>
 * </ul>
 */
public class ReplicaClient implements Closeable, Runnable {

    private static final Logger log = LoggerFactory.getLogger(ReplicaClient.class);

    private final String         primaryHost;
    private final int            primaryPort;
    private final CommandExecutor executor;
    private final CommandParser  parser;

    /** The highest offset this replica has applied to its local store. */
    private final AtomicLong replicaOffset = new AtomicLong(-1);

    private volatile Socket       socket;
    private volatile PrintWriter  out;
    private volatile BufferedReader in;
    private volatile boolean      running = true;

    public ReplicaClient(String primaryHost, int primaryPort, CommandExecutor executor) {
        this.primaryHost = primaryHost;
        this.primaryPort = primaryPort;
        this.executor    = executor;
        this.parser      = new CommandParser();
    }

    // ── Runnable entry point ─────────────────────────────────────────────────

    @Override
    public void run() {
        try {
            connect();
            performHandshake();
            replicationLoop();
        } catch (IOException e) {
            if (running) {
                log.error("Replication connection lost: {}", e.getMessage());
            }
        } finally {
            close();
        }
    }

    // ── Lifecycle ────────────────────────────────────────────────────────────

    private void connect() throws IOException {
        log.info("Connecting to primary {}:{}", primaryHost, primaryPort);
        socket = new Socket(primaryHost, primaryPort);
        socket.setTcpNoDelay(true);
        out = new PrintWriter(
            new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), false);
        in  = new BufferedReader(
            new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
        log.info("Connected to primary {}:{}", primaryHost, primaryPort);
    }

    /**
     * Perform the replication handshake.
     *
     * <p>Sends {@code REPLCONF <lastOffset>} and parses the response:
     * <ul>
     *   <li>{@code FULLRESYNC <masterOffset>} — primary sends a full snapshot stream.</li>
     *   <li>{@code CONTINUE <fromOffset>} — partial resync, primary streams missed commands.</li>
     * </ul>
     */
    private void performHandshake() throws IOException {
        long lastOffset = replicaOffset.get();
        sendLine("REPLCONF " + lastOffset);
        log.info("Sent REPLCONF with lastOffset={}", lastOffset);

        String response = in.readLine();
        if (response == null) {
            throw new IOException("Primary closed connection during handshake");
        }

        if (response.startsWith("FULLRESYNC")) {
            log.info("Full resync initiated: {}", response);
            receiveSyncStream();
        } else if (response.startsWith("CONTINUE")) {
            log.info("Partial resync: {}", response);
            // Partial resync: the live loop will receive missed commands
        } else {
            throw new IOException("Unexpected handshake response: " + response);
        }
    }

    /**
     * Receive the full-resync snapshot stream.
     *
     * <p>The primary streams SET commands as {@code <offset> <command>} lines
     * until {@code FULLRESYNC_END}.
     */
    private void receiveSyncStream() throws IOException {
        int count = 0;
        String line;
        while ((line = in.readLine()) != null) {
            if ("FULLRESYNC_END".equals(line.trim())) {
                log.info("Full resync complete: {} commands applied", count);
                return;
            }
            if (line.startsWith("*REPL ")) {
                applyReplLine(line);
                count++;
            }
        }
        throw new IOException("Primary closed connection during full resync");
    }

    /**
     * Main replication loop — reads and applies commands until disconnected.
     */
    private void replicationLoop() throws IOException {
        log.info("Replication loop started (offset={})", replicaOffset.get());
        String line;
        while (running && (line = in.readLine()) != null) {
            if (line.startsWith("*REPL ")) {
                applyReplLine(line);
            } else if (line.startsWith("REPLGETACK")) {
                // Primary requesting our current offset
                sendAck(replicaOffset.get());
            }
        }
        if (running) {
            throw new IOException("Primary disconnected");
        }
    }

    /**
     * Parse and apply a {@code *REPL <offset> <command>} line.
     */
    private void applyReplLine(String line) {
        // Format: "*REPL <offset> <command text>"
        String body = line.substring(6).trim();  // strip "*REPL "
        int spaceIdx = body.indexOf(' ');
        if (spaceIdx < 0) {
            log.warn("Malformed REPL line: {}", line);
            return;
        }
        long offset;
        try {
            offset = Long.parseLong(body.substring(0, spaceIdx));
        } catch (NumberFormatException e) {
            log.warn("Malformed REPL offset in: {}", line);
            return;
        }
        String commandText = body.substring(spaceIdx + 1).trim();

        Command cmd = parser.parse(commandText);
        executor.execute(cmd);

        replicaOffset.set(offset);
        sendAck(offset);
    }

    private void sendAck(long offset) {
        sendLine("*ACK " + offset);
    }

    private void sendLine(String line) {
        PrintWriter w = out;
        if (w != null) {
            w.println(line);
            w.flush();
        }
    }

    /** The highest offset this replica has applied. -1 if nothing applied yet. */
    public long getReplicaOffset() {
        return replicaOffset.get();
    }

    @Override
    public void close() {
        running = false;
        Socket s = socket;
        if (s != null && !s.isClosed()) {
            try {
                s.close();
            } catch (IOException e) {
                log.debug("Error closing replica client socket: {}", e.getMessage());
            }
        }
    }
}
