package com.somesh.kvstore.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.somesh.kvstore.engine.CommandExecutor;
import com.somesh.kvstore.engine.CommandResult;
import com.somesh.kvstore.engine.KVStore;
import com.somesh.kvstore.protocol.Command;
import com.somesh.kvstore.protocol.CommandParser;
import com.somesh.kvstore.protocol.ResponseSerializer;

/**
 * Handles one client connection for its entire lifetime.
 *
 * Lifecycle:
 *   Created by TcpServer.acceptLoop() on every new connection.
 *   Submitted to thread pool as a Runnable.
 *   Runs until client disconnects, times out, or server shuts down.
 *   Cleans up its socket via try-with-resources — no leaks.
 *
 * Thread confinement:
 *   Each instance runs on exactly one thread-pool thread.
 *   No instance-level state is shared between handlers.
 *   KVStore, CommandExecutor, CommandParser, ResponseSerializer are all
 *   injected and either thread-safe or stateless — safe to share.
 *
 * Why Runnable and not Callable?
 *   Callable<V> returns a result and can throw checked exceptions.
 *   Runnable returns void. ClientHandler never produces a value for
 *   the thread pool to collect — it just runs until done. Runnable
 *   is the correct interface. The thread pool accepts both via submit().
 */
public class ClientHandler implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(ClientHandler.class);

    // ── Injected dependencies ────────────────────────────────────────
    // All either stateless (parser, serializer) or thread-safe (kvStore, executor).
    // Safe to share across all ClientHandler instances.

    private final Socket             clientSocket;
    private final KVStore            kvStore;
    private final CommandExecutor    executor;
    private final CommandParser      parser;
    private final ResponseSerializer serializer;

    public ClientHandler(
            Socket clientSocket,
            KVStore kvStore,
            CommandExecutor executor,
            CommandParser parser,
            ResponseSerializer serializer) {
        this.clientSocket = clientSocket;
        this.kvStore      = kvStore;
        this.executor     = executor;
        this.parser       = parser;
        this.serializer   = serializer;
    }

    // ── Runnable entry point ─────────────────────────────────────────

    @Override
    public void run() {
        // Cache address before socket closes — useful for logging after disconnect
        String clientAddress = clientSocket.getRemoteSocketAddress().toString();
        log.info("Client connected: {}", clientAddress);

        /*
         * try-with-resources on the socket itself.
         *
         * Why wrap socket here and not just the streams?
         * Closing a Socket automatically closes its InputStream and OutputStream.
         * If we only closed the streams, the socket fd would leak until GC.
         * Wrapping the socket ensures the fd is released on any exit path —
         * normal return, exception, or thread interrupt.
         *
         * The streams (BufferedReader, PrintWriter) are also in try-with-resources
         * but their close() is redundant — socket.close() handles it.
         * Explicit stream close is defensive: if stream construction fails
         * mid-way, earlier streams still get closed.
         */
        try (
            Socket         socket = clientSocket;
            BufferedReader in     = new BufferedReader(
                new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            PrintWriter    out    = new PrintWriter(
                new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8),
                false)  // autoFlush=false — explained in detail below
        ) {
            readWriteLoop(in, out, clientAddress);

        } catch (IOException e) {
            // Reaches here only if stream construction fails (rare).
            // Errors inside readWriteLoop are caught there.
            log.error("Failed to set up streams for client {}: {}",
                      clientAddress, e.getMessage());
        }

        log.info("Client disconnected: {}", clientAddress);
        // Thread returns to pool here — ready for next connection
    }

    // ── Core read/write loop ─────────────────────────────────────────

    /**
     * Process commands from this client until disconnected or interrupted.
     *
     * The loop structure:
     *   1. Read one line (blocks on network IO)
     *   2. Check for null (client closed connection)
     *   3. Parse → execute → serialize → write
     *   4. Flush
     *   5. Repeat
     *
     * Error philosophy:
     *   - IOException from the socket → exit loop, close connection
     *   - Bugs in parse/execute/serialize → catch, return -ERR, keep looping
     *   - Server shutdown (thread interrupted) → exit loop cleanly
     */
    private void readWriteLoop(BufferedReader in, PrintWriter out, String clientAddress)
            throws IOException {

        String rawLine;

        while (!Thread.currentThread().isInterrupted()) {

            // ── Read ────────────────────────────────────────────────
            /*
             * readLine() blocks here until:
             *   (a) A complete line arrives  → returns the line (no \r\n)
             *   (b) Client closes connection → returns null
             *   (c) SO_TIMEOUT expires       → throws SocketTimeoutException
             *   (d) Socket closed externally → throws IOException
             *   (e) Thread interrupted       → throws InterruptedIOException
             *                                   (subclass of IOException)
             *
             * This is cooperative scheduling — the thread consumes zero CPU
             * while blocked here. The OS parks it and wakes it when data arrives.
             */
            try {
                rawLine = in.readLine();
            } catch (SocketTimeoutException e) {
                // SO_TIMEOUT set in TcpServer — client idle too long
                log.info("Client {} idle timeout — closing connection", clientAddress);
                sendError(out, "ERR connection timeout");
                break;
            } catch (InterruptedIOException e) {
                // Thread was interrupted while blocked in readLine().
                // This happens during server shutdown (shutdownNow() interrupts threads).
                log.debug("Client handler for {} interrupted during read", clientAddress);
                Thread.currentThread().interrupt();  // restore interrupt flag
                break;
            }

            // null → client sent TCP FIN (graceful close)
            if (rawLine == null) {
                log.debug("Client {} closed connection gracefully", clientAddress);
                break;
            }

            // ── Process ─────────────────────────────────────────────
            /*
             * Catch-all around the processing pipeline.
             *
             * Any uncaught exception in parse/execute/serialize is a bug in
             * our code, not a client error. We must NOT let it:
             *   - Propagate to run() → that closes the connection (unfair to client)
             *   - Silently disappear → that makes bugs invisible
             *
             * Correct behaviour: log it, return a generic -ERR, keep the
             * connection alive. The next command might be fine.
             *
             * In production you'd also increment an error counter metric here.
             */
            try {
                processLine(rawLine, out, clientAddress);
            } catch (Exception e) {
                log.error("Unexpected error processing command from {}: {}",
                          clientAddress, e.getMessage(), e);
                sendError(out, "ERR internal server error");
            }
        }
    }

    // ── Single command processing ────────────────────────────────────

    /**
     * Parse one raw line, execute it, serialize the result, write to socket.
     *
     * Kept in its own method so readWriteLoop stays readable and
     * this step is independently testable.
     */
    private void processLine(String rawLine, PrintWriter out, String clientAddress)
            throws IOException {

        log.trace("← [{}] {}", clientAddress, rawLine);

        // Parse raw text → Command object
        Command cmd = parser.parse(rawLine);

        // Execute Command → CommandResult
        // Returns null for empty lines (parser returns UNKNOWN with 0 args)
        CommandResult result = executor.execute(cmd);

        // null = empty line — nothing to write back
        if (result == null) return;

        // Serialize CommandResult → RESP string
        String response = serializer.serialize(result);

        log.trace("→ [{}] {}", clientAddress,
                  response.replace("\r\n", "\\r\\n")); // readable in logs

        // Write + flush
        out.print(response);
        out.flush();
        /*
         * Why out.print() and not out.println()?
         *
         * RESP responses already end with \r\n — built into each serializer method.
         * println() appends another \n → double terminator → client parse error.
         * print() writes exactly what we give it. Explicit flush() sends it.
         *
         * Why flush() after every command?
         *
         * PrintWriter wraps a BufferedOutputStream (via OutputStreamWriter).
         * Without flush(), responses sit in an 8KB buffer and only go on the
         * wire when the buffer is full or the stream closes. For a request-
         * response protocol, that means the client waits forever.
         * Flush after every response = client sees it immediately.
         *
         * Performance note: in a high-throughput scenario you'd batch multiple
         * responses and flush once (pipeline mode). Week 1 — one flush per
         * response is correct and simple.
         */
    }

    // ── Helpers ──────────────────────────────────────────────────────

    /**
     * Write a RESP error directly to the socket.
     * Used for connection-level errors (timeout, shutdown) where we haven't
     * gone through the normal execute/serialize path.
     */
    private void sendError(PrintWriter out, String message) {
        try {
            out.print("-" + message + "\r\n");
            out.flush();
        } catch (Exception e) {
            // Socket may already be closed — ignore
            log.debug("Could not send error to client: {}", e.getMessage());
        }
    }
}