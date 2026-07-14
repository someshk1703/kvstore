package com.somesh.kvstore.persistence;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.somesh.kvstore.engine.CommandExecutor;
import com.somesh.kvstore.engine.KVStore;
import com.somesh.kvstore.protocol.Command;
import com.somesh.kvstore.protocol.CommandParser;

/**
 * Append-Only File writer — the write-ahead log for the KV store.
 *
 * <h2>How it works</h2>
 * Every write command (SET, DEL, EXPIRE, PERSIST) is appended as a text line
 * immediately after the in-memory store is updated. On restart, {@link #replay}
 * reads the file line by line, re-parses each command, and re-executes it to
 * rebuild the in-memory state.
 *
 * <h2>fsync modes</h2>
 * <ul>
 *   <li>{@link FsyncMode#ALWAYS} — flushes the OS buffer after every write.
 *       Strongest durability (zero data loss on crash), lowest throughput.</li>
 *   <li>{@link FsyncMode#EVERYSEC} — a background thread flushes once per second.
 *       At most 1 second of data can be lost on an unclean shutdown.</li>
 * </ul>
 *
 * <h2>AOF format</h2>
 * One command per line, in the same inline text format the parser understands:
 * <pre>
 *   SET foo bar
 *   SET baz qux EX 30
 *   DEL foo
 *   # SNAPSHOT 1720000000000
 *   SET newkey val
 * </pre>
 * Lines starting with {@code #} are comment/marker lines (not commands).
 * The {@code # SNAPSHOT <ts>} marker records when a snapshot was taken; it is
 * used by {@link CrashRecovery} to replay only the AOF delta after the snapshot.
 *
 * <h2>Thread safety</h2>
 * All public methods are {@code synchronized} on {@code this}. This means only
 * one thread writes at a time, which is correct: we cannot interleave partial
 * command lines. The lock is held only for the duration of a write + optional
 * flush — O(1) — so contention is negligible.
 *
 * <h2>Interview angles this covers</h2>
 * <ul>
 *   <li>WAL pattern: why write the log before (or atomically with) the in-memory update</li>
 *   <li>fsync trade-off: durability vs throughput</li>
 *   <li>Crash recovery: what happens to a truncated last line</li>
 *   <li>AOF rewrite / compaction: why and how (atomic swap)</li>
 * </ul>
 */
public class AOFWriter implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(AOFWriter.class);

    // ── Fsync mode ────────────────────────────────────────────────────────────

    /**
     * Controls when the OS write buffer is flushed to disk.
     *
     * <p>This is NOT the same as Java's BufferedWriter.flush() — that only moves
     * data from the Java buffer to the OS. A real fsync() call is needed to
     * guarantee data reaches durable storage. For simplicity, we treat
     * BufferedWriter.flush() as our "fsync" — sufficient for interview purposes,
     * though a production system would use FileDescriptor.sync() or FileChannel.force().
     */
    public enum FsyncMode {
        /** Flush after every write. Zero data loss on crash. */
        ALWAYS,
        /** Flush once per second in a background thread. At most 1 s of data loss. */
        EVERYSEC
    }

    // ── State ─────────────────────────────────────────────────────────────────

    private final Path aofPath;
    private final FsyncMode fsyncMode;
    private BufferedWriter writer;
    private ScheduledExecutorService scheduler;

    // ── Constructor ───────────────────────────────────────────────────────────

    public AOFWriter(Path aofPath, FsyncMode fsyncMode) throws IOException {
        this.aofPath   = aofPath;
        this.fsyncMode = fsyncMode;

        // Ensure parent directory exists (e.g. data/)
        if (aofPath.getParent() != null) {
            Files.createDirectories(aofPath.getParent());
        }

        // Open in append mode — existing content is preserved, new writes go to the end
        this.writer = new BufferedWriter(new FileWriter(aofPath.toFile(), true));

        if (fsyncMode == FsyncMode.EVERYSEC) {
            startEverysecFlusher();
        }
    }

    // ── Core write API ────────────────────────────────────────────────────────

    /**
     * Appends a command line to the AOF.
     *
     * <p>Called by {@link CommandExecutor} after every successful write command.
     * {@code command} must be the reconstructed text form, e.g. {@code "SET foo bar EX 30"}.
     *
     * @param command the command text to log; must not be null or empty
     * @throws IOException if the write or flush fails
     */
    public synchronized void log(String command) throws IOException {
        writer.write(command);
        writer.newLine();
        if (fsyncMode == FsyncMode.ALWAYS) {
            writer.flush();
        }
    }

    /**
     * Appends a snapshot marker line ({@code # SNAPSHOT <timestamp_ms>}).
     *
     * <p>Called by {@link SnapshotManager} after each successful snapshot.
     * {@link CrashRecovery} uses this marker to find the boundary between the
     * snapshot's data and the post-snapshot AOF delta.
     *
     * @param timestampMs wall-clock time when the snapshot completed, in ms since epoch
     */
    public synchronized void logSnapshotMarker(long timestampMs) throws IOException {
        writer.write("# SNAPSHOT " + timestampMs);
        writer.newLine();
        writer.flush();  // always flush markers — they must not be buffered
    }

    // ── Replay ────────────────────────────────────────────────────────────────

    /**
     * Replays the full AOF against the given {@link KVStore} via a replay-mode
     * {@link CommandExecutor} (one without an AOFWriter wired in, so replayed
     * commands are not re-logged).
     *
     * <p>Lines beginning with {@code #} (marker/comment lines) are skipped.
     * Blank lines are skipped. Malformed lines produce a warning but do NOT
     * abort recovery — this is the correct behaviour for a truncated last line
     * after an unclean shutdown.
     *
     * @param store    the store to populate
     * @param executor a replay-mode executor (no AOFWriter)
     * @throws IOException if the file cannot be read
     */
    public void replay(KVStore store, CommandExecutor executor) throws IOException {
        if (!Files.exists(aofPath)) {
            log.info("No AOF file found at {} — starting empty", aofPath);
            return;
        }

        log.info("Replaying full AOF: {}", aofPath);
        CommandParser parser = new CommandParser();
        int[] counts = {0, 0};   // [applied, skipped]

        try (Stream<String> lines = Files.lines(aofPath)) {
            lines.forEach(line -> {
                if (line.startsWith("#") || line.isBlank()) return;
                try {
                    Command cmd = parser.parse(line);
                    executor.execute(cmd);
                    counts[0]++;
                } catch (Exception e) {
                    log.warn("Skipping malformed AOF line '{}': {}", line, e.getMessage());
                    counts[1]++;
                }
            });
        }

        log.info("AOF replay complete: {} commands applied, {} skipped", counts[0], counts[1]);
    }

    /**
     * Replays only the AOF entries written <em>after</em> the given snapshot timestamp.
     *
     * <p>The AOF is scanned for a {@code # SNAPSHOT <ts>} marker where
     * {@code ts >= snapshotTimestampMs}. All lines after that marker are replayed.
     * Lines before the matching marker are skipped (they are already captured in
     * the snapshot).
     *
     * @param store               the store (already populated from snapshot)
     * @param executor            a replay-mode executor
     * @param snapshotTimestampMs the timestamp recorded in the snapshot
     * @throws IOException if the file cannot be read
     */
    public void replayAfter(KVStore store, CommandExecutor executor, long snapshotTimestampMs)
            throws IOException {
        if (!Files.exists(aofPath)) {
            log.info("No AOF file found — nothing to replay after snapshot");
            return;
        }

        log.info("Replaying AOF delta after snapshot timestamp {}", snapshotTimestampMs);
        CommandParser parser = new CommandParser();
        // Start in replay zone only if snapshot timestamp is 0 (replay everything)
        boolean[] inReplayZone = {snapshotTimestampMs <= 0};
        int[] counts = {0, 0};

        try (Stream<String> lines = Files.lines(aofPath)) {
            lines.forEach(line -> {
                if (line.isBlank()) return;

                // Look for the snapshot marker that corresponds to our snapshot
                if (line.startsWith("# SNAPSHOT ")) {
                    try {
                        long markerTs = Long.parseLong(line.substring("# SNAPSHOT ".length()).trim());
                        // Enter replay zone once we've passed the marker for our snapshot
                        if (markerTs >= snapshotTimestampMs) {
                            inReplayZone[0] = true;
                        }
                    } catch (NumberFormatException e) {
                        log.warn("Malformed SNAPSHOT marker line: '{}'", line);
                    }
                    return;  // marker lines are never executed as commands
                }

                // Skip other comment lines and pre-snapshot commands
                if (line.startsWith("#") || !inReplayZone[0]) return;

                try {
                    Command cmd = parser.parse(line);
                    executor.execute(cmd);
                    counts[0]++;
                } catch (Exception e) {
                    log.warn("Skipping malformed AOF line '{}': {}", line, e.getMessage());
                    counts[1]++;
                }
            });
        }

        log.info("AOF delta replay complete: {} commands applied, {} skipped", counts[0], counts[1]);
    }

    /**
     * Replays all AOF entries written after the <em>last</em> {@code # SNAPSHOT} marker.
     *
     * <p>Used by {@link CrashRecovery} when both snapshot and AOF are present.
     * The "last marker" approach is robust: it does not require timestamp comparison
     * between the AOF marker and the snapshot file's mtime (which can differ by
     * milliseconds due to OS file timestamp resolution).
     *
     * <p>If no snapshot marker is found in the AOF, the full AOF is replayed
     * (conservative: may re-apply commands already in the snapshot, but data
     * correctness is preserved since SET and DEL are idempotent in sequence).
     *
     * @param store    the store (already populated from snapshot)
     * @param executor a replay-mode executor
     * @throws IOException if the file cannot be read
     */
    public void replayAfterLastSnapshot(KVStore store, CommandExecutor executor)
            throws IOException {
        if (!Files.exists(aofPath)) {
            log.info("No AOF file found — nothing to replay after snapshot");
            return;
        }

        // Two-pass: first find last snapshot marker line index, then replay after it
        java.util.List<String> allLines = Files.readAllLines(aofPath);
        int lastMarkerIndex = -1;
        for (int i = 0; i < allLines.size(); i++) {
            if (allLines.get(i).startsWith("# SNAPSHOT ")) {
                lastMarkerIndex = i;
            }
        }

        int replayFrom = lastMarkerIndex + 1;   // 0 if no marker found (replay full AOF)
        log.info("Replaying AOF after last snapshot marker (line {})", lastMarkerIndex);

        CommandParser parser = new CommandParser();
        int applied = 0, skipped = 0;

        for (int i = replayFrom; i < allLines.size(); i++) {
            String line = allLines.get(i);
            if (line.isBlank() || line.startsWith("#")) continue;
            try {
                Command cmd = parser.parse(line);
                executor.execute(cmd);
                applied++;
            } catch (Exception e) {
                log.warn("Skipping malformed AOF line '{}': {}", line, e.getMessage());
                skipped++;
            }
        }

        log.info("AOF delta replay complete: {} commands applied, {} skipped", applied, skipped);
    }

    // ── Compaction (rewrite) ──────────────────────────────────────────────────

    /**
     * Compacts the AOF by writing one {@code SET} per live key, then atomically
     * swapping the old AOF for the new one.
     *
     * <p>Why this matters: after running for hours, the AOF accumulates many
     * redundant entries (e.g. 100 SETs to the same key). A rewrite reduces it to
     * the minimal set of commands to reconstruct current state.
     *
     * <p>The atomic swap ({@link StandardCopyOption#ATOMIC_MOVE}) ensures no reader
     * or writer ever sees a half-written file. If the swap fails, the original AOF
     * is preserved and the temp file is cleaned up.
     *
     * @param store the live store to compact from
     * @throws IOException if the rewrite fails
     */
    public synchronized void rewrite(KVStore store) throws IOException {
        Path tmpPath = aofPath.resolveSibling(aofPath.getFileName() + ".rewrite.tmp");

        try (BufferedWriter tmp = new BufferedWriter(new FileWriter(tmpPath.toFile()))) {
            store.getStore().forEach((key, entry) -> {
                if (entry.isExpired()) return;   // skip expired keys — they won't exist after replay
                try {
                    if (entry.hasTtl()) {
                        long remainingMs = entry.remainingTtlMs();
                        if (remainingMs > 0) {
                            // Use PX (milliseconds) to preserve sub-second precision
                            tmp.write("SET " + key + " " + entry.value + " PX " + remainingMs);
                            tmp.newLine();
                        }
                        // If remainingMs <= 0, key is effectively expired — omit it
                    } else {
                        tmp.write("SET " + key + " " + entry.value);
                        tmp.newLine();
                    }
                } catch (IOException e) {
                    log.error("Error writing AOF rewrite entry for key '{}'", key, e);
                    throw new RuntimeException(e);  // surface to caller
                }
            });
            tmp.flush();
        }

        // Close the current writer before the atomic move
        writer.close();
        try {
            Files.move(tmpPath, aofPath,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            // Fallback: non-atomic move is still better than nothing on limited filesystems
            log.warn("ATOMIC_MOVE not supported — falling back to REPLACE_EXISTING");
            Files.move(tmpPath, aofPath, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            // Move failed — clean up temp file, old AOF is still intact
            Files.deleteIfExists(tmpPath);
            throw e;
        }

        // Reopen writer on the compacted file
        this.writer = new BufferedWriter(new FileWriter(aofPath.toFile(), true));
        log.info("AOF rewrite complete: {}", aofPath);
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    @Override
    public synchronized void close() throws IOException {
        if (scheduler != null) {
            scheduler.shutdown();
        }
        if (writer != null) {
            writer.flush();
            writer.close();
        }
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    /**
     * Starts the background flush thread for {@link FsyncMode#EVERYSEC}.
     *
     * <p>A daemon thread so it doesn't prevent JVM shutdown.
     */
    private void startEverysecFlusher() {
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "aof-flusher");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(() -> {
            try {
                synchronized (AOFWriter.this) {
                    writer.flush();
                }
            } catch (IOException e) {
                log.error("AOF background flush failed", e);
            }
        }, 1, 1, TimeUnit.SECONDS);
    }
}
