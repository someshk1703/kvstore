package com.somesh.kvstore.persistence;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.somesh.kvstore.config.ServerConfig;
import com.somesh.kvstore.engine.KVStore;
import com.somesh.kvstore.engine.ValueEntry;

/**
 * Manages periodic full snapshots of the KV store keyspace (Redis RDB style).
 *
 * <h2>Snapshot format</h2>
 * Simple tab-separated text, one entry per line:
 * <pre>
 *   key\tvalue\texpiresAt_epoch_ms
 * </pre>
 * {@code expiresAt} is {@code -1} for keys with no TTL. This format is easy to
 * read in a debugger and doesn't require a schema — good enough for Week 3.
 * A production system would use a compact binary format (e.g. length-prefixed
 * fields) to reduce I/O and parse time.
 *
 * <h2>Atomic write</h2>
 * The snapshot is written to a temp file ({@code snapshot.tmp}) and then renamed
 * atomically to {@code snapshot.rdb}. This guarantees the live snapshot file is
 * always complete — a reader never sees a half-written file, even if the JVM is
 * killed mid-write.
 *
 * <h2>Concurrency</h2>
 * {@link #takeSnapshot()} is {@code synchronized} so it runs only one instance
 * at a time. The snapshot thread iterates over {@code store.getStore()}, which is
 * a {@link java.util.concurrent.ConcurrentHashMap}. ConcurrentHashMap's iterator
 * provides a weakly-consistent view (it will not throw ConcurrentModificationException
 * and reflects some but not necessarily all updates made since the iterator was created),
 * so concurrent writers can proceed without blocking — the snapshot captures a
 * "best-effort" point-in-time view. This is the same trade-off Redis makes with RDB.
 *
 * <h2>Snapshot triggers</h2>
 * <ol>
 *   <li>Time-based: every {@link ServerConfig#SNAPSHOT_INTERVAL_SECONDS} seconds</li>
 *   <li>Write-count: every {@link ServerConfig#SNAPSHOT_WRITE_THRESHOLD} write operations</li>
 * </ol>
 *
 * <h2>Interview angles this covers</h2>
 * <ul>
 *   <li>Why is the atomic rename important? (readers see complete or old, never partial)</li>
 *   <li>ConcurrentHashMap weakly-consistent iteration vs. a true point-in-time snapshot</li>
 *   <li>RPO trade-off: how much data can you lose between snapshots?</li>
 *   <li>RTO trade-off: smaller snapshots → faster cold-start</li>
 * </ul>
 */
public class SnapshotManager implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(SnapshotManager.class);

    private final Path snapshotPath;
    private final KVStore store;
    private final AOFWriter aofWriter;
    private final ScheduledExecutorService scheduler;
    private final AtomicLong writeCount = new AtomicLong(0);

    // ── Constructor ───────────────────────────────────────────────────────────

    public SnapshotManager(Path snapshotPath, KVStore store, AOFWriter aofWriter)
            throws IOException {
        this.snapshotPath = snapshotPath;
        this.store        = store;
        this.aofWriter    = aofWriter;

        // Ensure the data directory exists
        if (snapshotPath.getParent() != null) {
            Files.createDirectories(snapshotPath.getParent());
        }

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "snapshot-scheduler");
            t.setDaemon(true);
            return t;
        });

        // Time-based trigger
        long intervalSec = ServerConfig.SNAPSHOT_INTERVAL_SECONDS;
        scheduler.scheduleAtFixedRate(
            this::takeSnapshot, intervalSec, intervalSec, TimeUnit.SECONDS);
    }

    // ── Write-count trigger ───────────────────────────────────────────────────

    /**
     * Called by {@link com.somesh.kvstore.engine.CommandExecutor} on every
     * successful write command. When the write count crosses the threshold,
     * a snapshot is submitted to the background scheduler.
     */
    public void onWrite() {
        long count = writeCount.incrementAndGet();
        if (count % ServerConfig.SNAPSHOT_WRITE_THRESHOLD == 0) {
            scheduler.submit(this::takeSnapshot);
        }
    }

    // ── Snapshot write ────────────────────────────────────────────────────────

    /**
     * Serialises the current keyspace to a temp file, then atomically renames
     * it to the live snapshot path.
     *
     * <p>Synchronized so only one snapshot runs at a time (two concurrent scheduled
     * triggers can't interleave their writes). Uses ConcurrentHashMap's weakly-
     * consistent iterator — writers continue unblocked, but the snapshot may miss
     * very recent writes (they'll be in the AOF delta instead).
     */
    public synchronized void takeSnapshot() {
        Path tmpPath       = snapshotPath.resolveSibling("snapshot.tmp");
        long snapshotTime  = System.currentTimeMillis();
        int[] written = {0};

        log.info("Starting snapshot at timestamp {}", snapshotTime);

        try {
            // ── Step 1: Write to temp file ──────────────────────────────────
            try (BufferedWriter writer = Files.newBufferedWriter(tmpPath)) {
                for (Map.Entry<String, ValueEntry> e : store.getStore().entrySet()) {
                    ValueEntry ve = e.getValue();
                    if (ve.isExpired()) continue;   // don't persist expired keys

                    writer.write(e.getKey());
                    writer.write('\t');
                    writer.write(ve.value);
                    writer.write('\t');
                    writer.write(Long.toString(ve.expiresAt));
                    writer.newLine();
                    written[0]++;
                }
            }

            // ── Step 2: Atomic rename ───────────────────────────────────────
            try {
                Files.move(tmpPath, snapshotPath,
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                log.warn("ATOMIC_MOVE not supported — falling back to REPLACE_EXISTING");
                Files.move(tmpPath, snapshotPath, StandardCopyOption.REPLACE_EXISTING);
            }

            log.info("Snapshot complete: {} keys written to {}", written[0], snapshotPath);

            // ── Step 3: Write snapshot marker to AOF ───────────────────────
            if (aofWriter != null) {
                aofWriter.logSnapshotMarker(snapshotTime);
            }

        } catch (IOException e) {
            log.error("Snapshot failed — old snapshot preserved", e);
            // Clean up temp file to avoid confusion
            try {
                Files.deleteIfExists(tmpPath);
            } catch (IOException ignored) { /* best effort */ }
        }
    }

    // ── Snapshot load ─────────────────────────────────────────────────────────

    /**
     * Loads the snapshot file into the given {@link KVStore}.
     *
     * <p>Called at startup by {@link CrashRecovery}. Expired keys (whose
     * {@code expiresAt} has already passed) are silently skipped.
     * Malformed lines produce a warning but do not abort loading.
     *
     * @param targetStore the store to populate
     * @return the last-modified timestamp of the snapshot file in ms since epoch,
     *         or {@code -1} if the file does not exist
     * @throws IOException if the file exists but cannot be read
     */
    public long loadSnapshot(KVStore targetStore) throws IOException {
        if (!Files.exists(snapshotPath)) {
            log.info("No snapshot file at {} — nothing to load", snapshotPath);
            return -1;
        }

        long lastModified = Files.getLastModifiedTime(snapshotPath).toMillis();
        int[] counts = {0, 0};   // [loaded, skipped]

        log.info("Loading snapshot from {} (last modified: {})", snapshotPath, lastModified);

        try (BufferedReader reader = Files.newBufferedReader(snapshotPath)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) continue;

                // Format: key\tvalue\texpiresAt
                String[] parts = line.split("\t", 3);
                if (parts.length < 3) {
                    log.warn("Skipping malformed snapshot line (expected 3 tab-separated fields): '{}'", line);
                    counts[1]++;
                    continue;
                }

                String key   = parts[0];
                String value = parts[1];
                long expiresAt;
                try {
                    expiresAt = Long.parseLong(parts[2]);
                } catch (NumberFormatException e) {
                    log.warn("Skipping snapshot line with non-numeric expiresAt: '{}'", line);
                    counts[1]++;
                    continue;
                }

                // Compute remaining TTL for set()
                long ttlMs;
                if (expiresAt == -1L) {
                    ttlMs = -1;   // no expiry
                } else {
                    ttlMs = expiresAt - System.currentTimeMillis();
                    if (ttlMs <= 0) {
                        // Key was live when snapshot was taken, but has since expired
                        counts[1]++;
                        continue;
                    }
                }

                // Use direct KVStore.set() — does not go through CommandExecutor,
                // so no AOF logging happens during snapshot load (correct behaviour)
                targetStore.set(key, value, ttlMs);
                counts[0]++;
            }
        }

        log.info("Snapshot load complete: {} keys loaded, {} skipped (expired or malformed)",
            counts[0], counts[1]);
        return lastModified;
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    @Override
    public void close() {
        scheduler.shutdown();
    }
}
