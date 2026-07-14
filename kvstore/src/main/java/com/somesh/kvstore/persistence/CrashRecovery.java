package com.somesh.kvstore.persistence;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.somesh.kvstore.engine.CommandExecutor;
import com.somesh.kvstore.engine.KVStore;

/**
 * Orchestrates startup recovery for the KV store.
 *
 * <h2>Recovery decision tree</h2>
 * <pre>
 * ┌────────────────────┬──────────────────────────────────────────────────────┐
 * │ Files present      │ Action                                               │
 * ├────────────────────┼──────────────────────────────────────────────────────┤
 * │ AOF + snapshot     │ Load snapshot; replay AOF entries after snapshot     │
 * │ AOF only           │ Replay full AOF                                      │
 * │ Snapshot only      │ Load snapshot                                        │
 * │ Neither            │ Start empty                                          │
 * └────────────────────┴──────────────────────────────────────────────────────┘
 * </pre>
 *
 * <h2>Why load snapshot before replaying AOF?</h2>
 * Replaying the full AOF from scratch is O(N) in the total number of write
 * commands ever executed. If the server has run for weeks, that could be
 * millions of commands. Loading a snapshot is O(K) where K is the current
 * live keyspace — much faster for long-running servers.
 *
 * The snapshot captures all state up to its timestamp. The AOF captures every
 * mutation, including mutations <em>after</em> the snapshot. Together they give
 * you the complete picture: snapshot as baseline, AOF as delta.
 *
 * <h2>The {@code # SNAPSHOT} marker</h2>
 * {@link AOFWriter} appends {@code # SNAPSHOT <timestamp_ms>} to the AOF after
 * each snapshot completes. {@code CrashRecovery} finds this marker to determine
 * which AOF lines post-date the snapshot. Lines before the marker are already
 * captured in the snapshot and must NOT be replayed (doing so would apply them
 * twice — e.g. incrementing a counter twice).
 *
 * <h2>Replay-mode executor</h2>
 * Recovery creates a fresh {@link CommandExecutor} with <em>no</em>
 * {@link AOFWriter} attached. This means replayed commands execute against the
 * {@link KVStore} without being re-logged to the AOF — which would be wrong
 * (they're already there) and would cause the file to grow unboundedly.
 *
 * <h2>Interview angles this covers</h2>
 * <ul>
 *   <li>Why load snapshot first instead of just replaying the full AOF?</li>
 *   <li>What is "double-application" of a command and why is it bad?</li>
 *   <li>What happens if recovery is interrupted (e.g. OOM during replay)?</li>
 * </ul>
 */
public class CrashRecovery {

    private static final Logger log = LoggerFactory.getLogger(CrashRecovery.class);

    private final Path aofPath;
    private final Path snapshotPath;

    // ── Constructor ───────────────────────────────────────────────────────────

    public CrashRecovery(Path aofPath, Path snapshotPath) {
        this.aofPath      = aofPath;
        this.snapshotPath = snapshotPath;
    }

    // ── Recovery ──────────────────────────────────────────────────────────────

    /**
     * Recovers the given {@link KVStore} from whatever persistence files are
     * present on disk. Should be called once at server startup, before any
     * client connections are accepted.
     *
     * <p>Uses a replay-mode {@link CommandExecutor} (no {@link AOFWriter}) so
     * replayed commands are not re-logged.
     *
     * @param store   the (initially empty) store to populate
     * @param aofWriter the active AOFWriter used to replay — passed to AOFWriter
     *                  only as the executor argument; replayed commands do NOT
     *                  get logged back to AOF
     * @throws IOException if a file that exists cannot be read
     */
    public void recover(KVStore store, AOFWriter aofWriter) throws IOException {
        boolean aofExists      = Files.exists(aofPath);
        boolean snapshotExists = Files.exists(snapshotPath);

        if (!aofExists && !snapshotExists) {
            log.info("No persistence files found — starting with empty store");
            return;
        }

        // Create a replay-mode executor: no AOFWriter, no SnapshotManager
        // Replayed commands execute against the store but are NOT re-logged
        CommandExecutor replayExecutor = new CommandExecutor(store);

        if (snapshotExists && aofExists) {
            recoverFromBoth(store, aofWriter, replayExecutor);
        } else if (aofExists) {
            recoverFromAofOnly(store, aofWriter, replayExecutor);
        } else {
            recoverFromSnapshotOnly(store);
        }
    }

    // ── Private recovery paths ────────────────────────────────────────────────

    /**
     * Both files present: load snapshot baseline, then replay AOF delta.
     */
    private void recoverFromBoth(KVStore store, AOFWriter aofWriter,
                                 CommandExecutor replayExecutor) throws IOException {
        log.info("Recovery: both snapshot and AOF present");

        // Load snapshot → captures state up to snapshot timestamp
        SnapshotManager sm = new SnapshotManager(snapshotPath, store, null);
        sm.loadSnapshot(store);
        sm.close();

        // Replay only AOF entries after the last # SNAPSHOT marker.
        // Using "last marker" is more robust than timestamp comparison:
        // the marker timestamp and the snapshot file's mtime may differ by
        // milliseconds, whereas the marker position in the AOF is exact.
        aofWriter.replayAfterLastSnapshot(store, replayExecutor);
    }

    /**
     * AOF only: replay the complete AOF from start to finish.
     */
    private void recoverFromAofOnly(KVStore store, AOFWriter aofWriter,
                                    CommandExecutor replayExecutor) throws IOException {
        log.info("Recovery: AOF only (no snapshot)");
        aofWriter.replay(store, replayExecutor);
    }

    /**
     * Snapshot only: load it directly into the store.
     */
    private void recoverFromSnapshotOnly(KVStore store) throws IOException {
        log.info("Recovery: snapshot only (no AOF)");
        SnapshotManager sm = new SnapshotManager(snapshotPath, store, null);
        sm.loadSnapshot(store);
        sm.close();
    }
}
