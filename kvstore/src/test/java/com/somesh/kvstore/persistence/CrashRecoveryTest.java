package com.somesh.kvstore.persistence;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.somesh.kvstore.engine.KVStore;
import com.somesh.kvstore.persistence.AOFWriter.FsyncMode;

/**
 * Tests for {@link CrashRecovery} — the startup recovery orchestrator.
 *
 * Covers all four startup paths:
 *   1. Both AOF and snapshot present
 *   2. AOF only
 *   3. Snapshot only
 *   4. Neither (start empty)
 *
 * And two critical correctness properties:
 *   - No double-application of commands already in the snapshot
 *   - DEL in AOF post-dating a snapshot correctly removes the key
 */
class CrashRecoveryTest {

    @TempDir
    Path tempDir;

    private Path aofPath;
    private Path snapshotPath;

    @BeforeEach
    void setUp() {
        aofPath      = tempDir.resolve("kvstore.aof");
        snapshotPath = tempDir.resolve("kvstore.rdb");
    }

    // ══════════════════════════════════════════════════════════════════════
    // TEST 1 — Neither file: starts empty
    // ══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("When neither AOF nor snapshot exists, store starts empty")
    void recover_neitherFile_startsEmpty() throws IOException {
        KVStore store = new KVStore();
        CrashRecovery cr = new CrashRecovery(aofPath, snapshotPath);

        try (AOFWriter aof = new AOFWriter(aofPath, FsyncMode.ALWAYS)) {
            cr.recover(store, aof);
        }

        // No AOF file should have been created by just recovering on an empty state
        // Store should be empty
        assertThat(store.size()).isZero();
    }

    // ══════════════════════════════════════════════════════════════════════
    // TEST 2 — AOF only: full AOF replay
    // ══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("When only AOF exists, full AOF is replayed")
    void recover_aofOnly_fullReplay() throws IOException {
        // Write AOF with 3 commands
        try (AOFWriter setup = new AOFWriter(aofPath, FsyncMode.ALWAYS)) {
            setup.log("SET persist1 hello");
            setup.log("SET persist2 world");
            setup.log("SET persist3 java");
        }

        // Recover
        KVStore store = new KVStore();
        CrashRecovery cr = new CrashRecovery(aofPath, snapshotPath);

        try (AOFWriter aof = new AOFWriter(aofPath, FsyncMode.ALWAYS)) {
            cr.recover(store, aof);
        }

        assertThat(store.get("persist1")).isEqualTo("hello");
        assertThat(store.get("persist2")).isEqualTo("world");
        assertThat(store.get("persist3")).isEqualTo("java");
    }

    // ══════════════════════════════════════════════════════════════════════
    // TEST 3 — Snapshot only: snapshot is loaded
    // ══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("When only snapshot exists, snapshot is loaded")
    void recover_snapshotOnly_loadsSnapshot() throws IOException {
        // Write snapshot with 2 keys
        KVStore setupStore = new KVStore();
        setupStore.set("snap1", "alpha");
        setupStore.set("snap2", "beta");

        try (SnapshotManager sm = new SnapshotManager(snapshotPath, setupStore, null)) {
            sm.takeSnapshot();
        }

        // Recover (no AOF)
        KVStore store = new KVStore();
        CrashRecovery cr = new CrashRecovery(aofPath, snapshotPath);

        try (AOFWriter aof = new AOFWriter(aofPath, FsyncMode.ALWAYS)) {
            cr.recover(store, aof);
        }

        assertThat(store.get("snap1")).isEqualTo("alpha");
        assertThat(store.get("snap2")).isEqualTo("beta");
        // AOF was created by opening, but snapshot didn't trigger so it should be empty
        // This is acceptable — the AOF will capture future writes
    }

    // ══════════════════════════════════════════════════════════════════════
    // TEST 4 — Both files: snapshot + AOF delta
    // ══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("When both files exist, snapshot loaded then AOF delta replayed")
    void recover_bothFiles_snapshotPlusDeltaReplayed() throws IOException {
        long snapshotTs = System.currentTimeMillis() - 1000;   // 1 second ago

        // Snapshot has snap1, snap2
        KVStore setupStore = new KVStore();
        setupStore.set("snap1", "alpha");
        setupStore.set("snap2", "beta");

        try (SnapshotManager sm = new SnapshotManager(snapshotPath, setupStore, null)) {
            sm.takeSnapshot();
        }

        // AOF has: pre-snapshot commands (should be skipped) + marker + post-snapshot
        try (AOFWriter setup = new AOFWriter(aofPath, FsyncMode.ALWAYS)) {
            setup.log("SET snap1 old_val");         // pre-snapshot — should be skipped
            setup.logSnapshotMarker(snapshotTs);    // snapshot marker
            setup.log("SET after1 new");            // post-snapshot — MUST be replayed
            setup.log("SET after2 newer");          // post-snapshot — MUST be replayed
        }

        // Recover
        KVStore store = new KVStore();
        CrashRecovery cr = new CrashRecovery(aofPath, snapshotPath);

        try (AOFWriter aof = new AOFWriter(aofPath, FsyncMode.ALWAYS)) {
            cr.recover(store, aof);
        }

        // Snapshot keys present
        assertThat(store.get("snap1")).isEqualTo("alpha");   // snapshot value wins
        assertThat(store.get("snap2")).isEqualTo("beta");
        // Post-snapshot delta applied
        assertThat(store.get("after1")).isEqualTo("new");
        assertThat(store.get("after2")).isEqualTo("newer");
    }

    // ══════════════════════════════════════════════════════════════════════
    // TEST 5 — DEL in AOF post-dating snapshot removes the key
    // ══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("DEL in AOF post-dating snapshot removes the key after recovery")
    void recover_delAfterSnapshot_keyAbsent() throws IOException {
        long snapshotTs = System.currentTimeMillis() - 1000;

        // Snapshot has 'condemned'
        KVStore setupStore = new KVStore();
        setupStore.set("condemned", "soon_gone");
        setupStore.set("survivor",  "stays");

        try (SnapshotManager sm = new SnapshotManager(snapshotPath, setupStore, null)) {
            sm.takeSnapshot();
        }

        // AOF: snapshot marker, then DEL of 'condemned'
        try (AOFWriter setup = new AOFWriter(aofPath, FsyncMode.ALWAYS)) {
            setup.logSnapshotMarker(snapshotTs);
            setup.log("DEL condemned");
        }

        // Recover
        KVStore store = new KVStore();
        CrashRecovery cr = new CrashRecovery(aofPath, snapshotPath);

        try (AOFWriter aof = new AOFWriter(aofPath, FsyncMode.ALWAYS)) {
            cr.recover(store, aof);
        }

        assertThat(store.get("condemned")).isNull();
        assertThat(store.get("survivor")).isEqualTo("stays");
    }

    // ══════════════════════════════════════════════════════════════════════
    // TEST 6 — Truncated last line in AOF doesn't abort recovery
    // ══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("Truncated last AOF line is skipped and recovery completes")
    void recover_truncatedAofLine_recoveryCompletes() throws IOException {
        // Write one good command, then a truncated/malformed one
        try (AOFWriter setup = new AOFWriter(aofPath, FsyncMode.ALWAYS)) {
            setup.log("SET goodkey goodval");
        }
        Files.writeString(aofPath, "PARTIAL_COM",
            java.nio.file.StandardOpenOption.APPEND);

        // Recover — must not throw
        KVStore store = new KVStore();
        CrashRecovery cr = new CrashRecovery(aofPath, snapshotPath);

        try (AOFWriter aof = new AOFWriter(aofPath, FsyncMode.ALWAYS)) {
            cr.recover(store, aof);
        }

        // Good key recovered
        assertThat(store.get("goodkey")).isEqualTo("goodval");
    }

    // ══════════════════════════════════════════════════════════════════════
    // TEST 7 — Replayed commands are NOT re-logged to AOF
    // ══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("Recovery does not write replayed commands back into the AOF")
    void recover_replayMode_noRelogging() throws IOException {
        // Write AOF with 2 commands
        try (AOFWriter setup = new AOFWriter(aofPath, FsyncMode.ALWAYS)) {
            setup.log("SET foo bar");
            setup.log("SET baz qux");
        }

        long linesBefore = Files.lines(aofPath).count();

        // Recover — replayMode executor has no AOFWriter
        KVStore store = new KVStore();
        CrashRecovery cr = new CrashRecovery(aofPath, snapshotPath);

        try (AOFWriter aof = new AOFWriter(aofPath, FsyncMode.ALWAYS)) {
            cr.recover(store, aof);
        }

        // The AOF file should have the same number of lines
        long linesAfter = Files.lines(aofPath).count();
        assertThat(linesAfter).isEqualTo(linesBefore);
    }
}
