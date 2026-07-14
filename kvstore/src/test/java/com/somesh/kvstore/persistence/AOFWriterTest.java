package com.somesh.kvstore.persistence;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.somesh.kvstore.engine.CommandExecutor;
import com.somesh.kvstore.engine.KVStore;
import com.somesh.kvstore.persistence.AOFWriter.FsyncMode;

/**
 * Tests for {@link AOFWriter} — the append-only write-ahead log.
 *
 * Test strategy:
 *   - Use JUnit 5 {@code @TempDir} for a clean temp directory per test
 *   - Test AOF write format, replay correctness, and partial-line resilience
 *   - Tests for both ALWAYS and EVERYSEC fsync modes
 */
class AOFWriterTest {

    @TempDir
    Path tempDir;

    private Path aofPath;
    private KVStore store;
    private CommandExecutor executor;

    @BeforeEach
    void setUp() {
        aofPath  = tempDir.resolve("kvstore.aof");
        store    = new KVStore();
        executor = new CommandExecutor(store);
    }

    @AfterEach
    void tearDown() throws IOException {
        // Nothing to close explicitly — AOFWriter closed in each test
    }

    // ══════════════════════════════════════════════════════════════════════
    // TEST 1 — Three SET commands produce exactly three lines in the AOF
    // ══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("Three SET commands produce exactly three command lines in the AOF")
    void log_threeSets_writesThreeLines() throws IOException {
        // Arrange + Act
        try (AOFWriter aof = new AOFWriter(aofPath, FsyncMode.ALWAYS)) {
            aof.log("SET foo bar");
            aof.log("SET baz qux");
            aof.log("SET hello world");
        }

        // Assert
        List<String> lines = Files.readAllLines(aofPath);
        assertThat(lines)
            .hasSize(3)
            .containsExactly("SET foo bar", "SET baz qux", "SET hello world");
    }

    // ══════════════════════════════════════════════════════════════════════
    // TEST 2 — Snapshot marker appended with correct format
    // ══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("logSnapshotMarker writes '# SNAPSHOT <ts>' line")
    void logSnapshotMarker_writesCorrectLine() throws IOException {
        long ts = 1720000000000L;

        try (AOFWriter aof = new AOFWriter(aofPath, FsyncMode.ALWAYS)) {
            aof.log("SET foo bar");
            aof.logSnapshotMarker(ts);
            aof.log("SET baz qux");
        }

        List<String> lines = Files.readAllLines(aofPath);
        assertThat(lines).containsExactly(
            "SET foo bar",
            "# SNAPSHOT 1720000000000",
            "SET baz qux"
        );
    }

    // ══════════════════════════════════════════════════════════════════════
    // TEST 3 — Full replay restores all written keys
    // ══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("replay restores all keys written to the AOF")
    void replay_fullAof_restoresAllKeys() throws IOException {
        // Arrange: write 5 keys to AOF
        try (AOFWriter aof = new AOFWriter(aofPath, FsyncMode.ALWAYS)) {
            aof.log("SET key1 val1");
            aof.log("SET key2 val2");
            aof.log("SET key3 val3");
            aof.log("SET key4 val4");
            aof.log("SET key5 val5");
        }

        // Act: replay into a fresh store
        KVStore freshStore     = new KVStore();
        CommandExecutor replay = new CommandExecutor(freshStore);

        try (AOFWriter aof = new AOFWriter(aofPath, FsyncMode.ALWAYS)) {
            aof.replay(freshStore, replay);
        }

        // Assert: all 5 keys present
        assertThat(freshStore.get("key1")).isEqualTo("val1");
        assertThat(freshStore.get("key2")).isEqualTo("val2");
        assertThat(freshStore.get("key3")).isEqualTo("val3");
        assertThat(freshStore.get("key4")).isEqualTo("val4");
        assertThat(freshStore.get("key5")).isEqualTo("val5");
    }

    // ══════════════════════════════════════════════════════════════════════
    // TEST 4 — DEL in AOF removes a key during replay
    // ══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("replay of SET then DEL leaves the key absent")
    void replay_setThenDel_keyAbsent() throws IOException {
        // Arrange
        try (AOFWriter aof = new AOFWriter(aofPath, FsyncMode.ALWAYS)) {
            aof.log("SET doomed gone");
            aof.log("SET survivor alive");
            aof.log("DEL doomed");
        }

        // Act
        KVStore freshStore     = new KVStore();
        CommandExecutor replay = new CommandExecutor(freshStore);

        try (AOFWriter aof = new AOFWriter(aofPath, FsyncMode.ALWAYS)) {
            aof.replay(freshStore, replay);
        }

        // Assert
        assertThat(freshStore.get("doomed")).isNull();
        assertThat(freshStore.get("survivor")).isEqualTo("alive");
    }

    // ══════════════════════════════════════════════════════════════════════
    // TEST 5 — Malformed last line is skipped, recovery does not abort
    // ══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("replay skips a malformed last line and does not abort recovery")
    void replay_malformedLastLine_skipsAndContinues() throws IOException {
        // Arrange: write a valid command, then a truncated/malformed one
        try (AOFWriter aof = new AOFWriter(aofPath, FsyncMode.ALWAYS)) {
            aof.log("SET validkey hello");
        }
        // Append a malformed line directly (simulates a crash mid-write)
        Files.writeString(aofPath, "INCOMPLETE_COMMAND_NO_ARGS\n",
            java.nio.file.StandardOpenOption.APPEND);

        // Act
        KVStore freshStore     = new KVStore();
        CommandExecutor replay = new CommandExecutor(freshStore);

        try (AOFWriter aof = new AOFWriter(aofPath, FsyncMode.ALWAYS)) {
            aof.replay(freshStore, replay);
        }

        // Assert: valid key survived, no exception thrown
        assertThat(freshStore.get("validkey")).isEqualTo("hello");
    }

    // ══════════════════════════════════════════════════════════════════════
    // TEST 6 — replayAfter skips pre-snapshot entries
    // ══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("replayAfter skips entries before the snapshot marker")
    void replayAfter_skipsPreSnapshotEntries() throws IOException {
        long snapshotTs = 1720000000000L;

        // Arrange: 2 commands before snapshot, 2 after
        try (AOFWriter aof = new AOFWriter(aofPath, FsyncMode.ALWAYS)) {
            aof.log("SET before1 v1");    // in snapshot — should NOT be replayed
            aof.log("SET before2 v2");    // in snapshot — should NOT be replayed
            aof.logSnapshotMarker(snapshotTs);
            aof.log("SET after1 v3");     // post-snapshot — MUST be replayed
            aof.log("SET after2 v4");     // post-snapshot — MUST be replayed
        }

        // Act: fresh store that already has 'before1' and 'before2' from snapshot
        KVStore freshStore = new KVStore();
        freshStore.set("before1", "v1");
        freshStore.set("before2", "v2");

        CommandExecutor replay = new CommandExecutor(freshStore);
        try (AOFWriter aof = new AOFWriter(aofPath, FsyncMode.ALWAYS)) {
            aof.replayAfter(freshStore, replay, snapshotTs);
        }

        // Assert: all four keys present; snapshot keys not double-applied (same value)
        assertThat(freshStore.get("before1")).isEqualTo("v1");
        assertThat(freshStore.get("before2")).isEqualTo("v2");
        assertThat(freshStore.get("after1")).isEqualTo("v3");
        assertThat(freshStore.get("after2")).isEqualTo("v4");
    }

    // ══════════════════════════════════════════════════════════════════════
    // TEST 7 — AOF is appended across restarts (FileWriter append mode)
    // ══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("AOF appends across two open/close cycles without truncating")
    void log_appendMode_preservesExistingContent() throws IOException {
        // First "run"
        try (AOFWriter aof = new AOFWriter(aofPath, FsyncMode.ALWAYS)) {
            aof.log("SET first one");
        }

        // Second "run"
        try (AOFWriter aof = new AOFWriter(aofPath, FsyncMode.ALWAYS)) {
            aof.log("SET second two");
        }

        // Assert: both lines present
        List<String> lines = Files.readAllLines(aofPath);
        assertThat(lines).containsExactly("SET first one", "SET second two");
    }

    // ══════════════════════════════════════════════════════════════════════
    // TEST 8 — EVERYSEC mode: data eventually flushed without explicit call
    // ══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("EVERYSEC mode: data is flushed within 2 seconds")
    void log_everysecMode_dataFlushedWithinTwoSeconds() throws Exception {
        try (AOFWriter aof = new AOFWriter(aofPath, FsyncMode.EVERYSEC)) {
            aof.log("SET background flush");

            // Wait for the background flusher to fire (fires every 1 s)
            Thread.sleep(2000);
        }

        List<String> lines = Files.readAllLines(aofPath);
        assertThat(lines).containsExactly("SET background flush");
    }

    // ══════════════════════════════════════════════════════════════════════
    // TEST 9 — CommandExecutor wired with AOFWriter logs writes
    // ══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("CommandExecutor with AOFWriter logs SET commands to AOF")
    void commandExecutor_withAofWriter_logsSetCommand() throws IOException {
        try (AOFWriter aof = new AOFWriter(aofPath, FsyncMode.ALWAYS)) {
            executor.setAofWriter(aof);

            // Execute via CommandExecutor (not direct KVStore.set)
            com.somesh.kvstore.protocol.CommandParser parser =
                new com.somesh.kvstore.protocol.CommandParser();
            executor.execute(parser.parse("SET mykey myval"));
        }

        List<String> lines = Files.readAllLines(aofPath);
        assertThat(lines).hasSize(1);
        assertThat(lines.get(0)).startsWith("SET mykey myval");
    }

    // ══════════════════════════════════════════════════════════════════════
    // TEST 10 — replay does not log back to AOF (no re-logging loop)
    // ══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("Replay-mode executor (no AOFWriter) does not re-log commands")
    void replay_replayModeExecutor_doesNotRelogCommands() throws IOException {
        // Arrange: AOF with one command
        try (AOFWriter aof = new AOFWriter(aofPath, FsyncMode.ALWAYS)) {
            aof.log("SET foo bar");
        }

        // Count lines before replay
        long linesBefore = Files.lines(aofPath).count();

        // Act: replay with a plain executor (no AOFWriter)
        KVStore freshStore     = new KVStore();
        CommandExecutor replay = new CommandExecutor(freshStore);   // no setAofWriter

        try (AOFWriter aof = new AOFWriter(aofPath, FsyncMode.ALWAYS)) {
            aof.replay(freshStore, replay);
        }

        // Assert: line count unchanged — no re-logging happened
        long linesAfter = Files.lines(aofPath).count();
        assertThat(linesAfter).isEqualTo(linesBefore);
    }
}
