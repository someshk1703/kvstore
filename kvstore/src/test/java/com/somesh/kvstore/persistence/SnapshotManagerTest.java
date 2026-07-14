package com.somesh.kvstore.persistence;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.somesh.kvstore.engine.KVStore;

/**
 * Tests for {@link SnapshotManager} — periodic full-keyspace serialisation.
 */
class SnapshotManagerTest {

    @TempDir
    Path tempDir;

    private Path snapshotPath;
    private KVStore store;

    @BeforeEach
    void setUp() {
        snapshotPath = tempDir.resolve("kvstore.rdb");
        store        = new KVStore();
    }

    // ══════════════════════════════════════════════════════════════════════
    // TEST 1 — Snapshot file is created by takeSnapshot()
    // ══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("takeSnapshot creates the snapshot file")
    void takeSnapshot_createsFile() throws IOException {
        store.set("k1", "v1");

        try (SnapshotManager sm = new SnapshotManager(snapshotPath, store, null)) {
            sm.takeSnapshot();
        }

        assertThat(snapshotPath).exists();
    }

    // ══════════════════════════════════════════════════════════════════════
    // TEST 2 — Snapshot format: tab-separated key/value/expiresAt
    // ══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("Snapshot file contains tab-separated key, value, expiresAt lines")
    void takeSnapshot_writesCorrectFormat() throws IOException {
        store.set("alpha", "one");
        store.set("beta",  "two");

        try (SnapshotManager sm = new SnapshotManager(snapshotPath, store, null)) {
            sm.takeSnapshot();
        }

        List<String> lines = Files.readAllLines(snapshotPath);
        assertThat(lines).hasSize(2);
        // Each line must have exactly 3 tab-separated fields
        for (String line : lines) {
            String[] parts = line.split("\t", 3);
            assertThat(parts).hasSize(3);
            // expiresAt should be -1 (no TTL) or a parseable long
            long expiresAt = Long.parseLong(parts[2]);
            assertThat(expiresAt).isEqualTo(-1L);   // no TTL was set
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    // TEST 3 — loadSnapshot restores all keys into a fresh store
    // ══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("loadSnapshot restores all keys exactly")
    void loadSnapshot_restoresAllKeys() throws IOException {
        store.set("city",    "Bangalore");
        store.set("country", "India");
        store.set("lang",    "Java");

        try (SnapshotManager sm = new SnapshotManager(snapshotPath, store, null)) {
            sm.takeSnapshot();
        }

        // Load into a fresh store
        KVStore fresh = new KVStore();
        try (SnapshotManager sm = new SnapshotManager(snapshotPath, fresh, null)) {
            sm.loadSnapshot(fresh);
        }

        assertThat(fresh.get("city")).isEqualTo("Bangalore");
        assertThat(fresh.get("country")).isEqualTo("India");
        assertThat(fresh.get("lang")).isEqualTo("Java");
    }

    // ══════════════════════════════════════════════════════════════════════
    // TEST 4 — Expired keys are not written to the snapshot
    // ══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("Expired keys are excluded from the snapshot")
    void takeSnapshot_expiredKeys_notIncluded() throws Exception {
        store.set("live",    "yes");
        store.set("expired", "no", 50);   // 50 ms TTL

        // Wait for the key to expire
        Thread.sleep(100);

        try (SnapshotManager sm = new SnapshotManager(snapshotPath, store, null)) {
            sm.takeSnapshot();
        }

        KVStore fresh = new KVStore();
        try (SnapshotManager sm = new SnapshotManager(snapshotPath, fresh, null)) {
            sm.loadSnapshot(fresh);
        }

        assertThat(fresh.get("live")).isEqualTo("yes");
        assertThat(fresh.get("expired")).isNull();
    }

    // ══════════════════════════════════════════════════════════════════════
    // TEST 5 — Keys with TTL: remaining TTL preserved through snapshot
    // ══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("Keys with TTL have a positive remaining TTL after snapshot load")
    void loadSnapshot_keyWithTtl_remainingTtlPositive() throws IOException {
        store.set("session", "abc", 60_000);   // 60 s TTL

        try (SnapshotManager sm = new SnapshotManager(snapshotPath, store, null)) {
            sm.takeSnapshot();
        }

        KVStore fresh = new KVStore();
        try (SnapshotManager sm = new SnapshotManager(snapshotPath, fresh, null)) {
            sm.loadSnapshot(fresh);
        }

        long ttl = fresh.ttl("session");
        assertThat(ttl).isPositive();
        assertThat(ttl).isLessThanOrEqualTo(60);
    }

    // ══════════════════════════════════════════════════════════════════════
    // TEST 6 — loadSnapshot returns -1 when no snapshot file exists
    // ══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("loadSnapshot returns -1 when the snapshot file does not exist")
    void loadSnapshot_noFile_returnsMinusOne() throws IOException {
        KVStore fresh = new KVStore();
        try (SnapshotManager sm = new SnapshotManager(snapshotPath, fresh, null)) {
            long result = sm.loadSnapshot(fresh);
            assertThat(result).isEqualTo(-1);
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    // TEST 7 — loadSnapshot skips malformed lines without aborting
    // ══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("loadSnapshot skips malformed lines and loads the rest")
    void loadSnapshot_malformedLine_skipsAndContinues() throws IOException {
        // Write a snapshot with one good line and one bad line
        Files.writeString(snapshotPath,
            "goodkey\tgoodval\t-1\n" +
            "MALFORMED_LINE_NO_TABS\n" +
            "goodkey2\tgoodval2\t-1\n");

        KVStore fresh = new KVStore();
        try (SnapshotManager sm = new SnapshotManager(snapshotPath, fresh, null)) {
            sm.loadSnapshot(fresh);
        }

        // Good lines loaded; malformed line skipped without exception
        assertThat(fresh.get("goodkey")).isEqualTo("goodval");
        assertThat(fresh.get("goodkey2")).isEqualTo("goodval2");
    }

    // ══════════════════════════════════════════════════════════════════════
    // TEST 8 — Atomic write: old snapshot preserved if write fails mid-way
    // ══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("Snapshot writes to temp file and renames — original preserved on failure")
    void takeSnapshot_atomicWrite_tempFileCleanedUp() throws IOException {
        store.set("k", "v");

        try (SnapshotManager sm = new SnapshotManager(snapshotPath, store, null)) {
            sm.takeSnapshot();
        }

        // Temp file must not exist after a successful snapshot
        Path tmpPath = snapshotPath.resolveSibling("snapshot.tmp");
        assertThat(tmpPath).doesNotExist();
        assertThat(snapshotPath).exists();
    }

    // ══════════════════════════════════════════════════════════════════════
    // TEST 9 — Snapshot marker written to AOF after snapshot
    // ══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("takeSnapshot writes a '# SNAPSHOT <ts>' marker to the AOF")
    void takeSnapshot_writesSnapshotMarkerToAof() throws IOException {
        Path aofPath = tempDir.resolve("kvstore.aof");
        store.set("key", "val");

        try (AOFWriter aof = new AOFWriter(aofPath, AOFWriter.FsyncMode.ALWAYS);
             SnapshotManager sm = new SnapshotManager(snapshotPath, store, aof)) {

            sm.takeSnapshot();
        }

        List<String> aofLines = Files.readAllLines(aofPath);
        assertThat(aofLines).hasSize(1);
        assertThat(aofLines.get(0)).startsWith("# SNAPSHOT ");
        // Marker timestamp must be a parseable long
        long ts = Long.parseLong(aofLines.get(0).substring("# SNAPSHOT ".length()).trim());
        assertThat(ts).isPositive();
    }

    // ══════════════════════════════════════════════════════════════════════
    // TEST 10 — Empty store produces an empty snapshot file
    // ══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("Snapshot of empty store produces an empty file")
    void takeSnapshot_emptyStore_emptyFile() throws IOException {
        try (SnapshotManager sm = new SnapshotManager(snapshotPath, store, null)) {
            sm.takeSnapshot();
        }

        assertThat(snapshotPath).exists();
        List<String> lines = Files.readAllLines(snapshotPath).stream()
            .filter(l -> !l.isBlank()).toList();
        assertThat(lines).isEmpty();
    }
}
