package com.somesh.kvstore.engine;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for KVStore — the core storage engine.
 *
 * Test philosophy applied here:
 *
 *   1. One assertion focus per test — if a test fails you know exactly what broke.
 *      A test named "set_then_get_returns_value" should only care about that behaviour,
 *      not also check DEL. Multi-concern tests produce ambiguous failure signals.
 *
 *   2. Arrange / Act / Assert structure in every test — three visual blocks separated
 *      by blank lines. Arrange: set up state. Act: call the thing under test.
 *      Assert: verify the result. Makes tests readable without comments.
 *
 *   3. Test names describe behaviour, not implementation —
 *      "get_missingKey_returnsNull" not "testGet1". Failing test name = free
 *      bug report.
 *
 *   4. Fresh store per test (@BeforeEach) — tests must not share state. A test
 *      that depends on another test's side effects is not a unit test.
 *
 *   5. Avoid Thread.sleep() where possible; use it honestly where unavoidable
 *      (TTL expiry) and document exactly why.
 */
class KVStoreTest {

    // ── Test subject ─────────────────────────────────────────────────────────
    // Fresh instance for every test — no state leaks between tests.
    // @BeforeEach runs before every single @Test method.
    private KVStore store;

    @BeforeEach
    void setUp() {
        store = new KVStore();
    }


    // ════════════════════════════════════════════════════════════════════════
    // TEST 1 — SET then GET returns the stored value
    //
    // The most fundamental contract: what you write, you can read back.
    // If this fails, nothing else matters.
    // ════════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("SET then GET returns the stored value")
    void set_thenGet_returnsStoredValue() {
        // Arrange
        String key   = "username";
        String value = "somesh";

        // Act
        store.set(key, value);
        String result = store.get(key);

        // Assert
        assertThat(result)
            .as("GET after SET should return the stored value")
            .isEqualTo(value);
    }

    @Test
    @DisplayName("GET on a missing key returns null")
    void get_missingKey_returnsNull() {
        // No arrange — store is empty

        // Act
        String result = store.get("nonexistent-key");

        // Assert
        // isNull() produces a better failure message than isEqualTo(null):
        // "expecting actual to be null but was: <...>"  vs a generic NPE-style message
        assertThat(result)
            .as("GET on a key that was never SET should return null")
            .isNull();
    }


    // ════════════════════════════════════════════════════════════════════════
    // TEST 2 — DEL returns 1 when key exists, 0 when it doesn't
    //
    // Redis DEL returns the count of deleted keys.
    // These are two separate behaviours — worth two separate tests.
    // ════════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("DEL on existing key returns 1")
    void del_existingKey_returnsOne() {
        // Arrange
        store.set("city", "chennai");

        // Act
        int deleted = store.del("city");

        // Assert
        assertThat(deleted)
            .as("DEL on an existing key should return 1")
            .isEqualTo(1);
    }

    @Test
    @DisplayName("DEL on missing key returns 0")
    void del_missingKey_returnsZero() {
        // No arrange

        // Act
        int deleted = store.del("ghost-key");

        // Assert
        assertThat(deleted)
            .as("DEL on a key that never existed should return 0")
            .isEqualTo(0);
    }

    @Test
    @DisplayName("GET after DEL returns null")
    void get_afterDel_returnsNull() {
        // Arrange
        store.set("session", "abc123");

        // Act
        store.del("session");
        String result = store.get("session");

        // Assert
        assertThat(result)
            .as("GET on a DELeted key should return null, not the old value")
            .isNull();
    }

    @Test
    @DisplayName("DEL on multiple keys returns count of those that existed")
    void del_multipleKeys_returnsCorrectDeletedCount() {
        // Arrange — only 2 of 3 keys exist
        store.set("k1", "v1");
        store.set("k2", "v2");
        // "k3" is never set

        // Act
        int deleted = store.del("k1", "k2", "k3");

        // Assert — only 2 should count
        assertThat(deleted)
            .as("DEL k1 k2 k3 where k3 doesn't exist should return 2, not 3")
            .isEqualTo(2);
    }


    // ════════════════════════════════════════════════════════════════════════
    // TEST 3 — SET on an existing key returns the OLD value (overwrite)
    //
    // The overwrite semantics: the new value replaces the old one, and
    // set() returns what was there before so callers have access to both.
    //
    // This is the GETSET pattern — useful for atomic swap operations.
    // Redis has a dedicated GETSET command. We expose it via set()'s return value.
    // ════════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("Overwriting a key returns the previous value")
    void set_overwriteExistingKey_returnsPreviousValue() {
        // Arrange
        store.set("counter", "1");

        // Act — overwrite with a new value
        String previousValue = store.set("counter", "2");

        // Assert — return value is the OLD value
        assertThat(previousValue)
            .as("SET on an existing key should return the previous value")
            .isEqualTo("1");
    }

    @Test
    @DisplayName("GET after overwrite returns the NEW value")
    void get_afterOverwrite_returnsNewValue() {
        // Arrange
        store.set("env", "dev");

        // Act
        store.set("env", "prod");
        String current = store.get("env");

        // Assert — store holds the new value
        assertThat(current)
            .as("GET after overwrite should return the new value, not the old one")
            .isEqualTo("prod");
    }

    @Test
    @DisplayName("SET on a new key returns null (no previous value)")
    void set_newKey_returnsNull() {
        // Act — first SET on this key
        String previous = store.set("brand-new-key", "brand-new-value");

        // Assert — nothing existed before
        assertThat(previous)
            .as("SET on a key that never existed should return null, not empty string")
            .isNull();
    }


    // ════════════════════════════════════════════════════════════════════════
    // TEST 4 — Missing key (never set) returns null from GET
    //
    // Already covered in the first test group. Adding a dedicated block here
    // to show the separation of concerns: "missing" vs "expired" both return
    // null, but they're different code paths internally. Test both explicitly.
    // ════════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("GET on a key that was set then deleted returns null")
    void get_deletedKey_returnsNull() {
        // Arrange
        store.set("temp", "value");

        // Act
        store.del("temp");
        String result = store.get("temp");

        // Assert
        assertThat(result).isNull();
    }

    @Test
    @DisplayName("EXISTS returns false for a missing key")
    void exists_missingKey_returnsFalse() {
        assertThat(store.exists("no-such-key")).isFalse();
    }

    @Test
    @DisplayName("EXISTS returns true for a present key")
    void exists_presentKey_returnsTrue() {
        store.set("alive", "yes");
        assertThat(store.exists("alive")).isTrue();
    }


    // ════════════════════════════════════════════════════════════════════════
    // TEST 5 — TTL expiry: key returns null after TTL elapses
    //
    // This is the hardest test to write correctly. Three problems to solve:
    //
    // Problem 1: Thread.sleep() makes tests flaky
    //   Sleep(100) on a loaded CI machine might not actually sleep 100ms.
    //   Mitigation: use a short TTL (50ms) and a generous sleep (200ms),
    //   giving 4× headroom. Still occasionally flaky but acceptable for unit tests.
    //   Production solution: inject a Clock interface and mock time in tests.
    //
    // Problem 2: What are we actually testing?
    //   Not Thread.sleep(). Not system scheduling. We're testing that:
    //   (a) ValueEntry.isExpired() correctly reports expiry after TTL
    //   (b) KVStore.get() performs lazy deletion when it detects expiry
    //   Testing (a) in isolation via ValueEntry tests is cleaner.
    //   Testing (b) here with a real sleep is the integration check.
    //
    // Problem 3: Test must not be order-dependent
    //   @BeforeEach gives us a fresh store. Sleep duration must not bleed
    //   into the next test. JUnit 5 runs tests in a deterministic but
    //   unspecified order by default — each test is fully isolated.
    // ════════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("GET returns null after TTL elapses (lazy expiry)")
    void get_afterTtlExpiry_returnsNull() throws InterruptedException {
        // Arrange — 50ms TTL: short enough that tests run fast,
        // long enough to not expire during the SET call itself
        long ttlMs = 50;
        store.set("ephemeral", "i-will-vanish", ttlMs);

        // Sanity check: key is accessible immediately after SET
        assertThat(store.get("ephemeral"))
            .as("Key should exist immediately after SET with TTL")
            .isEqualTo("i-will-vanish");

        // Act — sleep well past the TTL
        // 200ms = 4× the TTL. Even on a slow CI box, 50ms will have
        // elapsed after 200ms of wall-clock sleep.
        Thread.sleep(200);

        // Assert — lazy expiry: KVStore.get() checks isExpired() and
        // removes the entry on this very call
        assertThat(store.get("ephemeral"))
            .as("GET should return null after TTL elapses — lazy expiry must fire")
            .isNull();
    }

    @Test
    @DisplayName("Expired key is removed from store (lazy deletion side-effect)")
    void get_afterTtlExpiry_removesKeyFromStore() throws InterruptedException {
        // Arrange
        store.set("ghost", "boo", 50);
        Thread.sleep(200);

        // Act — this GET triggers lazy deletion
        store.get("ghost");

        // Assert — the entry is gone, not just returning null
        // size() checks the underlying ConcurrentHashMap entry count
        assertThat(store.size())
            .as("After lazy deletion, the key should be removed from the store entirely")
            .isEqualTo(0);
    }

    @Test
    @DisplayName("Key with no TTL (-1) does not expire")
    void get_noTtl_neverExpires() throws InterruptedException {
        // Arrange — explicit -1 TTL = no expiry
        store.set("permanent", "forever", -1);

        // Act — wait a bit (not that it matters — but proves it's not timing-sensitive)
        Thread.sleep(100);

        // Assert
        assertThat(store.get("permanent"))
            .as("Key with no TTL should never expire regardless of time elapsed")
            .isEqualTo("forever");
    }

    @Test
    @DisplayName("PTTL returns -2 after TTL elapses")
    void pttl_afterTtlExpiry_returnsMinusTwo() throws InterruptedException {
        // Arrange
        store.set("countdown", "tick", 50);
        Thread.sleep(200);

        // Act
        long ttl = store.pttl("countdown");

        // Assert — Redis convention: -2 = key does not exist (or has expired)
        assertThat(ttl)
            .as("PTTL on an expired key should return -2 (not found / expired)")
            .isEqualTo(-2L);
    }

    @Test
    @DisplayName("PTTL returns -1 for a key with no expiry")
    void pttl_noExpiry_returnsMinusOne() {
        store.set("immortal", "lives-forever");

        assertThat(store.pttl("immortal"))
            .as("PTTL on a key with no TTL set should return -1")
            .isEqualTo(-1L);
    }

    @Test
    @DisplayName("PTTL returns positive millis for a key not yet expired")
    void pttl_liveKey_returnsPositiveMillis() {
        // Arrange — 10 second TTL: plenty of time for this assertion
        store.set("alive", "yes", 10_000);

        // Act
        long remaining = store.pttl("alive");

        // Assert — remaining should be positive and reasonably close to 10000ms
        assertThat(remaining)
            .as("PTTL on a live key should return positive milliseconds")
            .isGreaterThan(0)
            .isLessThanOrEqualTo(10_000);
    }


    // ════════════════════════════════════════════════════════════════════════
    // BONUS — Concurrency test
    //
    // Not one of the 5 required tests, but this is the test that proves
    // ConcurrentHashMap is doing its job. 10 threads each doing 100 writes
    // and 100 reads — if there's a data race, size() or get() will be wrong.
    //
    // This is the test you show an interviewer when they ask "how do you
    // know your store is actually thread-safe?"
    // ════════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("Concurrent SET operations from multiple threads produce no data loss")
    void set_concurrentWrites_noDataLoss() throws InterruptedException {
        int threadCount = 10;
        int writesPerThread = 100;

        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        List<Future<?>> futures = new ArrayList<>();

        // Each thread writes its own keyed range — no intentional key collisions.
        // "thread-0-key-0" through "thread-9-key-99" = 1000 unique keys.
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            futures.add(pool.submit(() -> {
                for (int i = 0; i < writesPerThread; i++) {
                    String key   = "thread-" + threadId + "-key-" + i;
                    String value = "value-" + threadId + "-" + i;
                    store.set(key, value);
                }
            }));
        }

        // Wait for all threads to complete
        for (Future<?> f : futures) {
            try {
                f.get(5, TimeUnit.SECONDS);
            } catch (ExecutionException | TimeoutException e) {
                fail("Thread threw an exception during concurrent writes: " + e.getMessage());
            }
        }

        pool.shutdown();
        pool.awaitTermination(5, TimeUnit.SECONDS);

        // Assert — all 1000 writes survived
        int expectedSize = threadCount * writesPerThread;
        assertThat(store.size())
            .as("After %d threads each writing %d keys, store should hold %d entries",
                threadCount, writesPerThread, expectedSize)
            .isEqualTo(expectedSize);

        // Spot-check a few values are correct — not corrupted or lost
        assertThat(store.get("thread-0-key-0")).isEqualTo("value-0-0");
        assertThat(store.get("thread-5-key-50")).isEqualTo("value-5-50");
        assertThat(store.get("thread-9-key-99")).isEqualTo("value-9-99");
    }

    @Test
    @DisplayName("Concurrent SET and GET on the same key never returns corrupted data")
    void setAndGet_concurrent_noCorruption() throws InterruptedException {
        // One writer thread continuously updates a key.
        // Two reader threads continuously read it.
        // Neither reader should ever see null (the key always has a value)
        // nor a value that wasn't written by the writer.

        String key = "hotkey";
        store.set(key, "initial");

        List<String> validValues = List.of("initial", "update-1", "update-2", "update-3");
        List<String> invalidValues = new CopyOnWriteArrayList<>();

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch  = new CountDownLatch(3);

        // Writer thread
        Thread writer = new Thread(() -> {
            try {
                startLatch.await();
                store.set(key, "update-1");
                store.set(key, "update-2");
                store.set(key, "update-3");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                doneLatch.countDown();
            }
        });

        // Reader threads
        for (int i = 0; i < 2; i++) {
            Thread reader = new Thread(() -> {
                try {
                    startLatch.await();
                    for (int r = 0; r < 50; r++) {
                        String seen = store.get(key);
                        if (seen != null && !validValues.contains(seen)) {
                            invalidValues.add(seen);  // CopyOnWriteArrayList — thread-safe
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
            reader.start();
        }

        writer.start();
        startLatch.countDown();  // fire all threads simultaneously
        doneLatch.await(5, TimeUnit.SECONDS);

        assertThat(invalidValues)
            .as("Readers should only ever see values written by the writer, never corrupted data")
            .isEmpty();
    }
}