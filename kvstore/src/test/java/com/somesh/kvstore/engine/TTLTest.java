package com.somesh.kvstore.engine;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for TTL-related behaviour on KVStore.
 *
 * Tests cover:
 *   - SET with EX creates an expiring key
 *   - Lazy expiry on GET after TTL elapses
 *   - EXPIRE command sets TTL on an existing key
 *   - TTL command returns correct remaining time
 *   - PERSIST removes the TTL
 *   - Edge cases: missing key, no-expiry key
 *
 * Why Thread.sleep() here?
 *   TTL expiry is inherently time-dependent. Short sleeps (≤ 2s) are acceptable
 *   in tests that validate time-sensitive logic; they are not a testing anti-pattern
 *   when the behaviour under test is time itself.
 */
class TTLTest {

    private KVStore store;

    @BeforeEach
    void setUp() {
        store = new KVStore();
    }

    // ─── SET with EX ─────────────────────────────────────────────────────────

    @Test
    @DisplayName("SET key value EX 1 — key readable before expiry")
    void set_withTtl_keyReadableBeforeExpiry() {
        store.set("greeting", "hello", 5_000);   // 5 seconds

        assertThat(store.get("greeting")).isEqualTo("hello");
    }

    @Test
    @DisplayName("SET key value EX 1 — key returns null after expiry (lazy check)")
    void set_withTtl_returnsNullAfterExpiry() throws InterruptedException {
        store.set("temp", "value", 100);   // 100 ms TTL

        Thread.sleep(150);   // let the TTL elapse

        // Lazy expiry: KVStore.get() checks isExpired() and removes the entry.
        assertThat(store.get("temp"))
            .as("expired key should return null")
            .isNull();
    }

    @Test
    @DisplayName("SET without TTL — key never expires")
    void set_withoutTtl_keyNeverExpires() throws InterruptedException {
        store.set("permanent", "stays");   // no TTL

        Thread.sleep(100);

        assertThat(store.get("permanent"))
            .as("key with no TTL should still be present after 100ms")
            .isEqualTo("stays");
    }

    // ─── EXPIRE ──────────────────────────────────────────────────────────────

    @Test
    @DisplayName("EXPIRE on existing key — returns 1 and key expires")
    void expire_existingKey_setsExpiryAndKeyDisappearsAfterTtl() throws InterruptedException {
        store.set("expirable", "data");               // no initial TTL
        int result = store.expire("expirable", 100); // 100 ms

        assertThat(result)
            .as("EXPIRE should return 1 for an existing key")
            .isEqualTo(1);

        Thread.sleep(150);

        assertThat(store.get("expirable"))
            .as("key should be gone after TTL elapses")
            .isNull();
    }

    @Test
    @DisplayName("EXPIRE on missing key — returns 0")
    void expire_missingKey_returnsZero() {
        int result = store.expire("ghost", 5_000);

        assertThat(result)
            .as("EXPIRE on non-existent key should return 0")
            .isEqualTo(0);
    }

    // ─── TTL ─────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("TTL key — returns positive seconds when TTL is active")
    void ttl_keyWithActiveTtl_returnsPositiveSeconds() {
        store.set("countdown", "value", 30_000);   // 30 seconds

        long ttl = store.ttl("countdown");

        assertThat(ttl)
            .as("TTL should be positive for a key with active expiry")
            .isGreaterThan(0)
            .isLessThanOrEqualTo(30);
    }

    @Test
    @DisplayName("TTL key — returns -1 for key with no expiry")
    void ttl_keyWithNoExpiry_returnsMinusOne() {
        store.set("forever", "value");   // no TTL

        assertThat(store.ttl("forever"))
            .as("TTL on a key with no expiry should return -1")
            .isEqualTo(-1);
    }

    @Test
    @DisplayName("TTL key — returns -2 for missing key")
    void ttl_missingKey_returnsMinusTwo() {
        assertThat(store.ttl("nonexistent"))
            .as("TTL on a missing key should return -2")
            .isEqualTo(-2);
    }

    @Test
    @DisplayName("TTL key — returns -2 after key has expired")
    void ttl_afterExpiry_returnsMinusTwo() throws InterruptedException {
        store.set("flash", "gone", 100);

        Thread.sleep(150);

        assertThat(store.ttl("flash"))
            .as("TTL on an expired key should return -2 (treated as missing)")
            .isEqualTo(-2);
    }

    // ─── PERSIST ─────────────────────────────────────────────────────────────

    @Test
    @DisplayName("PERSIST key — removes TTL, key survives past original expiry")
    void persist_removesExpiry_keySurvives() throws InterruptedException {
        store.set("saveme", "data", 200);   // 200 ms TTL
        int result = store.persist("saveme");

        assertThat(result)
            .as("PERSIST should return 1 when TTL was removed")
            .isEqualTo(1);

        Thread.sleep(250);   // past what would have been the expiry

        assertThat(store.get("saveme"))
            .as("key should still exist after PERSIST removed its TTL")
            .isEqualTo("data");
    }

    @Test
    @DisplayName("PERSIST on key with no TTL — returns 0")
    void persist_keyWithNoTtl_returnsZero() {
        store.set("notimer", "value");   // no TTL

        assertThat(store.persist("notimer"))
            .as("PERSIST on a key with no TTL should return 0")
            .isEqualTo(0);
    }

    @Test
    @DisplayName("PERSIST on missing key — returns 0")
    void persist_missingKey_returnsZero() {
        assertThat(store.persist("ghost"))
            .as("PERSIST on a non-existent key should return 0")
            .isEqualTo(0);
    }

    @Test
    @DisplayName("PERSIST followed by TTL — TTL returns -1")
    void persist_thenTtl_returnsMinusOne() {
        store.set("key", "value", 30_000);
        store.persist("key");

        assertThat(store.ttl("key"))
            .as("After PERSIST, TTL should return -1 (no expiry)")
            .isEqualTo(-1);
    }
}
