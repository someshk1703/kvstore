package com.somesh.kvstore.replication;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link RingBuffer}.
 */
@DisplayName("RingBuffer")
class RingBufferTest {

    // ── Basic append + offset ──────────────────────────────────────────────

    @Test
    @DisplayName("first append returns offset 0")
    void firstAppendOffset() {
        RingBuffer<String> buf = new RingBuffer<>(4);
        assertThat(buf.append("a")).isEqualTo(0L);
    }

    @Test
    @DisplayName("offsets increment monotonically")
    void offsetsMonotonic() {
        RingBuffer<String> buf = new RingBuffer<>(4);
        for (int i = 0; i < 6; i++) {
            assertThat(buf.append("item" + i)).isEqualTo(i);
        }
    }

    @Test
    @DisplayName("getNextOffset reflects append count")
    void getNextOffset() {
        RingBuffer<String> buf = new RingBuffer<>(4);
        buf.append("a");
        buf.append("b");
        assertThat(buf.getNextOffset()).isEqualTo(2);
    }

    // ── contains() ─────────────────────────────────────────────────────────

    @Test
    @DisplayName("contains returns true for in-range offsets")
    void containsInRange() {
        RingBuffer<String> buf = new RingBuffer<>(4);
        buf.append("a");  // offset 0
        buf.append("b");  // offset 1
        buf.append("c");  // offset 2

        assertThat(buf.contains(0)).isTrue();
        assertThat(buf.contains(1)).isTrue();
        assertThat(buf.contains(2)).isTrue();
    }

    @Test
    @DisplayName("contains returns false for future offset")
    void containsFutureOffset() {
        RingBuffer<String> buf = new RingBuffer<>(4);
        buf.append("a");

        assertThat(buf.contains(5)).isFalse();
    }

    @Test
    @DisplayName("contains returns false for negative offset")
    void containsNegativeOffset() {
        RingBuffer<String> buf = new RingBuffer<>(4);
        assertThat(buf.contains(-1)).isFalse();
    }

    @Test
    @DisplayName("contains returns false for overwritten (evicted) offset")
    void containsEvicted() {
        RingBuffer<String> buf = new RingBuffer<>(3);  // capacity = 3
        buf.append("a");  // 0
        buf.append("b");  // 1
        buf.append("c");  // 2
        buf.append("d");  // 3  → overwrites slot 0 (offset 0 evicted)

        assertThat(buf.contains(0)).isFalse();  // evicted
        assertThat(buf.contains(1)).isTrue();   // still in buffer
        assertThat(buf.contains(2)).isTrue();
        assertThat(buf.contains(3)).isTrue();
    }

    // ── getRange() ─────────────────────────────────────────────────────────

    @Test
    @DisplayName("getRange returns all items from fromOffset to latest")
    void getRangeBasic() {
        RingBuffer<String> buf = new RingBuffer<>(8);
        buf.append("a");  // 0
        buf.append("b");  // 1
        buf.append("c");  // 2
        buf.append("d");  // 3

        List<String> result = buf.getRange(1);
        assertThat(result).containsExactly("b", "c", "d");
    }

    @Test
    @DisplayName("getRange returns empty list for evicted offset")
    void getRangeEvicted() {
        RingBuffer<String> buf = new RingBuffer<>(2);
        buf.append("a");  // 0
        buf.append("b");  // 1
        buf.append("c");  // 2  → overwrites 0

        // fromOffset=0 is evicted
        assertThat(buf.getRange(0)).isEmpty();
    }

    @Test
    @DisplayName("getRange with fromOffset = nextOffset returns empty")
    void getRangeAtNextOffset() {
        RingBuffer<String> buf = new RingBuffer<>(4);
        buf.append("x");  // 0

        // fromOffset=1 = nextOffset → nothing new
        assertThat(buf.getRange(1)).isEmpty();
    }

    // ── Wrap-around integrity ────────────────────────────────────────────────

    @Test
    @DisplayName("wraps around capacity and oldest items are overwritten")
    void wrapAround() {
        RingBuffer<String> buf = new RingBuffer<>(4);
        for (int i = 0; i < 6; i++) {
            buf.append("item" + i);
        }
        // Buffer holds offsets 2, 3, 4, 5 (oldest two were overwritten)
        assertThat(buf.contains(0)).isFalse();
        assertThat(buf.contains(1)).isFalse();
        assertThat(buf.contains(2)).isTrue();
        assertThat(buf.contains(5)).isTrue();

        List<String> range = buf.getRange(2);
        assertThat(range).containsExactly("item2", "item3", "item4", "item5");
    }

    // ── Concurrency ─────────────────────────────────────────────────────────

    @Test
    @DisplayName("concurrent appends from multiple threads assign unique monotonic offsets")
    void concurrentAppendsUnique() throws InterruptedException {
        RingBuffer<String> buf = new RingBuffer<>(1024);
        int threads = 8;
        int appendsPerThread = 100;
        List<Long> offsets = new ArrayList<>();
        Object lock = new Object();

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done  = new CountDownLatch(threads);

        for (int t = 0; t < threads; t++) {
            int tIdx = t;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < appendsPerThread; i++) {
                        long off = buf.append("t" + tIdx + "i" + i);
                        synchronized (lock) { offsets.add(off); }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }

        start.countDown();
        assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
        pool.shutdown();

        int total = threads * appendsPerThread;
        assertThat(offsets).hasSize(total);
        // All offsets must be unique
        assertThat(offsets.stream().distinct().count()).isEqualTo(total);
        // Offsets are in range [0, total)
        assertThat(offsets).allMatch(o -> o >= 0 && o < total);
    }

    @Test
    @DisplayName("getLatestOffset returns -1 before any appends")
    void latestOffsetEmpty() {
        RingBuffer<String> buf = new RingBuffer<>(4);
        assertThat(buf.getLatestOffset()).isEqualTo(-1);
    }

    @Test
    @DisplayName("getLatestOffset returns last appended offset")
    void latestOffset() {
        RingBuffer<String> buf = new RingBuffer<>(4);
        buf.append("a");
        buf.append("b");
        assertThat(buf.getLatestOffset()).isEqualTo(1);
    }
}
