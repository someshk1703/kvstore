package com.somesh.kvstore.benchmark;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import com.somesh.kvstore.engine.KVStore;

/**
 * Throughput benchmark for KVStore.
 *
 * <h2>Three scenarios</h2>
 * <ol>
 *   <li><b>Direct in-process</b> — calls {@link KVStore#set} directly, no TCP.
 *       Measures pure engine throughput, eliminating network and serialization cost.</li>
 *   <li><b>Single-threaded TCP</b> — one client, 100k SET ops over localhost TCP.</li>
 *   <li><b>Multi-threaded TCP</b> — 8 concurrent clients, 100k total ops split evenly.</li>
 *   <li><b>ConcurrentHashMap vs synchronized HashMap</b> — same in-process load on
 *       both implementations to quantify lock-free read benefit.</li>
 * </ol>
 *
 * <h2>How to run</h2>
 * Start the server first, then:
 * <pre>
 *   java -jar kvstore.jar --benchmark
 * </pre>
 * or call {@link #main(String[])} directly.
 *
 * <h2>Interpreting the numbers</h2>
 * Single-threaded TCP won't be 8x slower than 8-thread TCP — thread-pool overhead
 * and ConcurrentHashMap's per-segment locking mean returns diminish. The interesting
 * question is where the inflection point is and why.
 */
public class BenchmarkRunner {

    private static final int    OPS          = 100_000;
    private static final int    WARMUP_OPS   = 10_000;
    private static final int    THREAD_COUNT = 8;
    private static final String HOST         = "localhost";
    private static final int    PORT         = 6379;

    public static void main(String[] args) throws Exception {
        System.out.println("=== KVStore Throughput Benchmark ===");
        System.out.println("Operations per run : " + OPS);
        System.out.println("Warmup ops         : " + WARMUP_OPS);
        System.out.println();

        Map<String, Long> results = new java.util.LinkedHashMap<>();

        // ── 1. Direct in-process (ConcurrentHashMap-backed) ─────────────────
        results.put("Direct / ConcurrentHashMap", directBenchmark(false));

        // ── 2. Direct in-process (synchronized HashMap) ─────────────────────
        results.put("Direct / Synchronized HashMap", directBenchmark(true));

        // ── 3. TCP single-threaded ───────────────────────────────────────────
        results.put("TCP / single-threaded", tcpBenchmark(1, OPS));

        // ── 4. TCP multi-threaded (8 threads) ────────────────────────────────
        results.put("TCP / 8 threads", tcpBenchmark(THREAD_COUNT, OPS));

        // ── Print table ──────────────────────────────────────────────────────
        System.out.println("┌─────────────────────────────────────────┬──────────────────┐");
        System.out.println("│ Scenario                                │ Throughput       │");
        System.out.println("├─────────────────────────────────────────┼──────────────────┤");
        for (Map.Entry<String, Long> entry : results.entrySet()) {
            String label = String.format("%-39s", entry.getKey());
            String ops   = String.format("%,12d ops/s", entry.getValue());
            System.out.println("│ " + label + " │ " + ops + "   │");
        }
        System.out.println("└─────────────────────────────────────────┴──────────────────┘");

        System.out.println();
        Long concMap = results.get("Direct / ConcurrentHashMap");
        Long syncMap = results.get("Direct / Synchronized HashMap");
        if (concMap != null && syncMap != null && syncMap > 0) {
            double speedup = (double) concMap / syncMap;
            System.out.printf("ConcurrentHashMap is %.2fx faster than synchronized HashMap%n", speedup);
        }
        Long single = results.get("TCP / single-threaded");
        Long multi  = results.get("TCP / 8 threads");
        if (single != null && multi != null && single > 0) {
            double scale = (double) multi / single;
            System.out.printf("8-thread TCP is %.2fx single-thread (theoretical max: 8x)%n", scale);
            System.out.println("Gap = network round-trip serialisation + ConcurrentHashMap segment lock contention");
        }
    }

    // ── In-process benchmark ─────────────────────────────────────────────────

    private static long directBenchmark(boolean useSynchronized) throws Exception {
        String label = useSynchronized ? "synchronized HashMap" : "ConcurrentHashMap";
        System.out.print("Running direct benchmark (" + label + ")... ");

        if (useSynchronized) {
            // Warm up
            SynchronizedStore sync = new SynchronizedStore();
            for (int i = 0; i < WARMUP_OPS; i++) sync.set("k" + i, "v" + i);
            sync = new SynchronizedStore();

            long t0 = System.nanoTime();
            for (int i = 0; i < OPS; i++) sync.set("key:" + i, "value:" + i);
            long elapsed = System.nanoTime() - t0;
            long opsPerSec = (long) (OPS / (elapsed / 1e9));
            System.out.println(String.format("%,d ops/s", opsPerSec));
            return opsPerSec;

        } else {
            KVStore store = new KVStore();
            for (int i = 0; i < WARMUP_OPS; i++) store.set("k" + i, "v" + i);
            store = new KVStore();

            long t0 = System.nanoTime();
            for (int i = 0; i < OPS; i++) store.set("key:" + i, "value:" + i);
            long elapsed = System.nanoTime() - t0;
            long opsPerSec = (long) (OPS / (elapsed / 1e9));
            System.out.println(String.format("%,d ops/s", opsPerSec));
            return opsPerSec;
        }
    }

    // ── TCP benchmark ────────────────────────────────────────────────────────

    private static long tcpBenchmark(int threads, int totalOps) throws Exception {
        System.out.print("Running TCP benchmark (" + threads + " thread(s))... ");
        int opsPerThread = totalOps / threads;

        // Warmup with a single connection
        try (Socket s = new Socket(HOST, PORT)) {
            s.setSoTimeout(10_000);
            PrintWriter  out = new PrintWriter(s.getOutputStream(), false, StandardCharsets.UTF_8);
            BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
            for (int i = 0; i < Math.min(WARMUP_OPS, 1000); i++) {
                out.print("SET warmup:" + i + " v\r\n");
            }
            out.flush();
            for (int i = 0; i < Math.min(WARMUP_OPS, 1000); i++) in.readLine();
        } catch (Exception e) {
            System.out.println("\n  WARN: Server not reachable at " + HOST + ":" + PORT
                + " — skipping TCP benchmarks");
            return -1;
        }

        CountDownLatch ready  = new CountDownLatch(threads);
        CountDownLatch start  = new CountDownLatch(1);
        CountDownLatch done   = new CountDownLatch(threads);
        AtomicLong     errors = new AtomicLong();

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        for (int t = 0; t < threads; t++) {
            final int threadIdx = t;
            pool.submit(() -> {
                try (Socket s = new Socket(HOST, PORT)) {
                    s.setSoTimeout(30_000);
                    s.setTcpNoDelay(true);
                    PrintWriter    out = new PrintWriter(s.getOutputStream(), false, StandardCharsets.UTF_8);
                    BufferedReader in  = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));

                    ready.countDown();
                    start.await();

                    // Pipeline writes then read all responses
                    for (int i = 0; i < opsPerThread; i++) {
                        out.print("SET bench:" + threadIdx + ":" + i + " v\r\n");
                    }
                    out.flush();
                    for (int i = 0; i < opsPerThread; i++) {
                        String resp = in.readLine();
                        if (resp == null || resp.startsWith("-")) errors.incrementAndGet();
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    done.countDown();
                }
            });
        }

        ready.await();
        long t0 = System.nanoTime();
        start.countDown();
        done.await();
        long elapsed = System.nanoTime() - t0;

        pool.shutdown();
        long opsPerSec = (long) (totalOps / (elapsed / 1e9));
        System.out.println(String.format("%,d ops/s  (errors: %d)", opsPerSec, errors.get()));
        return opsPerSec;
    }

    // ── Synchronized HashMap stub ────────────────────────────────────────────

    /** Minimal synchronized store for direct comparison with ConcurrentHashMap. */
    private static class SynchronizedStore {
        private final Map<String, String> map = new HashMap<>();

        synchronized void set(String key, String value) {
            map.put(key, value);
        }

        synchronized String get(String key) {
            return map.get(key);
        }
    }

    // ── Entry point via Main ─────────────────────────────────────────────────

    /** Called by Main when --benchmark flag is present. */
    public static void runBenchmark() throws Exception {
        main(new String[0]);
    }
}
