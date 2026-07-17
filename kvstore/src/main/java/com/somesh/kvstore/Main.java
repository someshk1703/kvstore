package com.somesh.kvstore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.somesh.kvstore.benchmark.BenchmarkRunner;
import com.somesh.kvstore.cli.KvCli;
import com.somesh.kvstore.config.ServerConfig;
import com.somesh.kvstore.engine.KVStore;
import com.somesh.kvstore.http.HttpApiApplication;
import com.somesh.kvstore.server.TcpServer;

/**
 * Entry point for the KVStore server.
 *
 * <h2>Usage</h2>
 * <pre>
 *   # Standalone primary (default port 6379):
 *   java -jar kvstore.jar
 *
 *   # Standalone primary with HTTP API on port 8080:
 *   java -jar kvstore.jar --http
 *
 *   # Standalone primary with HTTP on custom port:
 *   java -jar kvstore.jar --http 9090
 *
 *   # Standalone primary on custom TCP port:
 *   java -jar kvstore.jar 6379
 *
 *   # Replica of primary at localhost:6380:
 *   java -jar kvstore.jar --replicaof localhost 6380
 *
 *   # Replica on custom client port:
 *   java -jar kvstore.jar 6381 --replicaof localhost 6380
 *
 *   # CLI client (single-shot):
 *   java -jar kvstore.jar --cli SET foo bar
 *   java -jar kvstore.jar --cli GET foo
 *
 *   # CLI interactive REPL:
 *   java -jar kvstore.jar --cli
 * </pre>
 *
 * <h2>Replication (Week 4)</h2>
 * When {@code --replicaof} is specified the server starts in replica mode:
 * write commands from external clients are rejected, and all data comes from
 * the replication stream. When running as a primary, pass {@code --with-replication}
 * to open the replication listener on {@link ServerConfig#REPLICATION_PORT}.
 *
 * <h2>HTTP API (Week 5)</h2>
 * Pass {@code --http} to start a Spring Boot HTTP layer on port 8080 alongside
 * the TCP server. Both share the same {@link KVStore} instance.
 */
public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        // ── CLI passthrough ──────────────────────────────────────────────────
        // If the first argument is --cli, hand control to KvCli and exit.
        if (args.length > 0 && "--cli".equals(args[0])) {
            String[] cliArgs = new String[args.length - 1];
            System.arraycopy(args, 1, cliArgs, 0, cliArgs.length);
            KvCli.run(cliArgs);
            return;
        }

        // ── Benchmark passthrough ────────────────────────────────────────────
        if (args.length > 0 && "--benchmark".equals(args[0])) {
            BenchmarkRunner.runBenchmark();
            return;
        }

        int    clientPort       = ServerConfig.SERVER_PORT;
        String replicaOfHost    = null;
        int    replicaOfPort    = -1;
        boolean withReplication = false;
        boolean withHttp        = false;
        // Railway (and similar platforms) inject PORT for the public-facing HTTP
        // service. Fall back to 8080 for local dev.
        int    httpPort         = System.getenv("PORT") != null
                                    ? Integer.parseInt(System.getenv("PORT"))
                                    : 8080;

        // Parse arguments
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--replicaof" -> {
                    replicaOfHost = args[++i];
                    replicaOfPort = Integer.parseInt(args[++i]);
                }
                case "--with-replication" -> withReplication = true;
                case "--http" -> {
                    withHttp = true;
                    // Optional next arg: HTTP port (only if it's a number)
                    if (i + 1 < args.length && args[i + 1].matches("\\d+")) {
                        httpPort = Integer.parseInt(args[++i]);
                    }
                }
                default -> clientPort = Integer.parseInt(args[i]);
            }
        }

        KVStore   store  = new KVStore();
        TcpServer server = new TcpServer(clientPort, store);

        // ── Start HTTP API if requested ──────────────────────────────────────
        if (withHttp) {
            log.info("Starting HTTP API on port {}", httpPort);
            HttpApiApplication.start(store, httpPort);
        }

        // ── Start TCP server in chosen mode ──────────────────────────────────
        if (replicaOfHost != null) {
            log.info("Starting KVStore in REPLICA mode on port {} (primary={}:{})",
                clientPort, replicaOfHost, replicaOfPort);
            server.startAsReplica(replicaOfHost, replicaOfPort);
        } else if (withReplication) {
            log.info("Starting KVStore in PRIMARY mode on port {} (replication port={})",
                clientPort, ServerConfig.REPLICATION_PORT);
            server.enableReplication(null);
        } else {
            log.info("Starting KVStore in STANDALONE mode on port {}", clientPort);
        }

        server.start();
    }
}
