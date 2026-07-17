package com.somesh.kvstore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.somesh.kvstore.config.ServerConfig;
import com.somesh.kvstore.engine.KVStore;
import com.somesh.kvstore.server.TcpServer;

/**
 * Entry point for the KVStore server.
 *
 * <h2>Usage</h2>
 * <pre>
 *   # Standalone primary (default port 6379):
 *   java -jar kvstore.jar
 *
 *   # Standalone primary on custom port:
 *   java -jar kvstore.jar 6379
 *
 *   # Replica of primary at localhost:6380:
 *   java -jar kvstore.jar --replicaof localhost 6380
 *
 *   # Replica on custom client port:
 *   java -jar kvstore.jar 6381 --replicaof localhost 6380
 * </pre>
 *
 * <h2>Replication (Week 4)</h2>
 * When {@code --replicaof} is specified the server starts in replica mode:
 * write commands from external clients are rejected, and all data comes from
 * the replication stream. When running as a primary, pass {@code --with-replication}
 * to open the replication listener on {@link ServerConfig#REPLICATION_PORT}.
 */
public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        int    clientPort    = ServerConfig.SERVER_PORT;
        String replicaOfHost = null;
        int    replicaOfPort = -1;
        boolean withReplication = false;

        // Parse arguments
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--replicaof" -> {
                    replicaOfHost = args[++i];
                    replicaOfPort = Integer.parseInt(args[++i]);
                }
                case "--with-replication" -> withReplication = true;
                default -> clientPort = Integer.parseInt(args[i]);
            }
        }

        KVStore   store  = new KVStore();
        TcpServer server = new TcpServer(clientPort, store);

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
