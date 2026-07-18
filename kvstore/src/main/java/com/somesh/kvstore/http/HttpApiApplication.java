package com.somesh.kvstore.http;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import com.somesh.kvstore.engine.KVStore;
import com.somesh.kvstore.persistence.AOFWriter;
import com.somesh.kvstore.replication.ReplicationManager;

/**
 * Spring Boot entry point for the HTTP API layer.
 *
 * <h2>Design rationale</h2>
 * The KVStore is constructed by {@link com.somesh.kvstore.Main} and shared with
 * the TCP server. To expose it as a Spring bean without a second instantiation,
 * we use a static holder set before {@link SpringApplication#run} is called.
 * This is intentional — the static field is written once before any Spring context
 * exists, so there is no concurrency hazard here.
 *
 * <p>The HTTP API does not own the store; it borrows it. TCP and HTTP are two
 * views of the same in-memory state, which is the whole interview demo point.
 *
 * <h2>Startup from Main</h2>
 * <pre>
 *   HttpApiApplication.start(kvStore, 8080);   // non-blocking — daemon thread
 * </pre>
 */
@SpringBootApplication
public class HttpApiApplication {

    private static final Logger log = LoggerFactory.getLogger(HttpApiApplication.class);

    // Written once by start() before Spring context is created.
    private static volatile KVStore sharedStore;
    private static volatile AOFWriter sharedAofWriter;
    private static volatile ReplicationManager sharedReplicationManager;

    // Tracks the server start time for uptime reporting.
    static final long START_TIME_MS = System.currentTimeMillis();

    /**
     * Starts Spring Boot on the given HTTP port in a daemon thread so it does
     * not block the TCP server's accept loop.
     *
     * @param store    the same KVStore instance used by the TCP server
     * @param httpPort port for the embedded Tomcat to bind (default: 8080)
     */
    public static void start(KVStore store, int httpPort) {
        start(store, null, null, httpPort);
    }

    /**
     * Full-featured start — also wires AOF and replication stats into the HTTP layer.
     *
     * @param store      the shared KVStore
     * @param aofWriter  the AOFWriter (may be null if persistence is disabled)
     * @param replication the ReplicationManager (may be null if not in primary mode)
     * @param httpPort   port for embedded Tomcat
     */
    public static void start(KVStore store, AOFWriter aofWriter,
                             ReplicationManager replication, int httpPort) {
        sharedStore = store;
        sharedAofWriter = aofWriter;
        sharedReplicationManager = replication;
        Thread thread = new Thread(() -> {
            SpringApplication app = new SpringApplication(HttpApiApplication.class);
            app.setDefaultProperties(Map.of(
                "server.port", String.valueOf(httpPort),
                // Suppress Spring Boot banner — it clutters our structured logs
                "spring.main.banner-mode", "off",
                // Use our existing logback configuration
                "logging.config", "classpath:logback.xml"
            ));
            app.run();
            log.info("HTTP API started on port {}", httpPort);
        }, "http-api-startup");
        thread.setDaemon(true);
        thread.start();
    }

    /**
     * Exposes the shared KVStore as a Spring bean so controllers can inject it.
     */
    @Bean
    public KVStore kvStore() {
        return sharedStore;
    }

    // ── Static accessors for optional subsystems ─────────────────────────────
    // Not exposed as Spring @Bean because they may be null (AOF disabled, standalone
    // mode) and Spring does not allow injecting null beans via constructor injection.
    // The controller reads them via these static getters instead.

    public static AOFWriter getSharedAofWriter() {
        return sharedAofWriter;
    }

    public static ReplicationManager getSharedReplicationManager() {
        return sharedReplicationManager;
    }

    /** Called from Main after replication is enabled to expose the manager via /api/info. */
    public static void setReplicationManager(ReplicationManager rm) {
        sharedReplicationManager = rm;
    }
}
