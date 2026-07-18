package com.somesh.kvstore.http;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.somesh.kvstore.engine.KVStore;
import com.somesh.kvstore.persistence.AOFWriter;
import com.somesh.kvstore.replication.ReplicationManager;

/**
 * REST API for the KVStore.
 *
 * <h2>Endpoints</h2>
 * <pre>
 *   GET    /api/keys/{key}              → 200 {key, value} | 404
 *   POST   /api/keys/{key}              → 201 {key, value} (body: {value, ttlSeconds?})
 *   DELETE /api/keys/{key}              → 204 | 404
 *   GET    /api/keys?pattern=user:*     → 200 [key, ...]
 *   GET    /api/info                    → 200 {server metadata}
 * </pre>
 *
 * <h2>Design constraints</h2>
 * This controller is intentionally thin. Every operation delegates directly to
 * {@link KVStore} with no extra logic layer. The HTTP protocol is just another
 * transport over the same store that the TCP server uses.
 */
@RestController
@RequestMapping("/api")
public class KeyValueController {

    private final KVStore kvStore;
    private final AOFWriter aofWriter;
    private final ReplicationManager replicationManager;

    public KeyValueController(KVStore kvStore, AOFWriter aofWriter,
                              ReplicationManager replicationManager) {
        this.kvStore = kvStore;
        this.aofWriter = aofWriter;
        this.replicationManager = replicationManager;
    }

    // ── GET /api/keys/{key} ──────────────────────────────────────────────────

    @GetMapping("/keys/{key}")
    public ResponseEntity<Map<String, String>> get(@PathVariable String key) {
        String value = kvStore.get(key);
        if (value == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(Map.of("key", key, "value", value));
    }

    // ── POST /api/keys/{key} ─────────────────────────────────────────────────

    @PostMapping("/keys/{key}")
    public ResponseEntity<Map<String, String>> set(
            @PathVariable String key,
            @RequestBody SetRequest body) {

        if (body.value() == null || body.value().isBlank()) {
            return ResponseEntity.badRequest().build();
        }

        if (body.ttlSeconds() != null && body.ttlSeconds() > 0) {
            kvStore.set(key, body.value(), body.ttlSeconds() * 1000L);
        } else {
            kvStore.set(key, body.value());
        }
        return ResponseEntity.status(HttpStatus.CREATED)
                             .body(Map.of("key", key, "value", body.value()));
    }

    // ── DELETE /api/keys/{key} ───────────────────────────────────────────────

    @DeleteMapping("/keys/{key}")
    public ResponseEntity<Void> delete(@PathVariable String key) {
        int deleted = kvStore.del(key);
        if (deleted == 0) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.noContent().build();
    }

    // ── GET /api/keys?pattern=... ────────────────────────────────────────────

    @GetMapping("/keys")
    public ResponseEntity<List<String>> keys(
            @RequestParam(defaultValue = "*") String pattern) {
        Set<String> matched = kvStore.keys(pattern);
        List<String> sorted = matched.stream().sorted().toList();
        return ResponseEntity.ok(sorted);
    }

    // ── GET /api/health ──────────────────────────────────────────────────────

    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of("status", "UP"));
    }

    // ── GET /api/info ────────────────────────────────────────────────────────

    @GetMapping("/info")
    public ResponseEntity<Map<String, Object>> info() {
        Runtime rt = Runtime.getRuntime();
        long uptimeSeconds = (System.currentTimeMillis() - HttpApiApplication.START_TIME_MS) / 1000;

        long hits   = kvStore.getHitCount();
        long misses = kvStore.getMissCount();
        long total  = hits + misses;
        String hitRatio = total == 0 ? "n/a"
            : String.format("%.1f%%", (hits * 100.0) / total);

        Map<String, Object> body = new LinkedHashMap<>();
        // Core store stats
        body.put("version",          "0.1.0");
        body.put("totalKeys",        kvStore.size());
        body.put("uptimeSeconds",    uptimeSeconds);
        body.put("usedMemoryBytes",  rt.totalMemory() - rt.freeMemory());
        body.put("maxMemoryBytes",   rt.maxMemory());
        // Hit / miss
        body.put("hitCount",         hits);
        body.put("missCount",        misses);
        body.put("hitRatio",         hitRatio);
        // Evictions
        body.put("evictions",        kvStore.getEvictionCount());
        // AOF persistence
        if (aofWriter != null) {
            body.put("aofEnabled",       true);
            body.put("aofFsyncMode",     aofWriter.getFsyncMode().name());
            body.put("aofCommandCount",  aofWriter.getCommandCount());
            body.put("aofPath",          aofWriter.getAofPath());
        } else {
            body.put("aofEnabled", false);
        }
        // Replication
        if (replicationManager != null) {
            body.put("replicaCount",       replicationManager.getReplicaCount());
            body.put("syncedReplicaCount", replicationManager.getSyncedReplicaCount());
            body.put("masterOffset",       replicationManager.getMasterOffset());
        } else {
            body.put("replicaCount", 0);
        }

        return ResponseEntity.ok(body);
    }

    // ── Request body record ──────────────────────────────────────────────────

    /** Request body for POST /api/keys/{key}. */
    record SetRequest(String value, Long ttlSeconds) {}
}
