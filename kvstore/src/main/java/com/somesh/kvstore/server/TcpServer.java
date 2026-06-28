package com.somesh.kvstore.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.somesh.kvstore.engine.CommandExecutor;
import com.somesh.kvstore.engine.KVStore;

/**
 * Accepts TCP connections and dispatches each client to a {@link ClientHandler}
 * running on a cached thread pool.
 *
 * <p>One {@link ServerSocket} is bound for the lifetime of the server.
 * Connections are accepted in a blocking loop on the calling thread.
 */
public class TcpServer {

    private static final Logger log = LoggerFactory.getLogger(TcpServer.class);

    private final int             port;
    private final CommandExecutor executor;
    private final ExecutorService threadPool;

    public TcpServer(int port, KVStore kvStore) {
        this.port       = port;
        this.executor   = new CommandExecutor(kvStore);
        this.threadPool = Executors.newCachedThreadPool();
    }

    /**
     * Start the server. Blocks indefinitely until the thread is interrupted.
     *
     * @throws IOException if the server socket cannot be bound
     */
    public void start() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            log.info("KVStore listening on port {}", port);
            while (!Thread.currentThread().isInterrupted()) {
                Socket client = serverSocket.accept();
                threadPool.submit(new ClientHandler(client, executor));
            }
        }
    }
}
