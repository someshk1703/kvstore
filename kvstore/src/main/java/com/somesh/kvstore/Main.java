package com.somesh.kvstore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.somesh.kvstore.engine.KVStore;
import com.somesh.kvstore.server.TcpServer;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);
    private static final int DEFAULT_PORT = 6379;

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_PORT;
        KVStore   store  = new KVStore();
        TcpServer server = new TcpServer(port, store);
        log.info("Starting KVStore on port {}", port);
        server.start();
    }
}
