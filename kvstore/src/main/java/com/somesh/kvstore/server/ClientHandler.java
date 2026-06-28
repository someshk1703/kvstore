package com.somesh.kvstore.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.somesh.kvstore.engine.CommandExecutor;
import com.somesh.kvstore.engine.CommandResult;
import com.somesh.kvstore.protocol.Command;
import com.somesh.kvstore.protocol.CommandParser;
import com.somesh.kvstore.protocol.ResponseSerializer;

/**
 * Handles a single client connection for its entire lifetime.
 *
 * <p>Read loop:
 * <ol>
 *   <li>Read one line from the socket.</li>
 *   <li>Parse it into a {@link Command} via {@link CommandParser}.</li>
 *   <li>Execute it via {@link CommandExecutor}.</li>
 *   <li>Serialize the {@link CommandResult} and write it back.</li>
 * </ol>
 *
 * <p>The loop exits when the client closes the connection or an IO error occurs.
 */
public class ClientHandler implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(ClientHandler.class);

    private final Socket          socket;
    private final CommandExecutor executor;

    public ClientHandler(Socket socket, CommandExecutor executor) {
        this.socket   = socket;
        this.executor = executor;
    }

    @Override
    public void run() {
        String remote = socket.getRemoteSocketAddress().toString();
        log.info("Client connected: {}", remote);

        try (
            BufferedReader in  = new BufferedReader(
                                    new InputStreamReader(socket.getInputStream()));
            PrintWriter    out = new PrintWriter(socket.getOutputStream(), /*autoFlush=*/true)
        ) {
            String line;
            while ((line = in.readLine()) != null) {
                Command       cmd    = CommandParser.parse(line);
                CommandResult result = executor.execute(cmd);
                out.print(ResponseSerializer.serialize(result));
                out.flush();
            }
        } catch (IOException e) {
            log.debug("IO error for {}: {}", remote, e.getMessage());
        } finally {
            try { socket.close(); } catch (IOException ignored) {}
            log.info("Client disconnected: {}", remote);
        }
    }
}
