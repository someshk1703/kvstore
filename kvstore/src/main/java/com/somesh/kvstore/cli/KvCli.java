package com.somesh.kvstore.cli;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ConnectException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import com.somesh.kvstore.config.ServerConfig;

/**
 * kv-cli — command-line client for KVStore.
 *
 * <h2>Modes</h2>
 * <pre>
 *   # Single-shot (args provided):
 *   java -jar kvstore.jar --cli SET foo bar
 *   java -jar kvstore.jar --cli GET foo
 *
 *   # Interactive REPL (no args):
 *   java -jar kvstore.jar --cli
 *   kvstore> SET foo bar
 *   +OK
 *   kvstore> GET foo
 *   "bar"
 *   kvstore> QUIT
 * </pre>
 *
 * <h2>Design</h2>
 * The CLI sends raw RESP-lite inline commands over TCP and prints the response
 * as received. It does not speak HTTP — the TCP path is the real wire format
 * and the primary demo tool.
 *
 * Connection errors (server not running, timeout) are caught and printed as a
 * human-readable message — no stack traces on a live demo.
 */
public class KvCli {

    private static final String DEFAULT_HOST = "localhost";
    private static final int    DEFAULT_PORT = ServerConfig.SERVER_PORT;
    private static final String PROMPT       = "kvstore> ";

    /**
     * Entry point called by {@code Main} when {@code --cli} flag is present.
     *
     * @param args arguments after {@code --cli} are treated as a single command
     *             (single-shot mode). An empty array starts interactive mode.
     */
    public static void run(String[] args) {
        String host = DEFAULT_HOST;
        int    port = DEFAULT_PORT;

        // Simple -h / -p flag parsing so the CLI can target remote hosts
        int cmdStart = 0;
        for (int i = 0; i < args.length; i++) {
            if ("-h".equals(args[i]) && i + 1 < args.length) {
                host = args[++i];
                cmdStart = i + 1;
            } else if ("-p".equals(args[i]) && i + 1 < args.length) {
                port = Integer.parseInt(args[++i]);
                cmdStart = i + 1;
            } else {
                cmdStart = i;
                break;
            }
        }

        String[] cmdArgs = Arrays.copyOfRange(args, cmdStart, args.length);

        try (Socket socket = new Socket(host, port)) {
            socket.setSoTimeout(5_000);
            PrintWriter  out = new PrintWriter(socket.getOutputStream(), true, StandardCharsets.UTF_8);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));

            if (cmdArgs.length > 0) {
                // Single-shot mode
                String cmd = String.join(" ", cmdArgs);
                out.print(cmd + "\r\n");
                out.flush();
                printResponse(in);
            } else {
                // Interactive REPL mode
                BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
                System.out.print(PROMPT);
                System.out.flush();

                String line;
                while ((line = stdin.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) {
                        System.out.print(PROMPT);
                        System.out.flush();
                        continue;
                    }
                    if ("quit".equalsIgnoreCase(line) || "exit".equalsIgnoreCase(line)) {
                        break;
                    }
                    out.print(line + "\r\n");
                    out.flush();
                    printResponse(in);
                    System.out.print(PROMPT);
                    System.out.flush();
                }
            }
        } catch (ConnectException e) {
            System.err.println("Could not connect to " + host + ":" + port
                + " — is the server running?");
        } catch (IOException e) {
            System.err.println("Connection error: " + e.getMessage());
        }
    }

    /**
     * Reads and formats a single RESP response from the server.
     *
     * <p>Handles all RESP-lite types:
     * <ul>
     *   <li>{@code +} — simple string (printed as-is)</li>
     *   <li>{@code -} — error (printed prefixed with ERROR)</li>
     *   <li>{@code :} — integer (printed as integer)</li>
     *   <li>{@code $} — bulk string (reads length then value)</li>
     *   <li>{@code *} — array (reads N elements recursively)</li>
     * </ul>
     */
    private static void printResponse(BufferedReader in) throws IOException {
        String line = in.readLine();
        if (line == null) {
            System.out.println("(disconnected)");
            return;
        }

        char type = line.charAt(0);
        String body = line.substring(1);

        switch (type) {
            case '+' -> System.out.println(body);

            case '-' -> System.out.println("(error) " + body);

            case ':' -> System.out.println("(integer) " + body);

            case '$' -> {
                int len = Integer.parseInt(body);
                if (len == -1) {
                    System.out.println("(nil)");
                } else {
                    // Read the bulk string value (+ discard trailing \r\n)
                    char[] buf = new char[len];
                    int read = 0;
                    while (read < len) {
                        int r = in.read(buf, read, len - read);
                        if (r == -1) break;
                        read += r;
                    }
                    in.readLine(); // consume trailing \r\n
                    System.out.println("\"" + new String(buf, 0, read) + "\"");
                }
            }

            case '*' -> {
                int count = Integer.parseInt(body);
                if (count == -1) {
                    System.out.println("(empty array)");
                } else {
                    for (int i = 0; i < count; i++) {
                        System.out.print((i + 1) + ") ");
                        printResponse(in);
                    }
                }
            }

            default -> System.out.println(line);
        }
    }
}
