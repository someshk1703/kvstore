package com.somesh.kvstore.protocol;

import com.somesh.kvstore.engine.CommandResult;

/**
 * Serialises a {@link CommandResult} to the RESP-lite wire format.
 *
 * <pre>
 *   OK      → "+OK\r\n"
 *   STRING  → "$N\r\n<value>\r\n"  or  "$-1\r\n" (null bulk)
 *   INTEGER → ":N\r\n"
 *   ERROR   → "-<message>\r\n"
 * </pre>
 */
public final class ResponseSerializer {

    private static final String CRLF = "\r\n";

    private ResponseSerializer() {}

    public static String serialize(CommandResult result) {
        return switch (result.type) {
            case OK      -> "+OK" + CRLF;
            case STRING  -> result.stringValue == null
                                ? "$-1" + CRLF
                                : "$" + result.stringValue.getBytes().length + CRLF
                                    + result.stringValue + CRLF;
            case INTEGER -> ":" + result.intValue + CRLF;
            case ERROR   -> "-" + result.errorMessage + CRLF;
        };
    }
}
