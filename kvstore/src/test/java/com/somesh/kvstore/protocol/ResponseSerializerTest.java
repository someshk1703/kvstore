package com.somesh.kvstore.protocol;

import com.somesh.kvstore.engine.CommandResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ResponseSerializerTest {

    private ResponseSerializer serializer;

    @BeforeEach
    void setUp() {
        serializer = new ResponseSerializer();
    }

    // ── OK ──────────────────────────────────────────────────────────

    @Test
    void serialize_ok_producesSimpleString() {
        assertThat(serializer.serialize(CommandResult.ok()))
                .isEqualTo("+OK\r\n");
    }

    // ── Bulk string ─────────────────────────────────────────────────

    @Test
    void serialize_string_producesBulkString() {
        assertThat(serializer.serialize(CommandResult.string("hello")))
                .isEqualTo("$5\r\nhello\r\n");
    }

    @Test
    void serialize_emptyString_producesZeroLengthBulk() {
        assertThat(serializer.serialize(CommandResult.string("")))
                .isEqualTo("$0\r\n\r\n");
    }

    @Test
    void serialize_nullString_producesNilBulk() {
        assertThat(serializer.serialize(CommandResult.string(null)))
                .isEqualTo("$-1\r\n");
    }

    @Test
    void serialize_unicodeValue_usesUtf8ByteLength() {
        // "🔑" is 4 bytes in UTF-8, not 1 char
        String value = "🔑";
        String resp = serializer.serialize(CommandResult.string(value));
        assertThat(resp).startsWith("$4\r\n");
    }

    // ── Integer ─────────────────────────────────────────────────────

    @Test
    void serialize_integerOne_producesColonOne() {
        assertThat(serializer.serialize(CommandResult.integer(1)))
                .isEqualTo(":1\r\n");
    }

    @Test
    void serialize_integerZero_producesColonZero() {
        assertThat(serializer.serialize(CommandResult.integer(0)))
                .isEqualTo(":0\r\n");
    }

    @Test
    void serialize_negativeTtl_producesNegativeInteger() {
        assertThat(serializer.serialize(CommandResult.integer(-1)))
                .isEqualTo(":-1\r\n");
        assertThat(serializer.serialize(CommandResult.integer(-2)))
                .isEqualTo(":-2\r\n");
    }

    // ── Error ───────────────────────────────────────────────────────

    @Test
    void serialize_error_producesDashPrefix() {
        assertThat(serializer.serialize(CommandResult.error("ERR unknown command")))
                .isEqualTo("-ERR unknown command\r\n");
    }

    @Test
    void serialize_errorWithEmbeddedNewline_sanitizes() {
        // Error messages must be single-line
        String resp = serializer.serialize(CommandResult.error("ERR bad\nvalue"));
        assertThat(resp).doesNotContain("\n\n");
        assertThat(resp).endsWith("\r\n");
    }

    @Test
    void serialize_nullError_producesGenericError() {
        assertThat(serializer.serialize(CommandResult.error(null)))
                .isEqualTo("-ERR unknown error\r\n");
    }

    // ── Full command flow ────────────────────────────────────────────

    @Test
    void fullFlow_set_get_del() {
        // SET → +OK
        assertThat(serializer.serialize(CommandResult.ok()))
                .isEqualTo("+OK\r\n");

        // GET hit → bulk string
        assertThat(serializer.serialize(CommandResult.string("bar")))
                .isEqualTo("$3\r\nbar\r\n");

        // GET miss → nil
        assertThat(serializer.serialize(CommandResult.string(null)))
                .isEqualTo("$-1\r\n");

        // DEL hit → :1
        assertThat(serializer.serialize(CommandResult.integer(1)))
                .isEqualTo(":1\r\n");

        // DEL miss → :0
        assertThat(serializer.serialize(CommandResult.integer(0)))
                .isEqualTo(":0\r\n");
    }
}