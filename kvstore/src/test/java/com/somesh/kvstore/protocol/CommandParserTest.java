package com.somesh.kvstore.protocol;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class CommandParserTest {

    private CommandParser parser;

    @BeforeEach
    void setUp() {
        parser = new CommandParser();
    }

    // ── Happy path ──────────────────────────────────────────────────

    @Test
    void set_withTwoArgs_parsesCorrectly() {
        Command cmd = parser.parse("SET foo bar");
        assertThat(cmd.type()).isEqualTo(CommandType.SET);
        assertThat(cmd.arg(0)).isEqualTo("foo");
        assertThat(cmd.arg(1)).isEqualTo("bar");
        assertThat(cmd.argCount()).isEqualTo(2);
    }

    @Test
    void set_withExOption_parsesAllFourArgs() {
        Command cmd = parser.parse("SET foo bar EX 10");
        assertThat(cmd.type()).isEqualTo(CommandType.SET);
        assertThat(cmd.argCount()).isEqualTo(4);
        assertThat(cmd.arg(2)).isEqualTo("EX");
        assertThat(cmd.arg(3)).isEqualTo("10");
    }

    @Test
    void get_parsesCorrectly() {
        Command cmd = parser.parse("GET foo");
        assertThat(cmd.type()).isEqualTo(CommandType.GET);
        assertThat(cmd.arg(0)).isEqualTo("foo");
    }

    @Test
    void del_multipleKeys_parsesAllKeys() {
        Command cmd = parser.parse("DEL a b c");
        assertThat(cmd.type()).isEqualTo(CommandType.DEL);
        assertThat(cmd.argCount()).isEqualTo(3);
        assertThat(cmd.args()).containsExactly("a", "b", "c");
    }

    @Test
    void ping_noArgs_parsesCorrectly() {
        Command cmd = parser.parse("PING");
        assertThat(cmd.type()).isEqualTo(CommandType.PING);
        assertThat(cmd.argCount()).isEqualTo(0);
    }

    // ── Case insensitivity ──────────────────────────────────────────

    @ParameterizedTest
    @ValueSource(strings = { "GET foo", "get foo", "Get foo", "gEt foo" })
    void get_caseInsensitive(String input) {
        Command cmd = parser.parse(input);
        assertThat(cmd.type()).isEqualTo(CommandType.GET);
    }

    // ── Whitespace normalization ─────────────────────────────────────

    @Test
    void set_extraSpacesBetweenTokens_normalizes() {
        Command cmd = parser.parse("SET   foo   bar");
        assertThat(cmd.type()).isEqualTo(CommandType.SET);
        assertThat(cmd.arg(0)).isEqualTo("foo");
        assertThat(cmd.arg(1)).isEqualTo("bar");
    }

    @Test
    void set_leadingAndTrailingSpaces_strips() {
        Command cmd = parser.parse("  SET foo bar  ");
        assertThat(cmd.type()).isEqualTo(CommandType.SET);
    }

    @Test
    void set_carriageReturnSuffix_strips() {
        // Telnet / Windows line endings
        Command cmd = parser.parse("SET foo bar\r");
        assertThat(cmd.type()).isEqualTo(CommandType.SET);
        assertThat(cmd.arg(1)).isEqualTo("bar");  // not "bar\r"
    }

    // ── Arity validation ────────────────────────────────────────────

    @Test
    void get_noArgs_returnsErrorCommand() {
        Command cmd = parser.parse("GET");
        assertThat(cmd.type()).isEqualTo(CommandType.UNKNOWN);
        assertThat(cmd.arg(0)).contains("wrong number of arguments");
        assertThat(cmd.arg(0)).contains("'get'");
    }

    @Test
    void get_tooManyArgs_returnsErrorCommand() {
        Command cmd = parser.parse("GET foo bar");
        assertThat(cmd.type()).isEqualTo(CommandType.UNKNOWN);
        assertThat(cmd.arg(0)).contains("wrong number of arguments");
    }

    @Test
    void set_onlyOneArg_returnsErrorCommand() {
        Command cmd = parser.parse("SET foo");
        assertThat(cmd.type()).isEqualTo(CommandType.UNKNOWN);
        assertThat(cmd.arg(0)).contains("wrong number of arguments");
    }

    // ── Unknown commands ────────────────────────────────────────────

    @Test
    void unknownCommand_returnsErrorCommand() {
        Command cmd = parser.parse("BLAH foo");
        assertThat(cmd.type()).isEqualTo(CommandType.UNKNOWN);
        assertThat(cmd.arg(0)).contains("unknown command");
        assertThat(cmd.arg(0)).contains("'blah'");
    }

    // ── Empty / null input ──────────────────────────────────────────

    @ParameterizedTest
    @ValueSource(strings = { "", "   ", "\t", "\r\n" })
    void emptyOrWhitespaceInput_returnsEmptyCommand(String input) {
        Command cmd = parser.parse(input);
        assertThat(cmd.type()).isEqualTo(CommandType.UNKNOWN);
        assertThat(cmd.argCount()).isEqualTo(0);  // empty command, not error
    }

    @Test
    void nullInput_returnsErrorCommand() {
        Command cmd = parser.parse(null);
        assertThat(cmd.type()).isEqualTo(CommandType.UNKNOWN);
        assertThat(cmd.argCount()).isEqualTo(1);  // has error message
    }
}