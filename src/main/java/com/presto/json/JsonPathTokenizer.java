package com.presto.json;

import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.AbstractIterator;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.lang.Character.isLetterOrDigit;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class JsonPathTokenizer
        extends AbstractIterator<String> {
    private static final char QUOTE = '\"';
    private static final char DOT = '.';
    private static final char OPEN_BRACKET = '[';
    private static final char CLOSE_BRACKET = ']';
    private static final char UNICODE_CARET = '\u2038';

    private final String path;
    private int index;

    public JsonPathTokenizer(String path) {
        this.path = requireNonNull(path, "path is null");

        if (path.isEmpty()) {
            throw invalidJsonPath();
        }

        // skip the start token
        match('$');
    }

    private static boolean isUnquotedPathCharacter(char c) {
        return c == ':' || isUnquotedSubscriptCharacter(c);
    }

    private static boolean isUnquotedSubscriptCharacter(char c) {
        return c == '_' || isLetterOrDigit(c);
    }

    @Override
    protected String computeNext() {
        if (!hasNextCharacter()) {
            return endOfData();
        }

        if (tryMatch(DOT)) {
            return matchPathSegment();
        }

        if (tryMatch(OPEN_BRACKET)) {
            String token = tryMatch(QUOTE) ? matchQuotedSubscript() : matchUnquotedSubscript();

            match(CLOSE_BRACKET);
            return token;
        }

        throw invalidJsonPath();
    }

    private String matchPathSegment() {
        // seek until we see a special character or whitespace
        int start = index;
        while (hasNextCharacter() && isUnquotedPathCharacter(peekCharacter())) {
            nextCharacter();
        }
        int end = index;

        String token = path.substring(start, end);

        // an empty unquoted token is not allowed
        if (token.isEmpty()) {
            throw invalidJsonPath();
        }

        return token;
    }

    private String matchUnquotedSubscript() {
        // seek until we see a special character or whitespace
        int start = index;
        while (hasNextCharacter() && isUnquotedSubscriptCharacter(peekCharacter())) {
            nextCharacter();
        }
        int end = index;

        String token = path.substring(start, end);

        // an empty unquoted token is not allowed
        if (token.isEmpty()) {
            throw invalidJsonPath();
        }

        return token;
    }

    private String matchQuotedSubscript() {
        // quote has already been matched

        // seek until we see the close quote
        int start = index;
        while (hasNextCharacter() && peekCharacter() != QUOTE) {
            nextCharacter();
        }
        int end = index;

        String token = path.substring(start, end);

        match(QUOTE);
        return token;
    }

    private boolean hasNextCharacter() {
        return index < path.length();
    }

    private void match(char expected) {
        if (!tryMatch(expected)) {
            throw invalidJsonPath();
        }
    }

    private boolean tryMatch(char expected) {
        if (!hasNextCharacter() || peekCharacter() != expected) {
            return false;
        }
        index++;
        return true;
    }

    private void nextCharacter() {
        index++;
    }

    private char peekCharacter() {
        return path.charAt(index);
    }

    private PrestoException invalidJsonPath() {
        return new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Invalid JSON path: '%s'", path));
    }

    @Override
    public String toString() {
        return path.substring(0, index) + UNICODE_CARET + path.substring(index);
    }
}
