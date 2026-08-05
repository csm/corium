package dev.corium.client;

import java.util.Objects;

/**
 * The immutable view one {@link Db} names, independent of the peer that serves
 * it. Both backends translate this into their own representation: a protocol
 * {@code DbViewSpec} remotely, and a derived native handle locally.
 */
final class DbView {
    /** Wire ordinals shared with the native adapter. */
    static final int CURRENT = 0;

    static final int AS_OF = 1;
    static final int SINCE = 2;
    static final int HISTORY = 3;
    static final int AS_OF_INSTANT = 4;
    static final int SINCE_INSTANT = 5;

    private final String databaseName;
    private final int kind;
    private final long value;

    private DbView(String databaseName, int kind, long value) {
        this.databaseName = Objects.requireNonNull(databaseName, "databaseName");
        this.kind = kind;
        this.value = value;
    }

    static DbView current(String databaseName) {
        return new DbView(databaseName, CURRENT, 0);
    }

    DbView derive(int kind, long value) {
        return new DbView(databaseName, kind, value);
    }

    String databaseName() { return databaseName; }
    int kind() { return kind; }
    long value() { return value; }
}
