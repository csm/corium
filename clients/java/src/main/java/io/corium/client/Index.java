package io.corium.client;

/** A covering datom index. */
public enum Index {
    EAVT("eavt"), AEVT("aevt"), AVET("avet"), VAET("vaet");

    private final String wireName;

    Index(String wireName) {
        this.wireName = wireName;
    }

    String wireName() {
        return wireName;
    }
}
