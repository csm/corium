package dev.corium.client;

/**
 * Asks a {@link LocalPeer} to discover the transactor's storage connection and
 * read published segments directly, instead of routing every read through the
 * transactor. The backend it discovers must be compiled in or provided by an
 * installed {@link StoragePlugin}.
 */
public final class DirectStorage {
    private final SegmentCache cache;

    private DirectStorage(SegmentCache cache) {
        this.cache = cache;
    }

    /** Reads segments straight from authoritative storage. */
    public static DirectStorage discover() {
        return new DirectStorage(null);
    }

    /** Reads segments through a bounded local cache. */
    public static DirectStorage discover(SegmentCache cache) {
        return new DirectStorage(cache);
    }

    /** The local segment cache, or {@code null} when reads are uncached. */
    public SegmentCache cache() {
        return cache;
    }
}
