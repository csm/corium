package dev.corium.client;

import io.grpc.Status;

import java.util.concurrent.CompletableFuture;

/**
 * Bindings to the Corium native peer library.
 *
 * <p>Composite values cross this boundary already encoded by
 * {@link CoriumCodec}, so the native adapter never maps Java values itself.
 * Asynchronous calls hand a {@link CompletableFuture} to the native side, which
 * completes it from the engine's runtime.
 */
final class Native {
    static {
        NativeLibraries.loadEngine();
        initialize();
        NativeLibraries.registerStoragePlugins();
    }

    private Native() {}

    /**
     * Loads the engine and every installed storage plugin, by initializing this
     * class. Call {@link NativeLibraries#ensureEngineLoaded()} instead, which
     * reports a load failure as a {@link NativeExtensionException}.
     */
    static void ensureLoaded() {}

    static native void initialize();

    static native String[] storageBackends();

    static native String[] loadStoragePlugin(String path);

    static native void discoverStorageBackend(
            String[] endpoints, String database, String token, boolean tls, byte[] tlsCa,
            String tlsDomain, CompletableFuture<String> future);

    static native void connectLocal(
            String[] endpoints, String database, String token, boolean tls, byte[] tlsCa,
            String tlsDomain, boolean directStorage, String cacheDirectory,
            long cacheCapacityBytes, long cacheMemoryBytes, CompletableFuture<Long> future);

    static native String peerDatabaseName(long peer);

    static native void peerDb(long peer, CompletableFuture<Long> future);

    static native void peerSync(long peer, CompletableFuture<Long> future);

    static native void peerTransact(long peer, byte[] txData, CompletableFuture<Object[]> future);

    static native void peerClose(long peer);

    static native void freePeer(long peer);

    static native void freeDb(long db);

    static native void dbQuery(long db, int viewKind, long viewValue, byte[] query,
                               byte[][] arguments, long fuel, CompletableFuture<Object[]> future);

    static native void dbPull(long db, int viewKind, long viewValue, byte[] pattern, byte[] entity,
                              CompletableFuture<byte[]> future);

    static native void dbDatoms(long db, int viewKind, long viewValue, String index,
                                byte[][] components, long limit,
                                CompletableFuture<Object[]> future);

    static native void dbStats(long db, int viewKind, long viewValue,
                               CompletableFuture<long[]> future);

    /**
     * Builds a categorized failure. The native adapter calls this so that both
     * peers raise the same exception type, and passes {@code -1} when the
     * failure did not come from an RPC.
     */
    static CoriumException error(String kind, String message, int grpcCode) {
        String code = grpcCode < 0 ? null : Status.fromCodeValue(grpcCode).getCode().name();
        return new CoriumException(message, ErrorKind.fromNative(kind), code, null);
    }
}
