package dev.corium.client;

import java.lang.ref.Reference;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * An in-process full peer.
 *
 * <p>A local peer runs the Corium engine through the native library, so it
 * indexes and queries in this JVM instead of asking a peer server to. It needs
 * the platform engine artifact on the classpath; direct storage additionally
 * needs the plugin artifact for the backend the transactor advertises.
 *
 * <pre>{@code
 * LocalPeer peer = LocalPeer.builder("https://transactor:4335", "people")
 *         .directStorage(DirectStorage.discover())
 *         .connect()
 *         .join();
 * }</pre>
 */
public final class LocalPeer implements Peer {
    private final long handle;
    private final String databaseName;
    private volatile boolean closed;

    private LocalPeer(long handle) {
        this.handle = handle;
        this.databaseName = Native.peerDatabaseName(handle);
        NativeHandles.releasePeerWith(this, handle);
    }

    /** Connects to one transactor endpoint. */
    public static Builder builder(String endpoint, String databaseName) {
        return builder(List.of(Objects.requireNonNull(endpoint, "endpoint")), databaseName);
    }

    /** Connects to transactor endpoints in failover order. */
    public static Builder builder(List<String> endpoints, String databaseName) {
        return new Builder(endpoints, databaseName);
    }

    /** The direct-storage backends registered in this process. */
    public static Set<String> availableStorageBackends() {
        NativeLibraries.ensureEngineLoaded();
        return Collections.unmodifiableSet(
                new LinkedHashSet<>(Arrays.asList(Native.storageBackends())));
    }

    @Override
    public String databaseName() {
        return databaseName;
    }

    @Override
    public CompletableFuture<Db> db() {
        ensureOpen();
        CompletableFuture<Long> future = new CompletableFuture<>();
        Native.peerDb(handle, future);
        Reference.reachabilityFence(this);
        return future.thenApply(this::value);
    }

    @Override
    public CompletableFuture<Db> sync() {
        ensureOpen();
        CompletableFuture<Long> future = new CompletableFuture<>();
        Native.peerSync(handle, future);
        Reference.reachabilityFence(this);
        return future.thenApply(this::value);
    }

    @Override
    public CompletableFuture<TxReport> transact(Object transactionData) {
        ensureOpen();
        CompletableFuture<Object[]> future = new CompletableFuture<>();
        Native.peerTransact(handle, CoriumCodec.encode(transactionData), future);
        Reference.reachabilityFence(this);
        return future.thenApply(this::report);
    }

    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        Native.peerClose(handle);
        Reference.reachabilityFence(this);
    }

    private Db value(long db) {
        return LocalDbBackend.value(db, databaseName);
    }

    private TxReport report(Object[] result) {
        long[] scalars = (long[]) result[0];
        String[] names = (String[]) result[1];
        long[] entities = (long[]) result[2];
        Map<String, Long> tempids = new LinkedHashMap<>();
        for (int index = 0; index < names.length; index++) {
            tempids.put(names[index], entities[index]);
        }
        return new TxReport(scalars[0], scalars[1], Instant.ofEpochMilli(scalars[2]), tempids,
                value(scalars[3]));
    }

    private void ensureOpen() {
        if (closed) throw new CoriumException("peer is closed", ErrorKind.CLOSED, null, null);
    }

    /** Builds an in-process peer. */
    public static final class Builder {
        private final List<String> endpoints;
        private final String databaseName;
        private String token;
        private boolean allowInsecureToken;
        private byte[] tlsCa;
        private String tlsDomain;
        private DirectStorage directStorage;

        private Builder(List<String> endpoints, String databaseName) {
            this.endpoints = List.copyOf(Objects.requireNonNull(endpoints, "endpoints"));
            this.databaseName = Objects.requireNonNull(databaseName, "databaseName");
            if (databaseName.isEmpty()) {
                throw new IllegalArgumentException("database name must not be empty");
            }
        }

        public Builder token(String token) {
            this.token = Objects.requireNonNull(token, "token");
            return this;
        }

        /** Allows a bearer token over plaintext, for local development only. */
        public Builder allowInsecureToken(boolean allowed) {
            this.allowInsecureToken = allowed;
            return this;
        }

        public Builder tlsCa(byte[] pem) {
            this.tlsCa = Objects.requireNonNull(pem, "pem").clone();
            return this;
        }

        public Builder tlsDomain(String domain) {
            this.tlsDomain = Objects.requireNonNull(domain, "domain");
            if (domain.isEmpty()) {
                throw new IllegalArgumentException("TLS domain must not be empty");
            }
            return this;
        }

        /** Reads published segments straight from the transactor's storage. */
        public Builder directStorage(DirectStorage storage) {
            this.directStorage = Objects.requireNonNull(storage, "storage");
            return this;
        }

        /** Connects the peer, loading the native engine on first use. */
        public CompletableFuture<LocalPeer> connect() {
            boolean tls = Endpoints.tls(endpoints, token != null, allowInsecureToken, tlsCa,
                    tlsDomain);
            NativeLibraries.ensureEngineLoaded();
            return requireStorageBackend(tls).thenCompose(ignored -> {
                SegmentCache cache = directStorage == null ? null : directStorage.cache();
                CompletableFuture<Long> future = new CompletableFuture<>();
                Native.connectLocal(
                        endpoints.toArray(new String[0]),
                        databaseName,
                        token,
                        tls,
                        tlsCa,
                        tlsDomain,
                        directStorage != null,
                        cache == null ? null : cache.directory().toAbsolutePath().toString(),
                        cache == null ? 0 : cache.capacityBytes(),
                        cache == null ? 0 : cache.memoryCapacityBytes(),
                        future);
                return future.thenApply(LocalPeer::new);
            });
        }

        /**
         * Fails early, and by name, when direct storage would need a plugin
         * artifact this process does not have.
         */
        private CompletableFuture<Void> requireStorageBackend(boolean tls) {
            if (directStorage == null) {
                return CompletableFuture.completedFuture(null);
            }
            CompletableFuture<String> discovered = new CompletableFuture<>();
            Native.discoverStorageBackend(endpoints.toArray(new String[0]), databaseName, token,
                    tls, tlsCa, tlsDomain, discovered);
            return discovered.thenAccept(backend -> {
                if (!availableStorageBackends().contains(backend)) {
                    throw new CoriumException(
                            "the " + backend + " storage plugin is not installed",
                            ErrorKind.STORAGE, null, null);
                }
            });
        }
    }
}
