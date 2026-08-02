package dev.corium.client;

import com.google.protobuf.ByteString;
import dev.corium.protocol.v1.Corium;
import dev.corium.protocol.v1.PeerServerGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static dev.corium.client.RemoteSupport.mapError;
import static dev.corium.client.RemoteSupport.unary;

/** A lightweight asynchronous client connected to {@code corium peer-server}. */
public final class RemotePeer implements Peer, AutoCloseable {
    public static final int PROTOCOL_VERSION = 1;

    private final RemoteClient client;
    private final String databaseName;
    private final ManagedChannel channel;
    private final PeerServerGrpc.PeerServerStub stub;
    private volatile boolean closed;

    RemotePeer(Builder builder) {
        this.databaseName = builder.databaseName;
        this.client = builder.client;
        this.channel = client.channel;
        PeerServerGrpc.PeerServerStub created = PeerServerGrpc.newStub(channel);
        if (builder.token != null) {
            Metadata headers = new Metadata();
            Metadata.Key<String> authorization = Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
            headers.put(authorization, "Bearer " + builder.token);
            created = created.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
        }
        this.stub = created;
    }

    public static Builder builder(RemoteClient client, String databaseName) {
        return new Builder(client, databaseName);
    }

    public String databaseName() { return databaseName; }

    @Override
    public Db db() {
        ensureOpen();
        return new Db(this);
    }

    /** Remote views are always current, so sync completes with a fresh current view. */
    @Override
    public CompletableFuture<Db> sync() {
        return CompletableFuture.completedFuture(db());
    }

    @Override
    public CompletableFuture<TxReport> transact(Object transactionData) {
        ensureOpen();
        Corium.TransactRequest request = Corium.TransactRequest.newBuilder()
                .setDb(databaseName)
                .setProtocolVersion(PROTOCOL_VERSION)
                .setTxData(ByteString.copyFrom(CoriumCodec.encode(transactionData)))
                .build();
        CompletableFuture<TxReport> future = new CompletableFuture<>();
        stub.transact(request, unary(future, response -> {
            Object decoded = CoriumCodec.decode(response.getTempids().toByteArray());
            if (!(decoded instanceof Map<?, ?>)) throw new DecodeException("transaction tempids are not a map");
            Map<String, Long> tempids = new LinkedHashMap<>();
            ((Map<?, ?>) decoded).forEach((key, value) -> {
                if (!(key instanceof String) || !(value instanceof Long)) {
                    throw new DecodeException("transaction tempids contain an invalid entry");
                }
                tempids.put((String) key, (Long) value);
            });
            return new TxReport(response.getBasisBefore(), response.getBasisT(),
                    Instant.ofEpochMilli(response.getTxInstant()), tempids, db());
        }));
        return future;
    }

    CompletableFuture<QueryResult> query(
            List<Corium.DbViewSpec> views, Object query, List<?> arguments, long fuel) {
        ensureOpen();
        Corium.QueryRequest request = Corium.QueryRequest.newBuilder()
                .addAllDbs(views)
                .setQuery(ByteString.copyFrom(CoriumCodec.encode(query)))
                .setArgs(ByteString.copyFrom(CoriumCodec.encode(arguments)))
                .setFuel(fuel)
                .build();
        CompletableFuture<QueryResult> future = new CompletableFuture<>();
        stub.query(request, new StreamObserver<Corium.QueryResultChunk>() {
            private Corium.ResultShape shape;
            private final List<Object> rows = new ArrayList<>();
            private Object single;
            private boolean receivedSingle;
            private boolean sawLast;

            @Override public void onNext(Corium.QueryResultChunk chunk) {
                try {
                    if (sawLast) throw new DecodeException("query stream sent a chunk after its last chunk");
                    if (shape == null) shape = chunk.getShape();
                    if (chunk.getShape() != shape) throw new DecodeException("query result shape changed between chunks");
                    if (shape == Corium.ResultShape.RESULT_SHAPE_UNSPECIFIED
                            || shape == Corium.ResultShape.UNRECOGNIZED) {
                        throw new DecodeException("query result has unspecified shape");
                    }
                    Object decoded = CoriumCodec.decode(chunk.getRows().toByteArray());
                    if (shape == Corium.ResultShape.RESULT_SHAPE_RELATION
                            || shape == Corium.ResultShape.RESULT_SHAPE_COLLECTION) {
                        if (!(decoded instanceof List<?>)) throw new DecodeException("chunked query result is not a vector");
                        rows.addAll((List<?>) decoded);
                    } else {
                        if (receivedSingle) throw new DecodeException("tuple/scalar query returned multiple chunks");
                        single = decoded;
                        receivedSingle = true;
                    }
                    sawLast = chunk.getLast();
                } catch (Throwable error) {
                    future.completeExceptionally(error);
                }
            }
            @Override public void onError(Throwable error) { future.completeExceptionally(mapError(error)); }
            @Override public void onCompleted() {
                if (future.isDone()) return;
                if (!sawLast || shape == null) {
                    future.completeExceptionally(new DecodeException("query stream ended before its last chunk"));
                    return;
                }
                Object value = shape == Corium.ResultShape.RESULT_SHAPE_RELATION
                        || shape == Corium.ResultShape.RESULT_SHAPE_COLLECTION
                        ? Collections.unmodifiableList(rows) : single;
                future.complete(new QueryResult(queryShape(shape), value));
            }
        });
        return future;
    }

    CompletableFuture<Object> pull(Corium.DbViewSpec view, Object pattern, Object entity) {
        ensureOpen();
        Corium.PullRequest request = Corium.PullRequest.newBuilder()
                .setDb(view)
                .setPattern(ByteString.copyFrom(CoriumCodec.encode(pattern)))
                .setEid(ByteString.copyFrom(CoriumCodec.encode(entity)))
                .build();
        CompletableFuture<Object> future = new CompletableFuture<>();
        stub.pull(request, unary(future, response -> CoriumCodec.decode(response.getResult().toByteArray())));
        return future;
    }

    CompletableFuture<List<Datom>> datoms(Corium.DbViewSpec view, Index index, List<?> components, long limit) {
        ensureOpen();
        Corium.DatomsRequest request = Corium.DatomsRequest.newBuilder()
                .setDb(view).setIndex(index.wireName())
                .setComponents(ByteString.copyFrom(CoriumCodec.encode(components)))
                .setLimit(limit).build();
        CompletableFuture<List<Datom>> future = new CompletableFuture<>();
        stub.datoms(request, new StreamObserver<Corium.DatomChunk>() {
            private final List<Datom> datoms = new ArrayList<>();
            private boolean sawLast;
            @Override public void onNext(Corium.DatomChunk chunk) {
                try {
                    if (sawLast) throw new DecodeException("datom stream sent a chunk after its last chunk");
                    datoms.addAll(CoriumCodec.decodeDatoms(chunk.getDatoms().toByteArray()));
                    sawLast = chunk.getLast();
                } catch (Throwable error) { future.completeExceptionally(error); }
            }
            @Override public void onError(Throwable error) { future.completeExceptionally(mapError(error)); }
            @Override public void onCompleted() {
                if (future.isDone()) return;
                if (!sawLast) future.completeExceptionally(new DecodeException("datom stream ended before its last chunk"));
                else future.complete(Collections.unmodifiableList(datoms));
            }
        });
        return future;
    }

    CompletableFuture<DbStats> stats(Corium.DbViewSpec view) {
        ensureOpen();
        Corium.DbStatsRequest request = Corium.DbStatsRequest.newBuilder().setDb(view).build();
        CompletableFuture<DbStats> future = new CompletableFuture<>();
        stub.dbStats(request, unary(future, response -> new DbStats(response.getBasisT(),
                response.getDatomCount(), response.getEntityCount(), response.getAttributeCount())));
        return future;
    }

    void ensureOpen() {
        client.ensureOpen();
        if (closed) throw new CoriumException("peer is closed");
    }

    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        // leave the channel from
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return channel.awaitTermination(timeout, unit);
    }

    private static URI parseEndpoint(String value) {
        try {
            URI uri = new URI(Objects.requireNonNull(value, "endpoint"));
            if (!("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme()))
                    || uri.getHost() == null || uri.getUserInfo() != null || uri.getQuery() != null
                    || uri.getFragment() != null
                    || (uri.getPath() != null && !uri.getPath().isEmpty() && !"/".equals(uri.getPath()))) {
                throw new IllegalArgumentException("endpoint must be an http:// or https:// origin");
            }
            return uri;
        } catch (URISyntaxException error) {
            throw new IllegalArgumentException("invalid endpoint", error);
        }
    }

    private static QueryResult.Shape queryShape(Corium.ResultShape shape) {
        switch (shape) {
            case RESULT_SHAPE_RELATION: return QueryResult.Shape.RELATION;
            case RESULT_SHAPE_COLLECTION: return QueryResult.Shape.COLLECTION;
            case RESULT_SHAPE_TUPLE: return QueryResult.Shape.TUPLE;
            case RESULT_SHAPE_SCALAR: return QueryResult.Shape.SCALAR;
            default: throw new DecodeException("query result has unspecified shape");
        }
    }

    public static final class Builder {
        private final RemoteClient client;
        private final String databaseName;
        private String token;
        private boolean allowInsecureToken;
        private byte[] tlsCa;
        private String tlsDomain;

        private Builder(RemoteClient client, String databaseName) {
            this.client = Objects.requireNonNull(client, "client");
            this.databaseName = Objects.requireNonNull(databaseName, "databaseName");
            if (databaseName.isEmpty()) throw new IllegalArgumentException("database name must not be empty");
        }

        public Builder token(String token) { this.token = Objects.requireNonNull(token, "token"); return this; }
        public Builder allowInsecureToken(boolean allowed) { this.allowInsecureToken = allowed; return this; }
        public Builder tlsCa(byte[] pem) { this.tlsCa = Objects.requireNonNull(pem, "pem").clone(); return this; }
        public Builder tlsDomain(String domain) {
            this.tlsDomain = Objects.requireNonNull(domain, "domain");
            if (domain.isEmpty()) throw new IllegalArgumentException("TLS domain must not be empty");
            return this;
        }
        public RemotePeer build() { return new RemotePeer(this); }
    }
}
