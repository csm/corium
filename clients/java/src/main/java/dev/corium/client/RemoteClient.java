package dev.corium.client;

import dev.corium.protocol.v1.CatalogGrpc;
import dev.corium.protocol.v1.Corium;
import dev.corium.protocol.v1.PeerServerGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static dev.corium.client.RemoteSupport.parseEndpoint;
import static dev.corium.client.RemoteSupport.unary;

public final class RemoteClient implements Client<RemotePeer>, AutoCloseable {
    public static final int PROTOCOL_VERSION = 1;

    final ManagedChannel channel;
    private final CatalogGrpc.CatalogStub catalogStub;
    private volatile boolean closed;
    private final boolean tls;
    private final ConcurrentHashMap<RemotePeer, RemotePeer> peers;
    private final String token;
    private final boolean allowInsecureToken;

    private RemoteClient(Builder builder) {
        peers = new ConcurrentHashMap<>();
        URI endpoint = parseEndpoint(builder.endpoint);
        tls = endpoint.getScheme().equalsIgnoreCase("https");
        token = builder.token;
        allowInsecureToken = builder.allowInsecureToken;
        if (builder.token != null && !tls && !builder.allowInsecureToken) {
            throw new IllegalArgumentException("bearer tokens require an https:// endpoint");
        }
        if (!tls && (builder.tlsCa != null || builder.tlsDomain != null)) {
            throw new IllegalArgumentException("custom TLS options require an https:// endpoint");
        }

        int port = endpoint.getPort() >= 0 ? endpoint.getPort() : (tls ? 443 : 80);
        NettyChannelBuilder channelBuilder = NettyChannelBuilder.forAddress(endpoint.getHost(), port);
        if (tls) {
            try {
                io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder ssl = GrpcSslContexts.forClient();
                if (builder.tlsCa != null) ssl.trustManager(TrustManagers.platformAndPem(builder.tlsCa));
                channelBuilder.sslContext(ssl.build());
            } catch (Exception error) {
                throw new IllegalArgumentException("could not configure TLS", error);
            }
            if (builder.tlsDomain != null) channelBuilder.overrideAuthority(builder.tlsDomain);
        } else {
            channelBuilder.usePlaintext();
        }
        this.channel = channelBuilder.build();
        CatalogGrpc.CatalogStub catalogStub = CatalogGrpc.newStub(channel);
        if (builder.token != null) {
            Metadata headers = new Metadata();
            Metadata.Key<String> authorization = Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
            headers.put(authorization, "Bearer " + builder.token);
            catalogStub = catalogStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
        }
        this.catalogStub = catalogStub;
    }

    @Override
    public RemotePeer peer(String dbName) {
        var builder = RemotePeer.builder(this, dbName).allowInsecureToken(allowInsecureToken);
        if (token != null) {
            builder.token(token);
        }
        return builder.build();
    }

    @Override
    public CompletableFuture<List<String>> listDatabases() {
        ensureOpen();
        Corium.ListDatabasesRequest request = Corium.ListDatabasesRequest.getDefaultInstance();
        CompletableFuture<List<String>> promise = new CompletableFuture<>();
        catalogStub.listDatabases(request, unary(promise, Corium.ListDatabasesResponse::getDbsList));
        return promise;
    }

    @Override
    public void close() {
        if (closed) return;
        peers.keySet().forEach(RemotePeer::close);
        peers.clear();
        closed = true;
        channel.shutdown();
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return channel.awaitTermination(timeout, unit);
    }

    public static Builder builder(String endpoint) {
        return new Builder(endpoint);
    }

    void ensureOpen() {
        if (closed) throw new CoriumException("peer is closed");
    }

    boolean isTls() {
        return tls;
    }

    void addPeer(RemotePeer peer) {
        ensureOpen();
        peers.putIfAbsent(peer, peer);
    }

    void removePeer(RemotePeer peer) {
        peers.remove(peer);
    }

    public static final class Builder {
        private final String endpoint;
        private String token;
        private boolean allowInsecureToken;
        private byte[] tlsCa;
        private String tlsDomain;

        private Builder(String endpoint) {
            this.endpoint = Objects.requireNonNull(endpoint, "endpoint");
        }

        public Builder token(String token) { this.token = Objects.requireNonNull(token, "token"); return this; }
        public Builder allowInsecureToken(boolean allowed) { this.allowInsecureToken = allowed; return this; }
        public Builder tlsCa(byte[] pem) { this.tlsCa = Objects.requireNonNull(pem, "pem").clone(); return this; }
        public Builder tlsDomain(String domain) {
            this.tlsDomain = Objects.requireNonNull(domain, "domain");
            if (domain.isEmpty()) throw new IllegalArgumentException("TLS domain must not be empty");
            return this;
        }
        public RemoteClient build() { return new RemoteClient(this); }
    }
}
