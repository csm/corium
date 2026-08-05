package dev.corium.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import dev.corium.protocol.v1.Corium;
import dev.corium.protocol.v1.PeerServerGrpc;
import io.grpc.Server;
import io.grpc.stub.StreamObserver;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

final class RemotePeerIntegrationTest {
    private final FakePeerServer service = new FakePeerServer();
    private Server server;

    @BeforeEach
    void startServer() throws Exception {
        server = NettyServerBuilder.forPort(0).addService(service).build().start();
    }

    @AfterEach
    void stopServer() throws Exception {
        if (server != null) server.shutdownNow().awaitTermination();
    }

    @Test
    void performsTheInitialRemoteApiOverGrpc() {
        try (RemotePeer peer = RemotePeer.builder(
                "http://127.0.0.1:" + server.getPort(), "people").build()) {
            Db db = peer.db().join().asOf(7);
            QueryResult names = db.query(Query.findCollection("?name")
                    .where("?entity", ":person/name", "?name")).join();
            assertEquals(QueryResult.Shape.COLLECTION, names.shape());
            assertEquals(List.of("Ada", "Grace"), names.value());
            assertEquals(7, service.queryRequest.getDbs(0).getAsOf());

            assertEquals(Map.of(new Keyword("person/name"), "Ada"),
                    db.pull(new Pull().attr("person/name"), EntityId.of(42)).join());

            List<Datom> datoms = db.datoms(Index.EAVT, EntityId.of(42)).join();
            assertEquals("Ada", datoms.get(0).value());
            assertTrue(datoms.get(0).added());

            DbStats stats = db.stats().join();
            assertEquals(7, stats.basisT());
            assertEquals(12, stats.datoms());

            TxReport report = peer.transact(new TxBuilder().entity(
                    EntityMap.withId("ada").set("person/name", "Ada"))).join();
            assertEquals(8, report.basisT());
            assertEquals(Instant.ofEpochMilli(1_700_000_000_000L), report.transactionInstant());
            assertEquals(Map.of("ada", 42L), report.tempids());
            assertTrue(CoriumCodec.decode(service.transactRequest.getTxData().toByteArray()) instanceof List<?>);
        }
    }

    private static final class FakePeerServer extends PeerServerGrpc.PeerServerImplBase {
        private Corium.QueryRequest queryRequest;
        private Corium.TransactRequest transactRequest;

        @Override
        public void query(Corium.QueryRequest request, StreamObserver<Corium.QueryResultChunk> observer) {
            queryRequest = request;
            observer.onNext(Corium.QueryResultChunk.newBuilder()
                    .setShape(Corium.ResultShape.RESULT_SHAPE_COLLECTION)
                    .setRows(bytes(List.of("Ada"))).setLast(false).build());
            observer.onNext(Corium.QueryResultChunk.newBuilder()
                    .setShape(Corium.ResultShape.RESULT_SHAPE_COLLECTION)
                    .setRows(bytes(List.of("Grace"))).setLast(true).build());
            observer.onCompleted();
        }

        @Override
        public void pull(Corium.PullRequest request, StreamObserver<Corium.PullResponse> observer) {
            observer.onNext(Corium.PullResponse.newBuilder()
                    .setResult(bytes(Map.of(new Keyword("person/name"), "Ada"))).build());
            observer.onCompleted();
        }

        @Override
        public void datoms(Corium.DatomsRequest request, StreamObserver<Corium.DatomChunk> observer) {
            byte[] datom = {1, 42, 2, 3, 1, 0x71, 0, 3, 'A', 'd', 'a'};
            observer.onNext(Corium.DatomChunk.newBuilder()
                    .setDatoms(ByteString.copyFrom(datom)).setLast(true).build());
            observer.onCompleted();
        }

        @Override
        public void dbStats(Corium.DbStatsRequest request, StreamObserver<Corium.DbStatsResponse> observer) {
            observer.onNext(Corium.DbStatsResponse.newBuilder().setBasisT(7)
                    .setDatomCount(12).setEntityCount(3).setAttributeCount(4).build());
            observer.onCompleted();
        }

        @Override
        public void transact(Corium.TransactRequest request, StreamObserver<Corium.TransactResponse> observer) {
            transactRequest = request;
            observer.onNext(Corium.TransactResponse.newBuilder()
                    .setBasisBefore(7).setBasisT(8).setTxInstant(1_700_000_000_000L)
                    .setTempids(bytes(Map.of("ada", 42L))).build());
            observer.onCompleted();
        }

        private static ByteString bytes(Object value) {
            return ByteString.copyFrom(CoriumCodec.encode(value));
        }
    }
}
