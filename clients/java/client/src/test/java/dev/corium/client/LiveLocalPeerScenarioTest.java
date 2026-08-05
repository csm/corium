package dev.corium.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/** A live in-process peer smoke scenario, enabled only by the scenario runner. */
@EnabledIfEnvironmentVariable(named = "CORIUM_SCENARIO_TRANSACTOR_ENDPOINT", matches = ".+")
final class LiveLocalPeerScenarioTest {
    private static final long TIMEOUT_SECONDS = 120;

    private static String endpoint() {
        return System.getenv("CORIUM_SCENARIO_TRANSACTOR_ENDPOINT");
    }

    private static String database() {
        return System.getenv().getOrDefault("CORIUM_SCENARIO_DATABASE", "people");
    }

    @Test
    void indexesAndQueriesInProcess() throws Exception {
        try (LocalPeer peer = connect(LocalPeer.builder(endpoint(), database()))) {
            DbStats before = await(await(peer.db()).stats());
            TxReport report = await(peer.transact(List.of()));
            Db after = await(peer.sync());

            assertTrue(report.basisT() > before.basisT());
            assertEquals(report.basisT(), await(after.stats()).basisT());
            assertEquals(report.basisT(), await(report.dbAfter().stats()).basisT());

            QueryResult names = await(after.query(Query.findCollection("?name")
                    .where("?entity", ":person/name", "?name")));
            assertEquals(QueryResult.Shape.COLLECTION, names.shape());
            assertTrue(names.value() instanceof List<?>);

            List<Datom> datoms = await(after.datoms(Index.AEVT, new Object[0]));
            assertTrue(datoms.size() <= after.stats().join().datoms());

            System.out.printf("java local peer reached %s: basis %d -> %d%n",
                    database(), before.basisT(), report.basisT());
        }
    }

    @Test
    void readsPublishedSegmentsDirectly() throws Exception {
        LocalPeer peer;
        try {
            peer = connect(LocalPeer.builder(endpoint(), database())
                    .directStorage(DirectStorage.discover()));
        } catch (ExecutionException error) {
            Throwable cause = error.getCause();
            // A scenario storage backend whose plugin artifact is absent is a
            // packaging condition, not a client failure.
            assumeTrue(!(cause instanceof CoriumException)
                            || ((CoriumException) cause).kind() != ErrorKind.STORAGE,
                    () -> "direct storage unavailable: " + cause.getMessage());
            throw error;
        }
        try (LocalPeer direct = peer) {
            Db db = await(direct.db());
            DbStats stats = await(db.stats());
            QueryResult names = await(db.query(Query.findCollection("?name")
                    .where("?entity", ":person/name", "?name")));
            assertEquals(QueryResult.Shape.COLLECTION, names.shape());
            System.out.printf("java local peer read %s directly at basis %d%n",
                    database(), stats.basisT());
        }
    }

    private static LocalPeer connect(LocalPeer.Builder builder) throws Exception {
        return builder.connect().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private static <T> T await(java.util.concurrent.CompletableFuture<T> future) throws Exception {
        return future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
}
