package dev.corium.client;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A live peer-server smoke scenario, enabled only by the scenario runner. */
@EnabledIfEnvironmentVariable(named = "CORIUM_SCENARIO_PEER_ENDPOINT", matches = ".+")
final class LiveScenarioTest {
    @Test
    void accessesALiveDatabase() throws Exception {
        String endpoint = System.getenv("CORIUM_SCENARIO_PEER_ENDPOINT");
        String database = System.getenv().getOrDefault("CORIUM_SCENARIO_DATABASE", "people");

        try (RemotePeer peer = RemotePeer.builder(endpoint, database).build()) {
            DbStats before = peer.db().stats().get(30, TimeUnit.SECONDS);
            TxReport report = peer.transact(java.util.List.of()).get(30, TimeUnit.SECONDS);
            DbStats after = report.dbAfter().stats().get(30, TimeUnit.SECONDS);

            assertTrue(report.basisT() > before.basisT());
            assertEquals(report.basisT(), after.basisT());
            System.out.printf("java client reached %s: basis %d -> %d%n",
                    database, before.basisT(), after.basisT());
        }
    }
}
