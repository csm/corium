import dev.corium.client.Db;
import dev.corium.client.EntityMap;
import dev.corium.client.LocalPeer;
import dev.corium.client.LookupRef;
import dev.corium.client.Peer;
import dev.corium.client.Pull;
import dev.corium.client.Query;
import dev.corium.client.RemotePeer;
import dev.corium.client.TxBuilder;

/** Uses the same builders with either Corium peer topology. */
public final class People {
    public static void main(String[] args) {
        String endpoint = System.getenv().getOrDefault("CORIUM_ENDPOINT", "http://127.0.0.1:4334");
        String database = System.getenv().getOrDefault("CORIUM_DATABASE", "people");
        try (Peer peer = System.getenv("CORIUM_REMOTE") != null
                ? RemotePeer.builder(endpoint, database).build()
                : LocalPeer.builder(endpoint, database).connect().join()) {
            loadAndQuery(peer);
        }
    }

    private static void loadAndQuery(Peer peer) {
        peer.transact(new TxBuilder().entity(
                EntityMap.withId("ada")
                        .set("person/name", "Ada")
                        .set("person/age", 36))).join();

        var adults = Query.findCollection("?name")
                .inScalar("?minimum")
                .where("?entity", ":person/name", "?name")
                .where("?entity", ":person/age", "?age")
                .where(Query.gte("?age", "?minimum"));
        Db db = peer.sync().join();
        System.out.println(db.query(adults, 18).join().value());
        System.out.println(db.pull(
                new Pull().dbId().attr("person/name").attr("person/age"),
                new LookupRef("person/name", "Ada")).join());
    }
}
