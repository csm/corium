import dev.corium.client.EntityMap;
import dev.corium.client.Query;
import dev.corium.client.RemotePeer;
import dev.corium.client.TxBuilder;

public final class People {
    public static void main(String[] args) {
        String endpoint = args.length > 0 ? args[0] : "http://127.0.0.1:4336";
        try (RemotePeer peer = RemotePeer.builder(endpoint, "people").build()) {
            var report = peer.transact(new TxBuilder().entity(
                    EntityMap.withId("ada")
                            .set("person/name", "Ada")
                            .set("person/age", 36))).join();
            var adults = Query.findCollection("?name")
                    .inScalar("?minimum")
                    .where("?entity", ":person/name", "?name")
                    .where("?entity", ":person/age", "?age")
                    .where(Query.gte("?age", "?minimum"));
            System.out.println(report.dbAfter().query(adults, 18).join().value());
        }
    }
}
