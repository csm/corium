# Corium Java client

`corium-client` is the asynchronous Java 11+ client for a Corium peer server.
It follows the same immutable database-value model as the Python client and
uses `CompletableFuture` for network operations.

```java
import dev.corium.client.EntityMap;
import dev.corium.client.Query;
import dev.corium.client.RemotePeer;
import dev.corium.client.TxBuilder;

try (RemotePeer peer = RemotePeer.builder(
        "http://127.0.0.1:4336", "people").build()) {
    var tx = new TxBuilder().entity(
            EntityMap.withId("ada").set("person/name", "Ada"));
    var report = peer.transact(tx).join();

    var names = report.dbAfter().query(
            Query.findCollection("?name")
                    .where("?entity", ":person/name", "?name"))
            .join();
    System.out.println(names.value());
}
```

The initial Java client is remote-only. Unlike Python's `LocalPeer`, it does
not load the Rust peer library through a native binding.

The client supports these features:

- Current, as-of, since, history, and wall-clock database views
- Queries and Pull
- Transactions
- Datom scans and database statistics
- TLS and bearer authentication
- Custom certificate authorities

Java values map to the Corium boundary as follows:

| Java value | Corium value |
|---|---|
| `null`, `Boolean`, integral `Number`, floating `Number`, `String` | EDN scalar |
| `Keyword`, `Symbol`, `EdnList`, `Tagged`, `EntityId` | dedicated boundary type |
| `List`, `Map`, `Set` | vector, map, set |
| `Instant`, `UUID`, `byte[]` | `#inst`, `#uuid`, `#bytes` |
| any `EdnForm` | value from `toEdn()` |

Bearer tokens require `https://` unless local development explicitly calls
`allowInsecureToken(true)`. An HTTPS endpoint uses platform trust roots by
default. Use `tlsCa(pemBytes)` and `tlsDomain("corium.internal")` for private
PKI deployments.

From the `clients/java` directory, run the build and tests:

```sh
mvn verify
```

The build generates Java and gRPC sources from the canonical protocol schema.
The schema is at `crates/corium-protocol/proto/corium.proto`.

## Publishing

The release workflow publishes `dev.corium:corium-client` to Maven Central.
The `release` Maven profile creates source and Javadoc JARs. It signs each
artifact and sends the deployment to the Central Publisher Portal. The workflow
waits until the deployment is public.

Add these secrets to the `maven-central` GitHub environment:

- `MAVEN_CENTRAL_USERNAME`: username from a Central Portal user token
- `MAVEN_CENTRAL_TOKEN`: password from the same user token
- `MAVEN_GPG_PRIVATE_KEY`: ASCII-armored private signing key
- `MAVEN_GPG_PASSPHRASE`: passphrase for the signing key

Publish the public signing key to a public key server. Make sure that the
Central Portal account can publish the verified `dev.corium` namespace.
