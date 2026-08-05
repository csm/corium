# Corium Java client

`corium-client` is the asynchronous Java 11+ client for Corium. It follows the
same immutable database-value model as the Python client and uses
`CompletableFuture` for every operation that reaches a peer.

```java
import dev.corium.client.EntityMap;
import dev.corium.client.Peer;
import dev.corium.client.Query;
import dev.corium.client.RemotePeer;
import dev.corium.client.TxBuilder;

try (Peer peer = RemotePeer.builder(
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

## Local and remote peers

`Peer` is the common interface. `RemotePeer` is a lightweight client that talks
to a `corium peer-server` over gRPC. `LocalPeer` runs a full peer inside the
JVM through the native Corium engine, so it indexes and queries in this process
and talks to a transactor directly:

```java
try (Peer peer = LocalPeer.builder("https://transactor:4335", "people")
        .directStorage(DirectStorage.discover())
        .connect()
        .join()) {
    var db = peer.db().join();
    System.out.println(db.stats().join().basisT());
}
```

Both peers produce the same `Db` values, and every `Db` view — current, as-of,
since, history, and the wall-clock variants — works identically. Only a remote
peer can join across databases in one query; an in-process peer rejects the
multi-database `query` overload.

`DirectStorage.discover()` asks the transactor for its storage connection and
reads published segments straight from it. Pass a `SegmentCache` to bound how
much a peer keeps locally:

```java
DirectStorage.discover(new SegmentCache(Path.of("/var/cache/corium"), 8L << 30));
```

## Artifacts

| Artifact | Contents |
|---|---|
| `dev.corium:corium-client` | The Java API. Enough on its own for `RemotePeer`. |
| `dev.corium:corium-client:<platform>` | The native engine, required by `LocalPeer`. |
| `dev.corium:corium-turso:<platform>` | Turso direct-storage plugin |
| `dev.corium:corium-postgres:<platform>` | PostgreSQL direct-storage plugin |
| `dev.corium:corium-s3:<platform>` | S3 direct-storage plugin |

A remote-only dependency stays pure Java. Add the platform classifier for your
deployment to get an in-process peer:

```xml
<dependency>
  <groupId>dev.corium</groupId>
  <artifactId>corium-client</artifactId>
  <version>0.1.0</version>
</dependency>
<dependency>
  <groupId>dev.corium</groupId>
  <artifactId>corium-client</artifactId>
  <version>0.1.0</version>
  <classifier>linux-x86_64</classifier>
</dependency>
```

The classifiers are `linux-x86_64`, `linux-aarch64`, `macos-x86_64`,
`macos-aarch64`, and `windows-x86_64`. Direct storage additionally needs the
plugin artifact for the backend the transactor advertises; filesystem storage
is built into the engine. `LocalPeer.availableStorageBackends()` reports what
this process can open, and a connection that would need a missing plugin fails
by name before it opens anything. See
[`artifacts/README.md`](artifacts/README.md).

Native libraries are extracted from those jars to a cache directory the first
time a peer loads them. Set `corium.native.directory` (or
`CORIUM_NATIVE_DIRECTORY`) to load a locally built engine and plugins from a
Cargo output directory instead, and `corium.native.cache` to choose where
extracted libraries live.

## Features

- Current, as-of, since, history, and wall-clock database views
- Queries and Pull
- Transactions
- Datom scans and database statistics
- TLS and bearer authentication
- Custom certificate authorities
- In-process peers with optional direct storage

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

Failures raise `CoriumException`, whose `kind()` is the same category the
native engine and the peer server report — `CONNECTION`, `QUERY`,
`TRANSACTION`, `STORAGE`, `CLOSED`, and so on — so error handling does not
depend on which peer produced it.

## Building

From the `clients/java` directory, run the build and tests:

```sh
mvn verify
```

The build generates Java and gRPC sources from the canonical protocol schema at
`crates/corium-protocol/proto/corium.proto`.

To exercise `LocalPeer` from a checkout, build the engine and, if you need
direct storage, the plugin libraries:

```sh
cargo build -p corium-jni -p corium-store-turso
```

Tests then find them through `target/debug`. Package the per-platform jars
with:

```sh
cargo build --release -p corium-jni -p corium-store-turso \
  -p corium-store-postgres -p corium-store-s3
python3 clients/java/scripts/build_native_jars.py \
  --target-directory target/release \
  --classifier linux-x86_64 \
  --output dist
```

## Publishing

The release workflow publishes `dev.corium:corium-client` and the three storage
plugin artifacts to Maven Central. The `release` Maven profile creates source
and Javadoc JARs, signs each artifact, and sends the deployment to the Central
Publisher Portal. The `native` profile attaches the per-platform jars collected
in `dist`. The workflow waits until the deployment is public.

Add these secrets to the `maven-central` GitHub environment:

- `MAVEN_CENTRAL_USERNAME`: username from a Central Portal user token
- `MAVEN_CENTRAL_TOKEN`: password from the same user token
- `MAVEN_GPG_PRIVATE_KEY`: ASCII-armored private signing key
- `MAVEN_GPG_PASSPHRASE`: passphrase for the signing key

Publish the public signing key to a public key server. Make sure that the
Central Portal account can publish the verified `dev.corium` namespace.
