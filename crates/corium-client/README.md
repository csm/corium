# corium-client

A fluent, async, Datomic-style client for corium. One API surface, two
backends: query the peer library directly from storage, or reach a peer server
over gRPC.

## What it does

`corium-client` is the ergonomic front door to the peer. Datalog queries,
pull specifications, and transactions are built as typesafe, immutable,
builder-patterned values that lower to the boundary EDN the engine parses — a
malformed query is a type error, not a runtime parse failure.

- **[`LocalPeer`]** wraps the [`corium-peer`](../corium-peer/README.md)
  `Connection`, so queries execute in-process against immutable database
  values read straight from storage — no round trip to the transactor.
- **[`RemotePeer`]** speaks the peer-server gRPC protocol, presenting the
  identical surface to processes that reach a hosted peer over the network.

Both implement the `Peer` trait and hand back `Db` values sharing `query`,
`pull`, `datoms`, `stats`, and the time-view methods `as_of` / `since` /
`history`.

## Example

```rust
use corium_client::{LocalPeer, Peer, Args};
use corium_client::query::{Query, data, var, attr, gte};
use corium_client::pull::Pull;
use corium_client::tx::{TxBuilder, EntityMap, tempid, lookup};
use corium_peer::ConnectConfig;

# async fn demo() -> Result<(), corium_client::ClientError> {
let peer = LocalPeer::connect(ConnectConfig::new("http://127.0.0.1:4334", "people")).await?;

// Transact, with the typed builder.
peer.transact(
    TxBuilder::new()
        .entity(EntityMap::with_id(tempid("alice"))
            .set("person/name", "Alice")
            .set("person/age", 39_i64))
        .build(),
).await?;

let db = peer.db().await?;

// [:find [?name ...] :in $ ?min
//  :where [?e :person/name ?name] [?e :person/age ?age] [(>= ?age ?min)]]
let adults = Query::find_coll(var("name"))
    .in_scalar(var("min"))
    .where_(data(var("e"), attr("person/name"), var("name")))
    .and(data(var("e"), attr("person/age"), var("age")))
    .and(gte(var("age"), var("min")));
let names: Vec<String> = db.query(&adults, Args::new().scalar(21_i64)).await?.values_as()?;

// Pull an entity by lookup ref.
let alice = db.pull(
    &Pull::new().attr("person/name").attr("person/age"),
    lookup("person/name", "Alice"),
).await?;
# Ok(())
# }
```

The remote client is a drop-in: swap `LocalPeer::connect` for
`RemotePeer::connect(endpoint, "people", token, tls)` and the same
query/pull/transact code runs against a peer server.

## Modules

| Module | What it provides |
|---|---|
| `query` | `Query`, `Var`, `Term`, `Clause`, find/aggregate/pull elements, `data`/`pred`/`not`/`or`/`rule` combinators |
| `pull` | `Pull` and `Attr` specs: attributes, `*`, `:db/id`, reverse refs, nesting, recursion, `:as`/`:limit`/`:default` |
| `tx` | `TxBuilder`, `EntityMap`, `tempid`/`lookup`/`eid` — the Datomic-dialect transaction forms |
| `result` | `QueryResult`, `Row`, and `ResultShape` with typed cell access |
| `value` | `IntoEdn` / `FromEdn` for boundary conversion and typed extraction |

## Dependencies

- Engine: `corium-core`, `corium-db`, `corium-query`.
- Peer/network: `corium-peer`, `corium-protocol`, `tonic`, `tokio`,
  `async-trait`.

See [`docs/architecture.md`](../../docs/architecture.md) and the peer library
for the topology this sits on top of.
