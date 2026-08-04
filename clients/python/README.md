# Corium Python client

`corium` is the asynchronous Python API for both Corium deployment modes:

- `LocalPeer` runs a full peer in-process.
- `RemotePeer` connects to `corium peer-server`.

Both satisfy the same runtime-checkable `Peer` protocol and return immutable
`Db` values. The native extension connects `LocalPeer` directly to Corium's
peer library and `RemotePeer` to `corium peer-server`.

```python
from corium import LocalPeer, Query

async with await LocalPeer.connect(
    "http://127.0.0.1:4334",
    database="people",
) as peer:
    db = await peer.db()
    rows = await db.query(Query.find("?name").where("?entity", ":person/name", "?name"))
```

Explicit `close()` (or `async with`) is required for deterministic shutdown.
Wall-clock database views accept timezone-aware, millisecond-precision
`datetime` values only. Custom `Tagged` values may use any tag except `bytes`,
`eid`, `inst`, and `uuid`, which are reserved for dedicated boundary types.
`https://` endpoints automatically enable platform TLS roots, and bearer
tokens are rejected for plaintext `http://` endpoints by default. Local
development may opt in explicitly with `allow_insecure_token=True`. Every
endpoint must include an `http://` or `https://` scheme. Datom scans use
`limit=None` for an explicitly unbounded scan.

## Query, Pull, and transaction builders

All builders are immutable: every fluent method returns a new value, and
`Db.query`, `Db.pull`, and `Peer.transact` accept either builders or the
existing raw data-form escape hatches.

```python
from corium import (
    EntityMap,
    Pull,
    Query,
    TxBuilder,
    data,
    gte,
    lookup,
    tempid,
)

tx = (
    TxBuilder()
    .entity(
        EntityMap.with_id(tempid("ada")).set("person/name", "Ada").set("person/age", 36)
    )
    .build()
)
report = await peer.transact(tx)

adults = (
    Query.find_collection("?name")
    .in_scalar("?minimum")
    .where(data("?entity", ":person/name", "?name"))
    .where("?entity", ":person/age", "?age")
    .where(gte("?age", "?minimum"))
)
names = await report.db_after.query(adults, 18)

person = await report.db_after.pull(
    Pull().db_id().attr("person/name").attr("person/age"),
    lookup("person/name", "Ada"),
)
```

Query builders cover relation, collection, tuple, and scalar results; scalar,
tuple, collection, relation, database, and rule inputs; data patterns;
predicates and functions; `not`, `not-join`, `or`, and `or-join`; rules; Pull
find expressions; and aggregates. Strings beginning with `?` are variables,
strings beginning with `:` are keywords, and `_` is the blank term. Use
`lit(...)` when one of those spellings must remain a string literal.

Pull builders cover wildcard and entity-id selections, reverse references,
nested patterns, bounded and unbounded recursion, aliases, defaults, and
limits. Transaction builders cover entity maps, temporary IDs, lookup
references, explicit `EntityId` values, add/retract/CAS/retract-entity
operations, and arbitrary raw forms.

See [`examples/people.py`](examples/people.py) for a complete topology-neutral
example. The package includes `py.typed`, so these APIs are visible to static
type checkers without a separate stub distribution.

Private PKI deployments can add a PEM certificate authority and override the
certificate DNS name without replacing the platform trust store:

```python
from pathlib import Path

peer = await RemotePeer.connect(
    "https://127.0.0.1:4336",
    database="people",
    token="secret",
    tls_ca=Path("ca.pem").read_bytes(),
    tls_domain="corium.internal",
)
```

## Direct storage

`LocalPeer` normally reconstructs its in-process database from the transactor's
gapless transaction stream. For faster cold starts, `DirectStorage` asks the
transactor for a separately usable, read-only storage connection and loads the
latest published snapshot before subscribing to the remaining transaction tail:

```python
from corium import DirectStorage, LocalPeer, SegmentCache

peer = await LocalPeer.connect(
    "https://transactor.example.com",
    database="people",
    token=token,
    storage=DirectStorage(
        cache=SegmentCache(
            "/var/cache/corium/people",
            capacity_bytes=256 * 1024**3,
        )
    ),
)
```

Filesystem storage is present in the `corium` package. Install the package for
the storage backend that the transactor uses:

```shell
python -m pip install corium corium-turso
# Or install corium-postgres or corium-s3.
```

Install all packages before the Python process starts. Corium loads each
installed plugin through the `corium.store_plugins` entry-point group.

Make sure that the required backend is available:

```python
from corium import available_storage_backends

assert "turso" in available_storage_backends()
```

Then pass `DirectStorage` to `LocalPeer.connect`, as shown in the first example.
The transactor supplies the separate read-only configuration for the backend.
You do not import the plugin package or give Corium a library path.

`available_storage_backends()` reports all loaded backends. A missing backend
causes a `StorageError` when `LocalPeer` connects.

WARNING: Install plugins only from trusted publishers. A plugin runs native
code in the Python process and can receive storage credentials.

Filesystem and Turso advertise local paths, so their direct-storage peers must
run on a host that can reach the same path as the transactor. Corium rejects a
missing store with `StorageError`; it does not create an empty store or silently
fall back to replaying the full transaction stream.

PostgreSQL and S3 discovery never reuses the transactor's write credentials.
The transactor must advertise its separately configured read-only PostgreSQL
URL or S3 credentials. Temporary S3 credentials are refreshed through
`GetStorageInfo`, and a refresh is rejected if the bucket, prefix, region, or
endpoint changes.

The native integration suite uses the same workload for local and remote peers:
query result conversion, Pull, datom scans, immutable views, stable caller-error
mapping, transactions, and deterministic close. Set
`CORIUM_TEST_LOCAL_ENDPOINT` and/or `CORIUM_TEST_REMOTE_ENDPOINT` to run it
against live services. The dedicated private-CA and bearer-auth test uses
`CORIUM_TEST_REMOTE_TLS_ENDPOINT`, `CORIUM_TEST_REMOTE_TLS_TOKEN`,
`CORIUM_TEST_REMOTE_TLS_CA`, and optionally
`CORIUM_TEST_REMOTE_TLS_DOMAIN`/`CORIUM_TEST_REMOTE_BAD_TOKEN`.
