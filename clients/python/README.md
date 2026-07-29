# Corium Python client

`corium` is the asynchronous Python API for both Corium deployment modes:

- `LocalPeer` runs a full peer in-process.
- `RemotePeer` connects to `corium peer-server`.

Both satisfy the same runtime-checkable `Peer` protocol and return immutable
`Db` values. The native extension connects `LocalPeer` directly to Corium's
peer library and `RemotePeer` to `corium peer-server`.

```python
from corium import LocalPeer

async with await LocalPeer.connect(
    "http://127.0.0.1:4334",
    database="people",
) as peer:
    db = await peer.db()
    rows = await db.query(raw_query_form)
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

Filesystem storage is present in the base `corium` artifact. Install at most
one of `corium-turso`, `corium-postgres`, or `corium-s3` alongside it to add
exactly one driver. Each artifact uses a distinct extension-module name and
depends on the common package, so it neither overwrites the base extension nor
pulls other drivers into a remote-only installation. The package selects the
installed artifact automatically and rejects ambiguous multi-artifact
installations. A wheel without the advertised backend rejects it with an
actionable `StorageError`, and
`available_storage_backends()` reports the current artifact. See
[`artifacts/`](artifacts/) for local builds. Publishing the platform wheel
matrix remains Phase 5 work.

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
