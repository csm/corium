# Corium Python client

`corium` is the asynchronous Python API for both Corium deployment modes:

- `LocalPeer` runs a full peer in-process.
- `RemotePeer` connects to `corium peer-server`.

Both satisfy the same runtime-checkable `Peer` protocol and return immutable
`Db` values. The native extension currently connects `RemotePeer` to
`corium peer-server`; the in-process `LocalPeer` adapter follows in Phase 3.

```python
from corium import RemotePeer

async with await RemotePeer.connect(
    "http://127.0.0.1:4336",
    database="people",
) as peer:
    db = await peer.db()
    rows = await db.query(raw_query_form)
```

Explicit `close()` (or `async with`) is required for deterministic shutdown.
Wall-clock database views accept timezone-aware `datetime` values only.
`https://` endpoints automatically enable platform TLS roots, and bearer
tokens are rejected for plaintext `http://` endpoints by default. Local
development may opt in explicitly with `allow_insecure_token=True`. Every
endpoint must include an `http://` or `https://` scheme. Datom scans use
`limit=None` for an explicitly unbounded scan.
