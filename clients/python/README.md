# Corium Python client

`corium` is the asynchronous Python API for both Corium deployment modes:

- `LocalPeer` runs a full peer in-process.
- `RemotePeer` connects to `corium peer-server`.

Both satisfy the same runtime-checkable `Peer` protocol and return immutable
`Db` values. The public Python layer is present in this first implementation
slice; the native extension that connects these types to `corium-ffi` follows
in the next delivery phase.

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
Wall-clock database views accept timezone-aware `datetime` values only.
