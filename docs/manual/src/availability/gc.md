# Garbage collection

Old index roots keep old segments alive until no reader needs them. Garbage
collection deletes the segments that no live root reaches.

Collection is epoch-based, and it is never urgent.

## The retention rule

A sweep deletes an unreachable blob only when the blob is older than the
retention window. The window must cover any reader that can still hold a stale
root.

The default window is 72 hours.

## Scheduled collection

The transactor runs collection on a schedule.

| Flag | Default | Effect |
|---|---|---|
| `--gc-interval <duration>` | `1h` | Interval between sweeps. `off` disables the duty. |
| `--gc-window <duration>` | `72h` | Retention window. |

Collection is serialized with index publication. The two never run at the same
time.

## Manual collection

Online collection asks a running transactor to sweep:

```sh
corium gc --transactor http://127.0.0.1:4334 --window 72h
```

Offline collection reads a data directory directly. The transactor must be
stopped:

```sh
corium gc --data-dir /srv/corium --window 72h
```

Both use the same retention rule. `--data-dir` and `--transactor` conflict.

An encrypted database needs the key for offline collection:

```sh
corium gc --data-dir /srv/corium --window 72h --storage-key file:/etc/corium/storage.key
```

Offline collection refuses to run on an encrypted database without the key.
Without the key, a sweep deletes the index chunks that it cannot follow.

## Zero window

> **CAUTION: Use `--window 0` only when no stale root and no in-flight reader
> can exist.** A zero window deletes a blob as soon as it is unreachable. A
> reader that still holds an older root then fails.

A safe use is a stopped system with no peers running.

## Why collection is safe

Deletion is the only mutation, and it touches only unreachable data. A bug in
the mark phase can therefore strand garbage. A generous window makes data loss
a non-risk.

Deleting a database deletes its root, and the sweep reclaims the blobs
afterward.

## Monitoring

Three counters report collection.

| Source | Fields |
|---|---|
| `corium db stats <db>` | `:gc-runs`, `:gc-swept-blobs` |
| Metrics endpoint | `corium_transactor_gc_runs_total`, `corium_transactor_gc_swept_blobs_total`, `corium_transactor_gc_retained_blobs_total` |
| `corium tui` | The `Metrics` panel |

A retained count that grows steadily means that the window is longer than it
needs to be, or that a root is pinning old segments.

## Tuning

| Situation | Setting |
|---|---|
| Peers hold long-lived database values | Raise `--gc-window` above the longest reader lifetime. |
| Storage cost matters more than reader tolerance | Lower `--gc-window`, and watch for reader errors. |
| Bulk load in progress | Set `--gc-interval off`, and run one manual sweep afterward. |
| A pause on the active transactor causes failover | Raise `--lease-ttl-ms`, or lengthen `--gc-interval`. |

Collection competes with index publication for processor time and storage
bandwidth. On a busy system, run it less often rather than with a shorter
window.
