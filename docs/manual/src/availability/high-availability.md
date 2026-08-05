# High availability

One transactor holds the write lease per database. A warm standby polls the
lease and takes over when it lapses.

This is an active and standby pair. It is not a consensus protocol. The root
store is the single arbiter.

## Starting a pair

Start both members identically, each with its own identity and endpoint:

```sh
corium transactor --data-dir /srv/corium --ha \
  --owner txor-a --advertise http://txor-a:4334 --listen 0.0.0.0:4334
corium transactor --data-dir /srv/corium --ha \
  --owner txor-b --advertise http://txor-b:4334 --listen 0.0.0.0:4334
```

Whichever starts first becomes active. The other stands by.

The standby rescans the catalog every lease-renewal interval, so a database
created on the active is picked up. It rejects client work with a `standby`
`FAILED_PRECONDITION` that names the current lease holder.

Give each member a stable `--owner`. A restarted member then re-acquires its
own unexpired lease at once.

## Storage requirements

Both members must see the same blob store, root store, and log.

| Store | Shared storage for a pair |
|---|---|
| `fs` | Needs a shared filesystem for `<data-dir>` on both members. |
| `postgres`, `s3` | Shared by construction. No shared filesystem is needed. |
| `mem`, `turso` | Not usable for a pair. |

> **CAUTION: Never run two members against diverged copies of a data
> directory.** The store is the source of truth.

## Peer failover

Peers list both endpoints and fail over automatically:

```sh
corium peer-server --db people \
  --transactor http://txor-a:4334,http://txor-b:4334
```

A library peer passes the same list through `ConnectConfig::with_failover`. A
peer with storage credentials can also rediscover the advertised endpoint of
the current holder from the database root.

## Guarantees

- **Takeover is ordinary crash recovery.** The standby acquires the lapsed
  lease, which atomically fences the deposed writer, replays the log tail, and
  serves.
- **No acknowledged transaction is ever lost or duplicated.** A post-append
  ownership check runs before every acknowledgement.
- **A deposed transactor can never publish.** Every root write is a
  compare-and-set on the record that holds the lease.
- **Peer subscriptions reconnect and backfill without gaps.**
- **A deposed member returns to standby on its own.** No operator action is
  needed after a garbage collection pause or a partition.

## Unavailability window

Writes are unavailable from the crash until takeover. The bound is the sum of
three terms.

1. One lease time-to-live, because the last renewal of the active must expire.
2. One standby poll interval, which is one third of the time-to-live.
3. Reconnect backoff on the peer.

With the defaults that is about 6.7 seconds plus backoff.

## Ambiguous transactions

A `transact` call that fails before it reaches the commit point is retried
transparently within `failover_timeout`. Standby rejection and connection
refusal are both in this class.

A call whose connection died mid-request is ambiguous. The transaction is
committed, or it is absent. The call surfaces an error, exactly like a
transactor crash between durability and reply.

> **On an ambiguous error, run `sync` and read before you resubmit.** A blind
> retry can write the data twice.

## Tuning

| Knob | Default | Effect |
|---|---|---|
| `--lease-ttl-ms` | 5000 | Failover detection bound. Renewals run at one third of it. |
| `--heartbeat-ms` | 10000 | Subscription heartbeats. A peer presumes the transactor dead after 3 missed intervals. |
| Peer `reconnect_min` / `reconnect_max` | 100 ms / 5 s | Reconnect backoff while endpoints rotate. |
| Peer `failover_timeout` | 30 s | How long a safe-to-retry transact failure rides out a takeover. |

A lower lease time-to-live gives faster takeover. It also costs more
root-store traffic, and it tolerates shorter garbage collection or input and
output pauses on the active.

Keep the heartbeat interval at or below the lease time-to-live. The heartbeat
is what detects a partition that TCP has not noticed.

Clock skew between members shifts detection latency only. It never affects
safety, because the root store is the single arbiter.

## Runbooks

The [runbooks chapter](../operations/runbooks.md) holds the procedures for
planned failover, a crashed active, split-brain suspicion, and both members
down.

## Fleet topology

> **Not implemented.** A fleet topology is specified. The design distributes
> databases across overlapping candidate sets and gives clients one
> load-balanced address, with the same lease and failover guarantees. It does
> not change the commands in this chapter. See
> [transactor-fleet.md](https://github.com/csm/corium/blob/main/docs/design/transactor-fleet.md).
