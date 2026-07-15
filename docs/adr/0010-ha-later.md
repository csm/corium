# ADR-0010: Single transactor now, lease-based HA designed in

**Status:** Accepted (2026-07-15)

## Context

Datomic achieves write availability with an active/standby transactor pair
coordinated by a storage-level lease — no consensus protocol. Building
failover immediately would delay the first end-to-end system; ignoring it
risks baking in assumptions (unfenced root writes, connection-bound peer
state) that make HA a rewrite.

## Decision

v1 milestones run a single transactor, but three HA-critical mechanisms are
in the v1 contracts: (1) the write lease record and its acquisition CAS in
the root store, (2) lease-version fencing validated atomically inside every
DbRoot CAS, (3) peer reconnect/resubscribe-from-basis with log backfill,
which doubles as ordinary reconnect handling. Failover itself — standby
process, takeover on lease expiry, peer lease-holder rediscovery — is
milestone M7.

## Consequences

- Crash recovery and HA takeover are the same code path (replay log tail),
  so M7 is incremental work plus simulation coverage, not a redesign.
- Single-transactor deployments carry a small constant cost (lease renewal,
  fencing checks) that also continuously exercises the HA-critical paths.
- Write availability until M7 is bounded by transactor restart time; the
  durable log guarantees no acked-write loss in the interim.
