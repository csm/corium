# corium-sim

Deterministic simulation harness for whole-system fault-injection tests.
Not published (`publish = false`).

## What it does

Provides the abstract seams a deterministic simulation drives instead of the
real world:

- **`Clock`** — a logical millisecond clock the simulation advances by hand.
- **`Storage`** — an in-memory byte store standing in for the storage service.

These let tests run the transactor/peer protocol against controlled time and
storage, injecting faults (crashes, takeovers, delays) at precise points and
replaying them deterministically.

## Dependencies

- `corium-core` only.

The engine crates are written to be async- and I/O-free precisely so they can
be exercised under this harness; the higher-level acceptance batteries (e.g.
the M7 HA takeover simulation) build on these seams.

## Architecture

Determinism is the whole point: with a logical clock and an in-memory store,
every scheduling and I/O decision is reproducible, so a failing interleaving can
be re-run bit-for-bit. Because `corium-core`, `corium-index`, `corium-store`,
`corium-log`, `corium-tx`, `corium-query`, and `corium-db` carry no tokio or
network dependency, the commit/publish/renew protocol can be simulated end to
end without real sockets or wall-clock time. See
[`docs/design/testing-strategy.md`](../../docs/design/testing-strategy.md).
