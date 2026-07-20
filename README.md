# corium

A database system in the style of Datomic — immutable, time-aware, and
fact-oriented, with Datalog queries and peer-local query execution — written
in Rust, paired with [Clojurust](https://github.com/csm/clojurust) for
EDN/Clojure data handling and database function execution.

The peer also exposes read-only SQL through the `corium-sql` Rust crate and the
`corium sql` interactive shell; see the [SQL interface](docs/sql.md).
`corium tui` opens a full-screen terminal dashboard — query workbench, live
store metrics, transaction feed, and schema browser; see the
[operations guide](docs/operations.md#terminal-dashboard-tui).

All roadmap milestones (M0–M7, through active/standby high availability)
are implemented. Start with the
[getting-started guide](docs/getting-started.md), work through the
[MusicBrainz example](examples/musicbrainz/README.md) for an end-to-end tour
(schema, data loader, and a Clojurust query REPL over in-memory, filesystem,
or Turso storage), use the [operations guide](docs/operations.md) for
PostgreSQL-backed deployment and recovery, and see [PLAN.md](PLAN.md) for
current status.
Design documents, the roadmap, and architecture decision records live in
[docs/](docs/).
