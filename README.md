# corium

A database system in the style of Datomic — immutable, time-aware, and
fact-oriented, with Datalog queries and peer-local query execution — written
in Rust, paired with [Clojurust](https://github.com/csm/clojurust) for
EDN/Clojure data handling and database function execution.

All roadmap milestones (M0–M7, through active/standby high availability)
are implemented. Start with the
[getting-started guide](docs/getting-started.md), use the
[operations guide](docs/operations.md) for deployment and recovery, and see
[PLAN.md](PLAN.md) for current status. Design documents, the roadmap, and
architecture decision records live in [docs/](docs/).
