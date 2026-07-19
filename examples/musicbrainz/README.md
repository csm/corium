# MusicBrainz example

A corium port of the [Datomic MusicBrainz
example](https://github.com/Datomic/mbrainz-sample): a schema for artists,
labels, releases, media, and tracks, a loader that installs the schema and
streams data in, and a Clojurust REPL with the `corium.api` client bindings
preloaded for running queries.

Everything here works over any of corium's storage backends — in-memory,
filesystem, or Turso — selected when you start the transactor.

## Quick start

The fastest path is the all-in-one demo, which starts a transactor, loads the
bundled sample data, and drops you into the REPL:

```sh
examples/musicbrainz/scripts/demo.sh mem      # or: fs, turso
```

Then, at the `mbrainz=>` prompt:

```clojure
(d/q '[:find (count ?a) . :where [?a :artist/name]] db)
;; => 3

(d/q '[:find ?name ?year
       :where [?a :artist/type :artist.type/group]
              [?a :artist/name ?name]
              [?a :artist/startYear ?year]] db)
;; => [["The Beatles" 1960] ["Radiohead" 1985]]

(d/q '[:find ?name ?year
       :where [?a :artist/name ?name]
              [(starts-with? ?name "Bob")]
              [?a :artist/startYear ?year]] db)
;; => [["Bob Dylan" 1941]]
```

Query predicates such as `starts-with?` are Corium Datalog built-ins. Use
their unqualified names rather than Clojure namespace-qualified names such as
`clojure.string/starts-with?`.

Type `:help` in the REPL for more example queries, `:quit` to exit.

## Doing it by hand

Run the three pieces in separate terminals so you can keep the transactor up.

**1. Start a transactor** over the store of your choice:

```sh
examples/musicbrainz/scripts/transactor.sh mem      # ephemeral, single process
examples/musicbrainz/scripts/transactor.sh fs       # persists under ./corium-mbrainz-data
examples/musicbrainz/scripts/transactor.sh turso    # persists in a Turso database
```

or drive the CLI directly:

```sh
cargo run -p corium-cli -- transactor --store fs --data-dir ./corium-mbrainz-data
cargo run -p corium-cli --features turso -- \
  transactor --store turso --data-dir ./corium-mbrainz-data
```

**2. Load the schema and data** (the loader creates the `mbrainz` database):

```sh
examples/musicbrainz/scripts/load.sh
# equivalently:
cargo run -p corium-mbrainz --bin mbrainz-load -- \
  --schema examples/musicbrainz/schema.edn \
  --data   examples/musicbrainz/data/sample.edn
```

**3. Query**, either from the Clojurust REPL or the built-in console:

```sh
examples/musicbrainz/scripts/repl.sh       # cljrs REPL, corium.api preloaded
examples/musicbrainz/scripts/console.sh    # EDN-Datalog console
```

## Storage backends

`--store` selects where the transactor keeps blobs and root pointers:

| Store   | Blobs + roots            | Log        | Notes |
|---------|--------------------------|------------|-------|
| `mem`   | in-memory                | in-memory  | Fully ephemeral; one process. Great for demos/tests. |
| `fs`    | `{data-dir}/store`       | `{data-dir}/logs` | The default; survives restarts. |
| `turso` | Turso (embeddable SQLite) | `{data-dir}/logs` | Durable index storage in Turso; the log stays local. Needs `--features turso`. |

Because the transaction log is appended synchronously by the commit pipeline,
it stays on the local filesystem for `fs` and `turso`; `mem` keeps it in a
process-shared in-memory log. See
[`docs/design/log-and-transactor.md`](../../docs/design/log-and-transactor.md).

## The two binaries

- **`mbrainz-load`** — a pure-Rust loader. It talks the ordinary peer API, so
  it is oblivious to the transactor's store. It creates the database from
  `schema.edn`, then streams the dataset: each top-level EDN **map** is
  batched (`--batch`, default 1000) into a transaction, and each top-level
  **vector** is applied verbatim as one atomic transaction. Forms stream one
  at a time, so multi-gigabyte files load without being read into memory.

- **`mbrainz-repl`** — a Clojurust REPL. It builds the full `corium.api`
  client environment (aliased `d`), connects, and binds `conn` and `db`. Each
  line you enter is read and evaluated; the value of the last form is printed.
  Refresh the database value after a load with `(def db (d/sync conn))`.

## Bringing your own (larger) dataset

The bundled `data/sample.edn` is deliberately tiny. To load the full Datomic
MusicBrainz dataset, produce an EDN file in the same shape — a sequence of
transactions (top-level vectors) and/or entity maps — and point the loader at
it:

```sh
cargo run -p corium-mbrainz --bin mbrainz-load -- \
  --schema examples/musicbrainz/schema.edn --data /path/to/mbrainz.edn --batch 2000
```

### Modeling notes (differences from Datomic)

Corium's v1 schema scope ([ADR-0009](../../docs/adr/0009-schema-scope.md))
omits a couple of things the Datomic example leans on, so this port adapts:

- **Enumerated values** (artist type, gender, release status, medium format,
  country, language) are `:db.type/keyword` values like
  `:artist.type/person`, not refs to `:db/ident` enum entities — corium does
  not install idents at transaction time.
- **Links use lookup refs** on the unique `:*/gid` attributes rather than
  value-position tempids (which the transaction layer does not resolve), so a
  referenced entity must be committed by an earlier transaction than the one
  that points at it. The sample data is ordered accordingly:
  artists + labels → tracks → media → releases. To make everything linkable,
  media and tracks carry a `:*/gid` here even though Datomic gives them none.
- **UUID literals** are 32 hex digits with no dashes: `#uuid "0000…0001"`.

## Files

```
examples/musicbrainz/
├── schema.edn          MusicBrainz schema (corium-adapted)
├── data/sample.edn     tiny bundled dataset (3 artists, 3 releases)
├── src/
│   ├── lib.rs          streaming EDN form reader + endpoint helpers
│   └── bin/
│       ├── load.rs     mbrainz-load
│       └── repl.rs     mbrainz-repl
└── scripts/            transactor.sh, load.sh, repl.sh, console.sh, demo.sh
```
