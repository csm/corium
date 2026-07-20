---
name: verify
description: Build, launch, and drive corium's CLI surfaces (transactor, console, sql, tui) against a live local transactor to verify changes end-to-end.
---

# Verifying corium changes end-to-end

## Build

```sh
cargo build -p corium-cli            # target/debug/corium
cargo build -p corium-mbrainz --bins # target/debug/mbrainz-load (generic data loader)
```

## Stand up a live database

```sh
W=$(mktemp -d)
target/debug/corium transactor --data-dir $W/data --listen 127.0.0.1:14334 \
  --index-interval-ms 1000 > $W/transactor.log 2>&1 &
target/debug/corium db create people --schema schema.edn --transactor http://127.0.0.1:14334
```

A minimal schema (`:person/name` string unique-identity indexed, `:person/age`
long) is enough for most flows; see docs/getting-started.md.

## Transact data

There is no `corium transact`; use the musicbrainz loader — it is generic
despite the name (any schema + a file of EDN entity maps):

```sh
target/debug/mbrainz-load --transactor http://127.0.0.1:14334 --db people \
  --schema schema.edn --data people.edn --batch 25 --skip-create
```

Entity maps **must** carry a `:db/id "tempid"`. Loop small loads with fresh
entity names to simulate continuous transaction traffic.

## Drive interactive surfaces (console, sql, tui)

Console and sql accept piped stdin (`echo ':stats\n:quit' | corium console …`).
The TUI needs a real terminal — use an isolated tmux server:

```sh
tmux -L corium new-session -d -x 170 -y 45 \
  'target/debug/corium tui people --transactor http://127.0.0.1:14334 --refresh-ms 500'
tmux -L corium send-keys '[:find ?e :where [?e :person/name]]' Enter
tmux -L corium capture-pane -p       # evidence
tmux -L corium kill-server           # cleanup
```

TUI keys: `Tab` cycles panels; `1`–`4` jump (outside the query editor);
`q`/`Ctrl-C`/`:quit` exit; `/` filters the schema panel.

## Gotchas

- Schema install via `db create` is not a logged transaction: a fresh
  database legitimately reports `basis-t 0` and `tx-count 0`.
- CI runs `cargo clippy --workspace --all-targets -- -D warnings` with
  pedantic lints and `cargo fmt --all -- --check`; run both before pushing.
- The workspace is large; first builds take minutes. `corium` and the
  loaders share the artifact dir, so parallel cargo invocations serialize
  on the file lock.
