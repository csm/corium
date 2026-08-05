# Query console

`corium console` opens an interactive Datalog console. The console is a peer.
Every query runs locally in the console process.

```sh
corium console people --transactor http://127.0.0.1:4334
```

## Input

The console accepts three kinds of input.

**EDN Datalog queries.** Enter the query on one line:

```clojure
[:find ?name ?age :where [?e :person/name ?name] [?e :person/age ?age]]
```

**Pull forms.**

```clojure
(pull [:person/name :person/age] 1000)
```

**Console commands.** Every command starts with a colon.

> **Partly implemented.** A console query takes database inputs only. A query
> with `:in` parameters other than `$` is rejected. Use a client library for a
> parameterized query.

The console is read-only. There is no transact command. See
[getting started](../getting-started.md) for the write paths.

## Commands

| Command | Effect |
|---|---|
| `:basis` | Print the basis and the active view. |
| `:as-of <t>` | Fix the view at transaction `<t>`. |
| `:as-of <timestamp>` | Fix the view at a UTC timestamp. |
| `:since <t>` | Show only facts added after `<t>`. |
| `:since <timestamp>` | The same, named by timestamp. |
| `:history on` | Show every assertion and retraction. |
| `:history off` | Return to the current view. |
| `:current` | Return to the current view. |
| `:schema` | Print every attribute. |
| `:schema <attr>` | Print one attribute, for example `:schema person/name`. |
| `:stats` | Print the basis and the datom, entity, and attribute counts. |
| `:timing on` | Report time and datoms scanned after each query. |
| `:timing off` | Stop reporting them. |
| `:watch` | Tail live transaction reports until `Ctrl-C`. |
| `:help` | Print the command list. |
| `:quit`, `:exit` | Leave the console. |

## Timestamps

`:as-of` and `:since` accept a transaction number, or a UTC timestamp.

```text
:as-of 10
:as-of 2026-07-25T09:30:00Z
:since 2026-07-25 09:30:00
```

A timestamp is `YYYY-MM-DD`, optionally with `HH:MM`, `HH:MM:SS`, or
`HH:MM:SS.mmm`. A timestamp selects the last transaction committed at or
before it. Resolution reads the `:db/txInstant` datom that every commit
asserts.

The SQL shell accepts the same two forms with `\as-of` and `\since`.

## Cost of a time view

> **Partly implemented.** A distinct time view costs a fold of the whole
> history on first read, not a fold of the view. A `:history on` console
> session on a large database is slow and uses memory in proportion to total
> history. See [time and database values](../theory/time.md).

## Bootstrap

By default the console replays the log from basis 0. On a large database that
is slow.

Add `--peer-bootstrap` when the console host can reach the storage backend:

```sh
corium console people --peer-bootstrap
```

The console then reads the published snapshot and subscribes from the index
basis. The transactor supplies the storage connection details, using the
read-only credential that you configured. See
[storage backends](../running/storage.md).

An encrypted database also needs `--storage-key`.

## Watching transactions

`:watch` tails the transaction report stream. Each report prints `t`, the
commit instant, and the datom count. Press `Ctrl-C` to stop the tail and
return to the prompt.

The `Transactions` panel of [`corium tui`](tui.md) shows the same stream with
a datom detail pane.
