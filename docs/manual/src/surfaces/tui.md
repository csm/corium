# Terminal dashboard

`corium tui` opens a full-screen dashboard over one database.

```sh
corium tui people --transactor http://127.0.0.1:4334
```

| Flag | Default | Effect |
|---|---|---|
| `--refresh-ms <n>` | 2000 | Metrics sample interval. The minimum is 250. |

The dashboard also accepts every [connection flag](../running/catalog.md).

The process owns the terminal, so it writes no tracing output.

## Navigation

Press `Tab` to cycle the panels. Press `1` to `4` to jump to one panel, from
outside the query editor.

Quit with `Ctrl-C` anywhere, with `q` outside the query editor, or with
`:quit`.

## Query panel

An editor for EDN Datalog queries, `(pull …)` forms, and every
[console command](console.md).

`Enter` runs the form when its brackets balance. Otherwise `Enter` inserts a
newline. `Alt-Enter` always inserts a newline.

A relation result renders as a scrollable table with `:find` headers. Every
run reports wall-clock time, the datoms scanned, and the basis that it ran
against. `↑` and `↓` recall the query history.

## Metrics panel

Data-store statistics, sampled from the transactor `Status` call on the
refresh interval:

- Basis, index basis, and index lag.
- Datom, entity, and attribute counts.
- Commit queue depth, transaction totals, and failure rate.
- Indexing and garbage collection counters.
- Lease ownership and the advertised endpoint.

The panel also draws sparklines for transaction frequency, status round-trip
latency observed by the peer, and index lag. It reports peer-side query
latency as last, average, and maximum.

This panel is the only surface that shows lease ownership without reading the
root record directly.

## Transactions panel

A live feed from the transaction report subscription of the peer. Each row
shows `t`, the commit time, and the datom count.

A detail pane shows the datoms of the selected transaction. Press `f` to
toggle follow-newest.

## Schema panel

The attribute table: ident, value type, cardinality, uniqueness, and the
index, component, and history flags.

Press `/` to filter the table.

## When to use the dashboard

Use the dashboard for live observation during a load, a failover test, or an
incident. Use the metrics endpoint for recorded monitoring. See
[monitoring](../operations/monitoring.md).
