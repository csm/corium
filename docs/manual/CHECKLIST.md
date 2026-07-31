# Operator manual — writing checklist

Status of every chapter in `docs/manual/src`. Update this file as you write.

Legend: `[x]` written and reviewed against the code. `[~]` written, but a note
below says what is missing. `[ ]` not started.

## Front matter

- [x] `introduction.md` — audience, structure, conventions, aside labels,
      terminology table.
- [x] `getting-started.md` — build, `--store mem` transactor, TOML schema,
      `db create`, write through `postgres-server --allow-writes`, console,
      SQL, TUI, stop.

## Theory of operation

- [x] `theory/index.md` — the five ideas and the operational consequences.
- [x] `theory/datoms.md` — datom shape, entity ids, partitions, `t`,
      transaction time as data, the nine value types, schema is data.
- [x] `theory/log.md` — commit point, record frame, pipeline, group commit,
      log layout per store, lease-versioned files, reading the log.
- [x] `theory/indexes.md` — four covering indexes, segments, database root,
      what a peer holds today, incremental publication, storage traits,
      collection, safety argument.
- [x] `theory/time.md` — database value, views, wall-clock naming, cost of a
      view, transaction reports, `tx-range`.
- [x] `theory/processes.md` — storage service, transactor, peer, peer server,
      the proposed operator service and fleet, "which process needs what".

## Running Corium

- [x] `running/installation.md` — toolchain, Cargo features, workspace build
      note, ports, `fs` layout, supervision and the `SIGINT`-only note.
- [x] `running/transactor.md` — every flag group, production example.
- [x] `running/storage.md` — five backends, read-only discovery credentials,
      per-backend detail, choosing a backend.
- [x] `running/config-file.md` — every EDN key, what the file does not hold.
- [x] `running/catalog.md` — connection flags, create, list, stats, delete.
- [x] `running/schema.md` — the fixed-at-creation rule, TOML, EDN, property
      meanings, inspection, reserved idents, migration procedure.
- [x] `running/indexing.md` — pacing, runtime overrides, `request-index`,
      bulk loading, watching lag.

## Client surfaces

- [x] `surfaces/console.md` — input kinds, command table, timestamps, view
      cost, bootstrap, watch.
- [x] `surfaces/tui.md` — navigation and the four panels.
- [x] `surfaces/sql.md` — projection, shell, wire server, write subset,
      security notes.
- [x] `surfaces/peer-server.md` — flags, fuel, bootstrap, segment cache,
      failover, thin-client contract, language clients.

## Security

- [x] `security/authentication.md` — permissive default, strict mode, client
      tokens, OIDC, TLS, recommended settings.
- [x] `security/authorization.md` — bootstrap, tuples, the 14 actions,
      default permissions, check, status, operating notes, lockout recovery,
      the unenforced view filters.
- [x] `security/encryption.md` — fixed at creation, key identities, which
      processes need keys, offline commands, status, rotate, rewrap, the
      unavailable and fenced states.

## Availability and data care

- [x] `availability/high-availability.md` — pair setup, storage requirements,
      peer failover, guarantees, unavailability window, ambiguous
      transactions, tuning.
- [x] `availability/backup.md` — online backup, incremental, the archive,
      where it can run, restore, verification, policy.
- [x] `availability/fork.md` — commands, what it copies, rules, when not to
      fork, cleanup.
- [x] `availability/gc.md` — retention rule, scheduled and manual, zero
      window, safety, monitoring, tuning.

## Operations

- [x] `operations/monitoring.md` — metrics endpoint, every metric name,
      `db stats`, logging targets, what to alert on.
- [x] `operations/runbooks.md` — 14 procedures: planned failover, crashed
      active, split brain, both down, restore, startup failure, refused
      writes, ambiguous transaction, total denial, index lag, peer memory,
      encrypted backup, full storage, log-replay recovery.

## Reference

- [x] `reference/commands.md` — every command and flag group, plus the
      commands that do not exist.
- [x] `reference/environment.md` — every variable, secret handling.
- [x] `reference/defaults.md` — default values by area, duration and size
      formats.
- [x] `reference/glossary.md` — 40 terms.

---

## Notes for the next writer

### Verified against the code, not against the existing docs

Several statements in `docs/operations.md` and `PLAN.md` are stale. The manual
follows the code. Where they disagree, the manual is right, and
`docs/operations.md` needs a separate correction pass:

1. `docs/operations.md` says `corium db stats` prints the lease owner. It does
   not. Only the TUI `Metrics` panel shows it. See
   `crates/corium-cli/src/main.rs:2228`.
2. `docs/operations.md` calls the peer SSD segment cache "proposed". It is
   implemented: `--segment-cache-dir`, `--segment-cache-capacity`,
   `--segment-cache-memory`, and six Prometheus metrics.
3. `PLAN.md` says encryption at rest is "specified but not yet built".
   Storage-level encryption is built. Attribute protection classes are not.
4. `docs/design/data-model.md` lists `BigInt` and `BigDecimal` value variants.
   `corium_core::Value` has nine variants and neither of those.
5. `docs/design/data-model.md` describes user partitions created as entities
   with `:db/ident`. `corium_core::Partition` has three variants and no way to
   add one.

### Gaps in the product that the manual records as asides

- **Schema is fixed at database creation.** `schema_from_edn` runs only in
  `create_db`. Nothing installs an attribute afterward, and a second
  `db create` ignores the schema file. This is the largest operational gap.
  The manual gives a copy-to-a-new-database migration procedure.
- **No `corium transact` command.** The console is read-only. The only
  CLI write path is `postgres-server --allow-writes`.
- **`corium backup` refuses an encrypted database.** Backup format 1 cannot
  carry the key manifest, so encrypted databases have no supported backup.
- **Authorization view filters are compiled but not enforced.** A filtered
  decision returns `UNIMPLEMENTED`. Creating `:authz.view/*` or
  `:authz.binding/*` entities breaks the requests they match.
- **KMS key identities do not resolve.** Only `file:` and `env:` work.
- **`SIGTERM` is not handled.** Only `SIGINT` triggers graceful shutdown and
  lease release.
- **`:db/doc` has no effect**, and user-defined partitions do not exist.
- **Native-backend log sealing is future work**, so replay and list cost grow
  with the tail since the last index publication.

### Not yet written, and worth adding later

- [ ] **Capacity planning chapter.** Peer memory tracks total history. There
      is no measured guidance, and no benchmark table for peer footprint per
      million datoms. `docs/benchmarks/` has an M3 baseline that could seed it.
- [ ] **Write-path tuning chapter.** `NodeConfig::max_commit_batch` and
      `max_commit_batch_bytes` are not exposed as CLI flags, so they were left
      out. See `docs/design/write-path-scaling.md` if flags are added.
- [ ] **Database function operations.** `--db-fn-fuel` and
      `--db-fn-memory-bytes` are documented in the transactor chapter only.
      A chapter on writing, deploying, and budgeting `:db/fn` code would help,
      but deployment of `:db/fn` code has no CLI surface today. Confirm how a
      `:db/fn` is installed before writing it.
- [ ] **Upgrade and version-compatibility chapter.** Storage format versions
      2 and 3 and backup format 1 are mentioned in the design docs. There is
      no documented upgrade procedure between Corium releases.
- [ ] **Multi-tenancy guidance.** The authorization model supports
      `tenant:` objects and rewrites, but the manual documents only the flat
      `database:` case. Read the rewrite rules in
      `crates/corium-authz/src/schema.rs` before writing it.
- [ ] **Worked example chapter.** A pointer to `examples/musicbrainz` exists
      in the repository README but not in the manual.

### Style rules used

- Prose follows ASD-STE100 Simplified Technical English where practical.
  Procedural sentences are 20 words or fewer, and descriptive sentences are
  25 or fewer. Conditions come before commands. No semicolons.
- One word per idea: "make sure that" (never check/verify/confirm), "run"
  (never execute/invoke/launch), "delete" for data, "configuration" (never
  config/settings), "print" for command output, "show" for interactive panels.
- Approved modals only: can, will, must. No should/would/may/might/could.
- Asides use two labels only: **Not implemented** and **Partly implemented**.
  An aside with no label is background information.
- Cross-references state what the other chapter holds and why it is needed,
  then link.

### Building the book

Install `mdbook`, then build:

```sh
cargo install mdbook --locked
mdbook build docs/manual
mdbook serve docs/manual
```

The book builds clean as of this writing. `docs/manual/book/` is in
`.gitignore`.

`book.toml` sets `create-missing = false`, so a broken `SUMMARY.md` link fails
the build rather than creating an empty page.
