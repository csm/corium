# Operator manual — writing checklist

Status of every chapter in `docs/manual/src`. Update this file as you write.

Legend: `[x]` written and reviewed against the code. `[~]` written, but a note
below says what is missing. `[ ]` not started.

Last full re-verification: against `main` at the merge that brought in
pluggable storage, planned schema migrations, contextual read authorization,
attribute protection, and the Java client.

## Front matter

- [x] `introduction.md` — audience, structure, conventions, aside labels,
      terminology table.
- [x] `getting-started.md` — build, `--store mem` transactor, TOML schema,
      `db create`, write through `postgres-server --allow-writes`, console,
      SQL, TUI, `schema update`, stop.

## Theory of operation

- [x] `theory/index.md` — the five ideas and the operational consequences.
- [x] `theory/datoms.md` — datom shape, entity ids, partitions, `t`,
      transaction time as data, the nine value types plus `Sealed`, schema is
      data, schema generation.
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

- [x] `running/installation.md` — toolchain, Cargo features, building a
      loadable driver, workspace build note, ports, `fs` layout, supervision
      and the `SIGINT`-only note.
- [x] `running/transactor.md` — every flag group, plugin flags, production
      example.
- [x] `running/storage.md` — five built-in backends, read-only discovery
      credentials, per-backend detail, runtime plugins, `store verify`,
      choosing a backend.
- [x] `running/config-file.md` — every EDN key, what the file does not hold.
- [x] `running/catalog.md` — connection flags, create, list, stats, delete.
- [x] `running/schema.md` — TOML and EDN, property meanings, enumerations,
      `schema update` plan/apply, execution classes, acknowledgement codes,
      `--prune` and retirement, audit trail, inspection, reserved idents.
- [x] `running/indexing.md` — pacing, runtime overrides, `request-index`,
      bulk loading, watching lag.

## Client surfaces

- [x] `surfaces/console.md` — input kinds, command table, timestamps, view
      cost, bootstrap, watch.
- [x] `surfaces/tui.md` — navigation and the four panels.
- [x] `surfaces/sql.md` — projection, shell, wire server, write subset,
      explicit transactions, ORM support, per-principal authentication.
- [x] `surfaces/peer-server.md` — flags, fuel, bootstrap, segment cache,
      failover, thin-client contract, per-principal service, language
      clients.

## Security

- [x] `security/authentication.md` — permissive default, strict mode, client
      tokens, OIDC, TLS, recommended settings.
- [x] `security/authorization.md` — bootstrap, tuples, the 15 actions,
      default permissions, check, status, operating notes, lockout recovery,
      views, key grants, surfaces that fail closed.
- [x] `security/encryption.md` — fixed at creation, key identities, which
      processes need keys, offline commands, status, rotate, rewrap, the
      unavailable and fenced states, the second layer.
- [x] `security/protection.md` — declaring a class, class options and their
      cost, which processes need class keys, per-principal key policy,
      upgrading a guarded server, forward-only protection changes.

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
      `db stats`, the schema audit query, logging targets, what to alert on.
- [x] `operations/runbooks.md` — 16 procedures: planned failover, crashed
      active, split brain, both down, restore, startup failure, refused
      writes, ambiguous transaction, schema change, blocked schema plan,
      total denial, index lag, peer memory, encrypted backup, full storage,
      log-replay recovery.

## Reference

- [x] `reference/commands.md` — every command and flag group, plus the
      commands that do not exist.
- [x] `reference/environment.md` — every variable, secret handling.
- [x] `reference/defaults.md` — default values by area, duration and size
      formats.
- [x] `reference/glossary.md` — 52 terms.

---

## Notes for the next writer

### Verified against the code, not against the existing docs

The manual follows the code. Where a design or reference document disagrees,
the manual is right and the other document needs a correction pass.

1. `crates/corium-pgwire/README.md` closes with "PostgreSQL usernames and
   passwords are not yet mapped to Corium principals". They are. The password
   field carries the caller's bearer token, and every statement is authorized
   as the resulting principal (ADR-0021).
2. `docs/thin-client-protocol.md` is titled "v1" and says every request sends
   `protocol_version = 1`. `corium_protocol::PROTOCOL_VERSION` is 3, and
   `MIN_SUPPORTED_PROTOCOL_VERSION` is 1.
3. `docs/design/encryption.md` opens by listing protection *changes* as not
   implemented. They are: `corium schema update` protects, unprotects, and
   re-classifies an attribute forward-only, as the same document's
   "Changing protection" section and `docs/design/schema-migrations.md`
   both state.
4. `docs/schema-toml.md` calls `corium schema update` "proposed" and says
   `doc` and `protection` are "design commitments rather than accepted
   fields". Both parse today, in TOML and in EDN.
5. `docs/operations.md` still tells an operator to read `:lease-owner` and
   `:lease-owner-endpoint` from `corium db stats`. It prints neither
   (`crates/corium-cli/src/main.rs:2345`). Only the TUI `Metrics` panel shows
   them, from the same `Status` call.
6. `docs/operations.md` still calls the peer SSD segment cache "proposed". It
   is implemented: `--segment-cache-dir`, `--segment-cache-capacity`,
   `--segment-cache-memory`, and six Prometheus metrics.
7. `docs/design/data-model.md` lists `BigInt`/`BigDec` value variants.
   `corium_core::Value` has ten variants and neither of those.
8. `docs/design/data-model.md` describes user partitions created as entities
   with `:db/ident`. `corium_core::Partition` has three variants and no way to
   add one.

### Corrected since the previous revision of this manual

These were recorded as gaps and are now implemented. The chapters were
rewritten, not patched.

- **Schema is no longer fixed at database creation.** `corium schema update`
  plans and applies attribute changes. The copy-to-a-new-database migration
  procedure is gone.
- **Authorization view filters are enforced.** A filtered decision no longer
  returns `UNIMPLEMENTED`. Attribute views and protection key grants both
  apply on the peer server and on pgwire.
- **Attribute protection classes are implemented** for the shape the schema
  can express, and have their own chapter.
- **pgwire maps a SQL client to a Corium principal**, and supports atomic
  explicit transactions and Hibernate.
- **`:db/doc` works**, and enumerated ident entities work.
- **Storage backends are pluggable at run time**, with `--store-plugin` and
  `corium store verify`.

### Gaps in the product that the manual records as asides

- **No `corium transact` command.** The console is read-only. The only CLI
  write path is `postgres-server --allow-writes`.
- **`rewrite` schema changes are always blocked.** The only `rewrite` step the
  planner emits is a cardinality collapse with conflicts, and it is refused.
  `destructive` changes can never run.
- **`corium schema` has only `update`.** `status`, `history`, and job
  inspection are planned.
- **A schema update cannot install a protection class**, only point an
  attribute at one. Class definitions stay create-time.
- **Backup and restore never change a database's encryption state.** An
  archive restores as it was taken; there is no re-keying restore, so no
  migration path onto (or off) storage encryption.
- **KMS key identities do not resolve.** Only `file:` and `env:` work.
- **`SIGTERM` is not handled.** Only `SIGINT` triggers graceful shutdown and
  lease release.
- **Only the transactor and `store verify` load storage plugins.** A
  storage-aware peer against a plugin backend fails.
- **Seal-through mode does not exist**, nor do `corium keys protect`,
  `unprotect`, or `audit`, nor class-key rotation and shredding.
- **`scope = "entity"` protection parses but does not seal.**
- **User-defined partitions do not exist**, and excision is out of scope.
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
      `:db/fn` code still has no CLI deployment surface. Confirm how a
      `:db/fn` is installed before writing it.
- [ ] **Upgrade and version-compatibility chapter.** Storage format 4, backup
      format 1, and thin-client protocol versions 1 through 3 are all live.
      There is no documented upgrade procedure between Corium releases.
- [ ] **Multi-tenancy guidance.** The authorization model supports
      `tenant:` objects and rewrites, but the manual documents only the flat
      `database:` case. Read the rewrite rules in
      `crates/corium-authz/src/schema.rs` before writing it.
- [ ] **Worked example chapter.** A pointer to `examples/musicbrainz` exists
      in the repository README but not in the manual.
- [ ] **Client library chapters.** The Python and Java clients now have query,
      pull, and transaction builders. The manual names them and links out. A
      chapter per client belongs in the client surfaces part.
- [ ] **Writing a storage plugin.** `docs/storage-plugins.md` is the author's
      guide. The manual covers only the operator's side.

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

### Publishing the book

`.github/workflows/publish-manual.yml` renders the book and mirrors it to a
web host over SFTP. It runs on a push to `main` that touches `docs/manual/`,
and on demand through `workflow_dispatch`.

The host, port, and remote directory come from repository variables
(`MANUAL_SFTP_HOST`, `MANUAL_SFTP_PORT`, `MANUAL_SFTP_REMOTE_DIR`), and a
manual run can override each one. The credentials are secrets
(`MANUAL_SFTP_USERNAME`, `MANUAL_SFTP_PASSWORD`), as is the optional
`MANUAL_SFTP_KNOWN_HOSTS` host key. The workflow header documents the
resolution order.

The remote directory is mirrored with `--delete`, so give the manual a
directory of its own. A `workflow_dispatch` run with `dry_run` set builds the
book, reports the target, and uploads nothing.

Every internal link and anchor resolves, and the style check finds no banned
modal, contraction, semicolon, or sentence over 25 words.
