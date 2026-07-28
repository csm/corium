# Operator Peer Service

Status: **specified, not implemented.** A dedicated process for operator-level
management — a peer whose job is running the database rather than querying it —
exposing a `corium-operator` gRPC service, a JSON/HTTP gateway, and eventually a
web UI. [ADR-0019](../adr/0019-operator-peer-service.md) records the decision.

## Why the CLI stops being enough

`corium` today is not just an admin client; for several duties it *is* the
implementation:

| Duty | What the CLI actually does |
|---|---|
| `corium backup` | Calls `GetStorageInfo`, then opens the blob store and native log **itself** and streams the range into an archive. It holds storage credentials and does data-plane work. |
| `corium gc --data-dir` | Runs mark-and-sweep in-process, **offline**, requiring the transactor to be stopped. |
| `corium restore` | Reads an archive and installs a root, again with direct storage access. |
| `corium authz *` | Writes policy datoms into the authorization database. |
| `corium db fork` | Kicks off a log copy whose duration scales with the database. |
| `corium keys protect --sweep` (proposed) | Re-asserts every current value of an attribute from a key-holding process — a long-running, resumable migration. |

Three problems follow, and the encryption work in
[encryption.md](encryption.md) made all three acute rather than theoretical.

**Long-running work has no home.** A sweep over a large attribute, a full
backup, a fork of a big database, and an epoch drain are all measured in hours.
Hosting them in an interactive CLI invocation means an operator's laptop lid, an
SSH timeout, or a CI job's deadline decides whether a migration finishes
halfway. They need a process that outlives the request, checkpoints, resumes,
reports progress, and can be cancelled.

**Credentials and keys spread outward.** Backup and GC need storage
credentials; the protection sweep needs *class keys*. Making those duties CLI
duties means distributing storage credentials — and, worse, the keys that layer
2 exists to withhold — to every workstation that might run an operation. One
audited process holding them deliberately is a smaller target than five people
holding them incidentally.

**There is no operator-level record of anything.** Who ran that GC? What did the
restore install? Which sweep is in flight, how far along, and against which
basis? Today the answer is whatever scrollback still exists. Nothing about a
CLI invocation is queryable afterwards.

None of this is an argument against the CLI. It is an argument that the CLI
should be a *client* of these duties rather than their implementation.

## What it is: a peer with an operator's job

The operator service is **a peer**. It connects to the transactor like any peer,
reads storage directly, holds immutable `Db` values, and runs queries locally.
It is not a new tier in the topology and not a new trust root for the data
plane — it is the peer whose workload happens to be operations.

That framing is the whole design, and it pays immediately: the time model,
query engine, schema view, tx-report subscription, and segment cache all arrive
for free, so a job like "re-assert every current value of `:person/ssn`" is
ordinary peer code with a job wrapper around it.

Two invariants bound it:

1. **Nothing depends on it.** No transactor, peer, peer server, or client ever
   requires the operator service to be reachable. A cluster with it down commits,
   queries, indexes, fails over, and serves exactly as it does now; scheduled
   duties simply do not run until it returns. It is a control plane, and the
   data plane must never acquire a dependency on a control plane.
2. **It adds no privileged path.** Every mutation it performs is one an
   authorized caller could perform through an existing RPC, subject to the same
   `Authorizer`. It concentrates duties, not authority.

The second invariant is what keeps this from being a back door: the service
holds credentials, but it does not hold *permissions* the policy database has
not granted.

## The duty inventory

**Moves into the service** (long-running, credential-holding, or scheduled):

- Backup — full and incremental, scheduled or on demand, with retention.
- Restore and clone.
- Fork.
- Garbage collection — including the online form, so `--data-dir` offline GC
  stops being the path anyone reaches for.
- Index publication requests and pacing policy.
- Encryption jobs: the protection sweep, storage re-key and epoch drain,
  key rewrap, and the pre-flight audits that precede a shred.
- Database deletion and its blob sweep.

**Stays in the CLI** (interactive, local, or a process launcher):

- `corium transactor`, `peer-server`, `postgres-server` — entry points, not
  duties.
- `console`, `sql`, `tui` — interactive surfaces that embed a peer of their own.
- `log` — local inspection of a data directory.
- Everything else, as a *client*: `corium db create` still works, routed through
  the service when one is configured and directly against the transactor when
  one is not.

**New, and only possible with a service**:

- A job registry with history, progress, and cancellation.
- Schedules (backup nightly, GC hourly) that are not cron on somebody's box.
- Plan/apply for destructive operations.
- Two-person approval for the irreversible ones.
- A fleet view spanning transactors, peer servers, and databases.

## Jobs

The job model is the core of the API; everything long-running is one.

```
Job {
  id, kind, database?, params,
  state: Queued | Running | Succeeded | Failed{error} | Cancelled,
  requested-by, approved-by?, requested-at, started-at?, finished-at?,
  progress: { unit, done, total?, phase },     // total absent when unknowable
  checkpoint,                                   // opaque, kind-specific, resumable
  basis-t,                                      // the basis the job is operating against
  result,                                       // structured, kind-specific
  owner-lease,                                  // which replica is running it
}
```

Rules every job kind obeys:

- **Resumable.** A job records a checkpoint it can restart from. This is not a
  new burden: backup already appends checkpoint frames, GC is mark-then-sweep,
  and the protection sweep skips values already sealed under the current class.
  A service restart resumes from the checkpoint rather than from the beginning.
- **Idempotent.** Re-running a job with the same parameters converges rather
  than duplicating. That, plus the ownership lease below, is what makes "did it
  run twice?" a question with a boring answer.
- **Singleton per target.** At most one job of a kind per database runs at a
  time, enforced by a claim in the operator database. A second submission
  either joins the running job or is rejected with its id.
- **Cancellable at a checkpoint,** not mid-write. Cancellation is a request; the
  job acknowledges it at its next boundary and reports what it completed.
- **Attributable.** Requester, approver, parameters, and basis are recorded
  before work starts.

### Plan and apply

Every destructive job kind answers a `plan` before it will `apply`:

| Job | Plan reports |
|---|---|
| GC | blobs unreachable, bytes reclaimable, retention window, oldest root retained |
| Protection sweep | values to seal, entities affected, transactions it will submit |
| Storage epoch drain | objects still on the old epoch, bytes to rewrite |
| Key shred | which attributes, how many datoms, which backups become partly unreadable |
| Database delete | basis, datom count, blob bytes, last backup time |

A plan is a job too — cheap, read-only, and recorded — so the thing an operator
approved is the thing that was measured. For a shred, whose result is by design
irreversible, a fresh plan is a **precondition**: apply refuses if no plan for
those parameters exists within a configured freshness window.

### Ownership and HA

The service is stateless over its database, so run as many replicas as you
like. Job execution is serialized by the same mechanism the transactor already
uses for the write lease: a CAS-fenced lease record, renewed while running,
with the fence checked before each checkpoint commit. A replica that loses the
lease stops at its next boundary; whichever replica holds it resumes from the
checkpoint. No new coordination primitive, and no new failure mode to reason
about — the argument in
[log-and-transactor.md](log-and-transactor.md) transfers directly.

## Its state is a Corium database

The operator service keeps its registry — jobs, schedules, approvals, fleet
observations, audit — in an ordinary Corium database, `corium_operator` by
default. This is the same decision [ADR-0014](../adr/0014-self-hosted-rebac-authz.md)
made for authorization policy, for the same reasons, and it earns the same
things: backup, restore, fork, `as-of`, and the log API apply to operational
history for free.

The consequences are worth stating because they are the payoff:

- **"Why did this run?"** is a query. `as-of` the job's basis shows the schedule,
  the approval, and the policy that admitted it, exactly as they were.
- **Job history is auditable data**, not log scrollback, and it is retained,
  replicated, and backed up by machinery that already exists.
- **The UI needs no separate store.** It reads the same database through the
  same peer.

Schema shape, tuple-flat in the style of the authz database:

| Entity | Attributes |
|---|---|
| Job | `:op.job/id`, `/kind`, `/database`, `/params`, `/state`, `/progress-done`, `/progress-total`, `/phase`, `/checkpoint`, `/basis-t`, `/requested-by`, `/approved-by`, `/result`, `/error` |
| Schedule | `:op.schedule/id`, `/kind`, `/database`, `/cron`, `/params`, `/enabled`, `/last-run`, `/retention` |
| Approval | `:op.approval/job`, `/principal`, `/at`, `/plan-id` |
| Node | `:op.node/endpoint`, `/role`, `/database`, `/last-seen`, `/version`, `/lease-state` |
| Audit | `:op.audit/principal`, `/action`, `/target`, `/decision`, `/authz-t`, `/at` |

Progress updates are transactions, so a chatty job would be a write amplifier.
Rule: progress is committed at checkpoints and at a bounded interval (default
5s), never per unit of work, and `:db/noHistory` on the progress attributes
keeps the churn out of the history indexes.

The registry database is a **convenience, not a dependency**: a job that cannot
write its progress keeps working and reports on completion. The service does not
become a way to stall operations because a bookkeeping write failed.

## API

A new gRPC service, not an extension of `Catalog`. `Catalog` lives on the
transactor, and the transactor is the one process whose latency budget belongs
to the commit pipeline — hanging hours-long orchestration off it is exactly the
coupling to avoid. The operator service *calls* `Catalog`; it does not become it.

```proto
service Operator {
  // Jobs
  rpc SubmitJob(SubmitJobRequest) returns (SubmitJobResponse);
  rpc PlanJob(PlanJobRequest) returns (PlanJobResponse);
  rpc ApproveJob(ApproveJobRequest) returns (ApproveJobResponse);
  rpc CancelJob(CancelJobRequest) returns (CancelJobResponse);
  rpc GetJob(GetJobRequest) returns (Job);
  rpc ListJobs(ListJobsRequest) returns (ListJobsResponse);
  rpc WatchJob(WatchJobRequest) returns (stream JobEvent);

  // Schedules
  rpc PutSchedule(PutScheduleRequest) returns (Schedule);
  rpc ListSchedules(ListSchedulesRequest) returns (ListSchedulesResponse);

  // Inspection
  rpc GetFleet(GetFleetRequest) returns (FleetView);
  rpc WatchFleet(WatchFleetRequest) returns (stream FleetEvent);
  rpc GetDatabase(GetDatabaseRequest) returns (DatabaseStatus);
  rpc GetStorage(GetStorageRequest) returns (StorageStatus);
  rpc GetKeys(GetKeysRequest) returns (KeyStatus);      // manifests, epochs, timelines
}
```

Control operations that are *immediate* rather than long-running (create a
database, set an index policy, grant a relationship) stay on their existing
services; the operator service proxies them so one endpoint serves an operator
tool, and so every action lands in one audit trail.

**JSON/HTTP gateway.** The same surface over HTTP with JSON bodies, which is
what the UI consumes and what makes the service scriptable from anything with
`curl`. This is the first real customer for the "HTTP/JSON gateway" the roadmap
has carried as future work, and building it here — over a small, operator-scoped
surface rather than the full query protocol — is the cheap way to land it.

**Authorization** reuses the existing seam: new `Action` variants (`SubmitJob`,
`ApproveJob`, `CancelJob`, `ReadFleet`, `ManageSchedule`, `ManageKeys`) and
object names the ReBAC policy already knows how to talk about
(`job:backup`, `database:music`, `catalog:*`, `class:protect/pii`). No new
policy language, and `corium authz check` explains an operator denial exactly as
it explains an application one.

## Trust, keys, and separation of duties

A process holding storage credentials, class keys, and admin authority over
every database is the most attractive target in the deployment. The design's
answer is not to pretend otherwise but to make the concentration deliberate,
narrow, and observable.

- **Key custody is opt-in per class.** The service holds the storage DEK if it
  is to run backup, restore, or GC. It holds a *class* key only if it is to run
  a sweep or re-key for that class, and only because an operator granted it
  through the keyring. A deployment that will not hand a class key to a shared
  service declines, and runs that job from a workstation with `corium keys` —
  which keeps working precisely because the CLI remains capable.
- **Key-touching jobs announce themselves.** A sweep's plan names the class,
  the key id, and the fact that the service will hold that key for the job's
  duration. The grant is recorded in the registry with the job.
- **Two-person approval for irreversible operations.** `shred`, `delete
  database`, and `restore` over an existing name require an approval from a
  principal other than the requester before they leave `Queued`. This is the
  capability a CLI structurally cannot offer, and it is the strongest single
  argument for a service: the operations that cannot be undone are exactly the
  ones that should not be one typo deep.
- **Everything is audited** to the `AuditSink` `corium-authz` already defines,
  and to the registry. Denials, grants, approvals, plans, applies, and key
  grants all carry the principal, the authz basis, and the job id.
- **The service authenticates as itself** to the transactor and storage, so its
  actions are attributable at those layers too, rather than borrowing an
  operator's credential.

## Fleet visibility

Operators need to see what is running, and Corium mostly already knows:

- **Transactors** are discoverable with no coupling at all — the database root
  record carries the lease holder, its lease version, expiry, and advertised
  endpoint. Reading roots gives an accurate active/standby picture, and polling
  `Status` on the advertised endpoints fills in basis, index lag, and queue
  depth.
- **Peer servers and pgwire servers** appear in no registry. They may
  **optionally** announce themselves (`--operator <endpoint>` on those
  processes, writing a `Node` record with a heartbeat). Announcement is
  best-effort and failure is ignored — otherwise the no-dependency invariant is
  gone.
- **Stale nodes** age out of the view by `last-seen`; the view reports
  observation time rather than pretending to be authoritative.

The fleet view is an *observation*, and the UI says so. Corium's authority about
who holds a lease is the root record, not this service's cache of it.

## Resource posture

A peer materializes the databases it opens in memory today (see
[indexes-and-storage.md](indexes-and-storage.md)), so an operator service that
naively opened every database in the catalog would be the largest process in the
deployment. Therefore:

- Databases are **opened on demand and evicted when idle**, the same posture the
  [fleet design](transactor-fleet.md) specifies for transactors.
- Inspection that does not need a `Db` value — catalog listing, fleet view, job
  status, storage stats — never opens one. Only jobs that operate on data do.
- Job concurrency is bounded by configuration, defaulting to one data-touching
  job at a time, because the constraint is memory and storage bandwidth rather
  than CPU.

## The CLI afterwards

The CLI keeps its whole surface and gains a routing rule:

```sh
export CORIUM_OPERATOR=https://ops.internal:4338

corium backup people ./people.backup      # submits a job, streams progress, exits when done
corium backup people ./people.backup --detach   # submits and prints the job id
corium jobs list|watch|cancel <id>              # the new client surface
corium keys protect :person/ssn --class :protect/pii --sweep   # submits a sweep job
```

Without `CORIUM_OPERATOR`, every command behaves exactly as it does today,
running the duty in-process. That fallback is not a transitional courtesy — it
is what keeps a single-node development database from needing a control plane,
and it is why the two invariants above are affordable.

`corium tui` retargets its metrics and transaction panels at the operator API
when one is configured, so terminal and web show the same fleet.

## The UI, eventually

A static single-page application served by the same process, over the JSON
gateway, with no separate deployment and no store of its own:

- Fleet: transactors and their lease state, peer servers, databases, basis and
  index lag.
- Databases: schema browser, stats, time-travel query workbench (the console's
  surface, in a browser).
- Jobs: running and historical, with live progress, logs, plans, and the
  approval flow.
- Keys: manifests, epochs, protection timelines, and `keys audit` exposure
  numbers — the place where "this attribute still has 12,481 plaintext current
  values" is a number an operator sees rather than a command they must know to
  run.

The rule that keeps it honest: **the UI is a client of a complete API, never a
privileged path.** Anything the UI can do, the CLI and `curl` can do, with the
same authorization and the same audit record. Ship the API first and let the UI
lag; the reverse produces a UI with capabilities nothing else can reach.

## What it must not become

- **A monitoring system.** Prometheus endpoints exist on the transactor and peer
  server; the service exposes its own and *links* to the others. It does not
  ingest, store, or alert on time series.
- **An application query front end.** That is the peer server. The workbench in
  the UI is an operator tool with operator limits, not a serving path.
- **A required dependency.** Restated because it is the invariant most likely to
  erode: the day a transactor refuses to start without the operator service, the
  design has failed.
- **A second source of truth.** Roots, the log, and the policy database remain
  authoritative. The registry records what the service *did*, never what the
  cluster *is*.

## Operating it

```sh
corium operator \
  --transactor http://transactor-a:4334 \
  --registry-db corium_operator \
  --listen 127.0.0.1:4338 \
  --http-listen 127.0.0.1:4339 \
  --storage-key file:/etc/corium/storage.key \
  --key :protect/pii=awskms:arn:… \
  --authz-db corium_authz \
  --require-approval shred,delete-database,restore-over \
  --max-concurrent-jobs 1 \
  --metrics-listen 127.0.0.1:9640
```

Ports follow the existing convention (transactor 4334, peer server 4336): gRPC
on 4338, HTTP/UI on 4339. `ServeFlags` and `ClientFlags` are shared with the
other servers, so TLS, tokens, OIDC, and `--authz-db` behave identically.

Metrics: `corium_operator_jobs{kind,state}`,
`corium_operator_job_duration_seconds{kind}`,
`corium_operator_job_progress{job}`, `corium_operator_lease_held`,
`corium_operator_databases_open`, `corium_operator_approval_pending`.

Failure behaviour:

| Condition | Behaviour |
|---|---|
| Registry database unreachable | jobs keep running; progress buffered; state reconciled on reconnect |
| Transactor unreachable | data-touching jobs pause at their checkpoint and retry with backoff |
| Lease lost to another replica | stop at the next checkpoint; the new holder resumes |
| Service killed mid-job | on restart, resume from checkpoint; a job with no checkpoint restarts |
| Class key missing for a sweep | the job refuses at submission, not halfway through |

## Implementation plan

1. **`corium-operator` crate**: job model, state machine, checkpointing, the
   ownership lease, and the registry schema, tested against an in-memory
   database with no service around it.
2. **The service**: `Operator` gRPC, `corium operator` launcher, authz actions,
   audit, and the first job kinds — GC and index publication, which are the
   simplest and already have transactor-side implementations to call.
3. **Backup, restore, fork** moved behind jobs, with the CLI routing to them.
   These are the duties whose current implementations already live in the CLI,
   so this is a move rather than a rewrite.
4. **Plan/apply and approvals**, including the plan-freshness precondition.
5. **JSON/HTTP gateway**, and `corium jobs` in the CLI.
6. **Encryption jobs**: protection sweep, epoch drain, rewrap, key audit —
   scheduled after [encryption.md](encryption.md)'s layer 2 lands, and the
   reason its long-running duties were specified as jobs from the start.
7. **Fleet view**, optional node self-registration, TUI retargeting.
8. **UI**, once the API has been stable through at least one release.

Acceptance:

- A job survives service restart at every checkpoint boundary and completes
  exactly once, verified by fault injection in `corium-sim` — the same harness
  the HA suite uses.
- Two replicas never run the same job; killing the lease holder mid-job has the
  other resume within the lease bound.
- Every duty removed from the CLI still works from the CLI with a service
  configured, and still works *without* one, with output diffed against today's.
- A cluster runs its full acceptance battery with the operator service stopped.
- Destructive jobs refuse without a fresh plan and a distinct approver, and both
  refusals are audited.
- The registry database being unavailable degrades progress reporting and
  nothing else.

## Open questions

- **Registry bootstrap.** The service needs a database to record jobs, and
  creating that database is itself an operation. `corium operator init` mirrors
  `corium authz init`, but the ordering (`--registry-db` naming a database that
  does not exist yet) needs the same fail-closed-then-recover behaviour the
  authz database has, rather than a startup failure.
- **Multi-transactor scope.** The flags above name one transactor. A deployment
  with the [fleet design](transactor-fleet.md)'s many-database placement wants
  the service to span the fleet, which means discovering transactors from roots
  rather than configuration. Probably the same work as the fleet view, done
  once.
- **Where jobs execute.** Everything here runs jobs *in* the service. A sweep
  over a very large attribute may want to run several workers; that turns the
  registry into a work queue and the service into a scheduler, which is a
  materially bigger system. Deliberately not proposed, but the job model should
  not preclude it.
- **Scheduling semantics.** Cron expressions are the obvious spelling and the
  usual source of surprise (time zones, missed runs after downtime, overlap
  with a still-running job). Overlap has an answer here — jobs are singleton per
  target — but catch-up policy does not yet.
- **UI authentication.** The gRPC and CLI surfaces use bearer tokens and OIDC. A
  browser wants a session, which means a redirect flow, cookies, and CSRF
  concerns that none of the current surfaces have. That is the part of the UI
  work that is not just a client.
- **Retention of the registry.** Job history is data, and data accumulates. A
  retention policy (or `:db/noHistory` on more of it) is needed before a busy
  deployment discovers the answer empirically.
