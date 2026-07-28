# ADR-0019: An operator peer service for management duties

**Status:** Proposed (2026-07-28); design in
[`docs/design/operator-service.md`](../design/operator-service.md). Relates to
[ADR-0012](0012-optional-authn-authz.md) and
[ADR-0014](0014-self-hosted-rebac-authz.md) (the authorization seam and the
self-hosted policy database it reuses), and to
[ADR-0017](0017-encryption-at-rest.md) / [ADR-0018](0018-attribute-protection-classes.md),
whose long-running key operations are what made this urgent.

## Context

Several operator duties are not implemented *by* a service — they are
implemented by the CLI. `corium backup` obtains a basis and storage connection
from the transactor and then reads the blob store and native log itself.
`corium gc --data-dir` runs mark-and-sweep in-process and requires the
transactor to be stopped. `restore`, `fork`, and the authz policy commands are
the same shape. Each holds credentials, and each runs for as long as it runs
inside whatever shell invoked it.

Three consequences have been tolerable and are about to stop being so.

Long-running work has no home: a backup, a fork, an epoch drain, and — newly —
a protection sweep are hour-scale operations whose completion currently depends
on an interactive process staying alive. Credentials spread outward: backup and
GC need storage credentials, and the protection sweep needs the *class keys*
that ADR-0018 exists to withhold, so making these CLI duties distributes exactly
the material the encryption design is careful about. And nothing is recorded:
who ran a GC, what a restore installed, how far a sweep progressed, and against
which basis are all questions whose answer today is terminal scrollback.

The transactor is the wrong place to fix this. It is the process whose latency
belongs to the commit pipeline, and hanging hour-long orchestration off
`Catalog` couples operations to the write path.

## Decision

Add a dedicated operator process — **a peer whose workload is operations** —
exposing an `Operator` gRPC service, a JSON/HTTP gateway, and eventually a web
UI, with the CLI becoming a client of it rather than an implementation of its
duties.

- **It is a peer, not a new tier.** It connects like any peer, reads storage,
  and holds `Db` values, so the time model, query engine, and schema view come
  for free and an operational job is ordinary peer code with a job wrapper.
- **Nothing depends on it.** No transactor, peer, or client requires it to be
  reachable; a cluster with it stopped behaves exactly as it does now. It is a
  control plane, and the data plane must not acquire a dependency on one.
- **It adds no privileged path.** Every mutation is one an authorized caller
  could make through an existing RPC, checked by the same `Authorizer` with new
  actions and ordinary object names. It concentrates duties, not authority.
- **Everything long-running is a job:** resumable from a checkpoint, idempotent,
  singleton per target, cancellable at a boundary, and attributable to a
  requester. Execution is serialized across replicas by the same CAS-fenced
  lease pattern the transactor uses for writes, held **per job target rather
  than per service**, so two service instances responsible for disjoint
  databases never contend.
- **Destructive jobs require a plan, and the irreversible ones require a second
  person.** `shred`, `delete database`, and restore-over-an-existing-name need a
  fresh plan and an approval from a principal other than the requester.
- **Its registry is an ordinary Corium database.** Jobs, schedules, approvals,
  fleet observations, and audit live in `corium_operator`, exactly as ReBAC
  policy lives in `corium_authz` — so operational history is backed up,
  time-travelable, and queryable with the machinery that already exists. The
  registry is a convenience, never a dependency: a job whose progress cannot be
  written keeps running.
- **The CLI keeps its entire surface** and routes duties through the service
  when `CORIUM_OPERATOR` names one, running them in-process when it does not.

## Consequences

- Migrations, backups, and sweeps finish because a service is running them, not
  because a terminal stayed open. Progress, cancellation, and resumption become
  API surface instead of hope.
- Storage credentials and class keys concentrate in one audited process rather
  than spreading to every workstation that might run an operation. That is a
  larger single target, deliberately chosen, and it is bounded: class-key
  custody is opt-in per class, announced in the job's plan, recorded with the
  job, and declinable — a deployment that will not grant a class key to a shared
  service runs that job from a workstation, which still works.
- Two-person approval becomes possible at all. A CLI cannot structurally offer
  it, and the operations that cannot be undone are exactly the ones that should
  not be one typo deep.
- Operational history becomes data: "why did this run at 14:03" is answered by
  reading the registry `as-of` the job's basis, the same sentence ADR-0014 makes
  about policy decisions.
- A new process is a new thing to deploy, secure, upgrade, and reason about, and
  a new database to bootstrap. For a single-node development database that cost
  is zero only because the CLI fallback keeps working with no service at all —
  which is the reason that fallback is a permanent part of the design rather
  than a migration aid.
- The JSON/HTTP gateway lands the roadmap's long-carried "HTTP/JSON gateway"
  item over a small operator-scoped surface rather than the full query protocol,
  which is the cheap way to get it.
- The UI is deferred behind a stable API on purpose. Shipping it first would
  produce capabilities reachable only from a browser; shipping the API first
  means the UI, the CLI, and `curl` are equally capable and equally audited.
- Multi-tenant operations are not designed here, and are deliberately not
  foreclosed. The likely shape — a service authenticated and dedicated to a
  slice, with a deployment running several — is affordable precisely because
  nothing depends on the service, so it can be run N times over disjoint slices
  with no coordination. What this ADR commits to is keeping the implicitly
  global things out: leases per target, globally unique job ids, a `scope`
  recorded on every job, approval authority checked on the job's target rather
  than the service, key and credential configuration keyed by database, a
  configurable registry name, and a fleet view that reports scope and
  observation time instead of promising deployment-wide truth. Grouping
  databases under a tenant needs no new policy language — ADR-0014's rewrites
  already express it — but database creation should be able to record its owning
  object so the grouping exists from the start.
- Fleet visibility is an observation, not a source of truth. Transactors are
  discoverable from root records with no coupling; peer servers may self-register
  best-effort, and a failed announcement is ignored — otherwise the
  no-dependency invariant is quietly gone.
