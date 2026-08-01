# Pluggable storage backends

**Status:** proposal (not implemented)

## Problem

Storage backends are selected at compile time. `corium-store` gates
`PostgresBlobStore`, `TursoBlobStore`, and `S3BlobStore` behind the
`postgres` / `turso` / `s3` features, and that gate propagates upward through
`corium-transactor`, `corium-ffi`, `corium-python`, and `corium-cli`. Every
layer repeats the same `#[cfg]` fan-out:

- `StoreSpec` and `NodeStore` (`crates/corium-transactor/src/backend.rs`) each
  carry one feature-gated variant per backend, and `LogBackend::for_spec`
  repeats the list a third time to decide which backends get a root-backed log.
- `DiscoveredStoreSpec` and `DiscoveredStore`
  (`crates/corium-store/src/discovery.rs`) repeat it again, twice, for the
  read-only client path — and `DiscoveredStore` re-implements five delegating
  methods purely to dispatch the enum.
- `corium_ffi::compiled_storage_backends` reports the build's feature set as a
  string list so language clients can find out what their own binary can do.

The cost lands hardest on the language clients. Because a native artifact can
only talk to a backend it was compiled with, the Python client ships **four**
wheels per platform — `corium`, `corium-turso`, `corium-postgres`,
`corium-s3` — each containing a complete copy of the engine, differing only in
one storage driver. `clients/python/src/corium/_api.py` then imports every
installed variant, asks the transactor which backend it advertises, and picks
the module whose `_storage_backends()` claims it. Installing two backends
means two full engine copies in one interpreter. The coming Java client would
have to repeat all of it.

Compile-time selection also means a third party cannot add a backend without
forking: there is no supported way to reach `BlobStore`/`RootStore` from
outside the workspace's feature graph.

## Goal

Make a storage backend a unit that can be **built, shipped, and loaded
separately** from the engine, so that:

1. one engine artifact serves every backend;
2. adding a backend to a deployment is an install step, not a rebuild;
3. an external author can publish a backend without touching this repo.

Non-goal: removing compiled-in backends. Static linking must remain available
for the transactor (single-binary deploys) and is mandatory on platforms
without `dlopen` (wasm, iOS).

**Platforms without `dlopen` are an accepted limitation, not a problem to
solve.** iOS gets one statically linked backend — Turso, which is the only one
that makes sense on a device; S3 and PostgreSQL direct-storage are
server-adjacent configurations that a mobile client should reach through a
transactor anyway. wasm keeps the filesystem/memory stores it has today. This
is why the dual `rlib` + `cdylib` delivery in §1 is a deliberate feature of the
design rather than transitional baggage: the static path is load-bearing for
real targets and is not scheduled to go away.

## Why a C ABI is unavoidable

Rust has no stable ABI. Two separately compiled artifacts — the `corium`
wheel and a `corium-turso` wheel — cannot exchange a `Box<dyn BlobStore>`,
even when built from the same source at the same commit, unless they are
compiled together by one `cargo` invocation. Whatever transport carries the
plugin (a `dlopen`ed `.so`, a Python capsule handed between two extension
modules, a JNI-loaded library), the *values* crossing the boundary must be
`#[repr(C)]` with `extern "C"` function pointers.

That constraint drives the rest of the design, including the parts that look
awkward (cursor-based listing, an explicit log-sink callback, a plugin-owned
runtime).

Helper crates do not remove this constraint — they generate and verify the
`repr(C)` boundary for you. See §3 for the `abi_stable` / `async-ffi`
evaluation.

## Alternatives considered

**Ship one fat wheel with `all-storage`.** Zero design work; solves the Python
artifact-selection problem outright. It does not solve external backends, and
it forces every user to carry the AWS SDK, `tokio-postgres`, and Turso. This is
the baseline any plugin design has to beat — it is a legitimate *interim* step
and worth taking if the plugin work slips.

**Out-of-process backend (gRPC sidecar).** No unsafe code, language-agnostic
plugins, clean isolation. Rejected as the primary mechanism because it adds a
network hop to every blob read on the peer's direct-storage path — which exists
precisely to *avoid* a hop through the transactor — and because it introduces
process lifecycle management into client libraries. Worth keeping as a future
option for exotic backends where per-op latency does not matter.

**WebAssembly component plugins.** Attractive on safety (the host crate needs
no `unsafe` of its own; `wasmtime` carries it). Rejected because the backends
we care about are exactly the ones that are hardest to run in wasm: the AWS
SDK, a Postgres wire driver with TLS, and an embedded SQLite fork. Revisit if
`wasi:http` and `wasi:sockets` mature.

## Design

### 1. Split backends into their own crates

Move `postgres_store.rs`, `turso_store.rs`, and `s3_store.rs` out of
`corium-store` into `corium-store-postgres`, `corium-store-turso`, and
`corium-store-s3`. Each is built as both an `rlib` and a `cdylib`:

```toml
[lib]
crate-type = ["rlib", "cdylib"]
```

One implementation, two delivery modes. The `rlib` keeps the existing
statically linked path working (the transactor's `postgres`/`turso`/`s3`
features become dependency toggles on these crates); the `cdylib` is the
loadable plugin. `corium-store` retains the traits, `MemoryStore`, `FsStore`,
encryption, the segment cache, and the key manifest — and loses all three
optional dependency sets.

### 2. A registry, not an enum

Replace the feature-gated enums with a process-wide registry keyed by backend
kind:

```rust
pub trait StorageBackend: Send + Sync {
    fn kind(&self) -> &str;                       // "s3", "turso", "acme-gcs"
    fn capabilities(&self) -> BackendCapabilities; // e.g. log placement
    async fn open(&self, config: &StoreConfig) -> Result<Arc<dyn FullStore>, StoreError>;
    async fn open_existing(&self, config: &StoreConfig) -> Result<Arc<dyn ReadStore>, StoreError>;
}
```

Compiled-in backends register at startup; loaded plugins register on load.
Everything downstream then holds a trait object:

- `NodeStore` becomes `Arc<dyn FullStore>` (`BlobStore + RootStore`); its
  ~40 lines of per-method match arms disappear.
- `DiscoveredStore` becomes a newtype over `Arc<dyn ReadStore>`; its five
  hand-written delegating methods disappear.
- `StoreSpec` / `DiscoveredStoreSpec` become `{ kind: String, config: StoreConfig }`.
- `LogBackend::for_spec`'s hardcoded "these three backends get a root-backed
  log" list becomes `capabilities().log_placement`, which the backend declares.
  External backends get the right log behaviour without the transactor knowing
  their names.
- `compiled_storage_backends()` becomes `available_storage_backends()`,
  reading the registry.

**This step is independently valuable** and carries no ABI risk. It can land
before any dynamic loading exists.

### 3. `corium-store-abi` — the contract crate

A small crate defining the boundary types, the ABI version, the entry-point
symbol, and the error codes. Both hosts and third-party plugin authors depend
on it; it is the only thing we promise stability on.

Entry point, resolved by symbol name:

```rust
#[unsafe(no_mangle)]
pub extern "C" fn corium_store_plugin_v1(out: *mut PluginDesc) -> i32;
```

`PluginDesc` reports the ABI version, the backend kinds served (a plugin may
serve several), capabilities, and the operation vtable.

#### Async: use `async-ffi`

`async-ffi` (0.5.1) provides `FfiFuture<T>`, a `#[repr(C)]` equivalent of
`Box<dyn Future<Output = T> + Send>`: any `Send + 'static` future converts with
`.into_ffi()`, and the waker vtable crosses the boundary. Plugin methods return
`FfiFuture<RResult<...>>` and the host `.await`s them, so the `async fn` shape
of `BlobStore`/`RootStore` survives to every existing call site with no
completion-callback plumbing, no `oneshot` bookkeeping, and no leaked
`user_data` boxes.

Caveat: panics inside `poll` are caught and surfaced as `FfiPoll::Panicked`,
but a panic in drop glue or in a waker vtable function aborts the process. The
plugin guide must say so.

#### The plugin owns its runtime — this is forced, not chosen

**Each dynamic library gets its own copy of tokio's thread-locals.** A plugin
calling `Handle::current()` does not see the host's runtime, because the two
tokio copies do not share TLS. `tokio-postgres` and the AWS SDK need their
reactor's timers and I/O driver, so the host cannot simply poll them.

The workable pattern: the plugin spawns work onto **its own** runtime and
returns an `FfiFuture` that only awaits a channel. Such a future needs no tokio
context to poll, so the host drives it normally.

Consequence for cancellation: dropping the `FfiFuture` drops the receiver, but
the spawned task keeps running unless the plugin wires an `AbortHandle` into
the future's drop path. Cancellation is *available*, not automatic — the ABI
should require it of conforming plugins and the testkit should check it.

The same duplication applies to any process-global state in shared
dependencies. Concretely: rustls's default `CryptoProvider` is global **per
copy**, so the PostgreSQL and S3 plugins each install their own at load.

#### Use `abi_stable` for the rest of the boundary

`abi_stable` (0.11.3) does not make Rust's ABI stable — it generates and
verifies the same `repr(C)` boundary we would hand-roll. Adopt it anyway, for
three things we would otherwise implement worse:

- **Load-time layout checking.** `StableAbi` embeds a structural type layout
  and the loader diffs expected against actual, naming the field that
  diverged. Hand-rolled, we get a version integer that is only as reliable as
  our discipline about bumping it.
- **Prefix types.** Purpose-built vtable evolution: append entries in later
  versions, older loaders keep working, accessors return `Option` for fields
  past the last known one. This replaces reserving spare vtable slots and
  hoping.
- **Allocator-safe owned types.** `RVec`/`RBox` carry the originating library's
  destructor in their own vtable, so the host can drop a buffer the plugin
  allocated. This removes paired alloc/free — and with it a whole class of
  use-after-free — from the design entirely.

`#[sabi_trait]` additionally lets the list cursor stay a trait object rather
than three loose function pointers.

Costs, accepted: `abi_stable` becomes a version-pinned dependency that every
third-party plugin must match (at 0.x, the minor version is the compatibility
boundary), and it gives nothing for async, which is why `async-ffi` covers that
half. It is also effectively Rust-to-Rust: `RVec`'s layout is not something a C
or Zig author would target. **This is the decision to revisit if we ever want
non-Rust backend authors** — that case wants a plain C ABI with a
cbindgen-generated header instead.

Expect one repo-specific friction: `StableAbi` is an unsafe trait, so its
derive emits `unsafe impl`, and `forbid(unsafe_code)` applies to macro-expanded
code. `corium-store-abi` will need to opt out of `[workspace.lints]` alongside
the loader.

#### Remaining boundary details

- **Listing is a cursor.** `BlobIdStream` cannot cross the boundary, so the ABI
  exposes `list_open` / `list_next` (batched) / `list_close`, and the host
  adapts that into the existing stream type.
- **Logging crosses via a callback.** `tracing` spans do not survive the
  boundary, so the host installs a log-sink function pointer at open time and
  the plugin emits structured events through it. Without this, plugin failures
  are invisible in transactor logs.

Configuration crosses as a JSON object (`kind` + `config`), which is
human-writable for the CLI (`--store s3:{"bucket":...}`), transportable on the
wire, and does not require third parties to depend on our protobuf schema.
Config carries credentials, so `Debug` must redact it exactly as
`DiscoveredStoreSpec` does today.

### 4. `corium-store-plugin` — the loader

A new host crate wrapping `abi_stable`'s `RootModule` loading. Together with
`corium-store-abi` it opts out of `[workspace.lints]`, following the precedent
already set by `corium-wasm/Cargo.toml`. These two crates are the only ones
that relax `unsafe_code`.

Behaviour:

- Verifies the ABI version and the `abi_stable` type layout before calling
  anything; refuses mismatches with a message naming both versions and, from
  the layout diff, the field that diverged.
- **Never unloads.** A `Library` dropped while its allocations, threads, or
  TLS destructors are live is the classic use-after-free in this pattern.
  Loaded libraries leak deliberately, for the process lifetime.
- Search order: explicit path from config/CLI → `CORIUM_STORE_PLUGINS`
  (path-separator-delimited files or directories) → a platform plugin
  directory. Never the current working directory.

**Security:** loading a plugin is arbitrary code execution in the host process,
and the host hands it storage credentials. Plugin directories must be
trust-controlled like any other library path, and the transactor should require
an explicit opt-in flag rather than scanning by default. This belongs in
`docs/operations.md`.

### 5. Wire protocol

`pb.StorageConnection`'s `oneof` is closed. Add an open variant:

```protobuf
message PluginStorage {
  string kind = 1;         // "acme-gcs"
  string config_json = 2;  // backend-defined, may carry read-only credentials
}
```

Existing variants stay for compatibility; the five known backends keep their
typed messages and are translated into `{kind, config}` at the edge, so old and
new clients interoperate. A client that receives a `kind` it has no plugin for
returns the existing `StorageConnectionError::Unsupported`, now naming the
missing plugin and how to install it.

### 6. Client packaging — the payoff

**Python.** `corium` becomes the only wheel containing the engine, and gains
the plugin loader. `corium-turso` and friends shrink to a single `.so` plus a
few lines of Python, advertised through an entry point:

```toml
[project.entry-points."corium.store_plugins"]
turso = "corium_turso:plugin_path"
```

At import, `_api.py` resolves the entry points and passes the paths to the
native module, which loads them. Two backends installed means two small
libraries, not two engines. The multi-module import fallback and the
"pick the module whose `_storage_backends()` claims this kind" dance in
`_api.py` (~50 lines) both disappear, as does the per-backend distribution
matrix in `.github/workflows/python-wheels.yml`.

**Java.** The same `.so` files ship in a per-platform jar, are extracted to a
cache directory, and are registered by path. No JNI-side backend variants.

**Transactor / CLI.** Unchanged by default: keeps statically linking whichever
backends its features enable, now via the split crates. `--store-plugin
<path>` adds one at runtime.

### 7. Conformance kit

Promote `crates/corium-store/tests/store.rs` into a published
`corium-store-testkit` crate so third-party backends can run the same suite —
this is the trait-level contract testing ADR-0007 already promised. Add
`corium store verify <kind> <config>` to run the kit against a live plugin, so
"is my backend correct?" is answerable without a Rust test harness.

## Consequences

- Encryption stays host-side: `EncryptedBlobStore` wraps the plugin store, so
  plugin code never sees plaintext or key material. Worth stating explicitly
  in the plugin guide.
- One extra copy per blob read (plugin `RVec` → host `Vec`). Segments are
  megabyte-scale and every real backend is network- or disk-bound, so this is
  noise. A borrowed-buffer fast path can be added later; `abi_stable` prefix
  types make that a non-breaking addition.
- Two delivery paths (rlib and cdylib) are maintained and tested indefinitely.
  This is by design, not debt: iOS statically links Turso, wasm keeps
  memory/filesystem, and single-binary transactor deploys stay first-class.
  CI must cover both, so the conformance kit runs against each backend twice —
  once linked, once loaded.
- Each loaded plugin brings its own copy of tokio, and of rustls where the
  backend needs TLS. Larger RSS, duplicated thread pools, and per-copy global
  state that each plugin must initialize itself.
- Version skew becomes a real failure mode, across two axes: our ABI version
  and the `abi_stable` version plugins were built against. Mitigations: the
  layout check at load, and surfacing loaded plugins and their versions in
  `corium doctor` / transactor startup logs.
- Debuggability across the boundary is worse: no unified backtraces, and
  tracing context only crosses through the log callback.

## Phasing

Each phase is shippable on its own.

0. **Extract and de-enum.** Move the three backends into their own crates;
   introduce the registry and `Arc<dyn FullStore>`; delete the `#[cfg]` fan-out
   in `backend.rs` and `discovery.rs`. No ABI, no behaviour change. Unlocks
   compile-time third-party backends immediately.
1. **ABI and loader.** `corium-store-abi` (`abi_stable` + `async-ffi`) +
   `corium-store-plugin`; ship `corium-store-turso` as the first `cdylib`
   (smallest driver, no cloud credentials in the test path); add
   `PluginStorage` to the proto; add `--store-plugin` to the CLI. Prove the
   plugin-owned-runtime pattern here — Turso is also the backend iOS statically
   links, so it exercises both delivery paths from the start.
2. **Client packaging.** Flip the Python wheels; delete the artifact-selection
   logic; collapse the wheel matrix. Java client consumes the same plugins from
   the start.
3. **Open the door.** Publish `corium-store-abi` and `corium-store-testkit`,
   write the third-party backend guide, add `corium store verify`, and record
   an ADR superseding the relevant part of ADR-0007.
