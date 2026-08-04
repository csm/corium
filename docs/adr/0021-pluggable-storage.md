# ADR-0021: Runtime-pluggable storage backends

**Status:** Accepted (2026-08-03)

## Context

Compile-time storage features duplicated the native engine across client
artifacts and required every engine layer to enumerate every driver. External
authors could not add a backend without joining Corium's feature graph.

## Decision

Storage drivers live in separate `rlib`/`cdylib` crates and enter a
process-wide registry keyed by backend kind. Engine code uses redacted JSON
configuration and trait objects. Dynamic Rust plugins use the versioned
`corium-store-abi` contract, `abi_stable` layout checks, and `async-ffi`
futures. Plugins own their async runtimes and loaded libraries are never
unloaded.

Memory and filesystem remain in the core crate. Statically linked driver
crates remain supported. This decision supersedes ADR-0007 only where that ADR
treated later drivers as additions to the core crate; its blob/root contract
and initial backend choice remain in force.

## Consequences

- A backend can be installed without rebuilding the engine.
- Python uses one native engine and discovers driver libraries through entry
  points.
- ABI and dependency-version skew become explicit load-time failures.
- A loaded plugin is trusted native code and may receive storage credentials.
- Dynamic drivers duplicate runtime and TLS process-global state by design.
