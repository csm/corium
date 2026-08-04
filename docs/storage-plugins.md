# Storage plugin guide

Corium storage plugins are Rust `cdylib` libraries that implement the v1
contract in `corium-store-abi`. The host validates the ABI generation and
`abi_stable` type layout, registers each factory returned by the root module,
and deliberately retains the library for process lifetime.

Use `corium-store-turso` as the reference implementation. A plugin must:

- build as both `rlib` and `cdylib` when it supports static and dynamic use;
- return one or more stable, lowercase backend kinds;
- parse the redacted JSON object passed to `open` and `open_existing`;
- implement immutable, content-addressed blobs and CAS-fenced roots;
- expose blob listing through the batched ABI cursor;
- declare its transaction-log placement and direct-access capability;
- never unwind across the ABI boundary.

Build a driver package by itself to produce its loadable `cdylib`, for example:

```sh
cargo build -p corium-store-turso --release
```

The bundled hosts enable the driver's `static-link` feature when they compile
it in. That feature suppresses the common dynamic-loader symbol, which lets one
host link multiple built-in drivers. Do not enable `static-link` when producing
a loadable plugin library.

## Runtime and cancellation

A dynamic driver owns its async runtime. It must spawn I/O work onto that
runtime and return an `async-ffi` future that only awaits a channel. It must
also tie an abort handle to the returned future so dropping the host future
cancels the spawned task. Use the shared helpers in
`corium_store_plugin::export` for this pattern. Panics in future drop glue or
waker callbacks can abort the process. Thus, ABI implementations must not
panic.

Each library has its own process globals. Drivers using rustls must install a
crypto provider inside their own library before starting TLS work.

## Data and security boundary

Configuration crosses the boundary as JSON and can contain credentials. Its
Rust debug representation is redacted, but plugin authors must also avoid
logging secrets. ABI-owned strings and vectors use `abi_stable` containers so
they are freed by their originating allocator.

The transactor keeps primary and read-only plugin configurations separate.
Configure discovery with `--plugin-read-only-config`,
`CORIUM_PLUGIN_READ_ONLY_CONFIG`, or `:plugin-read-only-config` in the EDN
configuration. Corium returns only this read-only configuration from
`GetStorageInfo`. If it is absent, discovery fails for the plugin store.

Encryption remains above the storage interface. The engine wraps a plugin
store with `EncryptedBlobStore`, so the driver receives ciphertext and never
receives storage-key material.

Loading a plugin is arbitrary native code execution. Operators must install
plugins only in trust-controlled directories and pass explicit paths. Corium
never searches the current working directory.

## Verification

Rust implementations can call `corium_store_testkit::verify` with an opened
`Arc<dyn FullStore>`. Operators can verify the dynamically loaded form:

```sh
corium store verify acme-gcs '{"bucket":"verification"}' \
  --store-plugin /opt/corium/plugins/libacme_gcs.so
```

Use a disposable namespace. The verifier creates uniquely named blobs and
roots, exercises idempotence, listing, CAS fencing, and deletion, and removes
the objects after a successful run.
