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

## Runtime and cancellation

A dynamic driver owns its async runtime. It must spawn I/O work onto that
runtime and return an `async-ffi` future that only awaits a channel. It must
also tie an abort handle to the returned future so dropping the host future
cancels the spawned task. The Turso plugin's `AbortOnDrop` adapter demonstrates
the required pattern. Panics in future drop glue or waker callbacks can abort
the process, so treat every ABI implementation as panic-free production code.

Each library has its own process globals. Drivers using rustls must install a
crypto provider inside their own library before starting TLS work.

## Data and security boundary

Configuration crosses the boundary as JSON and can contain credentials. Its
Rust debug representation is redacted, but plugin authors must also avoid
logging secrets. ABI-owned strings and vectors use `abi_stable` containers so
they are freed by their originating allocator.

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
