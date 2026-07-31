# corium-python

`corium-python` is the deliberately small PyO3 adapter between the public
Python package in `clients/python` and the runtime-neutral `corium-ffi`
facade.

It owns no database semantics. The crate converts Python boundary values to
Corium's composite encoding, adapts facade futures to `asyncio`, maps facade
errors to the public Python exception hierarchy, and exposes opaque local or
remote peer/database backends to the pure-Python API.

Build the mixed Python/Rust package from `clients/python`:

```shell
maturin develop
```

The base artifact includes filesystem direct-storage discovery. The
`clients/python/artifacts/{turso,postgres,s3}` projects build separately named
extension distributions with distinct module names and exactly one optional
native feature. They install alongside and depend on the common `corium`
package without replacing its base extension.
For local development, the features can also be selected directly:

```shell
maturin develop --features turso
maturin develop --features postgres
maturin develop --features s3
maturin develop --features all-storage
```

These features are independent and propagate only to `corium-ffi` and the
matching `corium-store` driver.
