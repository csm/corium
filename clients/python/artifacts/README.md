# Direct-storage plugin packaging

This repository does not currently build or publish official Python wrapper
packages for the Turso, PostgreSQL, or S3 plugins. The base `corium` package
supports filesystem direct storage. Use remote peer or replay mode for the
other storage backends.

The Python loader supports third-party and internal wrapper packages. A
wrapper package contains a storage shared library and a Python path provider.
It advertises that provider through the `corium.store_plugins` entry-point
group. The provider returns the installed path of the shared library.

The wrapper package is separate from the Rust plugin build. For example, this
command builds the Turso shared library:

```shell
cargo build -p corium-store-turso --release
```

This repository does not provide the wrapper project that `maturin` requires.
The `corium.available_storage_backends()` function reports all registered
backends. The loader rejects duplicate kinds and incompatible ABI layouts.
