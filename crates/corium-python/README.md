# corium-python

`corium-python` is the deliberately small PyO3 adapter between the public
Python package in `clients/python` and the runtime-neutral `corium-ffi`
facade.

It owns no database semantics. The crate converts Python boundary values to
Corium's composite encoding, adapts facade futures to `asyncio`, maps facade
errors to the public Python exception hierarchy, and exposes opaque remote
peer/database backends to the pure-Python API.

Build the mixed Python/Rust package from `clients/python`:

```shell
maturin develop
```
