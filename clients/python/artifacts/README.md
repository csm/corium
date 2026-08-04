# Optional direct-storage plugins

Driver distributions no longer contain a Corium Python extension or engine.
They contain one storage shared library plus a Python function that returns
its installed path through the `corium.store_plugins` entry-point group:

| Distribution | Plugin backend |
|---|---|
| `corium-turso` | Turso |
| `corium-postgres` | PostgreSQL |
| `corium-s3` | S3 |

Install the base `corium` distribution and any plugins you need. The base
package discovers their entry points and passes each library path to its native
loader. Installation and removal do not overwrite the base extension.

Build an artifact from its directory:

```shell
maturin build
```

`corium.available_storage_backends()` reports the registered union. The loader
rejects duplicate kinds and incompatible ABI or type layouts at import time.
