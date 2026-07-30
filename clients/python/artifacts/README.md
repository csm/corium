# Optional direct-storage artifacts

These are optional native extension distributions for the `corium` package:

| Distribution | Native feature |
|---|---|
| `corium` | filesystem only |
| `corium-turso` | Turso |
| `corium-postgres` | PostgreSQL |
| `corium-s3` | S3 |

Install the base `corium` distribution and at most one optional artifact. Each
artifact has its own extension-module name, depends on the common package, and
contains the native full-peer core plus one driver. It does not overwrite the
base extension, so installation and removal are safe. This keeps a
remote-only/base installation free of every optional storage dependency.

Build an artifact from its directory:

```shell
maturin build
```

The common package automatically selects an installed artifact and rejects
ambiguous installations. `corium.available_storage_backends()` verifies which
driver is active. Publishing the platform wheel matrix remains Phase 5 work.
