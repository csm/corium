# Scenario integration tests

This suite runs the complete scenario list against each durable/shared storage
backend: filesystem, Turso, PostgreSQL, and S3 through MinIO. Each backend gets
isolated database names, a disposable transactor, a storage-aware peer server,
and a storage-aware PostgreSQL-wire server. It contains these scenarios:

1. server setup
2. authz initialization
3. encrypted database initialization
4. attribute protection classes
5. storage-key rotation
6. schema updates
7. embedded/local peer access, including direct storage bootstrap
8. Python peer-server client access
9. Java peer-server client access
10. Rust peer-server client access
11. PostgreSQL-wire client access

The attribute-protection scenario writes through a key-holding embedded peer
and verifies keyed reads plus the `redact`, `hide`, and `error` missing-key
policies through a keyless peer. Storage encryption initialization and key
rotation run once per backend. The schema-update scenario exercises the shipped
plan-first CLI and verifies its expected additive change. Every scenario is an
independent report boundary, so a failure does not prevent later scenarios or
backends from running.

Build the CLI and install the Python client before running locally:

```sh
cargo build -p corium-cli --features postgres,turso,s3
python3 -m pip install -e clients/python
python3 tests/scenarios/run.py
```

Filesystem and Turso need no external service. PostgreSQL and S3 are included
in the report but marked `SKIP` unless their prerequisites are configured:

```sh
export CORIUM_TEST_POSTGRES_URL='postgresql://postgres:postgres@127.0.0.1:5432/corium_test?sslmode=disable'
export CORIUM_TEST_S3_BUCKET=corium-scenarios
export AWS_ENDPOINT_URL=http://127.0.0.1:9000
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
```

The S3 bucket must already exist. Use repeatable `--storage` arguments to run a
subset, for example `--storage fs --storage turso`.

Reports and service logs are written to `artifacts/scenarios/`. The runner
always exits with status zero after writing both `scenario-report.md` and
`scenario-report.json`; scenario status is report data, not a build gate.

GitHub Actions runs the suite only through manual dispatch for now. A release
trigger can be added after the scenarios and their expected outcomes settle.
