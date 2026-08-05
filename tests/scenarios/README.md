# Scenario integration tests

This suite runs the complete scenario list against each durable/shared storage
backend: filesystem, Turso, PostgreSQL, and S3 through MinIO. Each backend gets
isolated database names, a disposable transactor, a storage-aware peer server,
and a storage-aware PostgreSQL-wire server. It contains these scenarios:

1. server setup
2. authz initialization
3. encrypted database initialization
4. attribute protection classes
5. direct-peer JWT authentication, ReBAC authorization, and protected access
6. peer-server JWT authentication, ReBAC authorization, and protected access
7. pgwire authorization and protected access
8. storage-key rotation
9. schema updates
10. embedded/local peer access, including direct storage bootstrap
11. Python peer-server client access
12. Java peer-server client access
13. Rust peer-server client access
14. PostgreSQL-wire client access

The attribute-protection scenario writes through a key-holding embedded peer
and verifies keyed reads plus the `redact`, `hide`, and `error` missing-key
policies through a keyless peer. The security scenarios use
signed RS256 JWTs with issuer and audience validation, the self-hosted ReBAC
authorizer, and real direct-peer, peer-server, and PostgreSQL-wire clients.
The peer-server and pgwire scenarios put two principals on one key-holding
server and check that each sees only what policy grants: Alice reads the
protected value as plaintext, Bob reads the same column of the same database
value redacted. The pgwire client authenticates with its own bearer token in
the password field, so a SQL session is a Corium principal.

Known product gaps have a separate `LIMITATION` status: they are verified and
prominent in the report without being confused with a harness failure. The
schema-update scenario reviews the plan digest, applies the expected additive
change, verifies the installed attribute, and re-plans to confirm convergence.
Storage encryption initialization and key rotation run once per backend. Every
scenario is an independent report boundary, so a failure does not prevent later
scenarios or backends from running.

Build the CLI and install the Python client before running locally:

```sh
cargo build -p corium-cli --features postgres,turso,s3,oidc
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
