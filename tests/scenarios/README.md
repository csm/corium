# Scenario integration tests

This suite exercises a disposable transactor and peer server through operator
commands and the Python, Java, and Rust clients. It currently contains these
scenarios:

1. server setup
2. authz initialization
3. encrypted database initialization
4. attribute protection classes
5. direct-peer JWT authentication, ReBAC authorization, and protected access
6. peer-server JWT authentication, ReBAC authorization, and protected access
7. pgwire authorization and protected access
8. storage-key rotation
9. schema updates
10. Python client access
11. Java client access
12. Rust client access

The attribute-protection scenario exercises protected writes, keyed reads,
and keyless redaction through the real engine. The security scenarios use
signed RS256 JWTs with issuer and audience validation, the self-hosted ReBAC
authorizer, and real direct-peer, peer-server, and PostgreSQL-wire clients.
Known product gaps have a separate `LIMITATION` status: they are verified and
prominent in the report without being confused with a harness failure. The
schema-update scenario exercises the shipped plan-first CLI and verifies its
expected additive change. Every scenario is isolated by the runner, so a
failure does not prevent later scenarios from running.

Build the CLI and install the Python client before running locally:

```sh
cargo build -p corium-cli --features oidc
python3 -m pip install -e clients/python
python3 tests/scenarios/run.py
```

Reports and service logs are written to `artifacts/scenarios/`. The runner
always exits with status zero after writing both `scenario-report.md` and
`scenario-report.json`; scenario status is report data, not a build gate.

GitHub Actions runs the suite only through manual dispatch for now. A release
trigger can be added after the scenarios and their expected outcomes settle.
