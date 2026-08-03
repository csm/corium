# Scenario integration tests

This suite exercises a disposable transactor and peer server through operator
commands and the Python, Java, and Rust clients. It currently contains these
scenarios:

1. server setup
2. authz initialization
3. encrypted database initialization
4. attribute protection classes
5. storage-key rotation
6. schema updates
7. Python client access
8. Java client access
9. Rust client access

The attribute-protection scenario exercises protected writes, keyed reads,
and keyless redaction through the real engine. The schema-update scenario
exercises the shipped plan-first CLI and verifies its expected additive change.
Every scenario is isolated by the runner, so a failure does not prevent later
scenarios from running.

Build the CLI and install the Python client before running locally:

```sh
cargo build -p corium-cli
python3 -m pip install -e clients/python
python3 tests/scenarios/run.py
```

Reports and service logs are written to `artifacts/scenarios/`. The runner
always exits with status zero after writing both `scenario-report.md` and
`scenario-report.json`; scenario status is report data, not a build gate.

GitHub Actions runs the suite only through manual dispatch for now. A release
trigger can be added after the scenarios and their expected outcomes settle.
