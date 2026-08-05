#!/usr/bin/env python3
"""Run release-oriented integration scenarios and always emit a report.

Scenario failures are observations, not harness failures: every registered
scenario runs and this program exits zero after writing Markdown and JSON.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import signal
import socket
import subprocess
import sys
import tempfile
import time
import traceback
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Mapping, Sequence


REPOSITORY = Path(__file__).resolve().parents[2]
DEFAULT_TIMEOUT_SECONDS = 120


class ScenarioFailure(RuntimeError):
    """A scenario did not establish its intended outcome."""


class StorageUnavailable(RuntimeError):
    """A selected storage backend lacks an external prerequisite."""


class ScenarioLimitation(RuntimeError):
    """A scenario verified a known product limitation."""


@dataclass
class ScenarioResult:
    storage: str
    name: str
    status: str
    duration_seconds: float
    detail: str


class ManagedProcess:
    """A child service whose output is retained with the scenario report."""

    def __init__(
        self,
        name: str,
        command: Sequence[str],
        log_path: Path,
        env: Mapping[str, str] | None = None,
    ) -> None:
        self.name = name
        self.command = list(command)
        self.log_path = log_path
        log_path.parent.mkdir(parents=True, exist_ok=True)
        self._log = log_path.open("w", encoding="utf-8")
        self.process = subprocess.Popen(
            self.command,
            cwd=REPOSITORY,
            env=dict(env) if env is not None else None,
            stdout=self._log,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
        )

    def stop(self) -> None:
        if self.process.poll() is None:
            try:
                os.killpg(self.process.pid, signal.SIGTERM)
                self.process.wait(timeout=10)
            except (ProcessLookupError, subprocess.TimeoutExpired):
                if self.process.poll() is None:
                    os.killpg(self.process.pid, signal.SIGKILL)
                    self.process.wait(timeout=5)
        self._log.close()

    def failure_detail(self) -> str:
        self._log.flush()
        output = self.log_path.read_text(encoding="utf-8", errors="replace")
        return (
            f"{self.name} exited with {self.process.poll()}\n"
            f"command: {format_command(self.command)}\n"
            f"log: {self.log_path}\n{output[-4000:]}"
        )


@dataclass
class Context:
    storage: str
    storage_arguments: list[str]
    process_env: dict[str, str]
    corium_bin: Path
    report_dir: Path
    work_dir: Path
    transactor_port: int
    peer_port: int
    pgwire_port: int
    storage_key_uri: str
    class_key_uri: str
    people_database: str
    encrypted_database: str
    protected_database: str
    authz_database: str
    transactor: ManagedProcess | None = None
    peer: ManagedProcess | None = None
    pgwire: ManagedProcess | None = None
    server_ready: bool = False

    @property
    def transactor_endpoint(self) -> str:
        return f"http://127.0.0.1:{self.transactor_port}"

    @property
    def peer_endpoint(self) -> str:
        return f"http://127.0.0.1:{self.peer_port}"

    def stop_services(self) -> None:
        if self.pgwire is not None:
            self.pgwire.stop()
        if self.peer is not None:
            self.peer.stop()
        if self.transactor is not None:
            self.transactor.stop()


Scenario = tuple[str, Callable[[Context], str]]


@dataclass(frozen=True)
class StorageConfig:
    """Arguments and environment needed by one storage backend."""

    name: str
    arguments: tuple[str, ...]
    environment: Mapping[str, str]


def format_command(command: Sequence[str]) -> str:
    return shlex.join(str(part) for part in command)


def run_command(
    command: Sequence[str],
    *,
    env: dict[str, str] | None = None,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
) -> str:
    completed = subprocess.run(
        list(command),
        cwd=REPOSITORY,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=timeout,
        check=False,
    )
    output = completed.stdout.strip()
    if completed.returncode != 0:
        raise ScenarioFailure(
            f"command exited with {completed.returncode}: {format_command(command)}"
            + (f"\n{output[-6000:]}" if output else "")
        )
    return output[-6000:]


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def required_environment(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise StorageUnavailable(f"environment variable {name} is required")
    return value


def storage_config(name: str, work_dir: Path, run_id: str) -> StorageConfig:
    """Resolve one selected backend without provisioning external services."""

    environment = os.environ.copy()
    if name == "fs":
        return StorageConfig(name, ("--store", "fs"), environment)
    if name == "turso":
        return StorageConfig(
            name,
            ("--store", "turso", "--turso-path", str(work_dir / "store.db")),
            environment,
        )
    if name == "postgres":
        url = required_environment("CORIUM_TEST_POSTGRES_URL")
        environment["CORIUM_POSTGRES_READ_ONLY_URL"] = url
        return StorageConfig(
            name,
            ("--store", "postgres", "--postgres-url", url),
            environment,
        )
    if name == "s3":
        bucket = required_environment("CORIUM_TEST_S3_BUCKET")
        endpoint = required_environment("AWS_ENDPOINT_URL")
        region = required_environment("AWS_REGION")
        access_key = required_environment("AWS_ACCESS_KEY_ID")
        secret_key = required_environment("AWS_SECRET_ACCESS_KEY")
        environment["CORIUM_S3_READ_ONLY_ACCESS_KEY_ID"] = access_key
        environment["CORIUM_S3_READ_ONLY_SECRET_ACCESS_KEY"] = secret_key
        prefix = f"scenario/{run_id}/"
        return StorageConfig(
            name,
            (
                "--store",
                "s3",
                "--s3-bucket",
                bucket,
                "--s3-prefix",
                prefix,
                "--s3-region",
                region,
                "--s3-endpoint-url",
                endpoint,
            ),
            environment,
        )
    raise ValueError(f"unknown storage backend {name!r}")


def unique_ports(count: int) -> list[int]:
    ports: list[int] = []
    while len(ports) < count:
        port = free_port()
        if port not in ports:
            ports.append(port)
    return ports


def wait_for_port(process: ManagedProcess, port: int, timeout: int = 30) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.process.poll() is not None:
            raise ScenarioFailure(process.failure_detail())
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.25):
                return
        except OSError:
            time.sleep(0.1)
    raise ScenarioFailure(
        f"{process.name} did not listen on 127.0.0.1:{port} within {timeout}s\n"
        + process.failure_detail()
    )


def require_server(context: Context) -> None:
    if not context.server_ready:
        raise ScenarioFailure("server setup did not complete; scenario could not connect")
    for process in (context.transactor, context.peer, context.pgwire):
        if process is None or process.process.poll() is not None:
            raise ScenarioFailure("a server process stopped before this scenario ran")


def corium(context: Context, *arguments: str, timeout: int = 60) -> str:
    return run_command(
        [str(context.corium_bin), *arguments],
        env=context.process_env,
        timeout=timeout,
    )


def server_setup(context: Context) -> str:
    if not context.corium_bin.is_file():
        raise ScenarioFailure(
            f"Corium binary not found at {context.corium_bin}; build it with "
            "`cargo build -p corium-cli` or pass --corium-bin"
        )

    context.transactor = ManagedProcess(
        "transactor",
        [
            str(context.corium_bin),
            "transactor",
            *context.storage_arguments,
            "--data-dir",
            str(context.work_dir / "data"),
            "--listen",
            f"127.0.0.1:{context.transactor_port}",
            "--gc-interval",
            "off",
            "--storage-key",
            context.storage_key_uri,
            "--storage-key",
            context.class_key_uri,
        ],
        context.report_dir / "logs" / context.storage / "transactor.log",
        context.process_env,
    )
    wait_for_port(context.transactor, context.transactor_port)

    create_output = corium(
        context,
        "db",
        "create",
        context.people_database,
        "--schema",
        str(REPOSITORY / "tests/scenarios/schema.toml"),
        "--transactor",
        context.transactor_endpoint,
    )

    context.peer = ManagedProcess(
        "peer server",
        [
            str(context.corium_bin),
            "peer-server",
            "--db",
            context.people_database,
            "--listen",
            f"127.0.0.1:{context.peer_port}",
            "--transactor",
            context.transactor_endpoint,
            "--peer-bootstrap",
        ],
        context.report_dir / "logs" / context.storage / "peer-server.log",
        context.process_env,
    )
    wait_for_port(context.peer, context.peer_port)

    context.pgwire = ManagedProcess(
        "postgres wire server",
        [
            str(context.corium_bin),
            "postgres-server",
            "--database",
            context.people_database,
            "--listen",
            f"127.0.0.1:{context.pgwire_port}",
            "--transactor",
            context.transactor_endpoint,
            "--peer-bootstrap",
        ],
        context.report_dir / "logs" / context.storage / "postgres-server.log",
        context.process_env,
    )
    wait_for_port(context.pgwire, context.pgwire_port)
    stats_output = corium(
        context,
        "db",
        "stats",
        context.people_database,
        "--transactor",
        context.transactor_endpoint,
    )
    context.server_ready = True
    return (
        f"{context.storage} transactor, storage-aware peer server, and pgwire "
        f"server are ready.\n{create_output}\n{stats_output}"
    )


def authz_init(context: Context) -> str:
    require_server(context)
    initialized = corium(
        context,
        "authz",
        "init",
        "--db",
        context.authz_database,
        "--transactor",
        context.transactor_endpoint,
    )
    status = corium(
        context,
        "authz",
        "status",
        "--db",
        context.authz_database,
        "--transactor",
        context.transactor_endpoint,
    )
    return f"{initialized}\n{status}"


def encryption_init(context: Context) -> str:
    require_server(context)
    created = corium(
        context,
        "db",
        "create",
        context.encrypted_database,
        "--schema",
        str(REPOSITORY / "tests/scenarios/schema.toml"),
        "--storage-key",
        context.storage_key_uri,
        "--transactor",
        context.transactor_endpoint,
    )
    committed = run_command(
        [
            "cargo",
            "run",
            "--quiet",
            "-p",
            "corium-client",
            "--example",
            "local_scenario",
            "--",
            "peer",
            context.transactor_endpoint,
            context.encrypted_database,
        ],
        env=context.process_env,
        timeout=300,
    )
    status = corium(
        context,
        "keys",
        "status",
        context.encrypted_database,
        "--transactor",
        context.transactor_endpoint,
    )
    if "records-sealed 1" not in status:
        raise ScenarioFailure(
            f"encrypted transaction did not consume a sealed log record\n{status}"
        )
    return f"{created}\n{committed}\n{status}"


def attribute_protection_classes(context: Context) -> str:
    """Exercise all missing-key policies against this backend's transactor."""

    require_server(context)
    schema = context.work_dir / "protected-schema.edn"
    schema.write_text(
        f"""
{{:db.protect/ident :protect/redacting
 :db.protect/key \"{context.class_key_uri}\"}}
{{:db.protect/ident :protect/hiding
 :db.protect/key \"{context.class_key_uri}\"
 :db.protect/on-missing-key :db.protect.missing/hide}}
{{:db.protect/ident :protect/failing
 :db.protect/key \"{context.class_key_uri}\"
 :db.protect/on-missing-key :db.protect.missing/error}}
{{:db/ident :person/name
 :db/valueType :db.type/string
 :db/cardinality :db.cardinality/one
 :db/unique :db.unique/identity}}
{{:db/ident :person/ssn
 :db/valueType :db.type/string
 :db/cardinality :db.cardinality/one
 :db/protection :protect/redacting}}
{{:db/ident :person/mood
 :db/valueType :db.type/keyword
 :db/cardinality :db.cardinality/one
 :db/protection :protect/hiding}}
{{:db/ident :person/salary
 :db/valueType :db.type/long
 :db/cardinality :db.cardinality/one
 :db/protection :protect/failing}}
""".strip()
        + "\n",
        encoding="utf-8",
    )
    created = corium(
        context,
        "db",
        "create",
        context.protected_database,
        "--schema",
        str(schema),
        "--transactor",
        context.transactor_endpoint,
    )
    exercised = run_command(
        [
            "cargo",
            "run",
            "--quiet",
            "-p",
            "corium-client",
            "--example",
            "local_scenario",
            "--",
            "protection",
            context.transactor_endpoint,
            context.protected_database,
            context.class_key_uri,
        ],
        env=context.process_env,
        timeout=300,
    )
    return f"{created}\n{exercised}"


def local_peer_access(context: Context) -> str:
    """Exercise both embedded-peer API and direct shared-storage bootstrap."""

    require_server(context)
    embedded = run_command(
        [
            "cargo",
            "run",
            "--quiet",
            "-p",
            "corium-client",
            "--example",
            "local_scenario",
            "--",
            "peer",
            context.transactor_endpoint,
            context.people_database,
        ],
        env=context.process_env,
        timeout=300,
    )
    direct = corium(
        context,
        "sql",
        context.people_database,
        "--command",
        "SELECT COUNT(*) FROM corium.person",
        "--transactor",
        context.transactor_endpoint,
        "--peer-bootstrap",
        timeout=180,
    )
    return f"{embedded}\nStorage-aware local SQL peer:\n{direct}"


def pgwire_client_access(context: Context) -> str:
    require_server(context)
    return run_command(
        [
            sys.executable,
            str(REPOSITORY / "tests/scenarios/pgwire_client.py"),
            "127.0.0.1",
            str(context.pgwire_port),
            context.people_database,
        ],
        env=context.process_env,
        timeout=180,
    )


def security_test(test_name: str) -> str:
    """Run one independently isolated release-security scenario."""

    return run_command(
        [
            "cargo",
            "test",
            "--quiet",
            "-p",
            "corium-cli",
            "--features",
            "postgres,turso,s3,oidc",
            "--test",
            "scenario_security",
            test_name,
            "--",
            "--exact",
            "--nocapture",
        ],
        timeout=300,
    )


def direct_peer_security(context: Context) -> str:
    require_server(context)
    return security_test("direct_peer_real_jwt_authz_and_protected_access")


def peer_server_security(context: Context) -> str:
    require_server(context)
    return security_test("peer_server_real_jwt_authz_and_per_principal_class_keys")


def pgwire_security(context: Context) -> str:
    require_server(context)
    return security_test("pgwire_maps_sql_clients_onto_corium_principals")


def key_rotation(context: Context) -> str:
    require_server(context)
    rotated = corium(
        context,
        "keys",
        "rotate",
        context.encrypted_database,
        "--transactor",
        context.transactor_endpoint,
    )
    status = corium(
        context,
        "keys",
        "status",
        context.encrypted_database,
        "--transactor",
        context.transactor_endpoint,
    )
    if "storage-key-epoch 2" not in rotated and "active-epoch 2" not in status:
        raise ScenarioFailure(f"rotation did not report epoch 2\n{rotated}\n{status}")
    return f"{rotated}\n{status}"


def schema_updates(context: Context) -> str:
    """Plan, apply, and verify an additive schema update."""

    require_server(context)
    schema_path = str(REPOSITORY / "tests/scenarios/schema-updated.toml")
    plan_output = corium(
        context,
        "schema",
        "update",
        context.people_database,
        "--schema",
        schema_path,
        "--json",
        "--transactor",
        context.transactor_endpoint,
    )
    try:
        plan = json.loads(plan_output)
    except json.JSONDecodeError as error:
        raise ScenarioFailure(f"schema plan was not valid JSON\n{plan_output}") from error
    expected_change = any(
        change.get("ident") == ":person/email"
        and change.get("execution_class") == "additive"
        for change in plan.get("changes", [])
    )
    plan_digest = plan.get("plan_digest")
    if not plan.get("has_changes") or not expected_change:
        raise ScenarioFailure(
            f"schema plan omitted the expected additive change\n{plan_output}"
        )
    if not isinstance(plan_digest, str) or not plan_digest.startswith("sha256:"):
        raise ScenarioFailure(f"schema plan omitted its review digest\n{plan_output}")

    applied_output = corium(
        context,
        "schema",
        "update",
        context.people_database,
        "--schema",
        schema_path,
        "--apply",
        "--plan",
        plan_digest,
        "--json",
        "--transactor",
        context.transactor_endpoint,
    )
    try:
        applied = json.loads(applied_output).get("applied", {})
    except json.JSONDecodeError as error:
        raise ScenarioFailure(
            f"schema apply result was not valid JSON\n{applied_output}"
        ) from error
    if not applied.get("changed") or ":person/email" not in applied.get(
        "installed", []
    ):
        raise ScenarioFailure(
            f"schema apply did not install :person/email\n{applied_output}"
        )

    converged_output = corium(
        context,
        "schema",
        "update",
        context.people_database,
        "--schema",
        schema_path,
        "--json",
        "--transactor",
        context.transactor_endpoint,
    )
    try:
        converged = json.loads(converged_output)
    except json.JSONDecodeError as error:
        raise ScenarioFailure(
            f"post-apply schema plan was not valid JSON\n{converged_output}"
        ) from error
    if converged.get("has_changes") or converged.get("changes"):
        raise ScenarioFailure(
            f"schema still reports changes after apply\n{converged_output}"
        )

    return (
        f"Reviewed plan {plan_digest}.\n"
        f"Applied: {applied_output}\n"
        f"Verified converged plan: {converged_output}"
    )


def python_client_access(context: Context) -> str:
    require_server(context)
    return run_command(
        [
            sys.executable,
            str(REPOSITORY / "tests/scenarios/python_client.py"),
            context.peer_endpoint,
            context.people_database,
        ],
        env=context.process_env,
        timeout=180,
    )


def java_client_access(context: Context) -> str:
    require_server(context)
    env = context.process_env.copy()
    env.update(
        {
            "CORIUM_SCENARIO_PEER_ENDPOINT": context.peer_endpoint,
            "CORIUM_SCENARIO_DATABASE": context.people_database,
        }
    )
    remote = run_command(
        [
            "mvn",
            "-f",
            str(REPOSITORY / "clients/java/pom.xml"),
            "--batch-mode",
            "-pl",
            "client",
            "-Dtest=LiveScenarioTest",
            "test",
        ],
        env=env,
        timeout=300,
    )
    # The in-process peer needs the JNI engine, which the scenario workflow
    # builds beside the CLI. Skip it, rather than fail, when it is absent.
    engine = REPOSITORY / "target/debug/libcorium_jni.so"
    if not engine.is_file():
        engine = REPOSITORY / "target/debug/libcorium_jni.dylib"
    if not engine.is_file():
        return f"{remote}\nIn-process peer skipped: build corium-jni to run it"
    env["CORIUM_SCENARIO_TRANSACTOR_ENDPOINT"] = context.transactor_endpoint
    local = run_command(
        [
            "mvn",
            "-f",
            str(REPOSITORY / "clients/java/pom.xml"),
            "--batch-mode",
            "-pl",
            "client",
            "-Dtest=LiveLocalPeerScenarioTest",
            "test",
        ],
        env=env,
        timeout=600,
    )
    return f"{remote}\nIn-process Java peer:\n{local}"


def rust_client_access(context: Context) -> str:
    require_server(context)
    return run_command(
        [
            "cargo",
            "run",
            "--quiet",
            "-p",
            "corium-client",
            "--example",
            "scenario",
            "--",
            context.peer_endpoint,
            context.people_database,
        ],
        env=context.process_env,
        timeout=300,
    )


SCENARIOS: list[Scenario] = [
    ("server setup", server_setup),
    ("authz init", authz_init),
    ("encryption init", encryption_init),
    ("attribute protection classes", attribute_protection_classes),
    ("direct peer JWT, authz, and protected access", direct_peer_security),
    ("peer server JWT, authz, and protected access", peer_server_security),
    ("pgwire authz and protected access", pgwire_security),
    ("key rotation", key_rotation),
    ("schema updates", schema_updates),
    ("local peer access", local_peer_access),
    ("python client access", python_client_access),
    ("java client access", java_client_access),
    ("rust client access", rust_client_access),
    ("pgwire client access", pgwire_client_access),
]


def run_scenarios(context: Context) -> list[ScenarioResult]:
    results: list[ScenarioResult] = []
    for name, scenario in SCENARIOS:
        print(f"\n=== [{context.storage}] {name} ===", flush=True)
        started = time.monotonic()
        try:
            detail = scenario(context).strip() or "completed"
            status = "PASS"
        except subprocess.TimeoutExpired as error:
            detail = f"timed out after {error.timeout}s: {format_command(error.cmd)}"
            status = "FAIL"
        except ScenarioLimitation as error:
            detail = str(error) or "known product limitation verified"
            status = "LIMITATION"
        except Exception as error:  # Every scenario is an isolation boundary.
            detail = str(error) or traceback.format_exc()
            status = "FAIL"
        duration = time.monotonic() - started
        result = ScenarioResult(
            storage=context.storage,
            name=name,
            status=status,
            duration_seconds=round(duration, 3),
            detail=detail,
        )
        results.append(result)
        print(f"{status} ({duration:.2f}s)\n{detail}", flush=True)
    return results


def markdown_report(results: Sequence[ScenarioResult], generated_at: str) -> str:
    passes = sum(result.status == "PASS" for result in results)
    skips = sum(result.status == "SKIP" for result in results)
    limitations = sum(result.status == "LIMITATION" for result in results)
    failures = sum(result.status == "FAIL" for result in results)
    lines = [
        "# Scenario integration report",
        "",
        f"Generated: {generated_at}",
        "",
        (
            f"Result: **{passes} passed, {limitations} known limitations, "
            f"{failures} failed, {skips} skipped, {len(results)} total**. "
            "Failures are informational and do not change the runner exit code."
        ),
        "",
        "| Storage | Scenario | Status | Duration |",
        "|---|---|---:|---:|",
    ]
    for result in results:
        icon = {
            "PASS": "✅",
            "LIMITATION": "⚠️",
            "FAIL": "❌",
            "SKIP": "⏭️",
        }[result.status]
        lines.append(
            f"| {result.storage} | {result.name} | {icon} {result.status} | "
            f"{result.duration_seconds:.3f}s |"
        )
    lines.extend(["", "## Details", ""])
    for result in results:
        icon = {
            "PASS": "✅",
            "LIMITATION": "⚠️",
            "FAIL": "❌",
            "SKIP": "⏭️",
        }[result.status]
        lines.extend(
            [
                f"### {icon} [{result.storage}] {result.name}",
                "",
                "```text",
                result.detail.replace("```", "` ` `"),
                "```",
                "",
            ]
        )
    return "\n".join(lines)


def write_reports(report_dir: Path, results: Sequence[ScenarioResult]) -> None:
    report_dir.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(timezone.utc).isoformat()
    (report_dir / "scenario-report.md").write_text(
        markdown_report(results, generated_at), encoding="utf-8"
    )
    payload = {
        "generated_at": generated_at,
        "non_blocking": True,
        "results": [asdict(result) for result in results],
    }
    (report_dir / "scenario-report.json").write_text(
        json.dumps(payload, indent=2) + "\n", encoding="utf-8"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--corium-bin",
        type=Path,
        default=REPOSITORY / "target/debug/corium",
        help="path to a previously built corium CLI binary",
    )
    parser.add_argument(
        "--report-dir",
        type=Path,
        default=REPOSITORY / "artifacts/scenarios",
        help="directory for Markdown, JSON, and service logs",
    )
    parser.add_argument(
        "--storage",
        action="append",
        choices=("fs", "turso", "postgres", "s3"),
        help="storage backend to exercise (repeatable; defaults to all four)",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="list scenario names without running them",
    )
    return parser.parse_args()


def unavailable_results(storage: str, detail: str) -> list[ScenarioResult]:
    return [
        ScenarioResult(
            storage=storage,
            name=name,
            status="SKIP",
            duration_seconds=0.0,
            detail=detail,
        )
        for name, _ in SCENARIOS
    ]


def context_for_storage(
    storage: str,
    root: Path,
    run_id: str,
    args: argparse.Namespace,
) -> Context:
    work_dir = root / storage
    work_dir.mkdir(parents=True)
    storage_key = work_dir / "storage.key"
    class_key = work_dir / "pii.key"
    storage_key.write_bytes(os.urandom(32))
    class_key.write_bytes(os.urandom(32))
    config = storage_config(storage, work_dir, f"{run_id}-{storage}")
    transactor_port, peer_port, pgwire_port = unique_ports(3)
    database_prefix = f"scenario_{run_id}_{storage}"
    return Context(
        storage=storage,
        storage_arguments=list(config.arguments),
        process_env=dict(config.environment),
        corium_bin=args.corium_bin.resolve(),
        report_dir=args.report_dir.resolve(),
        work_dir=work_dir,
        transactor_port=transactor_port,
        peer_port=peer_port,
        pgwire_port=pgwire_port,
        storage_key_uri=f"file:{storage_key}",
        class_key_uri=f"file:{class_key}",
        people_database=f"{database_prefix}_people",
        encrypted_database=f"{database_prefix}_encrypted",
        protected_database=f"{database_prefix}_protected",
        authz_database=f"{database_prefix}_authz",
    )


def main() -> int:
    args = parse_args()
    storages = args.storage or ["fs", "turso", "postgres", "s3"]
    if args.list:
        for storage in storages:
            for name, _ in SCENARIOS:
                print(f"[{storage}] {name}")
        return 0

    args.report_dir.mkdir(parents=True, exist_ok=True)
    results: list[ScenarioResult] = []
    try:
        with tempfile.TemporaryDirectory(prefix="corium-scenarios-") as temporary:
            work_root = Path(temporary)
            run_id = re.sub(r"[^a-zA-Z0-9]", "", work_root.name)[-16:].lower()
            for storage in storages:
                try:
                    context = context_for_storage(
                        storage, work_root, run_id, args
                    )
                except StorageUnavailable as error:
                    detail = f"{storage} prerequisites unavailable: {error}"
                    print(f"\n=== [{storage}] skipped ===\n{detail}", flush=True)
                    results.extend(unavailable_results(storage, detail))
                    continue
                try:
                    results.extend(run_scenarios(context))
                finally:
                    context.stop_services()
    except Exception:
        detail = "scenario harness could not initialize:\n" + traceback.format_exc()
        completed = {(result.storage, result.name) for result in results}
        for storage in storages:
            for name, _ in SCENARIOS:
                if (storage, name) not in completed:
                    results.append(
                        ScenarioResult(storage, name, "FAIL", 0.0, detail)
                    )
    write_reports(args.report_dir.resolve(), results)

    print(f"\nReport: {args.report_dir / 'scenario-report.md'}")
    print("Scenario failures are non-blocking; exiting 0.")
    return 0


if __name__ == "__main__":
    try:
        exit_code = main()
    except Exception:
        # A harness defect should still be visible in a report and should not
        # turn this observation workflow into a release gate.
        fallback_dir = REPOSITORY / "artifacts/scenarios"
        fallback = [
            ScenarioResult(
                "harness",
                "scenario harness",
                "FAIL",
                0.0,
                traceback.format_exc(),
            )
        ]
        write_reports(fallback_dir, fallback)
        traceback.print_exc()
        print(f"Fallback report: {fallback_dir / 'scenario-report.md'}")
        raise SystemExit(0)
    raise SystemExit(exit_code)
