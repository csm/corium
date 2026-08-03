#!/usr/bin/env python3
"""Run release-oriented integration scenarios and always emit a report.

Scenario failures are observations, not harness failures: every registered
scenario runs and this program exits zero after writing Markdown and JSON.
"""

from __future__ import annotations

import argparse
import json
import os
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
from typing import Callable, Sequence


REPOSITORY = Path(__file__).resolve().parents[2]
DEFAULT_TIMEOUT_SECONDS = 120


class ScenarioFailure(RuntimeError):
    """A scenario did not establish its intended outcome."""


@dataclass
class ScenarioResult:
    name: str
    status: str
    duration_seconds: float
    detail: str


class ManagedProcess:
    """A child service whose output is retained with the scenario report."""

    def __init__(self, name: str, command: Sequence[str], log_path: Path) -> None:
        self.name = name
        self.command = list(command)
        self.log_path = log_path
        log_path.parent.mkdir(parents=True, exist_ok=True)
        self._log = log_path.open("w", encoding="utf-8")
        self.process = subprocess.Popen(
            self.command,
            cwd=REPOSITORY,
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
    corium_bin: Path
    report_dir: Path
    work_dir: Path
    transactor_port: int
    peer_port: int
    storage_key_uri: str
    class_key_uri: str
    transactor: ManagedProcess | None = None
    peer: ManagedProcess | None = None
    server_ready: bool = False

    @property
    def transactor_endpoint(self) -> str:
        return f"http://127.0.0.1:{self.transactor_port}"

    @property
    def peer_endpoint(self) -> str:
        return f"http://127.0.0.1:{self.peer_port}"

    def stop_services(self) -> None:
        if self.peer is not None:
            self.peer.stop()
        if self.transactor is not None:
            self.transactor.stop()


Scenario = tuple[str, Callable[[Context], str]]


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
    for process in (context.transactor, context.peer):
        if process is None or process.process.poll() is not None:
            raise ScenarioFailure("a server process stopped before this scenario ran")


def corium(context: Context, *arguments: str, timeout: int = 60) -> str:
    return run_command([str(context.corium_bin), *arguments], timeout=timeout)


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
        context.report_dir / "logs" / "transactor.log",
    )
    wait_for_port(context.transactor, context.transactor_port)

    create_output = corium(
        context,
        "db",
        "create",
        "people",
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
            "people",
            "--listen",
            f"127.0.0.1:{context.peer_port}",
            "--transactor",
            context.transactor_endpoint,
        ],
        context.report_dir / "logs" / "peer-server.log",
    )
    wait_for_port(context.peer, context.peer_port)
    stats_output = corium(
        context,
        "db",
        "stats",
        "people",
        "--transactor",
        context.transactor_endpoint,
    )
    context.server_ready = True
    return f"Transactor and peer server are ready.\n{create_output}\n{stats_output}"


def authz_init(context: Context) -> str:
    require_server(context)
    initialized = corium(
        context,
        "authz",
        "init",
        "--transactor",
        context.transactor_endpoint,
    )
    status = corium(
        context,
        "authz",
        "status",
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
        "encrypted",
        "--schema",
        str(REPOSITORY / "tests/scenarios/schema.toml"),
        "--storage-key",
        context.storage_key_uri,
        "--transactor",
        context.transactor_endpoint,
    )
    status = corium(
        context,
        "keys",
        "status",
        "encrypted",
        "--transactor",
        context.transactor_endpoint,
    )
    return f"{created}\n{status}"


def attribute_protection_classes(context: Context) -> str:
    """Exercise protected writes and keyless reads through the real engine."""

    require_server(context)
    return run_command(
        [
            "cargo",
            "test",
            "--quiet",
            "-p",
            "corium-client",
            "--test",
            "protection",
            "--",
            "--nocapture",
        ],
        timeout=300,
    )


def key_rotation(context: Context) -> str:
    require_server(context)
    rotated = corium(
        context,
        "keys",
        "rotate",
        "encrypted",
        "--transactor",
        context.transactor_endpoint,
    )
    status = corium(
        context,
        "keys",
        "status",
        "encrypted",
        "--transactor",
        context.transactor_endpoint,
    )
    if "storage-key-epoch 2" not in rotated and "active-epoch 2" not in status:
        raise ScenarioFailure(f"rotation did not report epoch 2\n{rotated}\n{status}")
    return f"{rotated}\n{status}"


def schema_updates(context: Context) -> str:
    """Exercise the shipped read-only, plan-first schema update CLI."""

    require_server(context)
    plan = corium(
        context,
        "schema",
        "update",
        "people",
        "--schema",
        str(REPOSITORY / "tests/scenarios/schema-updated.toml"),
        "--transactor",
        context.transactor_endpoint,
    )
    if ":person/email" not in plan or "ADDITIVE" not in plan:
        raise ScenarioFailure(f"schema plan omitted the expected additive change\n{plan}")
    return plan


def python_client_access(context: Context) -> str:
    require_server(context)
    return run_command(
        [
            sys.executable,
            str(REPOSITORY / "tests/scenarios/python_client.py"),
            context.peer_endpoint,
            "people",
        ],
        timeout=180,
    )


def java_client_access(context: Context) -> str:
    require_server(context)
    env = os.environ.copy()
    env.update(
        {
            "CORIUM_SCENARIO_PEER_ENDPOINT": context.peer_endpoint,
            "CORIUM_SCENARIO_DATABASE": "people",
        }
    )
    return run_command(
        [
            "mvn",
            "-f",
            str(REPOSITORY / "clients/java/pom.xml"),
            "--batch-mode",
            "-Dtest=LiveScenarioTest",
            "test",
        ],
        env=env,
        timeout=300,
    )


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
            "people",
        ],
        timeout=300,
    )


SCENARIOS: list[Scenario] = [
    ("server setup", server_setup),
    ("authz init", authz_init),
    ("encryption init", encryption_init),
    ("attribute protection classes", attribute_protection_classes),
    ("key rotation", key_rotation),
    ("schema updates", schema_updates),
    ("python client access", python_client_access),
    ("java client access", java_client_access),
    ("rust client access", rust_client_access),
]


def run_scenarios(context: Context) -> list[ScenarioResult]:
    results: list[ScenarioResult] = []
    for name, scenario in SCENARIOS:
        print(f"\n=== {name} ===", flush=True)
        started = time.monotonic()
        try:
            detail = scenario(context).strip() or "completed"
            status = "PASS"
        except subprocess.TimeoutExpired as error:
            detail = f"timed out after {error.timeout}s: {format_command(error.cmd)}"
            status = "FAIL"
        except Exception as error:  # Every scenario is an isolation boundary.
            detail = str(error) or traceback.format_exc()
            status = "FAIL"
        duration = time.monotonic() - started
        result = ScenarioResult(name, status, round(duration, 3), detail)
        results.append(result)
        print(f"{status} ({duration:.2f}s)\n{detail}", flush=True)
    return results


def markdown_report(results: Sequence[ScenarioResult], generated_at: str) -> str:
    passes = sum(result.status == "PASS" for result in results)
    failures = len(results) - passes
    lines = [
        "# Scenario integration report",
        "",
        f"Generated: {generated_at}",
        "",
        (
            f"Result: **{passes} passed, {failures} failed, {len(results)} total**. "
            "Failures are informational and do not change the runner exit code."
        ),
        "",
        "| Scenario | Status | Duration |",
        "|---|---:|---:|",
    ]
    for result in results:
        icon = "✅" if result.status == "PASS" else "❌"
        lines.append(
            f"| {result.name} | {icon} {result.status} | {result.duration_seconds:.3f}s |"
        )
    lines.extend(["", "## Details", ""])
    for result in results:
        icon = "✅" if result.status == "PASS" else "❌"
        lines.extend(
            [
                f"### {icon} {result.name}",
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
        "--list",
        action="store_true",
        help="list scenario names without running them",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.list:
        for name, _ in SCENARIOS:
            print(name)
        return 0

    args.report_dir.mkdir(parents=True, exist_ok=True)
    try:
        with tempfile.TemporaryDirectory(prefix="corium-scenarios-") as temporary:
            work_dir = Path(temporary)
            storage_key = work_dir / "storage.key"
            class_key = work_dir / "pii.key"
            storage_key.write_bytes(os.urandom(32))
            class_key.write_bytes(os.urandom(32))
            transactor_port = free_port()
            peer_port = free_port()
            while peer_port == transactor_port:
                peer_port = free_port()
            context = Context(
                corium_bin=args.corium_bin.resolve(),
                report_dir=args.report_dir.resolve(),
                work_dir=work_dir,
                transactor_port=transactor_port,
                peer_port=peer_port,
                storage_key_uri=f"file:{storage_key}",
                class_key_uri=f"file:{class_key}",
            )
            try:
                results = run_scenarios(context)
            finally:
                context.stop_services()
    except Exception:
        detail = "scenario harness could not initialize:\n" + traceback.format_exc()
        results = [
            ScenarioResult(name, "FAIL", 0.0, detail) for name, _ in SCENARIOS
        ]
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
