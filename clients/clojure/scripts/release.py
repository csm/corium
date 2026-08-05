#!/usr/bin/env python3
"""Keep the Clojure client and Java dependency versions aligned for releases."""

from __future__ import annotations

import argparse
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
WORKSPACE = ROOT / "Cargo.toml"
PROJECT = ROOT / "clients/clojure/version.edn"
DEPS = ROOT / "clients/clojure/deps.edn"
WORKSPACE_VERSION = re.compile(r'(?m)^version = "([^"]+)"$')
PROJECT_VERSION = re.compile(r'\{:version "([^"]+)"\}')
JAVA_VERSION = re.compile(
    r'(dev\.corium/corium-client(?:\$[a-z0-9_-]+)?\s+'
    r'\{:mvn/version ")([^"]+)("\})'
)
VALID_VERSION = re.compile(r"[0-9]+(?:\.[0-9A-Za-z]+)+(?:[-+.][0-9A-Za-z.-]+)?")


def _workspace_version() -> str:
    workspace = WORKSPACE.read_text().split("[workspace.package]", 1)[1]
    match = WORKSPACE_VERSION.search(workspace)
    if match is None:
        raise RuntimeError("workspace package version is missing")
    return match.group(1)


def _project_version() -> str:
    match = PROJECT_VERSION.fullmatch(PROJECT.read_text().strip())
    if match is None:
        raise RuntimeError("Clojure client version is missing")
    return match.group(1)


def _java_versions() -> list[str]:
    versions = [match.group(2) for match in JAVA_VERSION.finditer(DEPS.read_text())]
    if len(versions) != 6:
        raise RuntimeError(f"expected 6 Java client dependencies, found {len(versions)}")
    return versions


def check(expected_version: str | None = None) -> None:
    workspace_version = _workspace_version()
    project_version = _project_version()
    java_versions = _java_versions()
    mismatches: list[str] = []

    if len(set(java_versions)) != 1:
        mismatches.append("Java client dependency versions do not match")

    if expected_version is None:
        accepted = {workspace_version, f"{workspace_version}-SNAPSHOT"}
        if project_version not in accepted:
            mismatches.append(
                f"Clojure client version {project_version} does not match "
                f"workspace version {workspace_version}"
            )
    else:
        expected_version = expected_version.removeprefix("v")
        if workspace_version != expected_version:
            mismatches.append(
                f"workspace version {workspace_version} != {expected_version}"
            )
        if project_version != expected_version:
            mismatches.append(
                f"Clojure client version {project_version} != {expected_version}"
            )
        if any(version != expected_version for version in java_versions):
            mismatches.append(
                f"Java client dependencies do not use {expected_version}"
            )

    if mismatches:
        raise SystemExit("Clojure release version mismatch:\n" + "\n".join(mismatches))


def set_version(version: str) -> None:
    if VALID_VERSION.fullmatch(version) is None:
        raise SystemExit(f"invalid release version: {version!r}")

    PROJECT.write_text(f'{{:version "{version}"}}\n')
    updated, replacements = JAVA_VERSION.subn(
        rf"\g<1>{version}\g<3>", DEPS.read_text()
    )
    if replacements != 6:
        raise SystemExit(
            f"could not update all Java client dependencies: found {replacements}"
        )
    DEPS.write_text(updated)


def main() -> None:
    parser = argparse.ArgumentParser()
    subcommands = parser.add_subparsers(dest="command", required=True)
    check_parser = subcommands.add_parser("check")
    check_parser.add_argument("--expected")
    set_parser = subcommands.add_parser("set")
    set_parser.add_argument("version")
    arguments = parser.parse_args()

    if arguments.command == "set":
        set_version(arguments.version)
        check(arguments.version)
    else:
        check(arguments.expected)


if __name__ == "__main__":
    main()
