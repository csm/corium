#!/usr/bin/env python3
"""Keep the Java client version aligned with the workspace release."""

from __future__ import annotations

import argparse
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
WORKSPACE = ROOT / "Cargo.toml"
JAVA = ROOT / "clients/java"
# The parent declares the release version, and every module repeats it in its
# `<parent>` block, so one pattern covers the whole reactor.
PROJECTS = (
    JAVA / "pom.xml",
    JAVA / "client/pom.xml",
    JAVA / "artifacts/turso/pom.xml",
    JAVA / "artifacts/postgres/pom.xml",
    JAVA / "artifacts/s3/pom.xml",
)
WORKSPACE_VERSION = re.compile(r'(?m)^version = "([^"]+)"$')
PROJECT_VERSION = re.compile(
    r"(<artifactId>corium-java</artifactId>\s*<version>)([^<]+)(</version>)"
)
VALID_VERSION = re.compile(r"[0-9]+(?:\.[0-9A-Za-z]+)+(?:[-+.][0-9A-Za-z.-]+)?")


def _workspace_version() -> str:
    text = WORKSPACE.read_text()
    workspace = text.split("[workspace.package]", 1)[1]
    match = WORKSPACE_VERSION.search(workspace)
    if match is None:
        raise RuntimeError("workspace package version is missing")
    return match.group(1)


def _project_version() -> str:
    versions = {}
    for project in PROJECTS:
        match = PROJECT_VERSION.search(project.read_text())
        if match is None:
            raise RuntimeError(f"{project} has no corium-java version")
        versions[project] = match.group(2)
    distinct = set(versions.values())
    if len(distinct) != 1:
        raise SystemExit(
            "Java modules disagree on the release version:\n"
            + "\n".join(f"{project}: {version}" for project, version in versions.items())
        )
    return distinct.pop()


def check(expected_version: str | None = None) -> None:
    workspace_version = _workspace_version()
    project_version = _project_version()
    if expected_version is None:
        accepted = {workspace_version, f"{workspace_version}-SNAPSHOT"}
        if project_version not in accepted:
            raise SystemExit(
                f"Java client version {project_version} does not match "
                f"workspace version {workspace_version}"
            )
        return

    expected_version = expected_version.removeprefix("v")
    mismatches = []
    if workspace_version != expected_version:
        mismatches.append(
            f"workspace version {workspace_version} != {expected_version}"
        )
    if project_version != expected_version:
        mismatches.append(
            f"Java client version {project_version} != {expected_version}"
        )
    if mismatches:
        raise SystemExit("Java release version mismatch:\n" + "\n".join(mismatches))


def set_version(version: str) -> None:
    if VALID_VERSION.fullmatch(version) is None:
        raise SystemExit(f"invalid release version: {version!r}")
    for project in PROJECTS:
        text = project.read_text()
        updated, replacements = PROJECT_VERSION.subn(
            rf"\g<1>{version}\g<3>", text, count=1
        )
        if replacements != 1:
            raise SystemExit(f"could not set the version in {project}")
        project.write_text(updated)


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
