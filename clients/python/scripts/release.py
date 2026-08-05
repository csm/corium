#!/usr/bin/env python3
"""Keep the Python engine distribution version aligned with the workspace."""

from __future__ import annotations

import argparse
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
WORKSPACE = ROOT / "Cargo.toml"
BASE_PROJECT = ROOT / "clients/python/pyproject.toml"
PLUGIN_PROJECTS = tuple(
    ROOT / f"clients/python/artifacts/{backend}/pyproject.toml"
    for backend in ("postgres", "s3", "turso")
)
PROJECTS = (BASE_PROJECT, *PLUGIN_PROJECTS)
VERSION = re.compile(r'(?m)^version = "([^"]+)"$')
CORIUM_DEPENDENCY = re.compile(r'(?m)^dependencies = \["corium==([^"]+)"\]$')


def _workspace_version() -> str:
    text = WORKSPACE.read_text()
    workspace = text.split("[workspace.package]", 1)[1]
    match = VERSION.search(workspace)
    if match is None:
        raise RuntimeError("workspace package version is missing")
    return match.group(1)


def _project_version(path: Path) -> str:
    match = VERSION.search(path.read_text())
    if match is None:
        raise RuntimeError(f"{path} has no project version")
    return match.group(1)


def check(expected_version: str | None = None) -> None:
    expected = _workspace_version()
    mismatches: list[str] = []
    if expected_version is not None:
        expected_version = expected_version.removeprefix("v")
        if expected != expected_version:
            mismatches.append(
                f"release ref: {expected_version} != workspace {expected}"
            )
    for project in PROJECTS:
        actual = _project_version(project)
        if actual != expected:
            mismatches.append(f"{project.relative_to(ROOT)}: {actual} != {expected}")
        if project in PLUGIN_PROJECTS:
            dependency = CORIUM_DEPENDENCY.search(project.read_text())
            if dependency is None:
                mismatches.append(
                    f"{project.relative_to(ROOT)}: exact corium dependency is missing"
                )
            elif dependency.group(1) != expected:
                mismatches.append(
                    f"{project.relative_to(ROOT)}: corium dependency "
                    f"{dependency.group(1)} != {expected}"
                )
    if mismatches:
        raise SystemExit("Python release version mismatch:\n" + "\n".join(mismatches))


def set_version(version: str) -> None:
    if not re.fullmatch(r"[0-9]+(?:\.[0-9A-Za-z]+)+(?:[-+.][0-9A-Za-z.-]+)?", version):
        raise SystemExit(f"invalid release version: {version!r}")
    for project in PROJECTS:
        text = project.read_text()
        updated, replacements = VERSION.subn(f'version = "{version}"', text, count=1)
        if replacements != 1:
            raise SystemExit(f"could not set version in {project.relative_to(ROOT)}")
        if project in PLUGIN_PROJECTS:
            updated, replacements = CORIUM_DEPENDENCY.subn(
                f'dependencies = ["corium=={version}"]', updated, count=1
            )
            if replacements != 1:
                raise SystemExit(
                    f"could not set corium dependency in {project.relative_to(ROOT)}"
                )
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
        check()
    else:
        check(arguments.expected)


if __name__ == "__main__":
    main()
