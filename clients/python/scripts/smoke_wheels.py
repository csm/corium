#!/usr/bin/env python3
"""Install and import every compatible wheel combination in fresh venvs."""

from __future__ import annotations

import argparse
import subprocess
import sys
import tempfile
import venv
from pathlib import Path


def _run(*args: str) -> None:
    subprocess.run(args, check=True)


def _python(environment: Path) -> Path:
    if sys.platform == "win32":
        return environment / "Scripts/python.exe"
    return environment / "bin/python"


def smoke(wheel_directory: Path, artifact: str | None) -> None:
    with tempfile.TemporaryDirectory(prefix="corium-wheel-") as temporary:
        environment = Path(temporary) / "venv"
        venv.EnvBuilder(with_pip=True).create(environment)
        python = _python(environment)
        packages = ["corium"]
        if artifact is not None:
            packages.append(f"corium-{artifact}")
        _run(
            str(python),
            "-m",
            "pip",
            "install",
            "--no-index",
            "--find-links",
            str(wheel_directory),
            *packages,
        )
        expected = artifact or "filesystem"
        code = (
            "import corium; "
            "backends = corium.available_storage_backends(); "
            f"assert {expected!r} in backends, backends"
        )
        _run(str(python), "-c", code)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("wheel_directory", type=Path)
    parser.add_argument(
        "--base-only",
        action="store_true",
        help="only test the common artifact (used for pull requests)",
    )
    arguments = parser.parse_args()
    smoke(arguments.wheel_directory, None)
    if not arguments.base_only:
        for artifact in ("turso", "postgres", "s3"):
            smoke(arguments.wheel_directory, artifact)


if __name__ == "__main__":
    main()
