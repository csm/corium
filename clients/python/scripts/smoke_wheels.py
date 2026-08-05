#!/usr/bin/env python3
"""Install and import the Python engine and storage plugin wheels."""

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


def _install_and_check(
    wheel_directory: Path, packages: tuple[str, ...], expected: set[str]
) -> None:
    with tempfile.TemporaryDirectory(prefix="corium-wheel-") as temporary:
        environment = Path(temporary) / "venv"
        venv.EnvBuilder(with_pip=True).create(environment)
        python = _python(environment)
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
        code = (
            "import corium; "
            "backends = corium.available_storage_backends(); "
            f"expected = {expected!r}; "
            "assert expected == backends, (expected, backends)"
        )
        _run(str(python), "-c", code)


def smoke(wheel_directory: Path) -> None:
    _install_and_check(wheel_directory, ("corium",), {"filesystem"})
    _install_and_check(
        wheel_directory,
        ("corium", "corium-postgres", "corium-s3", "corium-turso"),
        {"filesystem", "postgres", "s3", "turso"},
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("wheel_directory", type=Path)
    arguments = parser.parse_args()
    smoke(arguments.wheel_directory)


if __name__ == "__main__":
    main()
