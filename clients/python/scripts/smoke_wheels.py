#!/usr/bin/env python3
"""Install and import the Python engine wheel in a fresh environment."""

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


def smoke(wheel_directory: Path) -> None:
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
            "corium",
        )
        code = (
            "import corium; "
            "backends = corium.available_storage_backends(); "
            "assert 'filesystem' in backends, backends"
        )
        _run(str(python), "-c", code)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("wheel_directory", type=Path)
    arguments = parser.parse_args()
    smoke(arguments.wheel_directory)


if __name__ == "__main__":
    main()
