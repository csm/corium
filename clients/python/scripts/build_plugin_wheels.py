#!/usr/bin/env python3
"""Build platform wheels around the Corium storage plugin libraries."""

from __future__ import annotations

import argparse
import base64
import csv
import datetime
import hashlib
import io
import os
import re
import sys
import zipfile
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
ARTIFACTS = ROOT / "clients/python/artifacts"
DEPENDENCY = re.compile(r'^dependencies = \["corium==([^"]+)"\]$', re.MULTILINE)
SAFE_TAG = re.compile(r"[A-Za-z0-9_.]+")


@dataclass(frozen=True)
class Plugin:
    backend: str
    distribution: str
    package: str
    crate_library: str

    @property
    def project(self) -> Path:
        return ARTIFACTS / self.backend


PLUGINS = {
    plugin.backend: plugin
    for plugin in (
        Plugin("turso", "corium-turso", "corium_turso", "corium_store_turso"),
        Plugin(
            "postgres",
            "corium-postgres",
            "corium_postgres",
            "corium_store_postgres",
        ),
        Plugin("s3", "corium-s3", "corium_s3", "corium_store_s3"),
    )
}


def _project_field(project: Path, field: str) -> str:
    text = (project / "pyproject.toml").read_text()
    match = re.search(rf'^{re.escape(field)} = "([^"]+)"$', text, re.MULTILINE)
    if match is None:
        raise RuntimeError(f"{project}/pyproject.toml has no {field!r} field")
    return match.group(1)


def _corium_requirement(project: Path) -> str:
    text = (project / "pyproject.toml").read_text()
    match = DEPENDENCY.search(text)
    if match is None:
        raise RuntimeError(f"{project}/pyproject.toml has no exact corium dependency")
    return match.group(1)


def _library_name(plugin: Plugin) -> str:
    if sys.platform == "win32":
        return f"{plugin.crate_library}.dll"
    if sys.platform == "darwin":
        return f"lib{plugin.crate_library}.dylib"
    return f"lib{plugin.crate_library}.so"


def _digest(data: bytes) -> str:
    encoded = base64.urlsafe_b64encode(hashlib.sha256(data).digest())
    return "sha256=" + encoded.rstrip(b"=").decode("ascii")


def _metadata(plugin: Plugin, version: str, description: str) -> bytes:
    return (
        "Metadata-Version: 2.2\n"
        f"Name: {plugin.distribution}\n"
        f"Version: {version}\n"
        f"Summary: {description}\n"
        "Description-Content-Type: text/markdown\n"
        "License: Apache-2.0 OR MIT\n"
        "Requires-Python: >=3.10\n"
        f"Requires-Dist: corium =={version}\n"
        "Classifier: Programming Language :: Python :: 3\n"
        "Classifier: Programming Language :: Python :: 3.10\n"
        "Classifier: Programming Language :: Python :: 3.11\n"
        "Classifier: Programming Language :: Python :: 3.12\n"
        "Classifier: Programming Language :: Python :: 3.13\n"
        "Project-URL: Documentation, "
        "https://github.com/csm/corium/tree/main/clients/python\n"
        "Project-URL: Issues, https://github.com/csm/corium/issues\n"
        "Project-URL: Repository, https://github.com/csm/corium\n"
        "\n"
        f"# {plugin.distribution}\n"
        "\n"
        f"{description}.\n"
    ).encode()


def _wheel_file(entries: dict[str, bytes], destination: Path) -> None:
    epoch = max(315532800, int(os.environ.get("SOURCE_DATE_EPOCH", "315532800")))
    timestamp = datetime.datetime.fromtimestamp(epoch, tz=datetime.timezone.utc)
    date_time = (
        timestamp.year,
        timestamp.month,
        timestamp.day,
        timestamp.hour,
        timestamp.minute,
        timestamp.second,
    )
    with zipfile.ZipFile(destination, "w", zipfile.ZIP_DEFLATED) as wheel:
        for name, data in entries.items():
            info = zipfile.ZipInfo(name, date_time=date_time)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            wheel.writestr(info, data)


def build(
    plugin: Plugin,
    target_directory: Path,
    platform_tag: str,
    output: Path,
) -> Path:
    """Build one wheel and return its path."""

    if SAFE_TAG.fullmatch(platform_tag) is None:
        raise ValueError(f"invalid wheel platform tag: {platform_tag!r}")
    declared_distribution = _project_field(plugin.project, "name")
    if declared_distribution != plugin.distribution:
        raise RuntimeError(
            f"{plugin.project} declares {declared_distribution}, "
            f"not {plugin.distribution}"
        )
    version = _project_field(plugin.project, "version")
    if _corium_requirement(plugin.project) != version:
        raise RuntimeError(f"{plugin.distribution} does not require corium=={version}")
    description = _project_field(plugin.project, "description")
    library_name = _library_name(plugin)
    library = target_directory / library_name
    if not library.is_file():
        raise FileNotFoundError(f"plugin library does not exist: {library}")

    wheel_tag = f"py3-none-{platform_tag}"
    normalized_distribution = plugin.distribution.replace("-", "_")
    normalized_version = re.sub(r"[^A-Za-z0-9.]+", "_", version)
    stem = f"{normalized_distribution}-{normalized_version}"
    dist_info = f"{stem}.dist-info"
    entries = {
        f"{plugin.package}/__init__.py": (
            plugin.project / "src" / plugin.package / "__init__.py"
        ).read_bytes(),
        f"{plugin.package}/{library_name}": library.read_bytes(),
        f"{dist_info}/METADATA": _metadata(plugin, version, description),
        f"{dist_info}/WHEEL": (
            "Wheel-Version: 1.0\n"
            "Generator: corium-build-plugin-wheels\n"
            "Root-Is-Purelib: false\n"
            f"Tag: {wheel_tag}\n"
        ).encode(),
        f"{dist_info}/entry_points.txt": (
            f"[corium.store_plugins]\n{plugin.backend} = {plugin.package}:plugin_path\n"
        ).encode(),
    }
    record = io.StringIO(newline="")
    writer = csv.writer(record, lineterminator="\n")
    for name, data in entries.items():
        writer.writerow((name, _digest(data), len(data)))
    record_name = f"{dist_info}/RECORD"
    writer.writerow((record_name, "", ""))
    entries[record_name] = record.getvalue().encode()

    output.mkdir(parents=True, exist_ok=True)
    destination = output / f"{stem}-{wheel_tag}.whl"
    _wheel_file(entries, destination)
    return destination


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target-directory", required=True, type=Path)
    parser.add_argument("--platform-tag", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument(
        "--backend",
        action="append",
        choices=sorted(PLUGINS),
        help="backend to package; the default packages all official plugins",
    )
    arguments = parser.parse_args()
    backends = arguments.backend or list(PLUGINS)
    for backend in backends:
        wheel = build(
            PLUGINS[backend],
            arguments.target_directory,
            arguments.platform_tag,
            arguments.output,
        )
        print(wheel)


if __name__ == "__main__":
    main()
