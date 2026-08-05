#!/usr/bin/env python3
"""Build platform jars around the Corium engine and storage plugin libraries."""

from __future__ import annotations

import argparse
import datetime
import os
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
PROJECT = ROOT / "clients/java/pom.xml"
PROJECT_VERSION = re.compile(
    r"<artifactId>corium-java</artifactId>\s*<version>([^<]+)</version>"
)
SAFE_CLASSIFIER = re.compile(r"[a-z0-9_]+-[a-z0-9_]+")
RESOURCE_PREFIX = "dev/corium/native"


@dataclass(frozen=True)
class Artifact:
    """One publishable Maven artifact and the Cargo library it carries."""

    artifact_id: str
    crate_library: str


ARTIFACTS = {
    artifact.artifact_id: artifact
    for artifact in (
        # The engine ships beside the pure-Java client jar, so a remote-only
        # dependency stays free of native code.
        Artifact("corium-client", "corium_jni"),
        Artifact("corium-turso", "corium_store_turso"),
        Artifact("corium-postgres", "corium_store_postgres"),
        Artifact("corium-s3", "corium_store_s3"),
    )
}


def _version() -> str:
    match = PROJECT_VERSION.search(PROJECT.read_text())
    if match is None:
        raise RuntimeError(f"{PROJECT} has no corium-java version")
    return match.group(1)


def _library_name(artifact: Artifact, classifier: str) -> str:
    system = classifier.split("-", 1)[0]
    if system == "windows":
        return f"{artifact.crate_library}.dll"
    if system == "macos":
        return f"lib{artifact.crate_library}.dylib"
    return f"lib{artifact.crate_library}.so"


def _jar(entries: dict[str, bytes], destination: Path) -> None:
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
    with zipfile.ZipFile(destination, "w", zipfile.ZIP_DEFLATED) as jar:
        for name, data in entries.items():
            info = zipfile.ZipInfo(name, date_time=date_time)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            jar.writestr(info, data)


def build(
    artifact: Artifact,
    target_directory: Path,
    classifier: str,
    output: Path,
) -> Path:
    """Build one platform jar and return its path."""

    if SAFE_CLASSIFIER.fullmatch(classifier) is None:
        raise ValueError(f"invalid platform classifier: {classifier!r}")
    version = _version()
    library_name = _library_name(artifact, classifier)
    library = target_directory / library_name
    if not library.is_file():
        raise FileNotFoundError(f"native library does not exist: {library}")

    entries = {
        "META-INF/MANIFEST.MF": (
            "Manifest-Version: 1.0\n"
            "Created-By: corium-build-native-jars\n"
            f"Implementation-Title: {artifact.artifact_id}\n"
            f"Implementation-Version: {version}\n"
            f"Corium-Native-Classifier: {classifier}\n"
        ).encode(),
        f"{RESOURCE_PREFIX}/{classifier}/{library_name}": library.read_bytes(),
    }

    output.mkdir(parents=True, exist_ok=True)
    destination = output / f"{artifact.artifact_id}-{version}-{classifier}.jar"
    _jar(entries, destination)
    return destination


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target-directory", required=True, type=Path)
    parser.add_argument(
        "--classifier",
        required=True,
        help="platform classifier, such as linux-x86_64 or macos-aarch64",
    )
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument(
        "--artifact",
        action="append",
        choices=sorted(ARTIFACTS),
        help="artifact to package; the default packages all of them",
    )
    arguments = parser.parse_args()
    for artifact_id in arguments.artifact or list(ARTIFACTS):
        print(
            build(
                ARTIFACTS[artifact_id],
                arguments.target_directory,
                arguments.classifier,
                arguments.output,
            )
        )


if __name__ == "__main__":
    main()
