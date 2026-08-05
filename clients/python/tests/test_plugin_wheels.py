from __future__ import annotations

import subprocess
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
BUILDER = ROOT / "clients/python/scripts/build_plugin_wheels.py"


def _library_name(backend: str) -> str:
    if sys.platform == "win32":
        return f"corium_store_{backend}.dll"
    if sys.platform == "darwin":
        return f"libcorium_store_{backend}.dylib"
    return f"libcorium_store_{backend}.so"


class PluginWheelTests(unittest.TestCase):
    def test_builder_packages_libraries_and_entry_points(self) -> None:
        with tempfile.TemporaryDirectory(prefix="corium-plugin-wheel-test-") as temp:
            temporary = Path(temp)
            target = temporary / "target"
            output = temporary / "dist"
            target.mkdir()
            for backend in ("postgres", "s3", "turso"):
                (target / _library_name(backend)).write_bytes(backend.encode())

            subprocess.run(
                [
                    sys.executable,
                    str(BUILDER),
                    "--target-directory",
                    str(target),
                    "--platform-tag",
                    "test_platform",
                    "--output",
                    str(output),
                ],
                check=True,
            )

            wheels = sorted(output.glob("*.whl"))
            self.assertEqual(len(wheels), 3)
            for wheel in wheels:
                distribution = wheel.name.split("-", 1)[0]
                package = distribution
                backend = distribution.removeprefix("corium_")
                dist_info = f"{distribution}-0.1.0.dist-info"
                with zipfile.ZipFile(wheel) as archive:
                    names = set(archive.namelist())
                    self.assertIn(f"{package}/{_library_name(backend)}", names)
                    self.assertIn(f"{dist_info}/entry_points.txt", names)
                    self.assertIn(f"{dist_info}/RECORD", names)


if __name__ == "__main__":
    unittest.main()
