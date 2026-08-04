"""Path provider for the official Corium Turso storage plugin."""

from __future__ import annotations

from pathlib import Path

_LIBRARY_NAMES = (
    "libcorium_store_turso.so",
    "libcorium_store_turso.dylib",
    "corium_store_turso.dll",
)


def plugin_path() -> Path:
    """Return the installed storage plugin path."""

    package = Path(__file__).resolve().parent
    for name in _LIBRARY_NAMES:
        library = package / name
        if library.is_file():
            return library
    raise RuntimeError("the corium-turso native library is missing")
