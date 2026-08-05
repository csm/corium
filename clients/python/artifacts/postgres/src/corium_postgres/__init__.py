"""Path provider for the official Corium PostgreSQL storage plugin."""

from __future__ import annotations

from pathlib import Path

_LIBRARY_NAMES = (
    "libcorium_store_postgres.so",
    "libcorium_store_postgres.dylib",
    "corium_store_postgres.dll",
)


def plugin_path() -> Path:
    """Return the installed storage plugin path."""

    package = Path(__file__).resolve().parent
    for name in _LIBRARY_NAMES:
        library = package / name
        if library.is_file():
            return library
    raise RuntimeError("the corium-postgres native library is missing")
