"""Shared Python API independent of local or remote deployment topology."""

from __future__ import annotations

import asyncio
import functools
import importlib
import os
from importlib.metadata import entry_points
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from types import TracebackType
from typing import Any, Protocol, TypeVar, runtime_checkable

from ._forms import lower_form
from .errors import ClosedError, NativeExtensionError, StorageError


class Index(str, Enum):
    """A covering datom index."""

    EAVT = "eavt"
    AEVT = "aevt"
    AVET = "avet"
    VAET = "vaet"


@dataclass(frozen=True, slots=True)
class DbStats:
    """Coarse statistics for one immutable database view."""

    basis_t: int
    datoms: int
    entities: int
    attributes: int


@dataclass(frozen=True, slots=True)
class Datom:
    """One datom returned by an index scan."""

    e: int
    a: int
    value: Any
    tx: int
    added: bool


@dataclass(frozen=True, slots=True)
class SegmentCache:
    """Bounded local cache for immutable direct-storage segments."""

    directory: str | os.PathLike[str]
    capacity_bytes: int
    memory_capacity_bytes: int | None = None

    def __post_init__(self) -> None:
        directory = os.fspath(self.directory)
        if not isinstance(directory, str):
            raise TypeError("segment cache directory must be a string path")
        if not directory:
            raise ValueError("segment cache directory must not be empty")
        object.__setattr__(self, "directory", directory)
        capacity = _nonnegative_int(self.capacity_bytes, "capacity_bytes")
        if capacity == 0:
            raise ValueError("capacity_bytes must be positive")
        if self.memory_capacity_bytes is not None:
            memory = _nonnegative_int(
                self.memory_capacity_bytes, "memory_capacity_bytes"
            )
            if memory > capacity:
                raise ValueError("memory_capacity_bytes must not exceed capacity_bytes")


@dataclass(frozen=True, slots=True)
class DirectStorage:
    """Discover the transactor's read-only storage connection."""

    cache: SegmentCache | None = None

    def __post_init__(self) -> None:
        if self.cache is not None and not isinstance(self.cache, SegmentCache):
            raise TypeError("cache must be a SegmentCache")


@dataclass(frozen=True, slots=True)
class _View:
    kind: str = "current"
    value: int | None = None


class _PeerState:
    __slots__ = ("closed",)

    def __init__(self) -> None:
        self.closed = False

    def ensure_open(self) -> None:
        if self.closed:
            raise ClosedError("peer is closed")


class _DbBackend(Protocol):
    database_name: str

    async def query(
        self,
        view: _View,
        query: Any,
        args: tuple[Any, ...],
        fuel: int | None,
    ) -> Any: ...

    async def pull(self, view: _View, pattern: Any, entity: Any) -> Any: ...

    async def datoms(
        self,
        view: _View,
        index: Index,
        components: tuple[Any, ...],
        limit: int,
    ) -> Sequence[Datom]: ...

    async def stats(self, view: _View) -> DbStats: ...


def _nonnegative_int(value: int, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    if value < 0:
        raise ValueError(f"{name} must be non-negative")
    return value


def _unix_millis(instant: datetime) -> int:
    if not isinstance(instant, datetime):
        raise TypeError("instant must be a datetime")
    if instant.tzinfo is None or instant.utcoffset() is None:
        raise ValueError("instant must be timezone-aware")
    if instant.microsecond % 1_000:
        raise ValueError("instant must have millisecond precision")
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = instant.astimezone(timezone.utc) - epoch
    return delta.days * 86_400_000 + delta.seconds * 1_000 + delta.microseconds // 1_000


@dataclass(frozen=True, slots=True)
class Db:
    """An immutable database value."""

    _backend: _DbBackend
    _state: _PeerState
    _view: _View = _View()

    def _ensure_open(self) -> None:
        self._state.ensure_open()

    @property
    def database_name(self) -> str:
        """The database represented by this value."""

        return self._backend.database_name

    def as_of(self, t: int) -> Db:
        """Return a value including transactions through ``t``."""

        self._ensure_open()
        return Db(
            self._backend,
            self._state,
            _View("as_of", _nonnegative_int(t, "t")),
        )

    def since(self, t: int) -> Db:
        """Return a value including assertions since ``t``."""

        self._ensure_open()
        return Db(
            self._backend,
            self._state,
            _View("since", _nonnegative_int(t, "t")),
        )

    def history(self) -> Db:
        """Return a full history value, including retractions."""

        self._ensure_open()
        return Db(self._backend, self._state, _View("history"))

    def as_of_instant(self, instant: datetime) -> Db:
        """Return a value as of the last transaction at or before ``instant``."""

        self._ensure_open()
        return Db(
            self._backend,
            self._state,
            _View("as_of_instant", _unix_millis(instant)),
        )

    def since_instant(self, instant: datetime) -> Db:
        """Return a value containing assertions since ``instant``."""

        self._ensure_open()
        return Db(
            self._backend,
            self._state,
            _View("since_instant", _unix_millis(instant)),
        )

    async def query(self, query: Any, *args: Any, fuel: int | None = None) -> Any:
        """Execute a raw form or typed query builder with positional inputs."""

        self._ensure_open()
        if fuel is not None:
            fuel = _nonnegative_int(fuel, "fuel")
        return await self._backend.query(
            self._view,
            lower_form(query),
            tuple(lower_form(arg) for arg in args),
            fuel,
        )

    async def pull(self, pattern: Any, entity: Any) -> Any:
        """Execute a raw form or typed Pull pattern for one entity."""

        self._ensure_open()
        return await self._backend.pull(
            self._view, lower_form(pattern), lower_form(entity)
        )

    async def datoms(
        self,
        index: Index | str,
        *components: Any,
        limit: int | None = None,
    ) -> Sequence[Datom]:
        """Scan a covering index; ``limit=None`` explicitly means unbounded."""

        self._ensure_open()
        try:
            parsed_index = Index(index)
        except ValueError as error:
            raise ValueError(f"unknown datom index: {index}") from error
        if limit is None:
            wire_limit = 0
        else:
            wire_limit = _nonnegative_int(limit, "limit")
            if wire_limit == 0:
                raise ValueError("limit must be positive or None for an unbounded scan")
        return await self._backend.datoms(
            self._view, parsed_index, components, wire_limit
        )

    async def stats(self) -> DbStats:
        """Return coarse statistics for this view."""

        self._ensure_open()
        return await self._backend.stats(self._view)

    async def basis_t(self) -> int:
        """Return the basis transaction for this view."""

        return (await self.stats()).basis_t


@dataclass(frozen=True, slots=True)
class _TxReportData:
    basis_before: int
    basis_t: int
    tx_instant: datetime
    tempids: dict[str, int]
    db_after: _DbBackend


@dataclass(frozen=True, slots=True)
class TxReport:
    """Result of a committed transaction."""

    basis_before: int
    basis_t: int
    tx_instant: datetime
    tempids: dict[str, int]
    db_after: Db


class _PeerBackend(Protocol):
    database_name: str

    async def db(self) -> _DbBackend: ...

    async def sync(self) -> _DbBackend: ...

    async def transact(self, tx_data: Any) -> _TxReportData: ...

    async def close(self) -> None: ...


@runtime_checkable
class Peer(Protocol):
    """Common runtime-checkable protocol implemented by both peer modes."""

    @property
    def database_name(self) -> str: ...

    async def db(self) -> Db: ...

    async def sync(self) -> Db: ...

    async def transact(self, tx_data: Any) -> TxReport: ...

    async def close(self) -> None: ...

    async def __aenter__(self) -> Peer: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...


_PeerT = TypeVar("_PeerT", bound="_BasePeer")


class _BasePeer(Peer):
    def __init__(self, backend: _PeerBackend) -> None:
        self._backend: _PeerBackend | None = backend
        self._database_name = backend.database_name
        self._state = _PeerState()
        self._close_lock = asyncio.Lock()

    @classmethod
    def _from_backend(cls: type[_PeerT], backend: _PeerBackend) -> _PeerT:
        """Construct a peer around an adapter backend (used by native/fake adapters)."""

        return cls(backend)

    @property
    def database_name(self) -> str:
        return self._database_name

    def _require_backend(self) -> _PeerBackend:
        self._state.ensure_open()
        if self._backend is None:
            raise ClosedError("peer is closed")
        return self._backend

    async def db(self) -> Db:
        return Db(await self._require_backend().db(), self._state)

    async def sync(self) -> Db:
        return Db(await self._require_backend().sync(), self._state)

    async def transact(self, tx_data: Any) -> TxReport:
        """Submit raw forms or typed transaction data."""

        report = await self._require_backend().transact(lower_form(tx_data))
        return TxReport(
            basis_before=report.basis_before,
            basis_t=report.basis_t,
            tx_instant=report.tx_instant,
            tempids=dict(report.tempids),
            db_after=Db(report.db_after, self._state),
        )

    async def close(self) -> None:
        async with self._close_lock:
            backend = self._backend
            if backend is None:
                return
            # Invalidate every derived Db while shutdown is in progress. If
            # the adapter cannot close, restore the open state so callers can
            # retry instead of losing the only handle to the resource.
            self._state.closed = True
            try:
                await backend.close()
            except BaseException:
                self._state.closed = False
                raise
            self._backend = None

    async def __aenter__(self: _PeerT) -> _PeerT:
        self._require_backend()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.close()


@functools.cache
def _native_modules() -> tuple[Any, ...]:
    module_name = "corium._corium"
    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError as error:
        if error.name != module_name:
            raise NativeExtensionError(
                f"the native extension {module_name} failed to load"
            ) from error
        raise NativeExtensionError(
            "the Corium native extension is not installed for this platform"
        ) from error
    except ImportError as error:
        raise NativeExtensionError(
            f"the native extension {module_name} failed to load"
        ) from error

    for entry_point in entry_points(group="corium.store_plugins"):
        try:
            provider = entry_point.load()
            path = provider() if callable(provider) else provider
            module._load_storage_plugin(os.fspath(path))
        except Exception as error:
            raise NativeExtensionError(
                f"the storage plugin {entry_point.name} failed to load"
            ) from error
    return (module,)


def _native_module() -> Any:
    """Return one native module for operations independent of storage drivers."""

    return _native_modules()[0]


def available_storage_backends() -> frozenset[str]:
    """Return direct-storage backends registered in the native engine."""

    return frozenset(_native_module()._storage_backends())


def _tls_from_endpoints(
    endpoints: Sequence[str],
    token: str | None,
    allow_insecure_token: bool,
    tls_ca: bytes | None,
    tls_domain: str | None,
) -> bool:
    if not endpoints:
        raise ValueError("at least one endpoint is required")
    schemes = set()
    for endpoint in endpoints:
        if "://" not in endpoint:
            raise ValueError(f"endpoint must include a scheme: {endpoint}")
        schemes.add(endpoint.split("://", 1)[0].lower())
    if len(schemes) != 1 or schemes not in ({"http"}, {"https"}):
        raise ValueError("endpoints must consistently use http:// or https://")
    tls = schemes == {"https"}
    if token is not None and not tls and not allow_insecure_token:
        raise ValueError("bearer tokens require https:// endpoints")
    if not tls and (tls_ca is not None or tls_domain is not None):
        raise ValueError("custom TLS options require https:// endpoints")
    if tls_ca is not None and not isinstance(tls_ca, bytes):
        raise TypeError("tls_ca must be PEM-encoded bytes")
    if tls_domain is not None:
        if not isinstance(tls_domain, str):
            raise TypeError("tls_domain must be a string")
        if not tls_domain:
            raise ValueError("tls_domain must not be empty")
    return tls


class LocalPeer(_BasePeer):
    """An in-process full peer."""

    @classmethod
    async def connect(
        cls,
        endpoints: str | Sequence[str],
        *,
        database: str,
        token: str | None = None,
        allow_insecure_token: bool = False,
        tls_ca: bytes | None = None,
        tls_domain: str | None = None,
        storage: DirectStorage | None = None,
    ) -> LocalPeer:
        """Connect a full peer; ``https://`` endpoints enable platform TLS.

        Set ``allow_insecure_token=True`` only for deliberate local development
        over plaintext HTTP. ``tls_ca`` adds a PEM certificate authority and
        ``tls_domain`` overrides the certificate DNS name for private PKI.
        ``storage=DirectStorage()`` discovers a read-only storage connection
        from the transactor so the peer can bootstrap from published segments.
        """

        if storage is not None and not isinstance(storage, DirectStorage):
            raise TypeError("storage must be DirectStorage or None")
        endpoint_list = [endpoints] if isinstance(endpoints, str) else list(endpoints)
        tls = _tls_from_endpoints(
            endpoint_list,
            token,
            allow_insecure_token,
            tls_ca,
            tls_domain,
        )
        options = {
            "database": database,
            "token": token,
            "tls": tls,
            "tls_ca": tls_ca,
            "tls_domain": tls_domain,
            "direct_storage": storage is not None,
            "segment_cache_directory": (
                storage.cache.directory
                if storage is not None and storage.cache is not None
                else None
            ),
            "segment_cache_capacity_bytes": (
                storage.cache.capacity_bytes
                if storage is not None and storage.cache is not None
                else None
            ),
            "segment_cache_memory_bytes": (
                storage.cache.memory_capacity_bytes
                if storage is not None and storage.cache is not None
                else None
            ),
        }
        module = _native_module()
        if storage is not None:
            storage_backend = await module._discover_storage_backend(
                endpoint_list,
                database=database,
                token=token,
                tls=tls,
                tls_ca=tls_ca,
                tls_domain=tls_domain,
            )
            if storage_backend not in module._storage_backends():
                raise StorageError(
                    f"storage plugin {storage_backend!r} is not installed"
                )
        backend = await module.connect_local(endpoint_list, **options)
        return cls(backend)


class RemotePeer(_BasePeer):
    """A lightweight client connected to ``corium peer-server``."""

    @classmethod
    async def connect(
        cls,
        endpoint: str,
        *,
        database: str,
        token: str | None = None,
        allow_insecure_token: bool = False,
        tls_ca: bytes | None = None,
        tls_domain: str | None = None,
    ) -> RemotePeer:
        """Connect to a peer server; ``https://`` enables platform TLS.

        Set ``allow_insecure_token=True`` only for deliberate local development
        over plaintext HTTP. ``tls_ca`` adds a PEM certificate authority and
        ``tls_domain`` overrides the certificate DNS name for private PKI.
        """

        tls = _tls_from_endpoints(
            [endpoint],
            token,
            allow_insecure_token,
            tls_ca,
            tls_domain,
        )
        backend = await _native_module().connect_remote(
            endpoint,
            database=database,
            token=token,
            tls=tls,
            tls_ca=tls_ca,
            tls_domain=tls_domain,
        )
        return cls(backend)
