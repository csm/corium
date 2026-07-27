"""Shared Python API independent of local or remote deployment topology."""

from __future__ import annotations

import importlib
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from types import TracebackType
from typing import Any, Protocol, Sequence, runtime_checkable

from .errors import ClosedError, ConnectionError


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
    v: Any
    tx: int
    added: bool


@dataclass(frozen=True, slots=True)
class _View:
    kind: str = "current"
    value: int | None = None


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
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = instant.astimezone(timezone.utc) - epoch
    return (
        delta.days * 86_400_000
        + delta.seconds * 1_000
        + delta.microseconds // 1_000
    )


@dataclass(frozen=True, slots=True)
class Db:
    """An immutable database value."""

    _backend: _DbBackend
    _view: _View = _View()

    @property
    def database_name(self) -> str:
        """The database represented by this value."""

        return self._backend.database_name

    def as_of(self, t: int) -> Db:
        """Return a value including transactions through ``t``."""

        return Db(self._backend, _View("as_of", _nonnegative_int(t, "t")))

    def since(self, t: int) -> Db:
        """Return a value including assertions since ``t``."""

        return Db(self._backend, _View("since", _nonnegative_int(t, "t")))

    def history(self) -> Db:
        """Return a full history value, including retractions."""

        return Db(self._backend, _View("history"))

    def as_of_instant(self, instant: datetime) -> Db:
        """Return a value as of the last transaction at or before ``instant``."""

        return Db(self._backend, _View("as_of_instant", _unix_millis(instant)))

    def since_instant(self, instant: datetime) -> Db:
        """Return a value containing assertions since ``instant``."""

        return Db(self._backend, _View("since_instant", _unix_millis(instant)))

    async def query(
        self, query: Any, *args: Any, fuel: int | None = None
    ) -> Any:
        """Execute a raw query form with positional inputs."""

        if fuel is not None:
            fuel = _nonnegative_int(fuel, "fuel")
        return await self._backend.query(self._view, query, args, fuel)

    async def pull(self, pattern: Any, entity: Any) -> Any:
        """Execute a raw Pull pattern for one entity."""

        return await self._backend.pull(self._view, pattern, entity)

    async def datoms(
        self,
        index: Index | str,
        *components: Any,
        limit: int = 0,
    ) -> Sequence[Datom]:
        """Scan a covering index from a component prefix."""

        try:
            parsed_index = Index(index)
        except ValueError as error:
            raise ValueError(f"unknown datom index: {index}") from error
        limit = _nonnegative_int(limit, "limit")
        return await self._backend.datoms(
            self._view, parsed_index, components, limit
        )

    async def stats(self) -> DbStats:
        """Return coarse statistics for this view."""

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


class _BasePeer:
    def __init__(self, backend: _PeerBackend) -> None:
        self._backend: _PeerBackend | None = backend
        self._database_name = backend.database_name

    @classmethod
    def _from_backend(cls, backend: _PeerBackend) -> _BasePeer:
        """Construct a peer around an adapter backend (used by native/fake adapters)."""

        return cls(backend)

    @property
    def database_name(self) -> str:
        return self._database_name

    def _require_backend(self) -> _PeerBackend:
        if self._backend is None:
            raise ClosedError("peer is closed")
        return self._backend

    async def db(self) -> Db:
        return Db(await self._require_backend().db())

    async def sync(self) -> Db:
        return Db(await self._require_backend().sync())

    async def transact(self, tx_data: Any) -> TxReport:
        report = await self._require_backend().transact(tx_data)
        return TxReport(
            basis_before=report.basis_before,
            basis_t=report.basis_t,
            tx_instant=report.tx_instant,
            tempids=dict(report.tempids),
            db_after=Db(report.db_after),
        )

    async def close(self) -> None:
        backend = self._backend
        if backend is None:
            return
        self._backend = None
        await backend.close()

    async def __aenter__(self) -> _BasePeer:
        self._require_backend()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.close()


def _native_module() -> Any:
    try:
        return importlib.import_module("corium._corium")
    except ImportError as error:
        raise ConnectionError(
            "the Corium native extension is not installed for this platform"
        ) from error


class LocalPeer(_BasePeer):
    """An in-process full peer."""

    @classmethod
    async def connect(
        cls,
        endpoints: str | Sequence[str],
        *,
        database: str,
        token: str | None = None,
        storage: Any = None,
    ) -> LocalPeer:
        """Connect an in-process full peer."""

        endpoint_list = [endpoints] if isinstance(endpoints, str) else list(endpoints)
        backend = await _native_module().connect_local(
            endpoint_list,
            database=database,
            token=token,
            storage=storage,
        )
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
    ) -> RemotePeer:
        """Connect to a hosted peer server."""

        backend = await _native_module().connect_remote(
            endpoint,
            database=database,
            token=token,
        )
        return cls(backend)
