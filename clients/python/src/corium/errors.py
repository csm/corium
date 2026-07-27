"""Stable public exception hierarchy for Corium clients."""

from __future__ import annotations


class CoriumError(Exception):
    """Base class for all Corium client failures."""

    def __init__(self, message: str, *, grpc_code: int | None = None) -> None:
        super().__init__(message)
        self.grpc_code = grpc_code


class ConnectionError(CoriumError):
    """A peer or peer server could not be reached."""


class AuthenticationError(CoriumError):
    """Credentials were missing or rejected."""


class PermissionDeniedError(CoriumError):
    """The authenticated caller is not authorized."""


class ProtocolError(CoriumError):
    """The remote endpoint violated or rejected the protocol contract."""


class QueryError(CoriumError):
    """A query or Pull operation failed."""


class TransactionError(CoriumError):
    """A transaction was rejected or could not commit."""


class DecodeError(CoriumError):
    """A boundary value could not be decoded losslessly."""


class StorageError(CoriumError):
    """Direct peer storage access failed."""


class FuelExhaustedError(QueryError):
    """Query execution consumed its fuel budget."""


class ClosedError(CoriumError):
    """An operation was attempted after explicit peer shutdown."""
