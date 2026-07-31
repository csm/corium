"""Shared lowering support for immutable public builder values."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from .values import EdnList


@runtime_checkable
class Form(Protocol):
    """A value that can lower itself to Corium boundary data."""

    def to_form(self) -> Any: ...


def lower_form(value: Any) -> Any:
    """Recursively lower builders while preserving EDN collection kinds."""

    if isinstance(value, Form):
        return lower_form(value.to_form())
    if isinstance(value, EdnList):
        return EdnList(lower_form(item) for item in value.items)
    if isinstance(value, list):
        return [lower_form(item) for item in value]
    if isinstance(value, tuple):
        return tuple(lower_form(item) for item in value)
    if isinstance(value, dict):
        return {
            lower_form(key): lower_form(item_value) for key, item_value in value.items()
        }
    if isinstance(value, frozenset):
        return frozenset(lower_form(item) for item in value)
    if isinstance(value, set):
        return {lower_form(item) for item in value}
    return value
