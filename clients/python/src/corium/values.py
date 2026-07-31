"""Lossless Python representations for Corium boundary values."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import Any

_RESERVED_TAGS = frozenset({"bytes", "eid", "inst", "uuid"})


def _boundary_name(value: str, kind: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{kind} must be a string")
    if not value or value.startswith(":") or any(char.isspace() for char in value):
        raise ValueError(
            f"{kind} must be a non-empty boundary name without ':' or whitespace"
        )
    return value


@dataclass(frozen=True, slots=True)
class Keyword:
    """A Corium keyword, distinct from an ordinary Python string."""

    value: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "value", _boundary_name(self.value, "keyword"))

    def __str__(self) -> str:
        return f":{self.value}"


@dataclass(frozen=True, slots=True)
class Symbol:
    """A Corium symbol."""

    value: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "value", _boundary_name(self.value, "symbol"))

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True, slots=True)
class EntityId:
    """An entity reference, distinct from an ordinary Python integer."""

    value: int

    def __post_init__(self) -> None:
        if isinstance(self.value, bool) or not isinstance(self.value, int):
            raise TypeError("entity id must be an integer")
        if self.value < 0 or self.value > (2**64 - 1):
            raise ValueError("entity id must fit an unsigned 64-bit integer")

    def __int__(self) -> int:
        return self.value


@dataclass(frozen=True, slots=True)
class EdnList:
    """An EDN list, distinct from a Python list (which represents a vector)."""

    items: tuple[Any, ...]

    def __init__(self, items: Iterable[Any] = ()) -> None:
        object.__setattr__(self, "items", tuple(items))

    def __iter__(self) -> Iterator[Any]:
        return iter(self.items)

    def __len__(self) -> int:
        return len(self.items)


@dataclass(frozen=True, slots=True)
class Tagged:
    """A custom tagged value whose tag is not reserved by Corium."""

    tag: str
    value: Any

    def __post_init__(self) -> None:
        tag = _boundary_name(self.tag, "tag")
        if tag in _RESERVED_TAGS:
            raise ValueError(
                f"tag {tag!r} is reserved for a dedicated Corium boundary type"
            )
        object.__setattr__(self, "tag", tag)
