"""Immutable builders for Corium Pull patterns."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any

from ._forms import lower_form
from .values import Keyword, Symbol


def _attr_name(value: str | Keyword) -> str:
    if isinstance(value, Keyword):
        return value.value
    if not isinstance(value, str):
        raise TypeError("attribute must be a string or Keyword")
    name = value[1:] if value.startswith(":") else value
    return Keyword(name).value


def _reverse_name(name: str) -> str:
    if "/" in name:
        namespace, local = name.rsplit("/", 1)
        return f"{namespace}/_{local}"
    return f"_{name}"


def _hashable_form(value: Any) -> Any:
    value = lower_form(value)
    if isinstance(value, list):
        return tuple(_hashable_form(item) for item in value)
    if isinstance(value, tuple):
        return tuple(_hashable_form(item) for item in value)
    if isinstance(value, dict):
        return frozenset(
            (_hashable_form(key), _hashable_form(item)) for key, item in value.items()
        )
    if isinstance(value, set):
        return frozenset(_hashable_form(item) for item in value)
    return value


@dataclass(frozen=True, slots=True)
class PullAttr:
    """One Pull attribute with optional reverse, alias, limit, and default."""

    name: str
    is_reverse: bool = False
    alias: Any | None = None
    has_alias: bool = False
    value_limit: int | None = None
    has_limit: bool = False
    default_value: Any | None = None
    has_default: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _attr_name(self.name))

    def reverse(self) -> PullAttr:
        return replace(self, is_reverse=True)

    def as_(self, key: Any) -> PullAttr:
        return replace(self, alias=key, has_alias=True)

    as_key = as_

    def limit(self, count: int) -> PullAttr:
        if isinstance(count, bool) or not isinstance(count, int):
            raise TypeError("Pull limit must be an integer")
        if count < 0:
            raise ValueError("Pull limit must be non-negative")
        return replace(self, value_limit=count, has_limit=True)

    def unlimited(self) -> PullAttr:
        return replace(self, value_limit=None, has_limit=True)

    def default(self, value: Any) -> PullAttr:
        return replace(self, default_value=value, has_default=True)

    def keyword(self) -> Keyword:
        name = _reverse_name(self.name) if self.is_reverse else self.name
        return Keyword(name)

    def to_form(self) -> Any:
        if not self.has_alias and not self.has_limit and not self.has_default:
            return self.keyword()
        result: list[Any] = [self.keyword()]
        if self.has_alias:
            result.extend((Keyword("as"), lower_form(self.alias)))
        if self.has_limit:
            result.extend((Keyword("limit"), self.value_limit))
        if self.has_default:
            result.extend((Keyword("default"), lower_form(self.default_value)))
        return result


def pull_attr(name: str | Keyword) -> PullAttr:
    """Build a Pull attribute selection."""

    return PullAttr(_attr_name(name))


@dataclass(frozen=True, slots=True)
class _PullItem:
    kind: str
    attribute: PullAttr | None = None
    value: Any | None = None

    def to_form(self) -> Any:
        if self.kind == "wildcard":
            return Symbol("*")
        if self.kind == "db_id":
            return Keyword("db/id")
        assert self.attribute is not None
        if self.kind == "attribute":
            return self.attribute.to_form()
        key = _hashable_form(self.attribute.to_form())
        if self.kind == "nested":
            assert isinstance(self.value, Pull)
            return {key: self.value.to_form()}
        if self.kind == "recurse":
            return {key: self.value}
        return {key: Symbol("...")}


@dataclass(frozen=True, slots=True)
class Pull:
    """An immutable Pull pattern."""

    _items: tuple[_PullItem, ...] = ()

    @classmethod
    def new(cls) -> Pull:
        return cls()

    def wildcard(self) -> Pull:
        return replace(self, _items=(*self._items, _PullItem("wildcard")))

    def db_id(self) -> Pull:
        return replace(self, _items=(*self._items, _PullItem("db_id")))

    def attr(self, attribute: str | Keyword | PullAttr) -> Pull:
        spec = attribute if isinstance(attribute, PullAttr) else pull_attr(attribute)
        return replace(self, _items=(*self._items, _PullItem("attribute", spec)))

    spec = attr

    def reverse(self, attribute: str | Keyword) -> Pull:
        return self.attr(pull_attr(attribute).reverse())

    def nested(self, attribute: str | Keyword | PullAttr, pattern: Pull) -> Pull:
        if not isinstance(pattern, Pull):
            raise TypeError("nested pattern must be a Pull")
        spec = attribute if isinstance(attribute, PullAttr) else pull_attr(attribute)
        return replace(
            self,
            _items=(*self._items, _PullItem("nested", spec, pattern)),
        )

    def recurse(self, attribute: str | Keyword | PullAttr, depth: int) -> Pull:
        if isinstance(depth, bool) or not isinstance(depth, int):
            raise TypeError("recursion depth must be an integer")
        if depth < 0:
            raise ValueError("recursion depth must be non-negative")
        spec = attribute if isinstance(attribute, PullAttr) else pull_attr(attribute)
        return replace(self, _items=(*self._items, _PullItem("recurse", spec, depth)))

    def recurse_unbounded(self, attribute: str | Keyword | PullAttr) -> Pull:
        spec = attribute if isinstance(attribute, PullAttr) else pull_attr(attribute)
        return replace(
            self,
            _items=(*self._items, _PullItem("recurse_unbounded", spec)),
        )

    def to_form(self) -> list[Any]:
        return [item.to_form() for item in self._items]
