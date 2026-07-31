"""Immutable builders for Corium transaction data."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any

from ._forms import lower_form
from .values import EntityId, Keyword


def _attr_name(value: str | Keyword) -> str:
    if isinstance(value, Keyword):
        return value.value
    if not isinstance(value, str):
        raise TypeError("attribute must be a string or Keyword")
    name = value[1:] if value.startswith(":") else value
    return Keyword(name).value


@dataclass(frozen=True, slots=True)
class TempId:
    """A named temporary entity id allocated by a transaction."""

    name: str

    def __post_init__(self) -> None:
        if not isinstance(self.name, str):
            raise TypeError("tempid must be a string")
        if not self.name:
            raise ValueError("tempid must not be empty")

    def to_form(self) -> str:
        return self.name


def tempid(name: str) -> TempId:
    return TempId(name)


@dataclass(frozen=True, slots=True)
class LookupRef:
    """A unique-attribute lookup reference."""

    attribute: str
    value: Any

    def __post_init__(self) -> None:
        object.__setattr__(self, "attribute", _attr_name(self.attribute))

    def to_form(self) -> tuple[Any, Any]:
        return Keyword(self.attribute), lower_form(self.value)


def lookup(attribute: str | Keyword, value: Any) -> LookupRef:
    return LookupRef(_attr_name(attribute), value)


def eid(value: int | EntityId) -> EntityId:
    """Build an explicit entity reference."""

    return value if isinstance(value, EntityId) else EntityId(value)


@dataclass(frozen=True, slots=True)
class EntityMap:
    """A map-form transaction entity."""

    entity_id: Any | None = None
    _pairs: tuple[tuple[str, Any], ...] = ()

    @classmethod
    def with_id(cls, entity_id: Any) -> EntityMap:
        return cls(entity_id)

    def set(self, attribute: str | Keyword, value: Any) -> EntityMap:
        return replace(
            self,
            _pairs=(*self._pairs, (_attr_name(attribute), value)),
        )

    def set_many(self, attribute: str | Keyword, values: Any) -> EntityMap:
        if isinstance(values, (str, bytes)):
            raise TypeError("set_many values must be an iterable of values")
        return self.set(attribute, list(values))

    def to_form(self) -> dict[Keyword, Any]:
        result: dict[Keyword, Any] = {}
        if self.entity_id is not None:
            result[Keyword("db/id")] = lower_form(self.entity_id)
        for attribute, value in self._pairs:
            result[Keyword(attribute)] = lower_form(value)
        return result


@dataclass(frozen=True, slots=True)
class TxData:
    """A finalized immutable sequence of transaction forms."""

    forms: tuple[Any, ...] = ()

    def to_form(self) -> list[Any]:
        return [lower_form(form) for form in self.forms]


@dataclass(frozen=True, slots=True)
class TxBuilder:
    """Build entity-map and list-form transaction data."""

    _forms: tuple[Any, ...] = ()

    @classmethod
    def new(cls) -> TxBuilder:
        return cls()

    def entity(self, entity: EntityMap) -> TxBuilder:
        if not isinstance(entity, EntityMap):
            raise TypeError("entity must be an EntityMap")
        return replace(self, _forms=(*self._forms, entity))

    def add(self, entity: Any, attribute: str | Keyword, value: Any) -> TxBuilder:
        return self.form(
            [
                Keyword("db/add"),
                lower_form(entity),
                Keyword(_attr_name(attribute)),
                lower_form(value),
            ]
        )

    def retract(self, entity: Any, attribute: str | Keyword, value: Any) -> TxBuilder:
        return self.form(
            [
                Keyword("db/retract"),
                lower_form(entity),
                Keyword(_attr_name(attribute)),
                lower_form(value),
            ]
        )

    def cas(
        self,
        entity: Any,
        attribute: str | Keyword,
        old: Any,
        new: Any,
    ) -> TxBuilder:
        return self.form(
            [
                Keyword("db/cas"),
                lower_form(entity),
                Keyword(_attr_name(attribute)),
                lower_form(old),
                lower_form(new),
            ]
        )

    def retract_entity(self, entity: Any) -> TxBuilder:
        return self.form([Keyword("db/retractEntity"), lower_form(entity)])

    def form(self, form: Any) -> TxBuilder:
        """Append a raw transaction form as an escape hatch."""

        return replace(self, _forms=(*self._forms, form))

    def build(self) -> TxData:
        return TxData(self._forms)

    def to_form(self) -> list[Any]:
        """Lower directly for APIs that receive an unfinished builder."""

        return self.build().to_form()
