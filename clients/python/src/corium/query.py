"""Immutable, typed builders for Corium Datalog queries and rules."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, replace
from typing import Any

from ._forms import Form, lower_form
from .pull import Pull
from .values import EdnList, Keyword, Symbol


def _nonempty_name(value: str, kind: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{kind} must be a string")
    if not value or any(char.isspace() for char in value):
        raise ValueError(f"{kind} must be non-empty and contain no whitespace")
    return value


def _keyword_name(value: str | Keyword) -> str:
    if isinstance(value, Keyword):
        return value.value
    name = _nonempty_name(value, "attribute")
    return name[1:] if name.startswith(":") else name


@dataclass(frozen=True, slots=True)
class Var:
    """A logic variable such as ``?entity``."""

    name: str

    def __post_init__(self) -> None:
        name = _nonempty_name(self.name, "variable")
        if not name.startswith("?"):
            name = f"?{name}"
        if name == "?":
            raise ValueError("variable name must not be empty")
        object.__setattr__(self, "name", name)

    def to_form(self) -> Symbol:
        return Symbol(self.name)


@dataclass(frozen=True, slots=True)
class Literal:
    """A query literal, useful when a string begins with ``?`` or ``:``."""

    value: Any

    def to_form(self) -> Any:
        return lower_form(self.value)


@dataclass(frozen=True, slots=True)
class Blank:
    """The Datalog blank term ``_``."""

    def to_form(self) -> Symbol:
        return Symbol("_")


def var(name: str) -> Var:
    """Build a logic variable."""

    return Var(name)


def attr(name: str | Keyword) -> Keyword:
    """Build an attribute keyword."""

    return Keyword(_keyword_name(name))


kw = attr


def lit(value: Any) -> Literal:
    """Force a value to be treated as a literal query term."""

    return Literal(value)


def blank() -> Blank:
    """Build the blank term."""

    return Blank()


def _as_var(value: str | Symbol | Var) -> Var:
    if isinstance(value, Var):
        return value
    if isinstance(value, Symbol):
        if not value.value.startswith("?"):
            raise ValueError(f"expected a variable, got {value}")
        return Var(value.value)
    return Var(value)


def _term_form(value: Any) -> Any:
    if isinstance(value, str):
        if value.startswith("?"):
            return Symbol(value)
        if value.startswith(":"):
            return Keyword(value[1:])
        if value == "_":
            return Symbol("_")
        return value
    return lower_form(value)


class Clause(Form):
    """Base type for typed ``:where`` clauses."""


@dataclass(frozen=True, slots=True)
class Pattern(Clause):
    """A data pattern with optional source, transaction, and assertion terms."""

    entity: Any
    attribute: Any
    value: Any
    source_name: str | None = None
    transaction: Any | None = None
    assertion: Any | None = None

    def source(self, name: str) -> Pattern:
        """Read the pattern from a named database source such as ``$history``."""

        name = _nonempty_name(name, "source")
        if not name.startswith("$"):
            name = f"${name}"
        return replace(self, source_name=name)

    def tx(self, value: Any) -> Pattern:
        """Bind the transaction position."""

        return replace(self, transaction=value)

    def added(self, value: Any) -> Pattern:
        """Bind the assertion flag on a history database."""

        return replace(self, assertion=value)

    def to_form(self) -> list[Any]:
        items: list[Any] = []
        if self.source_name is not None:
            items.append(Symbol(self.source_name))
        items.extend(
            (
                _term_form(self.entity),
                _term_form(self.attribute),
                _term_form(self.value),
            )
        )
        if self.transaction is not None or self.assertion is not None:
            items.append(
                Symbol("_")
                if self.transaction is None
                else _term_form(self.transaction)
            )
        if self.assertion is not None:
            items.append(_term_form(self.assertion))
        return items


def data(entity: Any, attribute: Any, value: Any) -> Pattern:
    """Build a data pattern ``[entity attribute value]``."""

    return Pattern(entity, attribute, value)


@dataclass(frozen=True, slots=True)
class Predicate(Clause):
    name: str
    args: tuple[Any, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _nonempty_name(self.name, "predicate"))

    def to_form(self) -> list[EdnList]:
        return [EdnList((Symbol(self.name), *(_term_form(arg) for arg in self.args)))]


def pred(name: str, *args: Any) -> Predicate:
    """Build a predicate clause."""

    return Predicate(name, tuple(args))


def gt(left: Any, right: Any) -> Predicate:
    return pred(">", left, right)


def lt(left: Any, right: Any) -> Predicate:
    return pred("<", left, right)


def gte(left: Any, right: Any) -> Predicate:
    return pred(">=", left, right)


def lte(left: Any, right: Any) -> Predicate:
    return pred("<=", left, right)


def eq(left: Any, right: Any) -> Predicate:
    return pred("=", left, right)


def neq(left: Any, right: Any) -> Predicate:
    return pred("not=", left, right)


@dataclass(frozen=True, slots=True)
class Binding:
    """The output binding of a query function call."""

    kind: str
    variables: tuple[Var, ...]

    @classmethod
    def scalar(cls, variable: str | Symbol | Var) -> Binding:
        return cls("scalar", (_as_var(variable),))

    @classmethod
    def tuple(cls, *variables: str | Symbol | Var) -> Binding:
        return cls("tuple", tuple(_as_var(item) for item in variables))

    @classmethod
    def collection(cls, variable: str | Symbol | Var) -> Binding:
        return cls("collection", (_as_var(variable),))

    @classmethod
    def relation(cls, *variables: str | Symbol | Var) -> Binding:
        return cls("relation", tuple(_as_var(item) for item in variables))

    def __post_init__(self) -> None:
        if self.kind not in {"scalar", "tuple", "collection", "relation"}:
            raise ValueError(f"unknown binding kind: {self.kind}")
        if not self.variables:
            raise ValueError("a binding must contain at least one variable")

    def to_form(self) -> Any:
        variables = [item.to_form() for item in self.variables]
        if self.kind == "scalar":
            return variables[0]
        if self.kind == "tuple":
            return variables
        if self.kind == "collection":
            return [variables[0], Symbol("...")]
        return [variables]


@dataclass(frozen=True, slots=True)
class Function(Clause):
    name: str
    args: tuple[Any, ...]
    binding: Binding

    def to_form(self) -> list[Any]:
        invocation = EdnList(
            (Symbol(self.name), *(_term_form(arg) for arg in self.args))
        )
        return [invocation, self.binding.to_form()]


@dataclass(frozen=True, slots=True)
class Call:
    """A query function call waiting for its output binding."""

    name: str
    args: tuple[Any, ...]

    def bind(self, binding: Binding) -> Function:
        return Function(self.name, self.args, binding)

    def bind_scalar(self, variable: str | Symbol | Var) -> Function:
        return self.bind(Binding.scalar(variable))


def call(name: str, *args: Any) -> Call:
    """Build a query function call."""

    return Call(_nonempty_name(name, "function"), tuple(args))


@dataclass(frozen=True, slots=True)
class NotClause(Clause):
    clauses: tuple[Clause, ...]
    variables: tuple[Var, ...] | None = None

    def to_form(self) -> EdnList:
        items: list[Any]
        if self.variables is None:
            items = [Symbol("not")]
        else:
            items = [
                Symbol("not-join"),
                [variable.to_form() for variable in self.variables],
            ]
        items.extend(clause.to_form() for clause in self.clauses)
        return EdnList(items)


def _clauses(items: Iterable[Clause]) -> tuple[Clause, ...]:
    clauses = tuple(items)
    if not clauses or not all(isinstance(item, Clause) for item in clauses):
        raise TypeError("expected one or more query clauses")
    return clauses


def not_(*clauses: Clause) -> NotClause:
    """Build a ``not`` clause."""

    return NotClause(_clauses(clauses))


def not_join(variables: Iterable[str | Symbol | Var], *clauses: Clause) -> NotClause:
    """Build a ``not-join`` clause."""

    return NotClause(
        _clauses(clauses), tuple(_as_var(variable) for variable in variables)
    )


@dataclass(frozen=True, slots=True)
class OrClause(Clause):
    branches: tuple[tuple[Clause, ...], ...]
    variables: tuple[Var, ...] | None = None

    def to_form(self) -> EdnList:
        items: list[Any]
        if self.variables is None:
            items = [Symbol("or")]
        else:
            items = [
                Symbol("or-join"),
                [variable.to_form() for variable in self.variables],
            ]
        for branch in self.branches:
            if len(branch) == 1:
                items.append(branch[0].to_form())
            else:
                items.append(
                    EdnList(
                        (
                            Symbol("and"),
                            *(clause.to_form() for clause in branch),
                        )
                    )
                )
        return EdnList(items)


def _branches(
    items: Iterable[Clause | Iterable[Clause]],
) -> tuple[tuple[Clause, ...], ...]:
    branches: list[tuple[Clause, ...]] = []
    for item in items:
        if isinstance(item, Clause):
            branches.append((item,))
        else:
            branches.append(_clauses(item))
    if not branches:
        raise ValueError("or requires at least one branch")
    return tuple(branches)


def or_(*branches: Clause | Iterable[Clause]) -> OrClause:
    """Build an ``or`` clause; pass a sequence for a multi-clause branch."""

    return OrClause(_branches(branches))


def or_join(
    variables: Iterable[str | Symbol | Var],
    *branches: Clause | Iterable[Clause],
) -> OrClause:
    """Build an ``or-join`` clause."""

    return OrClause(
        _branches(branches), tuple(_as_var(variable) for variable in variables)
    )


@dataclass(frozen=True, slots=True)
class RuleCall(Clause):
    name: str
    args: tuple[Any, ...]

    def to_form(self) -> EdnList:
        return EdnList((Symbol(self.name), *(_term_form(arg) for arg in self.args)))


def rule(name: str, *args: Any) -> RuleCall:
    """Invoke a rule from a query or another rule."""

    return RuleCall(_nonempty_name(name, "rule"), tuple(args))


@dataclass(frozen=True, slots=True)
class PullExpr:
    """A Pull expression in a query ``:find`` clause."""

    entity: Var
    pattern: Pull

    def to_form(self) -> EdnList:
        return EdnList((Symbol("pull"), self.entity.to_form(), self.pattern.to_form()))


def pull(entity: str | Symbol | Var, pattern: Pull) -> PullExpr:
    """Build a Pull expression for a query ``:find`` clause."""

    return PullExpr(_as_var(entity), pattern)


@dataclass(frozen=True, slots=True)
class Aggregate:
    """An aggregate expression such as ``(count ?entity)``."""

    operator: str
    variable: Var
    limit: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "operator", _nonempty_name(self.operator, "aggregate"))
        if self.limit is not None:
            if isinstance(self.limit, bool) or not isinstance(self.limit, int):
                raise TypeError("aggregate limit must be an integer")
            if self.limit < 0:
                raise ValueError("aggregate limit must be non-negative")

    def to_form(self) -> EdnList:
        items: list[Any] = [Symbol(self.operator)]
        if self.limit is not None:
            items.append(self.limit)
        items.append(self.variable.to_form())
        return EdnList(items)


def aggregate(
    operator: str, variable: str | Symbol | Var, *, limit: int | None = None
) -> Aggregate:
    return Aggregate(operator, _as_var(variable), limit)


def count(variable: str | Symbol | Var) -> Aggregate:
    return aggregate("count", variable)


def count_distinct(variable: str | Symbol | Var) -> Aggregate:
    return aggregate("count-distinct", variable)


def sum_(variable: str | Symbol | Var) -> Aggregate:
    return aggregate("sum", variable)


def avg(variable: str | Symbol | Var) -> Aggregate:
    return aggregate("avg", variable)


def min_(variable: str | Symbol | Var, *, limit: int | None = None) -> Aggregate:
    return aggregate("min", variable, limit=limit)


def max_(variable: str | Symbol | Var, *, limit: int | None = None) -> Aggregate:
    return aggregate("max", variable, limit=limit)


def _find_element(value: Any) -> Any:
    if isinstance(value, (Var, PullExpr, Aggregate)):
        return value.to_form()
    if isinstance(value, Symbol):
        return _as_var(value).to_form()
    if isinstance(value, str):
        return _as_var(value).to_form()
    raise TypeError("find elements must be variables, Pull expressions, or aggregates")


def _find_args(values: tuple[Any, ...]) -> tuple[Any, ...]:
    if (
        len(values) == 1
        and not isinstance(values[0], (str, Symbol, Var, PullExpr, Aggregate))
        and isinstance(values[0], Iterable)
    ):
        return tuple(values[0])
    return values


def _validated_find_args(values: tuple[Any, ...]) -> tuple[Any, ...]:
    values = _find_args(values)
    if not values:
        raise ValueError("find requires at least one element")
    for value in values:
        _find_element(value)
    return values


@dataclass(frozen=True, slots=True)
class Query:
    """An immutable Datalog query builder."""

    _shape: str
    _find: tuple[Any, ...]
    _with: tuple[Var, ...] = ()
    _inputs: tuple[tuple[str, Any], ...] = ()
    _where: tuple[Clause, ...] = ()

    @classmethod
    def find(cls, *elements: Any) -> Query:
        """Build a relation query."""

        return cls("relation", _validated_find_args(elements))

    @classmethod
    def find_collection(cls, element: Any) -> Query:
        """Build a collection query."""

        _find_element(element)
        return cls("collection", (element,))

    find_coll = find_collection

    @classmethod
    def find_tuple(cls, *elements: Any) -> Query:
        """Build a single-tuple query."""

        return cls("tuple", _validated_find_args(elements))

    @classmethod
    def find_scalar(cls, element: Any) -> Query:
        """Build a scalar query."""

        _find_element(element)
        return cls("scalar", (element,))

    def with_(self, *variables: str | Symbol | Var) -> Query:
        """Add aggregate grouping variables that are omitted from ``:find``."""

        return replace(
            self,
            _with=(
                *self._with,
                *(_as_var(variable) for variable in variables),
            ),
        )

    def in_db(self, name: str) -> Query:
        """Declare an additional database source."""

        name = _nonempty_name(name, "database source")
        if not name.startswith("$"):
            name = f"${name}"
        return replace(self, _inputs=(*self._inputs, ("db", name)))

    def in_rules(self) -> Query:
        """Declare a positional rule-set input."""

        return replace(self, _inputs=(*self._inputs, ("rules", None)))

    def in_scalar(self, variable: str | Symbol | Var) -> Query:
        return replace(self, _inputs=(*self._inputs, ("scalar", _as_var(variable))))

    def in_tuple(self, *variables: str | Symbol | Var) -> Query:
        if not variables:
            raise ValueError("tuple input requires at least one variable")
        return replace(
            self,
            _inputs=(
                *self._inputs,
                ("tuple", tuple(_as_var(item) for item in variables)),
            ),
        )

    def in_collection(self, variable: str | Symbol | Var) -> Query:
        return replace(
            self,
            _inputs=(*self._inputs, ("collection", _as_var(variable))),
        )

    in_coll = in_collection

    def in_relation(self, *variables: str | Symbol | Var) -> Query:
        if not variables:
            raise ValueError("relation input requires at least one variable")
        return replace(
            self,
            _inputs=(
                *self._inputs,
                ("relation", tuple(_as_var(item) for item in variables)),
            ),
        )

    in_rel = in_relation

    def where(self, *clause_or_terms: Any) -> Query:
        """Add a clause, or three terms that form a data pattern."""

        if len(clause_or_terms) == 1 and isinstance(clause_or_terms[0], Clause):
            clause = clause_or_terms[0]
        elif len(clause_or_terms) == 3:
            clause = data(*clause_or_terms)
        else:
            raise TypeError("where expects one clause or entity, attribute, value")
        return replace(self, _where=(*self._where, clause))

    and_where = where

    def has_inputs(self) -> bool:
        """Whether positional non-database inputs must be supplied."""

        return any(kind != "db" for kind, _ in self._inputs)

    def to_form(self) -> dict[Keyword, Any]:
        find = [_find_element(item) for item in self._find]
        if self._shape == "collection":
            find = [[find[0], Symbol("...")]]
        elif self._shape == "tuple":
            find = [find]
        elif self._shape == "scalar":
            find.append(Symbol("."))

        result: dict[Keyword, Any] = {Keyword("find"): find}
        if self._with:
            result[Keyword("with")] = [item.to_form() for item in self._with]
        if self._inputs:
            inputs: list[Any] = [Symbol("$")]
            for kind, value in self._inputs:
                if kind == "db":
                    inputs.append(Symbol(value))
                elif kind == "rules":
                    inputs.append(Symbol("%"))
                elif kind == "scalar":
                    inputs.append(value.to_form())
                elif kind == "tuple":
                    inputs.append([item.to_form() for item in value])
                elif kind == "collection":
                    inputs.append([value.to_form(), Symbol("...")])
                else:
                    inputs.append([[item.to_form() for item in value]])
            result[Keyword("in")] = inputs
        result[Keyword("where")] = [clause.to_form() for clause in self._where]
        return result


@dataclass(frozen=True, slots=True)
class RuleDefinition:
    """One immutable rule definition."""

    name: str
    free: tuple[Var, ...]
    required: tuple[Var, ...] = ()
    clauses: tuple[Clause, ...] = ()

    @classmethod
    def define(
        cls,
        name: str,
        *free: str | Symbol | Var,
        required: Iterable[str | Symbol | Var] = (),
    ) -> RuleDefinition:
        return cls(
            _nonempty_name(name, "rule"),
            tuple(_as_var(item) for item in free),
            tuple(_as_var(item) for item in required),
        )

    def where(self, *clause_or_terms: Any) -> RuleDefinition:
        if len(clause_or_terms) == 1 and isinstance(clause_or_terms[0], Clause):
            clause = clause_or_terms[0]
        elif len(clause_or_terms) == 3:
            clause = data(*clause_or_terms)
        else:
            raise TypeError("where expects one clause or entity, attribute, value")
        return replace(self, clauses=(*self.clauses, clause))

    and_where = where

    def to_form(self) -> list[Any]:
        head: list[Any] = [Symbol(self.name)]
        if self.required:
            head.append([item.to_form() for item in self.required])
        head.extend(item.to_form() for item in self.free)
        return [EdnList(head), *(clause.to_form() for clause in self.clauses)]


@dataclass(frozen=True, slots=True)
class RuleSet:
    """A positional query argument containing rule definitions."""

    definitions: tuple[RuleDefinition, ...] = ()

    def add(self, definition: RuleDefinition) -> RuleSet:
        if not isinstance(definition, RuleDefinition):
            raise TypeError("definition must be a RuleDefinition")
        return replace(self, definitions=(*self.definitions, definition))

    def to_form(self) -> list[Any]:
        return [definition.to_form() for definition in self.definitions]
