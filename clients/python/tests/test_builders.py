from __future__ import annotations

import unittest
from datetime import datetime, timezone
from typing import Any

from corium import (
    Binding,
    EdnList,
    EntityId,
    EntityMap,
    Keyword,
    LocalPeer,
    Pull,
    Query,
    RuleDefinition,
    RuleSet,
    Symbol,
    TxBuilder,
    attr,
    avg,
    call,
    count,
    data,
    eid,
    gte,
    lit,
    lookup,
    not_,
    not_join,
    or_,
    or_join,
    pull,
    pull_attr,
    rule,
    tempid,
    var,
)
from corium._api import DbStats, _TxReportData, _View


class QueryBuilderTests(unittest.TestCase):
    def test_proposed_query_shape_is_immutable_and_lowers_to_boundary_forms(
        self,
    ) -> None:
        base = Query.find("?name")
        query = (
            base.in_scalar("?min")
            .where("?e", ":person/name", "?name")
            .where(data("?e", attr("person/age"), "?age"))
            .where(gte("?age", "?min"))
        )

        self.assertEqual(base.to_form()[Keyword("where")], [])
        self.assertEqual(
            query.to_form(),
            {
                Keyword("find"): [Symbol("?name")],
                Keyword("in"): [Symbol("$"), Symbol("?min")],
                Keyword("where"): [
                    [Symbol("?e"), Keyword("person/name"), Symbol("?name")],
                    [Symbol("?e"), Keyword("person/age"), Symbol("?age")],
                    [EdnList((Symbol(">="), Symbol("?age"), Symbol("?min")))],
                ],
            },
        )
        self.assertTrue(query.has_inputs())

    def test_find_shapes_pull_aggregates_functions_and_boolean_clauses(
        self,
    ) -> None:
        pattern = Pull().attr("person/name")
        query = (
            Query.find(
                pull("?e", pattern),
                count("?friend"),
                avg("?age"),
            )
            .with_("?e")
            .where(
                or_(
                    data("?e", ":person/name", lit("?literal")),
                    (
                        data("?e", ":person/friend", "?friend"),
                        not_(data("?friend", ":person/blocked", True)),
                    ),
                )
            )
            .where(call("+", "?age", 1).bind(Binding.scalar("?next_age")))
        )

        form = query.to_form()
        self.assertEqual(
            form[Keyword("find")][0],
            EdnList(
                (
                    Symbol("pull"),
                    Symbol("?e"),
                    [Keyword("person/name")],
                )
            ),
        )
        self.assertEqual(
            form[Keyword("find")][1],
            EdnList((Symbol("count"), Symbol("?friend"))),
        )
        self.assertEqual(form[Keyword("with")], [Symbol("?e")])
        self.assertEqual(
            form[Keyword("where")][-1],
            [
                EdnList((Symbol("+"), Symbol("?age"), 1)),
                Symbol("?next_age"),
            ],
        )

        self.assertEqual(
            Query.find_collection("?name").to_form()[Keyword("find")],
            [[Symbol("?name"), Symbol("...")]],
        )
        self.assertEqual(
            Query.find_tuple("?name", "?age").to_form()[Keyword("find")],
            [[Symbol("?name"), Symbol("?age")]],
        )
        self.assertEqual(
            Query.find_scalar("?name").to_form()[Keyword("find")],
            [Symbol("?name"), Symbol(".")],
        )

    def test_rules_lower_as_a_positional_query_argument(self) -> None:
        adult = (
            RuleDefinition.define("adult", "?entity")
            .where("?entity", ":person/age", "?age")
            .where(gte("?age", 21))
        )
        required = RuleDefinition.define(
            "friend-of", "?friend", required=("?entity",)
        ).where("?entity", ":person/friend", "?friend")
        rules = RuleSet().add(adult).add(required)
        query = (
            Query.find("?name")
            .in_rules()
            .where(rule("adult", "?entity"))
            .where("?entity", ":person/name", "?name")
        )

        self.assertEqual(query.to_form()[Keyword("in")], [Symbol("$"), Symbol("%")])
        self.assertEqual(
            rules.to_form()[0][0],
            EdnList((Symbol("adult"), Symbol("?entity"))),
        )
        self.assertEqual(
            rules.to_form()[1][0],
            EdnList(
                (
                    Symbol("friend-of"),
                    [Symbol("?entity")],
                    Symbol("?friend"),
                )
            ),
        )

    def test_database_and_collection_inputs_join_clauses_and_history_terms(
        self,
    ) -> None:
        query = (
            Query.find("?entity")
            .in_db("history")
            .in_tuple("?low", "?high")
            .in_collection("?names")
            .in_relation("?name", "?age")
            .where(
                data("?entity", ":person/name", "?name")
                .source("history")
                .added("?added")
            )
            .where(
                not_join(
                    ("?entity",),
                    data("?entity", ":person/hidden", True),
                )
            )
            .where(
                or_join(
                    ("?entity",),
                    data("?entity", ":person/name", "Ada"),
                    (
                        data("?entity", ":person/age", "?age"),
                        gte("?age", "?low"),
                    ),
                )
            )
        )

        form = query.to_form()
        self.assertEqual(
            form[Keyword("in")],
            [
                Symbol("$"),
                Symbol("$history"),
                [Symbol("?low"), Symbol("?high")],
                [Symbol("?names"), Symbol("...")],
                [[Symbol("?name"), Symbol("?age")]],
            ],
        )
        self.assertEqual(
            form[Keyword("where")][0],
            [
                Symbol("$history"),
                Symbol("?entity"),
                Keyword("person/name"),
                Symbol("?name"),
                Symbol("_"),
                Symbol("?added"),
            ],
        )
        self.assertEqual(
            form[Keyword("where")][1],
            EdnList(
                (
                    Symbol("not-join"),
                    [Symbol("?entity")],
                    [
                        Symbol("?entity"),
                        Keyword("person/hidden"),
                        True,
                    ],
                )
            ),
        )
        self.assertEqual(form[Keyword("where")][2].items[0], Symbol("or-join"))

    def test_invalid_builder_inputs_fail_early(self) -> None:
        with self.assertRaises(ValueError):
            var("?")
        with self.assertRaises(TypeError):
            Query.find(42)
        with self.assertRaises(TypeError):
            Query.find("?e").where("?e", ":person/name")
        with self.assertRaises(ValueError):
            Query.find()
        with self.assertRaises(ValueError):
            or_()


class PullBuilderTests(unittest.TestCase):
    def test_pull_supports_options_nesting_reverse_refs_and_recursion(self) -> None:
        base = Pull().attr("person/name")
        pattern = (
            base.db_id()
            .wildcard()
            .reverse("person/manager")
            .nested("person/friends", Pull().attr("person/name"))
            .recurse("person/reports", 3)
            .recurse_unbounded("person/mentor")
            .attr(
                pull_attr("person/email").as_(Keyword("email")).limit(2).default("n/a")
            )
        )

        self.assertEqual(base.to_form(), [Keyword("person/name")])
        self.assertEqual(
            pattern.to_form(),
            [
                Keyword("person/name"),
                Keyword("db/id"),
                Symbol("*"),
                Keyword("person/_manager"),
                {Keyword("person/friends"): [Keyword("person/name")]},
                {Keyword("person/reports"): 3},
                {Keyword("person/mentor"): Symbol("...")},
                [
                    Keyword("person/email"),
                    Keyword("as"),
                    Keyword("email"),
                    Keyword("limit"),
                    2,
                    Keyword("default"),
                    "n/a",
                ],
            ],
        )
        self.assertEqual(
            Pull().attr(pull_attr("person/friends").unlimited()).to_form(),
            [
                [
                    Keyword("person/friends"),
                    Keyword("limit"),
                    None,
                ]
            ],
        )
        self.assertEqual(
            Pull().attr(pull_attr("person/name").as_(None)).to_form(),
            [[Keyword("person/name"), Keyword("as"), None]],
        )


class TransactionBuilderTests(unittest.TestCase):
    def test_entity_and_list_forms_lower_without_mutating_the_builder(self) -> None:
        base = TxBuilder()
        transaction = (
            base.entity(
                EntityMap.with_id(tempid("alice"))
                .set("person/name", "Alice")
                .set_many("person/aliases", ("Al", "A"))
            )
            .add(
                tempid("alice"),
                "person/friend",
                lookup("person/name", "Bob"),
            )
            .retract(eid(42), "person/active", False)
            .cas(lookup("person/name", "Alice"), "person/age", 39, 40)
            .retract_entity(EntityId(99))
            .build()
        )

        self.assertEqual(base.build().to_form(), [])
        self.assertEqual(
            transaction.to_form(),
            [
                {
                    Keyword("db/id"): "alice",
                    Keyword("person/name"): "Alice",
                    Keyword("person/aliases"): ["Al", "A"],
                },
                [
                    Keyword("db/add"),
                    "alice",
                    Keyword("person/friend"),
                    (Keyword("person/name"), "Bob"),
                ],
                [
                    Keyword("db/retract"),
                    EntityId(42),
                    Keyword("person/active"),
                    False,
                ],
                [
                    Keyword("db/cas"),
                    (Keyword("person/name"), "Alice"),
                    Keyword("person/age"),
                    39,
                    40,
                ],
                [Keyword("db/retractEntity"), EntityId(99)],
            ],
        )


class _BuilderDbBackend:
    database_name = "people"

    def __init__(self) -> None:
        self.calls: list[tuple[Any, ...]] = []

    async def query(
        self,
        view: _View,
        query: Any,
        args: tuple[Any, ...],
        fuel: int | None,
    ) -> Any:
        self.calls.append(("query", query, args))
        return []

    async def pull(self, view: _View, pattern: Any, entity: Any) -> Any:
        self.calls.append(("pull", pattern, entity))
        return {}

    async def datoms(
        self,
        view: _View,
        index: Any,
        components: tuple[Any, ...],
        limit: int,
    ) -> list[Any]:
        return []

    async def stats(self, view: _View) -> DbStats:
        return DbStats(0, 0, 0, 0)


class _BuilderPeerBackend:
    database_name = "people"

    def __init__(self) -> None:
        self.db_backend = _BuilderDbBackend()
        self.transaction: Any = None

    async def db(self) -> _BuilderDbBackend:
        return self.db_backend

    async def sync(self) -> _BuilderDbBackend:
        return self.db_backend

    async def transact(self, tx_data: Any) -> _TxReportData:
        self.transaction = tx_data
        return _TxReportData(
            0,
            1,
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            {},
            self.db_backend,
        )

    async def close(self) -> None:
        pass


class BuilderApiIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_public_operations_lower_builders_before_calling_backends(
        self,
    ) -> None:
        backend = _BuilderPeerBackend()
        peer = LocalPeer._from_backend(backend)
        db = await peer.db()

        query = Query.find("?name").in_rules().where("?e", ":person/name", "?name")
        rules = RuleSet().add(
            RuleDefinition.define("named", "?e").where("?e", ":person/name", "_")
        )
        await db.query(query, rules)
        await db.pull(Pull().attr("person/name"), lookup("person/name", "Ada"))
        await peer.transact(
            TxBuilder().entity(
                EntityMap.with_id(tempid("ada")).set("person/name", "Ada")
            )
        )

        self.assertIsInstance(backend.db_backend.calls[0][1], dict)
        self.assertIsInstance(backend.db_backend.calls[0][2][0], list)
        self.assertEqual(
            backend.db_backend.calls[1],
            (
                "pull",
                [Keyword("person/name")],
                (Keyword("person/name"), "Ada"),
            ),
        )
        self.assertEqual(
            backend.transaction,
            [
                {
                    Keyword("db/id"): "ada",
                    Keyword("person/name"): "Ada",
                }
            ],
        )
