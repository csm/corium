from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from typing import Any

from corium import (
    ClosedError,
    Datom,
    DbStats,
    EntityId,
    Index,
    Keyword,
    LocalPeer,
    Peer,
    RemotePeer,
    Symbol,
    Tagged,
)
from corium._api import _TxReportData, _View


class FakeDbBackend:
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
        self.calls.append(("query", view, query, args, fuel))
        return [["Ada"]]

    async def pull(self, view: _View, pattern: Any, entity: Any) -> Any:
        self.calls.append(("pull", view, pattern, entity))
        return {Keyword("person/name"): "Ada"}

    async def datoms(
        self,
        view: _View,
        index: Index,
        components: tuple[Any, ...],
        limit: int,
    ) -> list[Datom]:
        self.calls.append(("datoms", view, index, components, limit))
        return [Datom(1, 2, "Ada", 3, True)]

    async def stats(self, view: _View) -> DbStats:
        self.calls.append(("stats", view))
        return DbStats(basis_t=7, datoms=5, entities=2, attributes=3)


class FakePeerBackend:
    database_name = "people"

    def __init__(self) -> None:
        self.db_backend = FakeDbBackend()
        self.close_count = 0
        self.transactions: list[Any] = []

    async def db(self) -> FakeDbBackend:
        return self.db_backend

    async def sync(self) -> FakeDbBackend:
        return self.db_backend

    async def transact(self, tx_data: Any) -> _TxReportData:
        self.transactions.append(tx_data)
        return _TxReportData(
            basis_before=6,
            basis_t=7,
            tx_instant=datetime(2026, 1, 1, tzinfo=timezone.utc),
            tempids={"ada": 42},
            db_after=self.db_backend,
        )

    async def close(self) -> None:
        self.close_count += 1


class PeerApiTests(unittest.IsolatedAsyncioTestCase):
    async def test_local_and_remote_satisfy_the_same_protocol(self) -> None:
        for peer_type in (LocalPeer, RemotePeer):
            backend = FakePeerBackend()
            peer = peer_type._from_backend(backend)
            self.assertIsInstance(peer, Peer)
            self.assertEqual(peer.database_name, "people")

            db = await peer.db()
            self.assertEqual(await db.query(["query"], "Ada", fuel=20), [["Ada"]])
            report = await peer.transact([{"person/name": "Ada"}])
            self.assertEqual(report.tempids, {"ada": 42})
            self.assertEqual(await report.db_after.basis_t(), 7)

    async def test_database_views_are_immutable_and_forward_raw_operations(self) -> None:
        backend = FakePeerBackend()
        db = await LocalPeer._from_backend(backend).db()
        viewed = db.as_of(4).history()

        self.assertIsNot(viewed, db)
        self.assertEqual(db._view, _View())
        self.assertEqual(viewed._view, _View("history"))
        self.assertEqual(
            await db.pull([Keyword("person/name")], EntityId(1)),
            {Keyword("person/name"): "Ada"},
        )
        self.assertEqual(
            await db.datoms("eavt", EntityId(1), limit=1),
            [Datom(1, 2, "Ada", 3, True)],
        )
        self.assertEqual(await db.stats(), DbStats(7, 5, 2, 3))

    async def test_context_manager_closes_exactly_once(self) -> None:
        backend = FakePeerBackend()
        peer = RemotePeer._from_backend(backend)

        async with peer as entered:
            self.assertIs(entered, peer)

        await peer.close()
        self.assertEqual(backend.close_count, 1)
        with self.assertRaises(ClosedError):
            await peer.db()

    async def test_query_and_scan_limits_reject_invalid_values(self) -> None:
        db = await LocalPeer._from_backend(FakePeerBackend()).db()
        with self.assertRaises(ValueError):
            await db.query(["query"], fuel=-1)
        with self.assertRaises(TypeError):
            await db.datoms(Index.EAVT, limit=True)
        with self.assertRaises(ValueError):
            await db.datoms("nope")


class ValueAndTimeTests(unittest.IsolatedAsyncioTestCase):
    def test_boundary_wrappers_remain_distinct_from_builtins(self) -> None:
        self.assertNotEqual(Keyword("person/name"), "person/name")
        self.assertNotEqual(EntityId(42), 42)
        self.assertEqual(str(Keyword("person/name")), ":person/name")
        self.assertEqual(str(Symbol("name")), "name")
        self.assertEqual(Tagged("custom/tag", [1]).tag, "custom/tag")
        with self.assertRaises(ValueError):
            Keyword(":person/name")
        with self.assertRaises(ValueError):
            EntityId(-1)

    async def test_wall_clock_views_require_aware_datetimes(self) -> None:
        database = await LocalPeer._from_backend(FakePeerBackend()).db()
        with self.assertRaises(ValueError):
            database.as_of_instant(datetime(2026, 1, 1))

        aware = datetime(
            2026, 1, 1, 0, 0, 0, 123000, tzinfo=timezone(timedelta(hours=-8))
        )
        viewed = database.as_of_instant(aware)
        expected = int(aware.timestamp() * 1000)
        self.assertEqual(viewed._view, _View("as_of_instant", expected))


if __name__ == "__main__":
    unittest.main()
