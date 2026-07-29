from __future__ import annotations

import importlib
import os
import unittest
import uuid
from datetime import datetime, timezone
from typing import Any

from corium import EntityId, Keyword, Symbol, Tagged

_corium: Any
try:
    _corium = importlib.import_module("corium._corium")
except ImportError:
    _corium = None


@unittest.skipIf(_corium is None, "native extension is not built")
class NativeBoundaryTests(unittest.TestCase):
    def test_every_boundary_value_round_trips_losslessly(self) -> None:
        instant = datetime(2026, 7, 28, 12, 34, 56, 789000, tzinfo=timezone.utc)
        before_epoch = datetime(1960, 1, 2, 3, 4, 5, 678000, tzinfo=timezone.utc)
        identifier = uuid.UUID("12345678-1234-5678-1234-567812345678")
        values = [
            None,
            False,
            True,
            -(2**63),
            2**63 - 1,
            -0.0,
            "hello",
            b"\x00\xff",
            Keyword("person/name"),
            Symbol("?name"),
            EntityId(42),
            instant,
            before_epoch,
            identifier,
            [Keyword("find"), Symbol("?name")],
            frozenset({Keyword("person/name"), "Ada"}),
            {Keyword("person/name"): "Ada"},
            Tagged("custom/tag", [1, "two"]),
        ]
        for value in values:
            with self.subTest(value=value):
                self.assertEqual(_corium._roundtrip(value), value)

    def test_reserved_tags_cannot_collide_with_native_boundary_types(self) -> None:
        for tag, value in (
            ("bytes", "00ff"),
            ("eid", 42),
            ("inst", 0),
            ("uuid", "12345678123456781234567812345678"),
        ):
            tagged = object.__new__(Tagged)
            object.__setattr__(tagged, "tag", tag)
            object.__setattr__(tagged, "value", value)
            with self.subTest(tag=tag):
                with self.assertRaisesRegex(ValueError, "reserved"):
                    _corium._roundtrip(tagged)

    def test_datetime_requires_millisecond_precision(self) -> None:
        with self.assertRaisesRegex(ValueError, "millisecond precision"):
            _corium._roundtrip(
                datetime(2026, 7, 28, 12, 34, 56, 789123, tzinfo=timezone.utc)
            )


@unittest.skipIf(_corium is None, "native extension is not built")
class NativeConnectionTests(unittest.IsolatedAsyncioTestCase):
    async def test_local_peer_connection_failures_use_the_public_error_model(
        self,
    ) -> None:
        from corium import ConnectionFailedError

        with self.assertRaisesRegex(
            ConnectionFailedError, "at least one transactor endpoint"
        ):
            await _corium.connect_local(
                [],
                database="people",
                token=None,
                tls=False,
            )


async def _exercise_live_peer(test: unittest.TestCase, peer: Any) -> None:
    from corium import ClosedError

    db = await peer.db()
    query = [
        Keyword("find"),
        Symbol("?e"),
        Keyword("where"),
        [Symbol("?e"), Symbol("?a"), Symbol("?v")],
    ]
    test.assertIsInstance(await db.query(query), list)
    stats = await db.stats()
    test.assertEqual(await db.stats(), stats)
    report = await peer.transact([])
    test.assertGreater(report.basis_t, report.basis_before)
    await peer.close()
    with test.assertRaises(ClosedError):
        await db.stats()


@unittest.skipUnless(
    os.environ.get("CORIUM_TEST_LOCAL_ENDPOINT"),
    "set CORIUM_TEST_LOCAL_ENDPOINT to exercise an in-process full peer",
)
class NativeLocalTests(unittest.IsolatedAsyncioTestCase):
    async def test_local_peer_query_stats_transaction_and_close(self) -> None:
        from corium import LocalPeer

        peer = await LocalPeer.connect(
            os.environ["CORIUM_TEST_LOCAL_ENDPOINT"],
            database=os.environ.get("CORIUM_TEST_DATABASE", "people"),
            token=os.environ.get("CORIUM_TEST_LOCAL_TOKEN"),
            allow_insecure_token=True,
        )
        await _exercise_live_peer(self, peer)


@unittest.skipUnless(
    os.environ.get("CORIUM_TEST_REMOTE_ENDPOINT"),
    "set CORIUM_TEST_REMOTE_ENDPOINT to exercise a live peer-server",
)
class NativeRemoteTests(unittest.IsolatedAsyncioTestCase):
    async def test_remote_query_stats_and_close(self) -> None:
        from corium import RemotePeer

        peer = await RemotePeer.connect(
            os.environ["CORIUM_TEST_REMOTE_ENDPOINT"],
            database=os.environ.get("CORIUM_TEST_DATABASE", "people"),
            token=os.environ.get("CORIUM_TEST_REMOTE_TOKEN"),
            allow_insecure_token=True,
        )
        await _exercise_live_peer(self, peer)
