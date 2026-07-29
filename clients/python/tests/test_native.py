from __future__ import annotations

import importlib
import os
import unittest
import uuid
from datetime import datetime, timezone
from typing import Any

from corium import EntityId, Keyword, NativeExtensionError, Symbol, Tagged

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

    def test_local_peer_reports_the_phase_boundary(self) -> None:
        with self.assertRaisesRegex(NativeExtensionError, "Phase 3"):
            _corium.connect_local(
                ["http://127.0.0.1:4334"],
                database="people",
                token=None,
                tls=False,
            )


@unittest.skipUnless(
    os.environ.get("CORIUM_TEST_REMOTE_ENDPOINT"),
    "set CORIUM_TEST_REMOTE_ENDPOINT to exercise a live peer-server",
)
class NativeRemoteTests(unittest.IsolatedAsyncioTestCase):
    async def test_remote_query_stats_and_close(self) -> None:
        from corium import ClosedError, RemotePeer

        peer = await RemotePeer.connect(
            os.environ["CORIUM_TEST_REMOTE_ENDPOINT"],
            database=os.environ.get("CORIUM_TEST_DATABASE", "people"),
            token=os.environ.get("CORIUM_TEST_REMOTE_TOKEN"),
            allow_insecure_token=True,
        )
        db = await peer.db()
        query = [
            Keyword("find"),
            Symbol("?e"),
            Keyword("where"),
            [Symbol("?e"), Symbol("?a"), Symbol("?v")],
        ]
        self.assertIsInstance(await db.query(query), list)
        self.assertGreaterEqual((await db.stats()).basis_t, 0)
        report = await peer.transact([])
        self.assertGreater(report.basis_t, report.basis_before)
        await peer.close()
        with self.assertRaises(ClosedError):
            await db.stats()
