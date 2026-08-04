from __future__ import annotations

import importlib
import os
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from corium import EdnList, EntityId, Keyword, Pull, Symbol, Tagged, pull_attr

_corium: Any
try:
    _corium = importlib.import_module("corium._corium")
except ImportError:
    _corium = None


@unittest.skipIf(_corium is None, "native extension is not built")
class NativeBoundaryTests(unittest.TestCase):
    def test_native_engine_reports_available_storage_backends(self) -> None:
        backends = set(_corium._storage_backends())
        self.assertIn("filesystem", backends)
        self.assertTrue(backends.issubset({"filesystem", "postgres", "turso", "s3"}))

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
            EdnList((Symbol("count"), Symbol("?name"))),
            frozenset({Keyword("person/name"), "Ada"}),
            {Keyword("person/name"): "Ada"},
            Tagged("custom/tag", [1, "two"]),
        ]
        for value in values:
            with self.subTest(value=value):
                self.assertEqual(_corium._roundtrip(value), value)

    def test_pull_attribute_options_round_trip_as_a_vector(self) -> None:
        pattern = (
            Pull()
            .attr(
                pull_attr("person/nick")
                .as_(Keyword("nickname"))
                .limit(2)
                .default("n/a")
            )
            .to_form()
        )

        self.assertIsInstance(pattern[0], list)
        self.assertEqual(_corium._roundtrip(pattern), pattern)

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

    async def test_native_tls_options_require_tls(self) -> None:
        with self.assertRaisesRegex(ValueError, "require an https"):
            await _corium.connect_remote(
                "http://127.0.0.1:1",
                database="people",
                token=None,
                tls=False,
                tls_ca=b"private CA",
                tls_domain="localhost",
            )


async def _exercise_live_peer(test: unittest.TestCase, peer: Any) -> None:
    from corium import ClosedError, Index, QueryError

    db = await peer.db()
    test.assertEqual(db.database_name, peer.database_name)
    test.assertEqual((await peer.sync()).database_name, peer.database_name)

    relation_query = [
        Keyword("find"),
        Symbol("?e"),
        Keyword("where"),
        [Symbol("?e"), Symbol("?a"), Symbol("?v")],
    ]
    relation_rows = await db.query(relation_query)
    test.assertIsInstance(relation_rows, list)
    test.assertTrue(relation_rows)

    scalar_entity_query = [
        Keyword("find"),
        Symbol("?e"),
        Symbol("."),
        Keyword("where"),
        [Symbol("?e"), Symbol("?a"), Symbol("?v")],
    ]
    entity = await db.query(scalar_entity_query)
    test.assertIsInstance(entity, EntityId)
    pulled = await db.pull([Symbol("*")], entity)
    test.assertIsInstance(pulled, dict)
    test.assertTrue(pulled)

    datoms = await db.datoms(Index.EAVT, limit=1)
    test.assertEqual(len(datoms), 1)

    missing_argument_query = [
        Keyword("find"),
        Symbol("?e"),
        Keyword("in"),
        Symbol("$"),
        Symbol("?unused"),
        Keyword("where"),
        [Symbol("?e"), Symbol("?a"), Symbol("?v")],
    ]
    with test.assertRaises(QueryError):
        await db.query(missing_argument_query)

    stats = await db.stats()
    test.assertEqual(await db.stats(), stats)
    test.assertEqual(await db.as_of(stats.basis_t).stats(), stats)
    test.assertGreaterEqual((await db.history().stats()).basis_t, stats.basis_t)

    report = await peer.transact([])
    test.assertGreater(report.basis_t, report.basis_before)
    test.assertEqual(report.basis_before, stats.basis_t)
    test.assertEqual((await report.db_after.stats()).basis_t, report.basis_t)
    test.assertEqual(report.tx_instant.tzinfo, timezone.utc)

    await peer.close()
    with test.assertRaises(ClosedError):
        await db.stats()
    with test.assertRaises(ClosedError):
        await report.db_after.stats()


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


@unittest.skipUnless(
    os.environ.get("CORIUM_TEST_REMOTE_TLS_ENDPOINT")
    and os.environ.get("CORIUM_TEST_REMOTE_TLS_TOKEN")
    and os.environ.get("CORIUM_TEST_REMOTE_TLS_CA"),
    "set the CORIUM_TEST_REMOTE_TLS_* variables to exercise TLS and bearer auth",
)
class NativeRemoteTlsAuthTests(unittest.IsolatedAsyncioTestCase):
    async def test_private_ca_and_bearer_token_authentication(self) -> None:
        from corium import AuthenticationError, RemotePeer

        endpoint = os.environ["CORIUM_TEST_REMOTE_TLS_ENDPOINT"]
        self.assertTrue(endpoint.startswith("https://"))
        ca = Path(os.environ["CORIUM_TEST_REMOTE_TLS_CA"]).read_bytes()
        domain = os.environ.get("CORIUM_TEST_REMOTE_TLS_DOMAIN", "localhost")
        database = os.environ.get("CORIUM_TEST_DATABASE", "people")
        token = os.environ["CORIUM_TEST_REMOTE_TLS_TOKEN"]

        peer = await RemotePeer.connect(
            endpoint,
            database=database,
            token=token,
            tls_ca=ca,
            tls_domain=domain,
        )
        try:
            await (await peer.db()).stats()
        finally:
            await peer.close()

        for rejected_token in (
            None,
            os.environ.get("CORIUM_TEST_REMOTE_BAD_TOKEN", "definitely-wrong"),
        ):
            rejected = await RemotePeer.connect(
                endpoint,
                database=database,
                token=rejected_token,
                tls_ca=ca,
                tls_domain=domain,
            )
            try:
                with self.assertRaises(AuthenticationError) as raised:
                    await (await rejected.db()).stats()
                self.assertEqual(raised.exception.grpc_code, 16)
            finally:
                await rejected.close()
