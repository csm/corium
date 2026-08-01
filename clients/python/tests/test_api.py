from __future__ import annotations

import importlib
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import patch

import corium._api as api
from corium import (
    ClosedError,
    Datom,
    DbStats,
    DirectStorage,
    EntityId,
    Index,
    Keyword,
    LocalPeer,
    NativeExtensionError,
    Peer,
    RemotePeer,
    SegmentCache,
    Symbol,
    Tagged,
    available_storage_backends,
)
from corium._api import _TxReportData, _View

if TYPE_CHECKING:

    def _assert_peer_types(local: LocalPeer, remote: RemotePeer) -> tuple[Peer, Peer]:
        return local, remote


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

    def __init__(self, *, fail_close_once: bool = False) -> None:
        self.db_backend = FakeDbBackend()
        self.close_count = 0
        self.transactions: list[Any] = []
        self.fail_close_once = fail_close_once

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
        if self.fail_close_once:
            self.fail_close_once = False
            raise RuntimeError("close failed")


class FakeNative:
    def __init__(
        self, backends: list[str] | None = None, advertised_backend: str = "turso"
    ) -> None:
        self.calls: list[tuple[Any, ...]] = []
        self.backends = backends or ["filesystem", "turso"]
        self.advertised_backend = advertised_backend
        self.discovery_calls: list[tuple[Any, ...]] = []

    async def connect_local(
        self, endpoints: list[str], **options: Any
    ) -> FakePeerBackend:
        self.calls.append(("local", endpoints, options))
        return FakePeerBackend()

    async def connect_remote(self, endpoint: str, **options: Any) -> FakePeerBackend:
        self.calls.append(("remote", endpoint, options))
        return FakePeerBackend()

    def _storage_backends(self) -> list[str]:
        return self.backends

    async def _discover_storage_backend(
        self, endpoints: list[str], **options: Any
    ) -> str:
        self.discovery_calls.append((endpoints, options))
        return self.advertised_backend


class PeerApiTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        api._native_modules.cache_clear()

    def tearDown(self) -> None:
        api._native_modules.cache_clear()

    async def test_local_and_remote_satisfy_the_same_protocol(self) -> None:
        for peer_type in (LocalPeer, RemotePeer):
            backend = FakePeerBackend()
            peer = peer_type._from_backend(backend)
            self.assertIsInstance(peer, Peer)
            self.assertEqual(peer.database_name, "people")

            db = await peer.db()
            self.assertEqual(await db.query(["query"], "Ada", fuel=20), [["Ada"]])
            self.assertEqual((await peer.sync()).database_name, "people")
            report = await peer.transact([{"person/name": "Ada"}])
            self.assertEqual(report.tempids, {"ada": 42})
            self.assertEqual(await report.db_after.basis_t(), 7)

    async def test_local_direct_storage_selects_the_advertised_artifact(self) -> None:
        unsupported = FakeNative(["filesystem", "turso"], advertised_backend="s3")
        supported = FakeNative(["filesystem", "s3"])

        with patch.object(
            api, "_native_modules", return_value=(unsupported, supported)
        ):
            peer = await LocalPeer.connect(
                "http://127.0.0.1:4334",
                database="people",
                storage=DirectStorage(),
            )

        self.assertEqual(len(unsupported.discovery_calls), 1)
        self.assertEqual(len(unsupported.calls), 0)
        self.assertEqual(len(supported.calls), 1)
        await peer.close()

    async def test_database_views_are_immutable_and_forward_raw_operations(
        self,
    ) -> None:
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
        for index in Index:
            datoms = await db.datoms(index, EntityId(1), limit=1)
            self.assertEqual(datoms, [Datom(1, 2, "Ada", 3, True)])
            self.assertEqual(datoms[0].value, "Ada")
        self.assertEqual(await db.stats(), DbStats(7, 5, 2, 3))

    async def test_context_manager_closes_exactly_once(self) -> None:
        backend = FakePeerBackend()
        peer = RemotePeer._from_backend(backend)

        async with peer as entered:
            self.assertIs(entered, peer)
            db = await peer.db()
            report = await peer.transact([])

        await peer.close()
        self.assertEqual(backend.close_count, 1)
        with self.assertRaises(ClosedError):
            await peer.db()
        with self.assertRaises(ClosedError):
            await db.query(["query"])
        with self.assertRaises(ClosedError):
            await report.db_after.stats()
        with self.assertRaises(ClosedError):
            db.history()

    async def test_failed_close_can_be_retried(self) -> None:
        backend = FakePeerBackend(fail_close_once=True)
        peer = LocalPeer._from_backend(backend)
        db = await peer.db()

        with self.assertRaisesRegex(RuntimeError, "close failed"):
            await peer.close()
        self.assertEqual(await db.basis_t(), 7, "failed close restores open state")

        await peer.close()
        self.assertEqual(backend.close_count, 2)
        with self.assertRaises(ClosedError):
            await db.stats()

    async def test_query_and_scan_limits_reject_invalid_values(self) -> None:
        db = await LocalPeer._from_backend(FakePeerBackend()).db()
        with self.assertRaises(ValueError):
            await db.query(["query"], fuel=-1)
        with self.assertRaises(TypeError):
            await db.datoms(Index.EAVT, limit=True)
        with self.assertRaises(ValueError):
            await db.datoms(Index.EAVT, limit=0)
        with self.assertRaises(ValueError):
            await db.datoms("nope")
        await db.datoms(Index.EAVT, limit=None)

    async def test_tls_is_inferred_and_tokens_require_https(self) -> None:
        native = FakeNative()
        with (
            patch.object(api, "_native_module", return_value=native),
            patch.object(api, "_native_modules", return_value=(native,)),
        ):
            await LocalPeer.connect(
                "https://local.test",
                database="people",
                token="token",
                tls_ca=b"private CA",
                tls_domain="local.test",
            )
            await RemotePeer.connect(
                "https://remote.test", database="people", token="token"
            )

        self.assertEqual(native.calls[0][2]["tls"], True)
        self.assertEqual(native.calls[0][2]["tls_ca"], b"private CA")
        self.assertEqual(native.calls[0][2]["tls_domain"], "local.test")
        self.assertEqual(native.calls[0][2]["direct_storage"], False)
        self.assertEqual(native.calls[1][2]["tls"], True)

        with self.assertRaisesRegex(ValueError, "require https"):
            await LocalPeer.connect(
                "http://local.test", database="people", token="token"
            )
        with self.assertRaisesRegex(ValueError, "include a scheme"):
            await LocalPeer.connect(
                ["https://local.test", "local.test"],
                database="people",
            )
        with self.assertRaisesRegex(ValueError, "require https"):
            await RemotePeer.connect(
                "http://remote.test",
                database="people",
                tls_ca=b"private CA",
            )
        with self.assertRaisesRegex(ValueError, "must not be empty"):
            await RemotePeer.connect(
                "https://remote.test",
                database="people",
                tls_domain="",
            )
        with self.assertRaisesRegex(TypeError, "PEM-encoded bytes"):
            await RemotePeer.connect(
                "https://remote.test",
                database="people",
                tls_ca="not bytes",  # type: ignore[arg-type]
            )

        with (
            patch.object(api, "_native_module", return_value=native),
            patch.object(api, "_native_modules", return_value=(native,)),
        ):
            await LocalPeer.connect(
                "http://127.0.0.1:4334",
                database="people",
                token="dev",
                allow_insecure_token=True,
            )
            await RemotePeer.connect(
                "http://127.0.0.1:4335",
                database="people",
                token="dev",
                allow_insecure_token=True,
            )
        self.assertEqual(native.calls[-2][2]["tls"], False)
        self.assertNotIn("allow_insecure_token", native.calls[-2][2])
        self.assertEqual(native.calls[-1][2]["tls"], False)

    async def test_direct_storage_and_segment_cache_are_forwarded(self) -> None:
        native = FakeNative()
        storage = DirectStorage(
            cache=SegmentCache(
                Path("/tmp/corium-python-cache"),
                capacity_bytes=128 * 1024 * 1024,
            )
        )
        with (
            patch.object(api, "_native_module", return_value=native),
            patch.object(api, "_native_modules", return_value=(native,)),
        ):
            await LocalPeer.connect(
                "http://127.0.0.1:4334",
                database="people",
                storage=storage,
            )
            self.assertEqual(
                available_storage_backends(), frozenset({"filesystem", "turso"})
            )

        options = native.calls[0][2]
        self.assertEqual(options["direct_storage"], True)
        self.assertEqual(options["segment_cache_directory"], "/tmp/corium-python-cache")
        self.assertEqual(options["segment_cache_capacity_bytes"], 128 * 1024 * 1024)
        self.assertIsNone(options["segment_cache_memory_bytes"])

    async def test_direct_storage_configuration_is_validated(self) -> None:
        with self.assertRaisesRegex(ValueError, "must be positive"):
            SegmentCache("/tmp/cache", 0)
        with self.assertRaisesRegex(ValueError, "must not exceed"):
            SegmentCache("/tmp/cache", 1024, 2048)
        with self.assertRaisesRegex(TypeError, "storage must"):
            await LocalPeer.connect(
                "http://127.0.0.1:4334",
                database="people",
                storage=object(),  # type: ignore[arg-type]
            )

    def test_missing_native_extension_has_a_dedicated_error(self) -> None:
        def import_missing(module_name: str) -> object:
            raise ModuleNotFoundError(name=module_name)

        with patch.object(importlib, "import_module", side_effect=import_missing):
            with self.assertRaisesRegex(
                NativeExtensionError, "native extension is not installed"
            ):
                api._native_module()

    def test_installed_native_artifacts_are_all_available(self) -> None:
        turso = FakeNative(["filesystem", "turso"])
        s3 = FakeNative(["filesystem", "s3"])

        def import_one(module_name: str) -> object:
            if module_name == "corium._corium_turso":
                return turso
            if module_name == "corium._corium_s3":
                return s3
            raise ModuleNotFoundError(name=module_name)

        with patch.object(
            importlib, "import_module", side_effect=import_one
        ) as import_module:
            self.assertIs(api._native_module(), turso)
            self.assertIs(api._native_module(), turso)
            self.assertEqual(api._native_modules(), (turso, s3))
            self.assertEqual(
                available_storage_backends(),
                frozenset({"filesystem", "turso", "s3"}),
            )
            self.assertEqual(import_module.call_count, 4)


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
        for tag in ("bytes", "eid", "inst", "uuid"):
            with self.subTest(tag=tag):
                with self.assertRaisesRegex(ValueError, "reserved"):
                    Tagged(tag, 1)

    async def test_wall_clock_views_require_aware_datetimes(self) -> None:
        database = await LocalPeer._from_backend(FakePeerBackend()).db()
        with self.assertRaises(ValueError):
            database.as_of_instant(datetime(2026, 1, 1))
        with self.assertRaisesRegex(ValueError, "millisecond precision"):
            database.as_of_instant(
                datetime(2026, 1, 1, 0, 0, 0, 123456, tzinfo=timezone.utc)
            )

        aware = datetime(
            2026, 1, 1, 0, 0, 0, 123000, tzinfo=timezone(timedelta(hours=-8))
        )
        viewed = database.as_of_instant(aware)
        expected = int(aware.timestamp() * 1000)
        self.assertEqual(viewed._view, _View("as_of_instant", expected))
        self.assertEqual(
            database.since_instant(aware)._view,
            _View("since_instant", expected),
        )


if __name__ == "__main__":
    unittest.main()
