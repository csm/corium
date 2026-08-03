#!/usr/bin/env python3
"""Minimal live peer-server scenario for the Python client."""

from __future__ import annotations

import asyncio
import sys

from corium import RemotePeer


async def exercise(endpoint: str, database: str) -> None:
    peer = await RemotePeer.connect(endpoint, database=database)
    try:
        before = await (await peer.db()).stats()
        report = await peer.transact([])
        after = await report.db_after.stats()
        if report.basis_t <= before.basis_t or after.basis_t != report.basis_t:
            raise RuntimeError(
                f"unexpected basis progression: before={before.basis_t}, "
                f"report={report.basis_t}, after={after.basis_t}"
            )
        print(
            f"python client reached {database}: "
            f"basis {before.basis_t} -> {after.basis_t}"
        )
    finally:
        await peer.close()


def main() -> int:
    if len(sys.argv) != 3:
        raise SystemExit("usage: python_client.py PEER_ENDPOINT DATABASE")
    asyncio.run(exercise(sys.argv[1], sys.argv[2]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
