"""Use the same builders with either Corium peer topology."""

from __future__ import annotations

import asyncio
import os

from corium import (
    EntityMap,
    LocalPeer,
    Peer,
    Pull,
    Query,
    RemotePeer,
    TxBuilder,
    attr,
    data,
    gte,
    lookup,
    tempid,
)


async def load_and_query(peer: Peer) -> None:
    await peer.transact(
        TxBuilder()
        .entity(
            EntityMap.with_id(tempid("ada"))
            .set("person/name", "Ada")
            .set("person/age", 36)
        )
        .build()
    )

    adults = (
        Query.find_collection("?name")
        .in_scalar("?minimum")
        .where(data("?entity", attr("person/name"), "?name"))
        .where("?entity", ":person/age", "?age")
        .where(gte("?age", "?minimum"))
    )
    db = await peer.sync()
    print(await db.query(adults, 18))
    print(
        await db.pull(
            Pull().db_id().attr("person/name").attr("person/age"),
            lookup("person/name", "Ada"),
        )
    )


async def main() -> None:
    endpoint = os.environ.get("CORIUM_ENDPOINT", "http://127.0.0.1:4334")
    database = os.environ.get("CORIUM_DATABASE", "people")
    if os.environ.get("CORIUM_REMOTE"):
        peer: Peer = await RemotePeer.connect(endpoint, database=database)
    else:
        peer = await LocalPeer.connect(endpoint, database=database)
    async with peer:
        await load_and_query(peer)


if __name__ == "__main__":
    asyncio.run(main())
