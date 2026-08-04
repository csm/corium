#!/usr/bin/env python3
"""Small dependency-free PostgreSQL v3 client for the scenario matrix."""

from __future__ import annotations

import socket
import struct
import sys
from dataclasses import dataclass


@dataclass
class Message:
    tag: bytes
    body: bytes


def packet(*parts: bytes) -> bytes:
    body = b"".join(parts)
    return struct.pack("!I", len(body) + 4) + body


def read_exactly(stream: socket.socket, length: int) -> bytes:
    chunks: list[bytes] = []
    remaining = length
    while remaining:
        chunk = stream.recv(remaining)
        if not chunk:
            raise RuntimeError("pgwire server closed the connection")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def read_message(stream: socket.socket) -> Message:
    tag = read_exactly(stream, 1)
    length = struct.unpack("!I", read_exactly(stream, 4))[0]
    return Message(tag, read_exactly(stream, length - 4))


def error_text(body: bytes) -> str:
    fields = body.rstrip(b"\0").split(b"\0")
    return "; ".join(field[1:].decode("utf-8", "replace") for field in fields)


def read_until_ready(
    stream: socket.socket, password: str | None = None
) -> list[list[str | None]]:
    rows: list[list[str | None]] = []
    while True:
        message = read_message(stream)
        if message.tag == b"E":
            raise RuntimeError(f"pgwire error: {error_text(message.body)}")
        if message.tag == b"R":
            auth_method = struct.unpack("!I", message.body[:4])[0]
            if auth_method == 3:
                supplied = password or ""
                stream.sendall(b"p" + packet(supplied.encode(), b"\0"))
            elif auth_method != 0:
                raise RuntimeError(f"unsupported pgwire authentication method: {auth_method}")
        if message.tag == b"D":
            count = struct.unpack("!H", message.body[:2])[0]
            offset = 2
            row: list[str | None] = []
            for _ in range(count):
                length = struct.unpack("!i", message.body[offset : offset + 4])[0]
                offset += 4
                if length == -1:
                    row.append(None)
                else:
                    row.append(message.body[offset : offset + length].decode())
                    offset += length
            rows.append(row)
        if message.tag == b"Z":
            return rows


def query(stream: socket.socket, sql: str) -> list[list[str | None]]:
    stream.sendall(b"Q" + packet(sql.encode(), b"\0"))
    return read_until_ready(stream)


def exercise(host: str, port: int, database: str) -> None:
    with socket.create_connection((host, port), timeout=30) as stream:
        stream.settimeout(30)
        startup = packet(
            struct.pack("!I", 196608),
            b"user\0corium-scenario\0",
            b"database\0",
            database.encode(),
            b"\0\0",
        )
        stream.sendall(startup)
        read_until_ready(stream)

        databases = query(stream, "SHOW DATABASES")
        if [database] not in databases:
            raise RuntimeError(f"SHOW DATABASES omitted {database!r}: {databases!r}")

        rows = query(stream, "SELECT COUNT(*) FROM corium.person")
        if len(rows) != 1 or len(rows[0]) != 1:
            raise RuntimeError(f"unexpected COUNT result: {rows!r}")
        int(rows[0][0] or "")
        print(f"pgwire client reached {database}: count={rows[0][0]}")


def main() -> int:
    if len(sys.argv) != 4:
        raise SystemExit("usage: pgwire_client.py HOST PORT DATABASE")
    exercise(sys.argv[1], int(sys.argv[2]), sys.argv[3])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
