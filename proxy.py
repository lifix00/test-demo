"""Small HTTP/1.1 proxy used to reproduce network quality defects."""

import asyncio
import os
from urllib.parse import urlsplit

BUDGET = 0.35
MAX_ATTEMPTS = 3
RETRY_POST = False
MAX_CONCURRENCY = 2
CANCEL_UPSTREAM = True
UPSTREAM = urlsplit(os.environ.get("UPSTREAM_URL", "http://127.0.0.1:8090"))
SLOTS = asyncio.Semaphore(MAX_CONCURRENCY)


async def forward(method: str, path: str, body: bytes) -> tuple[int, bytes]:
    attempts = MAX_ATTEMPTS if method in {"GET", "HEAD"} or RETRY_POST else 1
    for attempt in range(attempts):
        reader, writer = await asyncio.open_connection(
            UPSTREAM.hostname, UPSTREAM.port or 80
        )
        try:
            writer.write(
                f"{method} {path} HTTP/1.1\r\nHost: upstream\r\nConnection: close\r\n"
                f"Content-Length: {len(body)}\r\n\r\n".encode()
                + body
            )
            await writer.drain()
            header = await reader.readuntil(b"\r\n\r\n")
            status = int(header.split(b" ", 2)[1])
            response = await reader.read()
        finally:
            writer.close()
            await writer.wait_closed()
        if status < 500 or attempt == attempts - 1:
            return status, response
    return 502, b"upstream error"


async def limited_forward(method: str, path: str, body: bytes) -> tuple[int, bytes]:
    async with SLOTS:
        return await forward(method, path, body)


async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    upstream: asyncio.Task[tuple[int, bytes]] | None = None
    disconnected: asyncio.Task[bytes] | None = None
    try:
        header = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 2)
        lines = header.decode("ascii").split("\r\n")
        method, path, _ = lines[0].split(" ", 2)
        if path == "/health":
            status, response = 200, b"ok"
        else:
            length = next(
                (
                    int(line.split(":", 1)[1])
                    for line in lines[1:]
                    if line.lower().startswith("content-length:")
                ),
                0,
            )
            if length > 65536 or method not in {"GET", "POST", "HEAD"}:
                raise ValueError("Unsupported request")
            body = await asyncio.wait_for(reader.readexactly(length), 2)
            upstream = asyncio.create_task(limited_forward(method, path, body))
            disconnected = asyncio.create_task(reader.read(1))
            if CANCEL_UPSTREAM:
                done, _ = await asyncio.wait(
                    {upstream, disconnected},
                    timeout=BUDGET,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if disconnected in done:
                    return
                if upstream not in done:
                    status, response = 504, b"budget exceeded"
                else:
                    status, response = upstream.result()
            else:
                status, response = await asyncio.wait_for(
                    asyncio.shield(upstream), BUDGET
                )
        writer.write(
            f"HTTP/1.1 {status} Result\r\nContent-Length: {len(response)}\r\n"
            "Connection: close\r\n\r\n".encode()
            + response
        )
        await writer.drain()
    except TimeoutError:
        writer.write(b"HTTP/1.1 504 Timeout\r\nContent-Length: 0\r\n\r\n")
    except (ValueError, ConnectionError, asyncio.IncompleteReadError, OSError):
        pass
    finally:
        if upstream and not upstream.done() and CANCEL_UPSTREAM:
            upstream.cancel()
            await asyncio.gather(upstream, return_exceptions=True)
        if disconnected:
            disconnected.cancel()
            await asyncio.gather(disconnected, return_exceptions=True)
        writer.close()
        await writer.wait_closed()


async def main() -> None:
    server = await asyncio.start_server(handle, "0.0.0.0", 8080)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
