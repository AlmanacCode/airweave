"""Verbose application logging must not enable raw HTTP transport traces."""

import asyncio
import logging

import httpx
import pytest

from airweave.core.logging import logger  # noqa: F401 - service logging initialization


@pytest.mark.asyncio
async def test_verbose_application_logging_excludes_url_and_response_cookie(caplog):
    """Exercise actual HTTPX/HTTPCore logging over a disposable loopback connection."""
    caplog.set_level(logging.DEBUG)

    async def respond(reader, writer):
        try:
            await reader.readuntil(b"\r\n\r\n")
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n"
                b"Set-Cookie: session=private-cookie-sentinel\r\n"
                b"Connection: close\r\n\r\nOK"
            )
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    async with await asyncio.start_server(respond, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        async with httpx.AsyncClient(trust_env=False) as client:
            response = await client.get(
                f"http://127.0.0.1:{port}/private-path-sentinel?token=private-query-sentinel"
            )
            assert response.text == "OK"

    logging.getLogger("httpx").warning("Transport warning remains observable")
    assert "Transport warning remains observable" in caplog.text
    assert "private-path-sentinel" not in caplog.text
    assert "private-query-sentinel" not in caplog.text
    assert "private-cookie-sentinel" not in caplog.text
