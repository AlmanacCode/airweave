"""Bound identity-encoded response bodies while owning iterator closure."""

from contextlib import aclosing

import httpx

from airweave.domains.storage import FileSkippedException


async def bounded_response_bytes(response: httpx.Response, maximum: int, *, label: str) -> bytes:
    """Caller requests identity encoding; the raw bound is then a decoded byte bound too."""
    if response.is_stream_consumed:
        if len(response.content) > maximum:
            raise FileSkippedException(f"{label} exceeds size limit", label)
        return response.content
    body = bytearray()
    async with aclosing(response.stream.__aiter__()) as chunks:
        async for chunk in chunks:
            if len(body) + len(chunk) > maximum:
                raise FileSkippedException(f"{label} exceeds size limit", label)
            body.extend(chunk)
    return bytes(body)
