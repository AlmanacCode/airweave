"""Async bounded process adapter for retained Apple bodies."""

import asyncio
import base64
import hashlib
import os
import sys
from pathlib import Path
from typing import Literal

from pydantic import ValidationError

from .models import (
    ApplePreparationError,
    BodyDecodeRequest,
    PreparationLimits,
    PreparedBody,
    PreparedMessageBody,
    PreparedNotesBody,
    WorkerReply,
)


async def _read_bounded(stream: asyncio.StreamReader, maximum_bytes: int) -> bytes:
    output = bytearray()
    while True:
        chunk = await stream.read(min(65536, maximum_bytes - len(output) + 1))
        if not chunk:
            return bytes(output)
        output.extend(chunk)
        if len(output) > maximum_bytes:
            raise ApplePreparationError("worker_output_limit")


async def _run_worker(request: bytes, *, maximum_response_bytes: int, wall_seconds: float) -> bytes:
    """Bound both pipes, terminate on failure/cancellation and always reap the child."""
    worker = Path(__file__).with_name("worker.py").resolve()
    try:
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-I",
            str(worker),
            str(len(request)),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=str(worker.parent),
            env={"PATH": os.defpath, "LANG": "C.UTF-8"},
            start_new_session=True,
        )
    except OSError as error:
        raise ApplePreparationError("worker_failed") from error
    assert process.stdin and process.stdout and process.stderr

    async def exchange() -> bytes:
        async def send() -> None:
            process.stdin.write(request)
            await process.stdin.drain()
            process.stdin.close()
            await process.stdin.wait_closed()

        readers = [
            asyncio.create_task(_read_bounded(process.stdout, maximum_response_bytes)),
            asyncio.create_task(_read_bounded(process.stderr, 8192)),
            asyncio.create_task(send()),
        ]
        try:
            output, _, _ = await asyncio.gather(*readers)
            await process.wait()
            return output
        finally:
            for task in readers:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*readers, return_exceptions=True)

    try:
        output = await asyncio.wait_for(exchange(), timeout=wall_seconds)
        if process.returncode != 0:
            raise ApplePreparationError("worker_failed")
    except TimeoutError as error:
        raise ApplePreparationError("worker_timeout") from error
    except (BrokenPipeError, ConnectionResetError) as error:
        raise ApplePreparationError("worker_failed") from error
    finally:
        if process.returncode is None:
            process.kill()
        await process.wait()
    return output


async def _prepare_body(
    original: bytes,
    *,
    body_format: Literal["notes_gzip_protobuf", "messages_typedstream"],
    limits: PreparationLimits,
) -> PreparedBody:
    """Use one worker protocol for both supported native source body formats."""
    if not original or len(original) > limits.maximum_input_bytes:
        raise ApplePreparationError("input_limit")
    request = (
        BodyDecodeRequest(
            format=body_format,
            original_base64=base64.b64encode(original).decode("ascii"),
            limits=limits,
        )
        .model_dump_json()
        .encode("utf-8")
    )
    output = await _run_worker(
        request, maximum_response_bytes=limits.response_bytes, wall_seconds=limits.wall_seconds
    )
    try:
        reply = WorkerReply.model_validate_json(output)
    except ValidationError as error:
        raise ApplePreparationError("invalid_worker_response") from error
    if reply.error is not None:
        raise ApplePreparationError(reply.error)
    result = reply.result
    if (
        result is None
        or result.original_sha256 != hashlib.sha256(original).hexdigest()
        or result.format != body_format
    ):
        raise ApplePreparationError("invalid_worker_response")
    if len(result.text.encode("utf-8")) > limits.maximum_text_bytes:
        raise ApplePreparationError("worker_output_limit")
    return result


async def prepare_notes_body(
    original: bytes, *, locked: bool, limits: PreparationLimits
) -> PreparedNotesBody:
    """Decode retained Notes content only while current visibility permits it.

    Locked bodies are rejected before process creation. CPU/wall/byte limits and
    sanitized environment do not provide filesystem/network sandboxing.
    """
    if locked:
        raise ApplePreparationError("locked_content")
    result = await _prepare_body(original, body_format="notes_gzip_protobuf", limits=limits)
    assert isinstance(result, PreparedNotesBody)  # Validated discriminated process response.
    return result


async def prepare_message_body(
    original: bytes, *, limits: PreparationLimits
) -> PreparedMessageBody:
    """Decode an attributed backing string using the installed server-only library.

    No native class unarchiver is used. Rich formatting/embedded originals remain
    separate retained data and explicit omissions. Caller owns current visibility.
    """
    result = await _prepare_body(original, body_format="messages_typedstream", limits=limits)
    assert isinstance(result, PreparedMessageBody)  # Validated discriminated process response.
    return result
