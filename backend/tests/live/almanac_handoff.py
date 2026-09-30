"""Optional cross-environment proof through the real owned-service HTTP router."""

import asyncio
import base64
import hashlib
import json
import os
import secrets
import socket
from email.header import decode_header, make_header
from email.message import Message
from email.utils import getaddresses
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import uvicorn
from fastapi import FastAPI, HTTPException, Request

from airweave.api import deps
from airweave.api.v1.api import api_router
from airweave.api.v1.endpoints.records import record_error_response
from airweave.db.session import get_db
from airweave.domains.entities.canonical.store import CanonicalStoreError


def digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


async def expectation(record, storage):
    """Fingerprint native data independently before crossing the HTTP/process boundary."""
    payload = record.payload
    headers = {h["name"].lower(): h["value"] for h in payload["payload"].get("headers", [])}
    bodies = {"text/plain": [], "text/html": []}
    body_blobs = 0

    async def walk(part, path):
        nonlocal body_blobs
        metadata = Message()
        for header in part.get("headers", []):
            metadata[header["name"]] = header["value"]
        if part.get("filename") or metadata.get_content_disposition() == "attachment":
            return
        if part.get("mimeType") in bodies:
            body = part.get("body", {})
            if "data" in body:
                value = body["data"]
                raw = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
            else:
                blob = next((b for b in record.blobs if b.source_path == path + "/body"), None)
                raw = await storage.read_file(blob.key, max_bytes=blob.size_bytes) if blob else b""
                body_blobs += int(blob is not None)
            bodies[part["mimeType"]].append(raw.decode(metadata.get_content_charset() or "utf-8"))
        for index, child in enumerate(part.get("parts", [])):
            await walk(child, f"{path}/parts/{index}")

    await walk(payload["payload"], "/payload")
    return {
        "record_id": str(record.id),
        "native_id": record.identity.native_id,
        "revision": record.revision,
        "observed_at": record.observed_at.isoformat(),
        "payload_digest": digest(json.dumps(payload, sort_keys=True, separators=(",", ":"))),
        "subject_digest": digest(str(make_header(decode_header(headers.get("subject", ""))))),
        "sender_digest": digest(
            json.dumps(
                [
                    [str(make_header(decode_header(name))), address]
                    for name, address in getaddresses([headers.get("from", "")])
                    if address
                ]
            )
        ),
        # Alternative bodies select one representation; mixed bodies may join leaves.
        # Exact MIME semantics are separately tested; here every rendered byte must
        # correspond to the native captured representation across process/HTTP boundaries.
        "text_digests": [
            digest(x) for x in [*bodies["text/plain"], "\n".join(bodies["text/plain"])]
        ],
        "html_digests": [digest(x) for x in [*bodies["text/html"], "\n".join(bodies["text/html"])]],
        "body_blobs": body_blobs,
    }


async def verify_almanac_reader(
    *, records, storage, query, sessions, organization_id, sync_id, root
):
    python = Path(os.environ["LIVE_ALMANAC_PYTHON"]).absolute()
    repository = Path(os.environ["LIVE_ALMANAC_ROOT"]).resolve()
    consumer = repository / "backend/tests/live/read_owned_gmail.py"
    if not python.is_file() or not consumer.is_file():
        raise ValueError("Explicit Almanac Python and consumer checkout are required")
    candidates = [r for r in records if r.blobs and r.completeness == "complete"]
    if not candidates:
        raise ValueError("A complete native message with a stored blob is required")
    thread_id = candidates[0].payload["threadId"]
    selected = [r for r in records if r.payload.get("threadId") == thread_id]
    expected = [await expectation(record, storage) for record in selected]
    probe_key = secrets.token_urlsafe(32)
    app = FastAPI()
    app.include_router(api_router)
    app.add_exception_handler(CanonicalStoreError, record_error_response)

    async def context(request: Request):
        if not secrets.compare_digest(request.headers.get("x-api-key", ""), probe_key):
            raise HTTPException(status_code=401)
        return SimpleNamespace(organization=SimpleNamespace(id=organization_id))

    async def database():
        async with sessions() as db:
            yield db

    app.dependency_overrides[deps.get_context] = context
    app.dependency_overrides[get_db] = database
    app.dependency_overrides[deps.get_canonical_query_service] = lambda: query
    app.dependency_overrides[deps.get_container] = lambda: SimpleNamespace(storage_backend=storage)
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(128)
    listener.setblocking(False)
    server = uvicorn.Server(uvicorn.Config(app, log_config=None, access_log=False, lifespan="off"))
    task = asyncio.create_task(server.serve(sockets=[listener]))
    process = None
    manifest = root / "almanac-handoff.json"
    try:
        async with asyncio.timeout(20):
            while not server.started:
                if task.done():
                    await task
                    raise RuntimeError("Local API server did not start")
                await asyncio.sleep(0.01)
        manifest.write_text(
            json.dumps(
                {
                    "origin": f"http://127.0.0.1:{listener.getsockname()[1]}",
                    "api_key": probe_key,
                    "organization_id": str(organization_id),
                    "sync_id": str(sync_id),
                    "account_id": str(uuid4()),
                    "source_connection_id": str(uuid4()),
                    "thread_id": thread_id,
                    "expected": expected,
                }
            )
        )
        manifest.chmod(0o600)
        # Provider keys/identities do not enter the consumer subprocess environment.
        environment = {
            "PATH": os.environ.get("PATH", ""),
            "PYTHONUNBUFFERED": "1",
            "PYTHONPATH": str(repository / "backend")
            + os.pathsep
            + str(repository / "shared/revtext"),
        }
        process = await asyncio.create_subprocess_exec(
            str(python),
            str(consumer),
            str(manifest),
            env=environment,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        async with asyncio.timeout(120):
            output, _ = await process.communicate()
        return consumer_result(output, process.returncode)
    finally:
        try:
            await stop_probe(process, server, task)
        finally:
            listener.close()
            manifest.unlink(missing_ok=True)


async def stop_probe(process, server, task):
    """Reap child/server before the caller destroys their private storage."""
    if process is not None and process.returncode is None:
        process.kill()
        await process.wait()
    server.should_exit = True
    try:
        async with asyncio.timeout(10):
            await task
    except TimeoutError:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


def consumer_result(output, returncode):
    """Accept only the consumer's structured result; suppress private diagnostics."""
    if not output.strip():
        raise RuntimeError("Consumer interpreter failed before structured verification")
    result = json.loads(output)
    if returncode or not result.get("verified"):
        safe_stage = result.get("stage")
        if safe_stage in {
            "handoff",
            "thread_read",
            "exact_record_read",
            "blob_read",
            "context_negative_check",
        }:
            print(json.dumps({"consumer_failed_stage": safe_stage}), flush=True)
        raise RuntimeError("Almanac consumer proof failed; private diagnostics suppressed")
    return result
