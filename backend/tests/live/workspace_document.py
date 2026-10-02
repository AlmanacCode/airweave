"""One explicit native Docs hydration sample; never a Drive enumeration or checkpoint."""

import asyncio
import hashlib
import os
import sys
from pathlib import Path

from provider_sample import rest_source

from airweave.adapters.storage.filesystem import FilesystemBackend
from airweave.domains.storage.file_service import FileService
from airweave.platform.sources.records.google_drive import BASE, file_record
from airweave.platform.sources.records.workspace_manifest import canonical_json


async def fetch_workspace_document(account, email, key, fence, root):
    storage = FilesystemBackend(root / "blobs")
    files = FileService(fence.job_id, storage, sync_id=fence.sync_id)
    files.MAX_FILE_SIZE_BYTES = 10 * 1024 * 1024
    requests = 0

    async def count_request(request):
        nonlocal requests
        if request.method != "GET" or requests >= 12:
            raise ValueError("Workspace read-only provider request budget exceeded")
        requests += 1

    async with (
        asyncio.timeout(150),
        rest_source("google_drive", account, email, key, fence, request_hook=count_request) as (
            source,
            identity_verification,
        ),
    ):
        listing = await source._get(
            BASE + "/files",
            params={
                "q": "trashed=false and mimeType='application/vnd.google-apps.document'",
                "pageSize": "1",
                "fields": "files(*)",
            },
        )
        items = listing.get("files", [])
        if len(items) != 1:
            raise ValueError("The selected account returned no document sample")
        captured = await source._capture_body(file_record(items[0]), files)
    if not any(blob.role == "representation_manifest" for blob in captured.blobs):
        raise ValueError("The document sample has no structured representation")
    # Fixed metadata only; never print native IDs, names or contents.
    print('{"document_sample_provider_requests": ' + str(requests) + "}", flush=True)
    return [captured], storage, identity_verification, 1


async def verify_workspace_document_reader(
    *, records, storage, query, sessions, organization_id, sync_id, root
):
    from almanac_handoff import run_consumer

    if len(records) != 1:
        raise ValueError("Workspace trial requires exactly one retained document")
    record = records[0]
    async with sessions() as db:
        result = await query.document(
            db, organization_id, sync_id, record.id, record.revision, storage
        )
    expected = hashlib.sha256(canonical_json(result.document)).hexdigest()
    backend = Path(__file__).resolve().parents[2]
    consumer = Path(__file__).with_name("workspace_document_read.py")
    repository = backend.parent
    python = Path(sys.executable)
    if os.environ.get("LIVE_ALMANAC_PYTHON"):
        python = Path(os.environ["LIVE_ALMANAC_PYTHON"]).absolute()
        repository = Path(os.environ["LIVE_ALMANAC_ROOT"]).resolve()
        consumer = repository / "backend/tests/live/read_owned_document.py"
        if not python.is_file() or not consumer.is_file():
            raise ValueError("Explicit Almanac Python and document consumer required")
    return await run_consumer(
        sessions=sessions,
        organization_id=organization_id,
        sync_id=sync_id,
        root=root,
        storage=storage,
        query=query,
        consumer=consumer,
        repository=repository,
        python=python,
        extra={"record_id": str(record.id), "revision": record.revision, "expected": expected},
    )
