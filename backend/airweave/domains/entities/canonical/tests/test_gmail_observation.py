"""Raw Gmail locator observations retain provenance even when blob bytes match."""

from copy import deepcopy
from datetime import datetime, timezone
from hashlib import sha256
from uuid import uuid4

import pytest

from airweave.domains.entities.canonical.projection_store import CanonicalProjectionStore
from airweave.domains.entities.canonical.requests import (
    BlobReference,
    CaptureRecord,
    RecordIdentity,
)
from airweave.domains.entities.canonical.store import capture_fingerprint
from airweave.domains.entities.canonical.tests.helpers import (
    bind_projection,
    capture,
    publish_prepared,
)

pytestmark = pytest.mark.integration


async def test_locator_rotation_preserves_originals_and_requires_current_publication(
    database, source
):
    service, fence = source
    await bind_projection(database, fence, "gmail")
    digest = sha256(b"identical message body").hexdigest()
    payload = {
        "id": "synthetic-message",
        "threadId": "synthetic-thread",
        "internalDate": "1700000000000",
        "labelIds": ["INBOX"],
        "payload": {
            "mimeType": "text/plain",
            "partId": "0",
            "body": {"size": 22, "attachmentId": "locator-A"},
        },
    }
    original = CaptureRecord(
        identity=RecordIdentity(record_type="message", native_id=payload["id"]),
        payload=payload,
        observed_at=datetime.now(timezone.utc),
        blobs=(
            BlobReference(
                key=f"canonical/{fence.sync_id}/blobs/sha256/{digest}",
                sha256=digest,
                size_bytes=22,
                media_type="text/plain",
                source_path="/payload/body",
            ),
        ),
    )
    rotated_payload = deepcopy(payload)
    rotated_payload["payload"]["body"]["attachmentId"] = "locator-B"
    rotated = original.model_copy(update={"payload": rotated_payload})
    assert capture_fingerprint(original) != capture_fingerprint(rotated)
    first = await capture(database, service, fence, original)
    projections = CanonicalProjectionStore()
    async with database() as db:
        work = (await projections.pending(db, fence.organization_id, fence.sync_id))[0]
        assert await publish_prepared(projections, db, work, uuid4(), 1)
    async with database() as db:
        assert not await projections.pending(db, fence.organization_id, fence.sync_id)
    second = await capture(database, service, fence, rotated)
    async with database() as db:
        pending = await projections.pending(db, fence.organization_id, fence.sync_id)
        history = await service.store.changes(db, fence.organization_id, fence.sync_id)
    assert first.changes[0].record.revision == 1
    assert second.changes[0].record.revision == 2
    assert len(pending) == 1 and pending[0].record.revision == 2
    assert [c.record.payload["payload"]["body"]["attachmentId"] for c in history.changes] == [
        "locator-A",
        "locator-B",
    ]
    replay = await capture(database, service, fence, rotated)
    assert replay.unchanged == 1 and not replay.changes
