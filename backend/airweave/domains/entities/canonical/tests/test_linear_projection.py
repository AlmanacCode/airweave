"""Original-only Linear projections with no provider calls or fabricated file content."""

import hashlib
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest

from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.projection_mappers import map_record
from airweave.domains.entities.canonical.requests import BlobReference, RecordIdentity
from airweave.platform.entities.linear import LinearLinkedAttachmentEntity

ISSUE = UUID(int=3)
STAMP = "2026-09-30T00:00:00Z"
CONTEXT = {
    "id": str(ISSUE),
    "identifier": "ENG-3",
    "title": "Native title",
    "url": "https://linear.app/example/issue/ENG-3",
    "team": {"id": str(UUID(int=2)), "name": "Engineering"},
    "project": None,
}


def original(kind, **extra):
    native_id = ISSUE if kind == "issue" else UUID(int=4)
    payload = {
        "id": str(native_id),
        "createdAt": STAMP,
        "updatedAt": STAMP,
        "url": "https://linear.app/native-link",
        **extra,
    }
    if kind == "issue":
        payload.update(CONTEXT)
    else:
        payload["issue"] = CONTEXT
    return SourceRecord(
        id=uuid4(),
        sync_id=uuid4(),
        identity=RecordIdentity(
            record_type=kind,
            native_id=str(native_id),
            container_id=None if kind == "issue" else str(ISSUE),
        ),
        revision=1,
        payload=payload,
        payload_schema_version=2,
        capture_hash="hash",
        content_hash=None,
        completeness="complete",
        observed_at=datetime.now(timezone.utc),
        source_created_at=None,
        source_updated_at=None,
        deleted_at=None,
        removal_reason=None,
        blobs=(),
        indexed_revision=None,
        indexed_pipeline_version=None,
    )


@pytest.mark.asyncio
async def test_native_issue_and_comment_render_without_provider_or_blob_access():
    for kind, fields in [
        ("issue", {"description": "**native**"}),
        ("comment", {"body": "reply", "user": None}),
    ]:
        record = original(kind, **fields)
        storage = AsyncMock()
        async with map_record(record, "linear", storage) as entities:
            entities = entities.entities
            assert entities[0].web_url == record.payload["url"]
            assert entities[0].created_at.tzinfo is not None
        storage.read_file.assert_not_called()
        assert record.payload.get("description", record.payload.get("body")) == next(
            iter(fields.values())
        )


@pytest.mark.asyncio
async def test_link_is_explicit_metadata_not_invented_file():
    record = original("attachment", title="Document").model_copy(update={"completeness": "partial"})
    async with map_record(record, "linear", AsyncMock()) as entities:
        entities = entities.entities
        assert isinstance(entities[0], LinearLinkedAttachmentEntity)
        assert "not retained" in entities[0].content_coverage


@pytest.mark.asyncio
async def test_missing_binary_cannot_claim_complete():
    with pytest.raises(ValueError, match="without retained bytes"):
        async with map_record(original("attachment", title="Document"), "linear", AsyncMock()):
            pass


@pytest.mark.asyncio
async def test_retained_file_verified_and_disposable():
    record = original("attachment", title="Document")
    data = b"%PDF fixture"
    digest = hashlib.sha256(data).hexdigest()
    ref = BlobReference(
        key=f"canonical/{record.sync_id}/blobs/sha256/{digest}",
        sha256=digest,
        size_bytes=len(data),
        media_type="application/pdf",
        source_path="/url",
    )
    record = record.model_copy(update={"blobs": (ref,)})
    storage = AsyncMock()
    storage.read_file.return_value = data
    async with map_record(record, "linear", storage) as entities:
        entities = entities.entities
        path = Path(entities[0].local_path)
        assert path.read_bytes() == data
        assert entities[0].issue_identifier == "ENG-3"
    assert not path.exists()
    storage.read_file.assert_awaited_once_with(ref.key, max_bytes=len(data))
    storage.read_file.return_value = b"corrupt"
    with pytest.raises(ValueError, match="digest"):
        async with map_record(record, "linear", storage):
            pass


@pytest.mark.asyncio
async def test_wrong_parent_or_missing_context_never_invents_metadata():
    record = original("comment", body="reply")
    record = record.model_copy(
        update={"identity": record.identity.model_copy(update={"container_id": str(UUID(int=99))})}
    )
    with pytest.raises(ValueError, match="parent"):
        async with map_record(record, "linear", AsyncMock()):
            pass
    payload = {**record.payload, "issue": {"id": str(ISSUE)}}
    with pytest.raises(ValueError):
        async with map_record(
            record.model_copy(update={"payload": payload}), "linear", AsyncMock()
        ):
            pass


def test_link_projection_is_registered():
    from airweave.domains.entities.registry import EntityDefinitionRegistry

    registry = EntityDefinitionRegistry()
    registry.build()
    assert registry.get_short_name_by_class(LinearLinkedAttachmentEntity)
