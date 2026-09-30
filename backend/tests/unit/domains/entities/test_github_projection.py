"""Offline search views must retain native text and use only owned blob storage."""

import hashlib
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.projection_mappers import map_record
from airweave.domains.entities.canonical.requests import BlobReference, RecordIdentity


def record(kind, payload, *, native_id="id"):
    return SourceRecord(
        id=uuid4(),
        sync_id=uuid4(),
        identity=RecordIdentity(record_type=kind, native_id=native_id),
        revision=1,
        payload=payload,
        payload_schema_version=1,
        capture_hash="a" * 64,
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


CONTEXT = {"repository_id": 1, "owner_id": 2, "full_name": "team/repo"}


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["comment", "review", "review_comment"])
async def test_native_conversation_text_and_exact_kind(kind):
    item = record(
        kind,
        {
            "native": {"id": 3, "body": "Native **Markdown**", "state": "APPROVED"},
            "repository": CONTEXT,
            "issue_number": 7,
        },
    )
    storage = AsyncMock()
    async with map_record(item, "github", storage) as views:
        assert views[0].text == "Native **Markdown**"
        assert views[0].resource_kind == kind
        assert views[0].state == "APPROVED"
    storage.read_file.assert_not_called()


@pytest.mark.asyncio
async def test_code_uses_verified_owned_blob_and_temporary_file():
    content = b"print('original')\n"
    item = record(
        "file",
        {
            "entry": {
                "path": "main.py",
                "mode": "100644",
                "type": "blob",
                "sha": "a" * 40,
                "size": len(content),
            },
            "path": "src/main.py",
            "ref": "main",
            "commit_sha": "b" * 40,
            "tree_sha": "c" * 40,
            "repository": CONTEXT,
        },
        native_id="src/main.py",
    )
    digest = hashlib.sha256(content).hexdigest()
    blob = BlobReference(
        key=f"canonical/{item.sync_id}/blobs/sha256/{digest}",
        sha256=digest,
        size_bytes=len(content),
        media_type="text/x-python",
    )
    item = item.model_copy(update={"blobs": (blob,)})
    storage = AsyncMock()
    storage.read_file.return_value = content
    async with map_record(item, "github", storage) as views:
        local = Path(views[0].local_path)
        assert local.read_bytes() == content
        assert views[0].commit_id == "b" * 40
    assert not local.exists()
    storage.read_file.assert_awaited_once_with(blob.key, max_bytes=len(content))


@pytest.mark.asyncio
async def test_missing_complete_bytes_keeps_projection_pending():
    item = record(
        "file",
        {
            "entry": {
                "path": "large",
                "mode": "100644",
                "type": "blob",
                "sha": "a" * 40,
                "size": 100,
            },
            "path": "large",
            "ref": "main",
            "commit_sha": "b" * 40,
            "tree_sha": "c" * 40,
            "repository": CONTEXT,
        },
        native_id="large",
    )
    with pytest.raises(ValueError, match="retained blob"):
        async with map_record(item, "github", AsyncMock()):
            pass
    partial = item.model_copy(update={"completeness": "partial"})
    async with map_record(partial, "github", AsyncMock()) as views:
        assert views[0].content_coverage == "partial" and views[0].text == ""
