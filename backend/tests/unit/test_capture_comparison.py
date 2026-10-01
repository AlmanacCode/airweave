"""Disposable trial diagnostics distinguish metadata rotation from changed bytes."""

import json
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.requests import BlobReference, RecordIdentity


def test_locator_changes_and_equal_size_different_bytes_are_not_conflated(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1] / "live"))
    from capture_comparison import compare_observations

    def record(content):
        digest = sha256(content).hexdigest()
        return SourceRecord(
            id=uuid4(),
            sync_id=uuid4(),
            identity=RecordIdentity(record_type="message", native_id="private-message"),
            revision=1,
            payload={"payload": {"parts": [{"body": {"attachmentId": "private-locator"}}]}},
            payload_schema_version=1,
            capture_hash="private-capture-hash",
            content_hash=None,
            completeness="complete",
            observed_at=datetime.now(timezone.utc),
            source_created_at=None,
            source_updated_at=None,
            deleted_at=None,
            removal_reason=None,
            blobs=(
                BlobReference(
                    key="private-key",
                    sha256=digest,
                    size_bytes=len(content),
                    source_path="/payload/parts/0/body",
                ),
            ),
            indexed_revision=None,
            indexed_pipeline_version=None,
        )

    rotated, changed = record(b"same"), record(b"old!")
    new_rotated = rotated.model_copy(
        update={
            "revision": 2,
            "payload": {"payload": {"parts": [{"body": {"attachmentId": "other-locator"}}]}},
        }
    )
    new_changed = changed.model_copy(update={"revision": 2, "blobs": record(b"new!").blobs})
    before = {row.id: row for row in (rotated, changed)}
    after = {row.id: row for row in (new_rotated, new_changed)}
    assert sum(blob.size_bytes for row in before.values() for blob in row.blobs) == sum(
        blob.size_bytes for row in after.values() for blob in row.blobs
    )
    result = compare_observations(before, after, "gmail")
    assert result["changed_revision_records"] == 2
    assert result["changed_payload_records"] == result["locator_only_payload_records"] == 1
    assert result["changed_field_categories"] == {"gmail_attachment_locator": 1}
    assert result["equal_blob_manifest_records"] == 1
    assert result["different_blob_manifest_records"] == 1
    assert result["equal_blob_size_different_sha_records"] == 1
    serialized = json.dumps(result)
    assert all(str(identity) not in serialized for identity in before)
    assert "private" not in serialized and "other-locator" not in serialized
    assert all(blob.sha256 not in serialized for row in after.values() for blob in row.blobs)
