"""Representation closure and compatibility, without changing Gmail MIME ownership."""

import hashlib
from datetime import datetime, timezone

import pytest
from airweave.domains.entities.canonical.requests import (
    BlobReference,
    CaptureRecord,
    RecordIdentity,
)
from airweave.domains.entities.canonical.store import capture_fingerprint
from airweave.platform.sources.records.export_manifest import (
    ExportManifestV3,
    parse_export_manifest,
)
from airweave.platform.sources.records.workspace_manifest import (
    DocsState,
    ExportState,
    WorkspaceManifestV1,
    canonical_json,
    parse_manifest,
    validate_document,
)
from pydantic import ValidationError


def descriptor(content, **extra):
    digest = hashlib.sha256(content).hexdigest()
    return BlobReference(
        key=f"canonical/sync/blobs/sha256/{digest}", sha256=digest, size_bytes=len(content), **extra
    )


def fixture_manifest():
    native = descriptor(b'{"documentId":"doc","tabs":[]}')
    model = WorkspaceManifestV1(
        file_id="doc",
        drive_version="7",
        export=ExportState(status="unavailable", reason="export_size_limit"),
        native=DocsState(status="complete", document_blob=native.sha256, embedded_media="retained"),
    )
    content = canonical_json(model.model_dump(mode="json"))
    return content, (native, descriptor(content, role="representation_manifest"))


def test_manifest_exact_closure_and_identity():
    content, blobs = fixture_manifest()
    assert (
        parse_manifest(content, file_id="doc", drive_version="7", blobs=blobs).native.status
        == "complete"
    )
    for bad_blobs in (blobs[1:], blobs + (descriptor(b"unreferenced"),), blobs + (blobs[0],)):
        with pytest.raises(ValueError):
            parse_manifest(content, file_id="doc", drive_version="7", blobs=bad_blobs)
    for file_id, version in (("foreign", "7"), ("doc", "8")):
        with pytest.raises(ValueError):
            parse_manifest(content, file_id=file_id, drive_version=version, blobs=blobs)


def test_generic_role_preserves_historical_shape_and_repeated_mime_digest():
    blob = descriptor(b"same", source_path="/payload/parts/0/body")
    other = blob.model_copy(update={"source_path": "/payload/parts/1/body"})
    record = CaptureRecord(
        identity=RecordIdentity(record_type="message", native_id="one"),
        payload={},
        observed_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        blobs=(blob, other),
    )
    assert "role" not in record.model_dump(mode="json")["blobs"][0]
    assert record.model_dump(mode="json")["blobs"][0]["media_type"] is None
    assert len(record.blobs) == 2
    assert (
        capture_fingerprint(record)
        == "97c6dc0cc6d1376f18aa2f04d7fc66781a3bcfb729dda843dc90ec7037d4bc9d"
    )
    with pytest.raises(ValidationError):
        descriptor(b"manifest", role="representation_manifest", source_path="/payload")
    with pytest.raises(ValidationError):
        CaptureRecord.model_validate(
            record.model_dump()
            | {
                "blobs": (
                    descriptor(b"first", role="representation_manifest"),
                    descriptor(b"second", role="representation_manifest"),
                )
            }
        )


def test_all_tabs_identity_and_unknown_native_fields():
    document = {
        "documentId": "doc",
        "futureField": {"kept": True},
        "tabs": [
            {
                "tabProperties": {"tabId": "a"},
                "documentTab": {},
                "childTabs": [
                    {
                        "tabProperties": {"tabId": "b"},
                        "documentTab": {"future": 1},
                    }
                ],
            }
        ],
    }
    validate_document(document, file_id="doc")
    assert "revisionId" not in document
    with pytest.raises(ValueError):
        validate_document(document, file_id="foreign")
    with pytest.raises(ValueError):
        validate_document({"documentId": "doc", "body": {}}, file_id="doc")
    document["tabs"].append(document["tabs"][0])
    with pytest.raises(ValueError):
        validate_document(document, file_id="doc")


def test_export_omission_manifest_binds_identity_and_exact_blob_closure():
    manifest = ExportManifestV3(
        file_id="slide",
        drive_version="7",
        media_type="application/vnd.google-apps.presentation",
        export=ExportState(status="unavailable", reason="export_size_limit"),
    )
    content = canonical_json(manifest.model_dump(mode="json"))
    digest = hashlib.sha256(content).hexdigest()
    marked = BlobReference(
        key="blob", sha256=digest, size_bytes=len(content), role="representation_manifest"
    )
    args = {
        "file_id": "slide",
        "drive_version": "7",
        "media_type": manifest.media_type,
        "blobs": (marked,),
    }
    assert parse_export_manifest(content, **args) == manifest
    for field, value in (
        ("file_id", "other"),
        ("drive_version", "8"),
        ("media_type", "application/pdf"),
    ):
        with pytest.raises(ValueError, match="identity/version/type"):
            parse_export_manifest(content, **(args | {field: value}))
    extra = BlobReference(key="extra", sha256="a" * 64, size_bytes=1)
    with pytest.raises(ValueError, match="every retained part"):
        parse_export_manifest(content, **(args | {"blobs": (marked, extra)}))
    with pytest.raises(ValueError, match="uniquely retained"):
        parse_export_manifest(
            content, **(args | {"blobs": (marked.model_copy(update={"size_bytes": 0}),)})
        )
