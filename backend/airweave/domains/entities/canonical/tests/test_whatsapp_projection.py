"""Offline WhatsApp interpretation; no connected account or media conversion."""

import hashlib
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from pydantic import ValidationError

from airweave.core.logging import logger
from airweave.domains.converters.txt import TxtConverter
from airweave.domains.entities.canonical.blob_materializer import BlobIntegrityError
from airweave.domains.entities.canonical.content_models import ContentProvenance, MatchedPart
from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.projection_mappers import map_record
from airweave.domains.entities.canonical.projection_models import ProjectionBinding, ProjectionWork
from airweave.domains.entities.canonical.projection_policy import excluded_from_search
from airweave.domains.entities.canonical.projector import (
    ProjectionContext,
    ProjectionConversionTracker,
    ProjectionRuntime,
    _select_inputs,
)
from airweave.domains.entities.canonical.requests import (
    BlobReference,
    RecordIdentity,
    parent_container_key,
)
from airweave.domains.entities.canonical.whatsapp_projection import map_whatsapp_message
from airweave.domains.sync_pipeline.pipeline.text_builder import TextualRepresentationBuilder
from airweave.platform.entities.whatsapp import WhatsAppAttachmentEntity, WhatsAppMessageEntity


def original(**fields):
    payload = {
        "object": "Message",
        "provider": "whatsapp",
        "id": "native:message",
        "chat_id": "group@g.us",
        "sender_id": "opaque@lid",
        "timestamp": "2026-10-02T12:00:00.000Z",
        "is_sender": False,
        "text": "مرحباً — नमस्ते दुनिया 🌍",
        "attachments": [],
        **fields,
    }
    return SourceRecord(
        id=uuid4(),
        sync_id=uuid4(),
        identity=RecordIdentity(
            record_type="whatsapp_message", native_id=payload["id"], container_id=payload["chat_id"]
        ),
        revision=1,
        payload=payload,
        payload_schema_version=1,
        capture_hash="capture",
        content_hash=None,
        completeness="partial",
        observed_at=datetime.now(timezone.utc),
        source_created_at=None,
        source_updated_at=None,
        deleted_at=None,
        removal_reason=None,
        blobs=(),
        indexed_revision=None,
        indexed_pipeline_version=None,
    )


def attachment(native_id="media", **fields):
    return {
        "object": "Attachment",
        "id": native_id,
        "type": "audio",
        "mimetype": "audio/ogg",
        "voice_note": True,
        **fields,
    }


def retained(record, content, path="/attachments/0"):
    digest = hashlib.sha256(content).hexdigest()
    return BlobReference(
        key=f"canonical/{record.sync_id}/blobs/sha256/{digest}",
        sha256=digest,
        size_bytes=len(content),
        source_path=path,
    )


@pytest.mark.asyncio
async def test_unicode_quote_group_sender_and_metadata_are_distinct(tmp_path):
    record = original(
        sender={
            "object": "User",
            "id": "opaque@lid",
            "display_name": "Profile name",
            "specifics": {"contact_name": "Saved label"},
        },
        quoted={
            "object": "QuotedMessage",
            "id": "missing-original",
            "text": "क्या कल मिलेंगे؟",
            "attachments": [],
        },
        is_seen=True,
        is_delivered=True,
        future_native_field={"retain": "unchanged"},
    )
    before = record.model_dump()
    storage = AsyncMock()
    body, quote = (await map_whatsapp_message(record, storage, tmp_path)).parts
    assert body.native_body.text == "مرحباً — नमस्ते दुनिया 🌍"
    assert quote.native_body.text == "क्या कल मिलेंगे؟"
    assert body.entity.sender_id == "opaque@lid" and body.entity.is_sender is False
    assert body.entity.chat_id == "group@g.us"
    assert body.entity.sender_display_name == "Profile name"
    assert body.entity.sender_contact_name == "Saved label"
    assert body.entity.sender_public_identifier is None
    assert quote.entity.message_id == "missing-original"
    assert quote.entity.owner_message_id == record.identity.native_id
    assert quote.entity.source_path == quote.part.key == "/quoted"
    assert quote.entity.sender_id is None and quote.entity.sent_at is None
    assert quote.entity.is_sender is None
    assert body.entity.entity_id != quote.entity.entity_id
    assert body.native_body.metadata_fields == ("text",)
    assert {
        name
        for name, field in WhatsAppMessageEntity.model_fields.items()
        if (field.json_schema_extra or {}).get("embeddable")
    } == {"text"}
    assert record.model_dump() == before
    storage.read_file.assert_not_called()


@pytest.mark.asyncio
async def test_self_message_preserves_exact_native_sender(tmp_path):
    record = original(chat_id="friend@lid", sender_id="self@lid", is_sender=True)
    mapped = await map_whatsapp_message(record, AsyncMock(), tmp_path)
    assert mapped.parts[0].entity.is_sender is True
    assert mapped.parts[0].entity.sender_id == "self@lid"
    assert mapped.parts[0].entity.chat_id == "friend@lid"


@pytest.mark.asyncio
async def test_voice_note_missing_image_and_quoted_media_use_shared_inputs(tmp_path):
    record = original(
        attachments=[
            attachment(),
            attachment(
                "missing",
                type="img",
                mimetype="image/webp",
                voice_note=None,
                sticker=True,
                is_unavailable=True,
            ),
        ],
        quoted={
            "id": "quote",
            "text": "quoted caption",
            "attachments": [attachment("quoted-media")],
        },
    )
    content = b"retained audio bytes; transcription not exercised"
    record = record.model_copy(update={"blobs": (retained(record, content),)})
    storage = AsyncMock()
    storage.read_file.return_value = content
    mapped = await map_whatsapp_message(record, storage, tmp_path)
    assert [p.part.key for p in mapped.parts] == [
        "/text",
        "/quoted",
        "/attachments/0",
        "/attachments/1",
        "/quoted/attachments/0",
    ]
    media = mapped.parts[2]
    assert isinstance(media.entity, WhatsAppAttachmentEntity)
    assert media.entity.voice_note is True and media.entity.attachment_id == "media"
    assert Path(media.entity.local_path).read_bytes() == content
    assert media.part.media_type == "audio/ogg" and media.native_body is None
    assert media.entity.url == ""
    assert mapped.parts[3].entity is None and mapped.parts[4].entity is None
    assert mapped.parts[0].native_body.text == record.payload["text"]


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["once", "replayable"])
async def test_restricted_content_never_projects_body_quote_or_retained_media(tmp_path, mode):
    record = original(
        view_mode=mode, attachments=[attachment()], quoted={"id": "quote", "text": "private quote"}
    )
    record = record.model_copy(update={"blobs": (retained(record, b"never read"),)})
    storage = AsyncMock()
    mapped = await map_whatsapp_message(record, storage, tmp_path)
    assert [p.part.key for p in mapped.parts] == ["/text", "/quoted", "/attachments/0"]
    assert all(p.entity is None and p.native_body is None for p in mapped.parts)
    storage.read_file.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("flag", ["is_deleted", "is_hidden", "is_event"])
async def test_deleted_hidden_and_system_events_do_not_leak_quotes(tmp_path, flag):
    record = original(**{flag: True}, quoted={"id": "quote", "text": "must not leak"})
    assert (await map_whatsapp_message(record, AsyncMock(), tmp_path)).parts == ()


@pytest.mark.asyncio
async def test_identity_complete_missing_media_and_blob_integrity_fail(tmp_path):
    storage = AsyncMock()
    record = original(attachments=[attachment()])
    with pytest.raises(ValueError, match="lacks a retained attachment"):
        await map_whatsapp_message(
            record.model_copy(update={"completeness": "complete"}), storage, tmp_path
        )
    with pytest.raises(ValueError, match="identity differs"):
        await map_whatsapp_message(
            record.model_copy(
                update={
                    "identity": RecordIdentity(
                        record_type="whatsapp_message", native_id="other", container_id="group@g.us"
                    )
                }
            ),
            storage,
            tmp_path,
        )
    with pytest.raises(ValueError, match="current native attachment"):
        await map_whatsapp_message(
            record.model_copy(update={"blobs": (retained(record, b"expected", "/attachments/9"),)}),
            storage,
            tmp_path,
        )
    storage.read_file.assert_not_called()
    storage.read_file.return_value = b"corrupt"
    with pytest.raises(BlobIntegrityError):
        await map_whatsapp_message(
            record.model_copy(update={"blobs": (retained(record, b"expected"),)}), storage, tmp_path
        )


@pytest.mark.asyncio
async def test_unsupported_retained_format_has_recoverable_descriptor(tmp_path):
    record = original(
        attachments=[attachment(type="future_format", mimetype="application/x-future")]
    )
    record = record.model_copy(update={"blobs": (retained(record, b"preserved original"),)})
    storage = AsyncMock()
    mapped = await map_whatsapp_message(record, storage, tmp_path)
    assert mapped.parts[1].omission == "unsupported_format"
    assert mapped.parts[1].part.key == "/attachments/0"
    assert mapped.parts[0].native_body.text == record.payload["text"]
    storage.read_file.assert_not_called()


@pytest.mark.asyncio
async def test_missing_native_text_is_not_invented_empty_content(tmp_path):
    record = original(text=None, attachments=[attachment(is_unavailable=True)])
    mapped = await map_whatsapp_message(record, AsyncMock(), tmp_path)
    assert mapped.parts[0].part.key == "/text"
    assert mapped.parts[0].entity is None and mapped.parts[0].native_body is None
    assert mapped.parts[1].entity is None
    empty = await map_whatsapp_message(original(text=""), AsyncMock(), tmp_path)
    assert empty.parts[0].native_body.text == ""


@pytest.mark.asyncio
async def test_map_record_entrypoint_prepares_native_text_and_verified_file(tmp_path):

    content = "مستند أصلي — मूल दस्तावेज".encode()
    record = original(
        attachments=[
            attachment(type="file", filename="note.txt", mimetype="text/plain"),
            attachment("missing", is_unavailable=True),
        ]
    )
    record = record.model_copy(update={"blobs": (retained(record, content),)})
    before = record.model_dump()
    storage = AsyncMock()
    storage.read_file.return_value = content
    work = ProjectionWork(
        organization_id=uuid4(),
        binding=ProjectionBinding(
            source_connection_id=uuid4(), source_name="whatsapp", collection_id=uuid4()
        ),
        record=record,
        pipeline_version=1,
        previous_generation=None,
    )
    async with map_record(record, "whatsapp", storage) as mapped:
        selected, coverage = _select_inputs(
            mapped, work, "whatsapp", uuid4(), lambda ext: ext == ".txt"
        )
        assert coverage.status == "partial"
        assert [part.outcome for part in coverage.parts] == [
            "indexed",
            "indexed",
            "unavailable_original",
        ]
        builder = TextualRepresentationBuilder(
            SimpleNamespace(for_extension=lambda ext: TxtConverter() if ext == ".txt" else None)
        )
        native = {
            part.entity.entity_id: part.native_body
            for part in mapped.parts
            if part.entity is not None and part.native_body is not None
        }
        built = await builder.build_with_text(
            selected,
            ProjectionContext(logger, "whatsapp"),
            ProjectionRuntime(ProjectionConversionTracker()),
            native_bodies=native,
            strict_conversion=True,
        )
        body, document = built.representations
        assert body.kind == "native_text"
        assert body.text[body.content_start :] == record.payload["text"]
        provenance = ContentProvenance(
            part=MatchedPart(part_index=0, key="/text", kind="body", title="WhatsApp message"),
            content_start=body.content_start,
            content_end=len(body.text),
        ).chunk_preview(body.text, body.text, 0, len(body.text))
        assert provenance.preview == record.payload["text"]
        assert provenance.original_chunk_start == body.content_start
        assert provenance.original_chunk_end == len(body.text)
        assert body.text[provenance.preview_start : provenance.preview_end] == provenance.preview
        assert provenance.preview_truncated is False
        assert document.text[document.content_start :] == content.decode()
        assert document.kind == "extracted_text"
        local_path = Path(mapped.parts[1].entity.local_path)
        assert local_path.read_bytes() == content
        assert selected[0].airweave_system_metadata.canonical_record_type == "whatsapp_message"
    assert not local_path.exists()
    assert record.model_dump() == before


@pytest.mark.asyncio
async def test_shared_entrypoint_policy_validates_exclusions_before_publishing_empty():

    storage = AsyncMock()
    for record in (original(is_hidden=True), original(is_deleted=True), original(is_event=True)):
        assert excluded_from_search(record, "whatsapp")
        async with map_record(record, "whatsapp", storage) as mapped:
            assert mapped.parts == ()
    chat = original().model_copy(
        update={
            "identity": RecordIdentity(record_type="whatsapp_chat", native_id="group@g.us"),
            "payload": {
                "object": "Chat",
                "provider": "whatsapp",
                "id": "group@g.us",
                "is_group": True,
                "is_1to1": False,
                "is_channel": False,
            },
        }
    )
    assert excluded_from_search(chat, "whatsapp")
    async with map_record(chat, "whatsapp", storage) as mapped:
        assert mapped.parts == ()
    for malformed in (
        original(is_hidden="yes"),
        original(is_deleted=True, timestamp="not-a-time"),
        chat.model_copy(update={"payload": {"object": "Chat", "id": "wrong"}}),
    ):
        with pytest.raises((ValidationError, ValueError)):
            excluded_from_search(malformed, "whatsapp")
        with pytest.raises((ValidationError, ValueError)):
            async with map_record(malformed, "whatsapp", storage):
                pass
    storage.read_file.assert_not_called()


@pytest.mark.asyncio
async def test_restricted_entrypoint_has_unavailable_coverage_and_zero_preparation_inputs():

    record = original(
        view_mode="once", quoted={"id": "private", "text": "private"}, attachments=[attachment()]
    )
    storage = AsyncMock()
    work = ProjectionWork(
        organization_id=uuid4(),
        binding=ProjectionBinding(
            source_connection_id=uuid4(), source_name="whatsapp", collection_id=uuid4()
        ),
        record=record,
        pipeline_version=1,
        previous_generation=None,
    )
    async with map_record(record, "whatsapp", storage) as mapped:
        selected, coverage = _select_inputs(mapped, work, "whatsapp", uuid4(), lambda ext: True)
        assert selected == []
        assert coverage.status == "unavailable"
        assert all(part.outcome == "unavailable_original" for part in coverage.parts)
    storage.read_file.assert_not_called()


@pytest.mark.asyncio
async def test_participant_collection_actual_entrypoint_excludes_only_valid_complete_original():
    parent = RecordIdentity(record_type="whatsapp_chat", native_id="group@g.us")
    payload = {
        "chat_id": parent.native_id,
        "pagination": "offset",
        "page_size": 20,
        "pages": [
            {
                "query": {"limit": 20, "offset": 0},
                "response": {
                    "data": [
                        {
                            "object": "GroupParticipant",
                            "is_admin": False,
                            "is_self": True,
                            "user": {
                                "object": "User",
                                "id": "opaque@lid",
                                "display_name": "مرحباً नमस्ते",
                            },
                            "unknown": {"keep": None},
                        }
                    ]
                },
            },
            {"query": {"limit": 20, "offset": 20}, "response": {"data": []}},
        ],
    }
    record = original().model_copy(
        update={
            "identity": RecordIdentity(
                record_type="whatsapp_chat_participants",
                native_id=parent.native_id,
                container_id=parent_container_key(parent),
            ),
            "parent": parent,
            "payload": payload,
            "completeness": "complete",
        }
    )
    before = record.model_dump()
    storage = AsyncMock()
    assert excluded_from_search(record, "whatsapp")
    async with map_record(record, "whatsapp", storage) as inputs:
        assert inputs.parts == ()
    assert record.model_dump() == before
    storage.read_file.assert_not_awaited()
    malformed = record.model_copy(update={"payload": payload | {"pages": payload["pages"][:1]}})
    with pytest.raises(ValueError, match="exhausted"):
        excluded_from_search(malformed, "whatsapp")
    wrong = record.model_copy(
        update={"identity": record.identity.model_copy(update={"container_id": "wrong"})}
    )
    with pytest.raises(ValueError, match="exact chat-owned"):
        excluded_from_search(wrong, "whatsapp")


@pytest.mark.asyncio
async def test_reaction_aggregate_entrypoint_requires_exact_message_scope_and_no_native_times():
    parent = RecordIdentity(
        record_type="whatsapp_message", native_id="same-message", container_id="group@g.us"
    )
    payload = {
        "chat_id": parent.container_id,
        "message_id": parent.native_id,
        "pagination": "offset",
        "page_size": 20,
        "pages": [{"query": {"limit": 20, "offset": 0}, "response": {"data": []}}],
    }
    record = original().model_copy(
        update={
            "identity": RecordIdentity(
                record_type="whatsapp_message_reactions",
                native_id=parent.native_id,
                container_id=parent_container_key(parent),
            ),
            "parent": parent,
            "payload": payload,
            "completeness": "complete",
        }
    )
    storage = AsyncMock()
    async with map_record(record, "whatsapp", storage) as inputs:
        assert inputs.parts == ()
    other = parent.model_copy(update={"container_id": "other@g.us"})
    for changes in [
        {"parent": other},
        {"payload": payload | {"chat_id": "other@g.us"}},
        {"source_updated_at": datetime.now(timezone.utc)},
        {"source_created_at": datetime.now(timezone.utc)},
        {"blobs": (retained(record, b"not-a-native-reaction-file"),)},
        {"payload": payload | {"pages": []}},
    ]:
        with pytest.raises(ValueError):
            excluded_from_search(record.model_copy(update=changes), "whatsapp")
