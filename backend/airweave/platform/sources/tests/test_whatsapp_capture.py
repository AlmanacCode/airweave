"""Native acquisition simulations; no private accounts or provider mutations."""

import hashlib
import traceback
from datetime import datetime, timezone
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import httpx
import pytest
from pydantic import ValidationError

from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.requests import (
    BlobReference,
    CompletedScope,
    RecordIdentity,
    parent_container_key,
)
from airweave.domains.entities.canonical.scan_models import ScanContinuation
from airweave.domains.sources.exceptions import SourceError, SourceRateLimitError, SourceServerError
from airweave.domains.storage import FileSkippedException
from airweave.platform.http_client.airweave_client import AirweaveHttpClient
from airweave.platform.http_client.unipile_transport import UnipileError, UnipileWhatsAppClient
from airweave.platform.sources.records.whatsapp_collections import (
    WhatsAppParticipantCollection,
    WhatsAppReactionCollection,
)
from airweave.platform.sources.records.whatsapp_models import (
    WhatsAppAccount,
    WhatsAppMessage,
    WhatsAppPage,
    WhatsAppParticipant,
    WhatsAppReaction,
    WhatsAppUser,
)
from airweave.platform.sources.whatsapp_capture import (
    WhatsAppCapture,
    WhatsAppCaptureConfig,
    WhatsAppProgress,
)

CHAT = {
    "object": "Chat",
    "provider": "whatsapp",
    "id": "group@lid",
    "is_group": True,
    "is_1to1": False,
    "is_channel": False,
}
MESSAGE = {
    "object": "Message",
    "provider": "whatsapp",
    "id": "msg",
    "chat_id": "group@lid",
    "sender_id": "person@lid",
    "timestamp": "2026-10-02T00:00:00Z",
    "is_sender": False,
    "text": "مرحباً नमस्ते 🌍",
    "native_unknown": {"x": [None, 3]},
    "quoted": {"id": "older", "text": "引用"},
}
ATTACHMENT = {
    "object": "Attachment",
    "id": "a/b",
    "type": "audio",
    "mimetype": "audio/ogg",
    "voice_note": True,
    "url": "https://untrusted.invalid/signed?secret=yes",
}


def source(mode="cursor", pages=10):
    client = Mock()
    client.account_id = "acc_bound"
    client.account = AsyncMock(
        return_value=WhatsAppAccount.model_validate(
            {
                "object": "Account",
                "id": "acc_bound",
                "user_id": "self@lid",
                "provider": "whatsapp",
                "status": "running",
                "is_locked": False,
            }
        )
    )
    client.owner_profile = AsyncMock(
        return_value=WhatsAppUser.model_validate(
            {
                "object": "UserProfile",
                "id": "self@lid",
                "display_name": "Self",
            }
        )
    )
    client.attachment = AsyncMock(return_value=(b"original-audio", "audio/ogg"))
    capture = WhatsAppCapture(
        client,
        WhatsAppCaptureConfig(
            account_id="acc_bound",
            account_user_id="self@lid",
            native_user_id="self@lid",
            pagination=mode,
            page_size=20,
            maximum_attachment_bytes=1000,
            participant_pagination="offset",
            maximum_participant_pages=10,
            maximum_participant_items=100,
            maximum_participant_bytes=100000,
            reaction_pagination="offset",
            maximum_reaction_pages=10,
            maximum_reaction_items=100,
            maximum_reaction_bytes=100000,
            max_pages_per_scope=pages,
        ),
    )
    return capture


def parent():
    return SourceRecord(
        id=uuid4(),
        sync_id=uuid4(),
        identity=RecordIdentity(record_type="whatsapp_chat", native_id="group@lid"),
        revision=1,
        payload=CHAT,
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


class Files:
    """Retain exact immutable bytes and native pointer association in the simulation."""

    def __init__(self):
        self.content = None

    async def store_canonical_blob(self, content, *, media_type):
        self.content = content
        digest = hashlib.sha256(content).hexdigest()
        return BlobReference(
            key=f"sha256/{digest}", sha256=digest, size_bytes=len(content), media_type=media_type
        )


def message_page(**fields):
    return WhatsAppPage[WhatsAppMessage].model_validate({"data": [MESSAGE], **fields})


def test_declared_cursor_mode_multpage_cycle_and_missing_end():
    capture = source()
    first, final = capture._advance(message_page(next_cursor="A"), WhatsAppProgress())
    assert not final
    second = message_page(next_cursor="B")
    second.data[0].id = "other"
    next_position, final = capture._advance(second, WhatsAppProgress.model_validate(first.value))
    assert not final
    with pytest.raises(ValueError, match="cursor repeated"):
        capture._advance(
            message_page(next_cursor="A"), WhatsAppProgress.model_validate(next_position.value)
        )
    end, final = capture._advance(WhatsAppPage[WhatsAppMessage](data=[]), WhatsAppProgress())
    assert final and end.value == {}


def test_offset_short_page_empty_end_and_repeated_page_budget():
    capture = source("offset", pages=2)
    position, final = capture._advance(message_page(), WhatsAppProgress())
    assert not final and position.value["offset"] == 20
    with pytest.raises(ValueError, match="previous page"):
        capture._advance(message_page(), WhatsAppProgress.model_validate(position.value))
    _, final = capture._advance(
        WhatsAppPage[WhatsAppMessage](data=[]), WhatsAppProgress.model_validate(position.value)
    )
    assert final
    with pytest.raises(ValueError, match="max_pages_per_scope"):
        capture._advance(message_page(), WhatsAppProgress(pages=2))


@pytest.mark.asyncio
async def test_mode_mismatch_denied_before_requests():
    capture = source("offset")
    with pytest.raises(ValueError, match="another pagination mode"):
        await capture.capture_page(
            CompletedScope(record_type="whatsapp_chat"),
            ScanContinuation(value={"cursor": "A"}),
            files=Files(),
        )
    capture.client.account.assert_not_awaited()


@pytest.mark.asyncio
async def test_bound_principal_and_nonrunning_status_classification():
    capture = source()
    with pytest.raises(UnipileError, match="identity"):
        WhatsAppCapture(Mock(account_id="acc_other"), capture.config)
    capture.client.account.return_value.user_id = "wrong@lid"
    with pytest.raises(SourceError, match="identity"):
        await capture.validate()
    capture.client.account.return_value.user_id = "self@lid"
    capture.client.account.return_value.status = "errored"
    with pytest.raises(SourceError, match="transient"):
        await capture.validate()
    capture.client.account.return_value.status = "disconnected"
    with pytest.raises(SourceError, match="authentication"):
        await capture.validate()


@pytest.mark.asyncio
async def test_native_payload_pointer_original_media_and_child_identity():
    capture = source()
    original = MESSAGE | {"attachments": [ATTACHMENT]}
    capture.client.messages = AsyncMock(
        return_value=WhatsAppPage[WhatsAppMessage].model_validate({"data": [original]})
    )
    retained_parent = parent()
    files = Files()
    page = await capture.capture_page(
        capture.child_scope(retained_parent, "whatsapp_message"),
        ScanContinuation(),
        files=files,
        parent=retained_parent,
    )
    record = page.records[0]
    assert record.payload == original
    assert record.identity.container_id == "group@lid"
    assert record.parent == retained_parent.identity
    assert record.blobs[0].source_path == "/attachments/0"
    assert record.blobs[0].sha256 == hashlib.sha256(files.content).hexdigest()
    capture.client.attachment.assert_awaited_once_with(
        "group@lid", "msg", "a/b", max_bytes=1000, expected_mimetype="audio/ogg"
    )
    assert (
        page.final
        and capture.capture_cycle_configuration.completion_policies["whatsapp_message"]
        == "discovery_only"
    )


@pytest.mark.asyncio
async def test_unavailable_media_is_partial_and_transient_fails_without_advancing():
    capture = source()
    original = MESSAGE | {"attachments": [ATTACHMENT | {"is_unavailable": True}]}
    message = WhatsAppMessage.model_validate(original)
    record = await capture._message(message, parent().identity, Files(), datetime.now(timezone.utc))
    assert record.payload == original and record.blobs == () and record.completeness == "partial"
    capture.client.attachment.assert_not_awaited()
    capture.client.attachment.side_effect = UnipileError("transient")
    with pytest.raises(UnipileError, match="transient"):
        await capture._message(
            WhatsAppMessage.model_validate(MESSAGE | {"attachments": [ATTACHMENT]}),
            parent().identity,
            Files(),
            datetime.now(timezone.utc),
        )


@pytest.mark.asyncio
async def test_restricted_and_deleted_messages_never_download():
    capture = source()
    for restriction in ({"is_hidden": True}, {"view_mode": "once"}, {"is_deleted": True}):
        message = WhatsAppMessage.model_validate(
            MESSAGE | restriction | {"attachments": [ATTACHMENT]}
        )
        record = await capture._message(
            message, parent().identity, Files(), datetime.now(timezone.utc)
        )
        assert record.blobs == ()
        assert record.kind == ("delete" if restriction.get("is_deleted") else "upsert")
    capture.client.attachment.assert_not_awaited()
    with pytest.raises(UnipileError, match="identity"):
        await capture._message(
            WhatsAppMessage.model_validate(MESSAGE | {"chat_id": "other@lid"}),
            parent().identity,
            Files(),
            datetime.now(timezone.utc),
        )


@pytest.mark.asyncio
async def test_unknown_size_skip_and_middle_page_transient_leave_progress_uncommitted():

    capture = source()
    native = MESSAGE | {"attachments": [ATTACHMENT]}
    capture.client.attachment.side_effect = FileSkippedException("size limit", "attachment")
    record = await capture._message(
        WhatsAppMessage.model_validate(native),
        parent().identity,
        Files(),
        datetime.now(timezone.utc),
    )
    assert record.payload == native and record.blobs == () and record.completeness == "partial"
    capture.client.messages = AsyncMock(
        return_value=WhatsAppPage[WhatsAppMessage].model_validate(
            {"data": [native, native | {"id": "second"}], "next_cursor": "next"}
        )
    )
    capture.client.attachment.side_effect = [(b"original", "audio/ogg"), UnipileError("transient")]
    retained_parent = parent()
    position = ScanContinuation()
    files = Files()
    with pytest.raises(SourceServerError):
        await capture.capture_page(
            capture.child_scope(retained_parent, "whatsapp_message"),
            position,
            parent=retained_parent,
            files=files,
        )
    assert position.value == {} and files.content == b"original"


@pytest.mark.asyncio
async def test_shared_rate_category_retains_known_timing_and_unknown_uses_server_recovery():

    capture = source()
    capture.client.account.side_effect = UnipileError("rate_limit", status=429, retry_after=9.25)
    with pytest.raises(SourceRateLimitError) as known:
        await capture.validate()
    assert known.value.retry_after == 9.25
    capture.client.account.side_effect = UnipileError("rate_limit", status=429)
    with pytest.raises(SourceServerError) as unknown:
        await capture.validate()
    assert unknown.value.status_code == 429 and unknown.value.__cause__.retry_after is None


def test_explicit_more_without_cursor_cannot_complete_capture_scope():
    capture = source()
    with pytest.raises(ValueError, match="has_more contradicts"):
        capture._advance(message_page(has_more=True), WhatsAppProgress())
    with pytest.raises(ValueError, match="has_more contradicts"):
        capture._advance(message_page(next_cursor="next", has_more=False), WhatsAppProgress())
    offset = source("offset")
    with pytest.raises(ValueError, match="has_more contradicts"):
        offset._advance(WhatsAppPage[WhatsAppMessage](data=[], has_more=True), WhatsAppProgress())


@pytest.mark.asyncio
async def test_owner_lookup_alias_and_resolved_native_principal_are_both_bound():
    capture = source()
    config = capture.config.model_copy(update={"account_user_id": "15550000000"})
    capture = WhatsAppCapture(capture.client, config)
    capture.client.account.return_value.user_id = config.account_user_id
    await capture.validate()
    capture.client.owner_profile.assert_awaited_once_with(capture.client.account.return_value)
    capture.client.owner_profile.return_value.id = "different@lid"
    with pytest.raises(SourceError, match="identity"):
        await capture.validate()
    capture.client.account.return_value.user_id = "different_lookup"
    with pytest.raises(SourceError, match="identity"):
        await capture.validate()
    other = config.model_copy(update={"account_user_id": "different_lookup"})
    assert (
        WhatsAppCapture(capture.client, other).capture_cycle_configuration
        != capture.capture_cycle_configuration
    )


def participant_page(user_id="member@lid", **fields):
    native = {
        "object": "GroupParticipant",
        "is_self": False,
        "is_admin": True,
        "user": {
            "object": "User",
            "id": user_id,
            "display_name": "مرحباً नमस्ते",
            "specifics": {"contact_name": "Saved name"},
        },
        "unknown_native": [None, "🌍"],
    }
    return WhatsAppPage[WhatsAppParticipant].model_validate({"data": [native], **fields})


@pytest.mark.asyncio
async def test_participant_collection_offset_retains_exact_envelopes_and_same_count_replacement():
    capture = source()
    owner = parent()
    first, second = (
        participant_page(envelope_extra={"preserved": None}),
        participant_page("other@lid"),
    )
    empty = type(first).model_validate({"data": [], "terminal_extra": 1})
    capture.client.participants = AsyncMock(side_effect=[first, second, empty])
    scope = capture.child_scope(owner, "whatsapp_chat_participants")
    page = await capture.capture_page(scope, ScanContinuation(), parent=owner, files=Files())
    record = page.records[0]
    assert page.final and page.continuation.value == {} and len(page.records) == 1
    assert record.identity.native_id == owner.identity.native_id
    assert record.identity.container_id == parent_container_key(owner.identity)
    assert record.parent == owner.identity and record.completeness == "complete"
    assert record.source_created_at is None and record.source_updated_at is None
    assert [p["response"] for p in record.payload["pages"]] == [
        first.original(),
        second.original(),
        empty.original(),
    ]
    assert [call.kwargs for call in capture.client.participants.await_args_list] == [
        {"limit": 20, "offset": 0},
        {"limit": 20, "offset": 20},
        {"limit": 20, "offset": 40},
    ]
    capture.client.participants.side_effect = [first, second, empty]
    same = await capture.capture_page(scope, ScanContinuation(), parent=owner, files=Files())
    assert same.records[0].payload == record.payload
    capture.client.participants.side_effect = [participant_page("replaced@lid"), second, empty]
    changed = await capture.capture_page(scope, ScanContinuation(), parent=owner, files=Files())
    assert changed.records[0].payload != record.payload


@pytest.mark.asyncio
async def test_participant_cursor_mode_independent_and_direct_chat_skips_endpoint():
    capture = source("offset")
    capture.config = capture.config.model_copy(update={"participant_pagination": "cursor"})
    capture.client.participants = AsyncMock(
        side_effect=[participant_page(next_cursor="next"), participant_page("other@lid")]
    )
    owner = parent()
    page = await capture.capture_page(
        capture.child_scope(owner, "whatsapp_chat_participants"),
        ScanContinuation(),
        parent=owner,
        files=Files(),
    )
    assert page.final and len(page.records[0].payload["pages"]) == 2
    assert capture.client.participants.await_args_list[1].kwargs == {"cursor": "next"}
    capture.client.participants.reset_mock()
    direct = owner.model_copy(update={"payload": CHAT | {"is_group": False, "is_1to1": True}})
    skipped = await capture.capture_page(
        capture.child_scope(direct, "whatsapp_chat_participants"),
        ScanContinuation(),
        parent=direct,
        files=Files(),
    )
    assert skipped.records == () and skipped.final
    capture.client.participants.assert_not_awaited()

    wrong_parent = owner.model_copy(update={"payload": CHAT | {"id": "wrong@lid"}})
    with pytest.raises(ValueError, match="identity"):
        await capture.capture_page(
            capture.child_scope(owner, "whatsapp_chat_participants"),
            ScanContinuation(),
            parent=wrong_parent,
            files=Files(),
        )
    capture.client.participants.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("maximum_participant_pages", 1),
        ("maximum_participant_items", 1),
        ("maximum_participant_bytes", 1),
    ],
)
async def test_participant_collection_budget_fails_before_any_commit(field, value):
    capture = source()
    capture.config = capture.config.model_copy(update={field: value})
    capture.client.participants = AsyncMock(
        side_effect=[participant_page(), participant_page("other@lid")]
    )
    owner = parent()
    continuation = ScanContinuation()
    with pytest.raises(ValueError, match=field):
        await capture.capture_page(
            capture.child_scope(owner, "whatsapp_chat_participants"),
            continuation,
            parent=owner,
            files=Files(),
        )
    assert continuation.value == {}


@pytest.mark.asyncio
async def test_participant_transient_repeat_and_bad_scope_do_not_return_partial_collection():
    capture = source()
    owner = parent()
    scope = capture.child_scope(owner, "whatsapp_chat_participants")
    capture.client.participants = AsyncMock(
        side_effect=[participant_page(), UnipileError("transient")]
    )
    with pytest.raises(SourceServerError):
        await capture.capture_page(scope, ScanContinuation(), parent=owner, files=Files())
    capture.client.participants.side_effect = [participant_page(), participant_page()]
    with pytest.raises(ValueError, match="repeated"):
        await capture.capture_page(scope, ScanContinuation(), parent=owner, files=Files())
    capture.client.participants.reset_mock()
    with pytest.raises(ValueError, match="scope"):
        await capture.capture_page(
            scope, ScanContinuation(value={"offset": 20}), parent=owner, files=Files()
        )
    capture.client.participants.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("overlap", ["changed_profile", "reordered", "within_page"])
async def test_participant_identity_overlap_rejected_capture_and_validation(
    overlap,
):
    one = participant_page().original()["data"][0]
    other = participant_page("other@lid").original()["data"][0]
    changed = one | {"user": one["user"] | {"display_name": "Changed नाम"}, "is_admin": False}
    native_pages = {
        "changed_profile": [{"data": [one]}, {"data": [changed]}],
        "reordered": [{"data": [one, other]}, {"data": [other, one]}],
        "within_page": [{"data": [one, changed]}],
    }[overlap]
    page_type = type(participant_page())
    capture = source()
    capture.client.participants = AsyncMock(
        side_effect=[page_type.model_validate(p) for p in native_pages]
    )
    owner = parent()
    position = ScanContinuation()
    with pytest.raises(ValueError, match="identity repeated"):
        await capture.capture_page(
            capture.child_scope(owner, "whatsapp_chat_participants"),
            position,
            parent=owner,
            files=Files(),
        )
    assert position.value == {}
    # Retained originals cannot bypass the same rule during offline exclusion validation.
    with pytest.raises(ValueError, match="identity repeated"):
        WhatsAppParticipantCollection.model_validate(
            {
                "chat_id": owner.identity.native_id,
                "pagination": "offset",
                "page_size": 20,
                "pages": [
                    {"query": {"limit": 20, "offset": index * 20}, "response": raw}
                    for index, raw in enumerate([*native_pages, {"data": []}])
                ],
            }
        )
    assert native_pages[0]["data"][0] == one


def reaction_page(values=("❤️",), **fields):
    return WhatsAppPage[WhatsAppReaction].model_validate(
        {
            "data": [
                {
                    "object": "Reaction",
                    "value": value,
                    "is_sender": False,
                    "sender": {"object": "User", "id": "member@lid", "display_name": "नाम"},
                    "unknown_native": [None, "عربي"],
                }
                for value in values
            ],
            **fields,
        }
    )


def message_parent(**fields):
    owner = parent()
    return owner.model_copy(
        update={
            "identity": RecordIdentity(
                record_type="whatsapp_message", native_id="msg", container_id="group@lid"
            ),
            "parent": owner.identity,
            "payload": MESSAGE | fields,
        }
    )


@pytest.mark.asyncio
async def test_reactions_raw_unicode_duplicates_counters_and_exact_message_parent():
    capture = source()
    owner = message_parent(reactions_counter=[])
    first = reaction_page(("❤️", "❤️", "👍🏽"), native_extra={"preserved": None})
    second = reaction_page(("❤️", "👨‍👩‍👧‍👦"))
    empty = type(first).model_validate({"data": []})
    capture.client.reactions = AsyncMock(side_effect=[first, second, empty])
    scope = capture.child_scope(owner, "whatsapp_message_reactions")
    result = await capture.capture_page(scope, ScanContinuation(), parent=owner, files=Files())
    record = result.records[0]
    assert record.parent == owner.identity and record.identity.native_id == "msg"
    assert record.identity.container_id == parent_container_key(owner.identity)
    assert record.source_created_at is None and record.source_updated_at is None
    assert result.final and result.continuation.value == {}
    assert [p["response"] for p in record.payload["pages"]] == [
        first.original(),
        second.original(),
        empty.original(),
    ]
    assert [c.kwargs for c in capture.client.reactions.await_args_list] == [
        {"limit": 20, "offset": 0},
        {"limit": 20, "offset": 20},
        {"limit": 20, "offset": 40},
    ]
    # Equal message IDs in another chat have distinct full-parent collection identities.
    other_chat = RecordIdentity(record_type="whatsapp_chat", native_id="other@lid")
    other = owner.model_copy(
        update={
            "identity": owner.identity.model_copy(update={"container_id": "other@lid"}),
            "parent": other_chat,
            "payload": owner.payload | {"chat_id": "other@lid"},
        }
    )
    assert (
        capture.child_scope(other, "whatsapp_message_reactions").container_id != scope.container_id
    )
    capture.client.reactions.side_effect = [empty]
    absent_counters = message_parent()
    refreshed = await capture.capture_page(
        capture.child_scope(absent_counters, "whatsapp_message_reactions"),
        ScanContinuation(),
        parent=absent_counters,
        files=Files(),
    )
    assert refreshed.records[0].payload["pages"][0]["response"] == {"data": []}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "flags",
    [
        {"is_deleted": True},
        {"is_hidden": True},
        {"is_event": True},
        {"view_mode": "once"},
        {"view_mode": "replayable"},
    ],
)
async def test_reaction_restricted_owner_never_fetches(flags):
    capture = source()
    capture.client.reactions = AsyncMock()
    owner = message_parent(**flags)
    result = await capture.capture_page(
        capture.child_scope(owner, "whatsapp_message_reactions"),
        ScanContinuation(),
        parent=owner,
        files=Files(),
    )
    assert result.final and result.records == ()
    capture.client.reactions.assert_not_awaited()
    capture.client.account.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [("maximum_reaction_pages", 1), ("maximum_reaction_items", 1), ("maximum_reaction_bytes", 1)],
)
async def test_reaction_budgets_fail_without_partial_record(field, value):
    capture = source()
    capture.config = capture.config.model_copy(update={field: value})
    capture.client.reactions = AsyncMock(side_effect=[reaction_page(), reaction_page(("👍🏽",))])
    owner = message_parent()
    continuation = ScanContinuation()
    with pytest.raises(ValueError, match=field):
        await capture.capture_page(
            capture.child_scope(owner, "whatsapp_message_reactions"),
            continuation,
            parent=owner,
            files=Files(),
        )
    assert continuation.value == {}


@pytest.mark.asyncio
async def test_reaction_transient_reordered_wholepage_cursor_and_parent_validation():
    capture = source()
    owner = message_parent()
    scope = capture.child_scope(owner, "whatsapp_message_reactions")
    first, reordered = reaction_page(("❤️", "👍🏽")), reaction_page(("👍🏽", "❤️"))
    capture.client.reactions = AsyncMock(side_effect=[first, UnipileError("transient")])
    with pytest.raises(SourceServerError):
        await capture.capture_page(scope, ScanContinuation(), parent=owner, files=Files())
    capture.client.reactions.side_effect = [first, reordered]
    with pytest.raises(ValueError, match="whole page repeated"):
        await capture.capture_page(scope, ScanContinuation(), parent=owner, files=Files())
    with pytest.raises(ValueError, match="whole page repeated"):
        WhatsAppReactionCollection.model_validate(
            {
                "chat_id": "group@lid",
                "message_id": "msg",
                "pagination": "offset",
                "page_size": 20,
                "pages": [
                    {"query": {"limit": 20, "offset": index * 20}, "response": page.original()}
                    for index, page in enumerate([first, reordered])
                ],
            }
        )
    capture.client.reactions.reset_mock()
    wrong = owner.model_copy(update={"payload": owner.payload | {"chat_id": "wrong@lid"}})
    with pytest.raises(ValueError, match="identity"):
        await capture.capture_page(scope, ScanContinuation(), parent=wrong, files=Files())
    capture.client.reactions.assert_not_awaited()
    capture.config = capture.config.model_copy(update={"reaction_pagination": "cursor"})
    capture.client.reactions.side_effect = [
        reaction_page(next_cursor="next"),
        reaction_page(("👍🏽",)),
    ]
    result = await capture.capture_page(scope, ScanContinuation(), parent=owner, files=Files())
    assert result.final and capture.client.reactions.await_args_list[1].kwargs == {"cursor": "next"}


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["native_parent", "collection", "continuation", "timestamp"])
async def test_validation_error_traceback_omits_private_inputs(boundary):
    sentinel = "synthetic" + "-private-value-" + "never-log"
    capture = source()
    owner = parent()
    with pytest.raises(ValidationError) as failure:
        if boundary == "native_parent":
            invalid = owner.model_copy(update={"payload": CHAT | {"is_group": sentinel}})
            await capture.capture_page(
                capture.child_scope(owner, "whatsapp_chat_participants"),
                ScanContinuation(),
                parent=invalid,
                files=Files(),
            )
        elif boundary == "collection":
            WhatsAppParticipantCollection.model_validate(
                {
                    "chat_id": sentinel,
                    "pagination": "offset",
                    "page_size": 20,
                    "pages": [
                        {
                            "query": {"offset": 0, "limit": 20},
                            "response": {
                                "data": [
                                    {
                                        "object": "GroupParticipant",
                                        "is_admin": sentinel,
                                        "is_self": False,
                                        "user": {"object": "User", "id": sentinel},
                                    }
                                ]
                            },
                        }
                    ],
                }
            )
        elif boundary == "continuation":
            await capture.capture_page(
                CompletedScope(record_type="whatsapp_chat"),
                ScanContinuation(value={"offset": sentinel}),
                files=Files(),
            )
        else:
            await capture._message(
                WhatsAppMessage.model_validate(MESSAGE | {"timestamp": sentinel}),
                owner.identity,
                Files(),
                datetime.now(timezone.utc),
            )
    error = failure.value
    assert sentinel not in str(error)
    assert sentinel not in "".join(traceback.format_exception(error))
    assert error.__cause__ is None  # No catch-all replacement or altered cause policy.


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mismatch", ["event_account", "event_chat", "parent_payload", "deleted_parent"]
)
async def test_exact_refresh_identity_mismatch_denied_before_any_provider_io(mismatch):
    capture = source()
    capture.client.message = AsyncMock()
    owner = parent()
    account_id, chat_id = "acc_bound", "group@lid"
    if mismatch == "event_account":
        account_id = "different-account"
    elif mismatch == "event_chat":
        chat_id = "different@lid"
    elif mismatch == "parent_payload":
        owner = owner.model_copy(update={"payload": CHAT | {"id": "different@lid"}})
    else:
        owner = owner.model_copy(update={"deleted_at": datetime.now(timezone.utc)})
    with pytest.raises(SourceError, match="identity"):
        await capture.acquire_message_refresh(
            event_account_id=account_id,
            event_chat_id=chat_id,
            event_message_id="msg",
            parent=owner,
            files=Files(),
        )
    capture.client.account.assert_not_awaited()
    capture.client.message.assert_not_awaited()
    capture.client.attachment.assert_not_awaited()


@pytest.mark.asyncio
async def test_exact_refresh_real_client_rejects_returned_message_identity():
    capture = source()
    requests = []

    def response(request):
        requests.append(request.url.path)
        return httpx.Response(200, json=MESSAGE | {"id": "different-message"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as raw:
        client = UnipileWhatsAppClient(
            AirweaveHttpClient(raw, uuid4(), "whatsapp", feature_flag_enabled=False),
            account_id="acc_bound",
            api_key="synthetic-test-key",
        )
        # The caller owns one run-level validation; acquisition performs only exact GET.
        capture.client = client
        with pytest.raises(SourceError, match="identity"):
            await capture.acquire_message_refresh(
                event_account_id="acc_bound",
                event_chat_id="group@lid",
                event_message_id="msg",
                parent=parent(),
                files=Files(),
            )
    assert requests == ["/v2/acc_bound/chats/group@lid/messages/msg"]


@pytest.mark.asyncio
async def test_exact_refresh_not_found_raises_without_tombstone_or_media_acquisition():
    capture = source()
    requests = []

    def response(request):
        requests.append(request.url.path)
        return httpx.Response(404, json={"object": "Error", "status": 404})

    files = Files()
    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as raw:
        capture.client = UnipileWhatsAppClient(
            AirweaveHttpClient(raw, uuid4(), "whatsapp", feature_flag_enabled=False),
            account_id="acc_bound",
            api_key="synthetic-test-key",
        )
        with pytest.raises(SourceError):
            await capture.acquire_message_refresh(
                event_account_id="acc_bound",
                event_chat_id="group@lid",
                event_message_id="msg",
                parent=parent(),
                files=files,
            )
    assert requests == ["/v2/acc_bound/chats/group@lid/messages/msg"]
    assert files.content is None


@pytest.mark.asyncio
async def test_exact_refresh_uses_identity_hint_and_retains_full_native_body_and_original_media():
    capture = source()
    native = MESSAGE | {
        "is_edited": True,
        "attachments": [ATTACHMENT],
        "extra_native": {"retain": [None, "नमस्ते"]},
    }
    capture.client.message = AsyncMock(return_value=WhatsAppMessage.model_validate(native))
    owner = parent()
    files = Files()
    # Event's sparse update contributes only a locator; authored text must come from exact GET.
    hint = {
        "account_id": "acc_bound",
        "chat_id": "group@lid",
        "message_id": "msg",
        "is_edited": True,
    }
    record = await capture.acquire_message_refresh(
        event_account_id=hint["account_id"],
        event_chat_id=hint["chat_id"],
        event_message_id=hint["message_id"],
        parent=owner,
        files=files,
    )
    assert record.payload == native and record.parent == owner.identity
    assert (
        record.payload["text"] == MESSAGE["text"] and record.payload["quoted"] == MESSAGE["quoted"]
    )
    assert record.blobs[0].source_path == "/attachments/0"
    assert record.blobs[0].sha256 == hashlib.sha256(files.content).hexdigest()
    capture.client.message.assert_awaited_once_with("group@lid", "msg")
    capture.client.account.assert_not_awaited()
    capture.client.owner_profile.assert_not_awaited()
