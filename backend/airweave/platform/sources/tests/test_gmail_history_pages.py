"""Bounded history replay uses current native message state, never event arrival state."""

import httpx
import pytest
from pydantic import ValidationError

from airweave.domains.entities.canonical.page_source import InvalidScanContinuation
from airweave.platform.sources.gmail_capture import BASE, GmailCapture, parse_history_page
from airweave.platform.sources.http_helpers import raise_for_status
from airweave.platform.sources.tests.test_gmail_capture import Provider, message


def history(ids, **extra):
    return {
        "historyId": "opaque:boundary",
        "history": [{"messages": [{"id": i} for i in ids]}],
        **extra,
    }


def translated_error(status):
    response = httpx.Response(status, request=httpx.Request("GET", BASE))
    try:
        raise_for_status(response, source_short_name="gmail", token_provider_kind="composio")
    except Exception as error:
        return error
    raise AssertionError("Expected source exception")


@pytest.mark.asyncio
async def test_one_history_entry_can_hydrate_more_than_500_messages_in_bounded_batches():
    ids = [f"m{i}" for i in range(1003)]
    provider = Provider([(f"/messages/{mid}", message(mid)) for mid in ids])
    capture = GmailCapture(provider.get, None)
    page = parse_history_page(history(ids))
    first = await capture.hydrate_history_page(page)
    assert len(first.records) == 500 and not first.complete
    second = await capture.hydrate_history_page(
        page, offset=first.next_offset, expected_fingerprint=page.fingerprint
    )
    final = await capture.hydrate_history_page(
        page, offset=second.next_offset, expected_fingerprint=page.fingerprint
    )
    assert len(second.records) == 500 and len(final.records) == 3
    assert final.next_offset == 1003 and final.complete
    assert [r.identity.native_id for r in (*first.records, *second.records, *final.records)] == ids
    assert not provider.steps


def test_all_native_arrays_ordered_dedup_and_opaque_boundary():
    raw = {
        "historyId": "opaque:end",
        "history": [
            {
                "messages": [{"id": "a"}],
                "messagesAdded": [{"message": {"id": "a"}}, {"message": {"id": "b"}}],
                "messagesDeleted": [{"message": {"id": "c"}}],
                "labelsAdded": [{"message": {"id": "d"}, "labelIds": ["L"]}],
                "labelsRemoved": [{"message": {"id": "e"}}],
            }
        ],
    }
    page = parse_history_page(raw)
    assert page.message_ids == ("a", "b", "c", "d", "e")
    assert page.history_id == "opaque:end"
    assert parse_history_page({**raw, "historyId": "new-current"}).fingerprint != page.fingerprint
    intermediate = {**raw, "nextPageToken": "next"}
    assert (
        parse_history_page(intermediate).fingerprint
        == parse_history_page({**intermediate, "historyId": "new-current"}).fingerprint
    )
    assert parse_history_page({**raw, "nextPageToken": "next"}).fingerprint != page.fingerprint


@pytest.mark.asyncio
async def test_changed_replayed_ids_reject_offset_before_any_reads():
    provider = Provider([])
    capture = GmailCapture(provider.get, None)
    old = parse_history_page(history(["a", "b"]))
    changed = parse_history_page(history(["b", "a"]))
    with pytest.raises(InvalidScanContinuation):
        await capture.hydrate_history_page(changed, offset=1, expected_fingerprint=old.fingerprint)
    with pytest.raises(ValueError, match="fingerprint"):
        await capture.hydrate_history_page(old, offset=1)
    assert not provider.calls


@pytest.mark.asyncio
async def test_current_state_overrides_deleted_event_and_typed_404_tombstones():
    provider = Provider(
        [
            ("/messages/alive", message("alive", labelIds=["TRASH"])),
            ("/messages/gone", translated_error(404)),
        ]
    )
    page = parse_history_page(
        {
            "historyId": "opaque",
            "history": [
                {"messagesDeleted": [{"message": {"id": "alive"}}, {"message": {"id": "gone"}}]}
            ],
        }
    )
    batch = await GmailCapture(provider.get, None).hydrate_history_page(page)
    assert batch.records[0].kind == "upsert"
    assert batch.records[0].payload["labelIds"] == ["TRASH"]
    assert batch.records[1].kind == "delete"
    assert batch.records[1].removal_reason == "provider_deleted"


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [401, 403, 429, 500])
async def test_non_missing_errors_propagate_from_message_and_history(status):
    error = translated_error(status)
    provider = Provider([("/messages/a", error), ("/history", error)])
    capture = GmailCapture(provider.get, None)
    with pytest.raises(type(error)):
        await capture.hydrate_history_page(parse_history_page(history(["a"])))
    with pytest.raises(type(error)):
        await capture.history_page("opaque")


@pytest.mark.asyncio
async def test_page_fetch_preserves_boundary_and_token_without_advancing_cursor():
    provider = Provider([("/history", history([], nextPageToken="next"))])
    page = await GmailCapture(provider.get, None).history_page(
        "opaque:start", "token", max_results=1
    )
    assert provider.calls[0][1] == {
        "startHistoryId": "opaque:start",
        "pageToken": "token",
        "maxResults": 1,
    }
    assert page.next_page_token == "next" and page.message_ids == ()


@pytest.mark.parametrize(
    "raw",
    [
        {"historyId": 123},
        {"historyId": ""},
        {"historyId": "x", "history": [{"messages": [{"id": 7}]}]},
        {"historyId": "x", "history": [{"messagesDeleted": [{}]}]},
    ],
)
def test_malformed_provider_history_never_becomes_empty_success(raw):
    with pytest.raises(ValidationError):
        parse_history_page(raw)


@pytest.mark.asyncio
async def test_history_hydration_preserves_native_payload_and_verified_mime_bytes():
    from unittest.mock import AsyncMock
    from uuid import uuid4

    from airweave.domains.storage.file_service import FileService

    storage = AsyncMock()
    files = FileService(uuid4(), storage, sync_id=uuid4())
    raw = message(
        "a",
        payload={
            "mimeType": "text/plain",
            "body": {
                "attachmentId": "body-id",
                "size": 3,
            },
        },
    )
    provider = Provider([("/messages/a", raw)])
    capture = GmailCapture(
        provider.get,
        None,
        files=files,
        attachment_get=AsyncMock(return_value={"data": "YWJj", "size": 3}),
    )
    batch = await capture.hydrate_history_page(parse_history_page(history(["a"])))
    (record,) = batch.records
    assert record.payload == raw and record.completeness == "complete"
    assert record.blobs[0].source_path == "/payload/body"
    storage.write_file.assert_awaited_once_with(record.blobs[0].key, b"abc")


@pytest.mark.asyncio
async def test_new_event_for_same_message_invalidates_partial_hydration():
    initial = {
        "historyId": "first",
        "history": [
            {"id": "event-one", "labelsAdded": [{"message": {"id": "a"}, "labelIds": ["INBOX"]}]}
        ],
    }
    changed = {
        "historyId": "second",
        "history": [
            *initial["history"],
            {"id": "event-two", "labelsRemoved": [{"message": {"id": "a"}, "labelIds": ["INBOX"]}]},
        ],
    }
    old, new = parse_history_page(initial), parse_history_page(changed)
    assert old.message_ids == new.message_ids == ("a",)
    provider = Provider([])
    with pytest.raises(InvalidScanContinuation):
        await GmailCapture(provider.get, None).hydrate_history_page(
            new, offset=1, expected_fingerprint=old.fingerprint
        )
    assert not provider.calls
