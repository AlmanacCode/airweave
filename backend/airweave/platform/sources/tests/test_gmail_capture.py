"""Gmail capture races and failures against scripted provider responses."""

from uuid import uuid4

import httpx
import pytest

from airweave.domains.entities.canonical.requests import CompletedScope, StartedScope
from airweave.domains.syncs.cursors.cursor import SyncCursor
from airweave.platform.cursors.gmail import GmailCursor
from airweave.platform.sources.gmail_capture import BASE, GmailCapture


def message(mid, **extra):
    return {"id": mid, "threadId": "t", "internalDate": "1000", "payload": {}, **extra}


def missing():
    response = httpx.Response(404, request=httpx.Request("GET", BASE))
    return httpx.HTTPStatusError("Gone", request=response.request, response=response)


class Provider:
    def __init__(self, steps):
        self.steps = list(steps)
        self.calls = []

    async def get(self, url, params=None):
        self.calls.append((url, params))
        expected, response = self.steps.pop(0)
        assert url == BASE + expected
        if isinstance(response, Exception):
            raise response
        return response


def cursor(**data):
    return SyncCursor(uuid4(), cursor_schema=GmailCursor, cursor_data=data)


@pytest.mark.asyncio
async def test_bootstrap_boundary_precedes_pagination_and_replays_concurrent_edits():
    provider = Provider(
        [
            ("/profile", {"historyId": "10"}),
            ("/messages", {"messages": [{"id": "a"}], "nextPageToken": "p2"}),
            ("/messages/a", message("a", labelIds=["INBOX"])),
            ("/messages", {"messages": [{"id": "b"}]}),
            ("/messages/b", message("b")),
            (
                "/history",
                {
                    "history": [{"labelsRemoved": [{"message": {"id": "a"}}]}],
                    "nextPageToken": "h2",
                    "historyId": "11",
                },
            ),
            (
                "/history",
                {"history": [{"messagesAdded": [{"message": {"id": "c"}}]}], "historyId": "12"},
            ),
            ("/messages/a", message("a", labelIds=[])),
            ("/messages/c", message("c")),
        ]
    )
    state = cursor()
    results = [r async for r in GmailCapture(provider.get, None).generate(state)]
    assert results[0] == StartedScope(record_type="message")
    assert [r.payload["id"] for r in results[1:-1]] == ["a", "b", "a", "c"]
    assert results[3].payload["labelIds"] == []
    assert isinstance(results[-1], CompletedScope)
    assert state.data["history_id"] == "12"
    assert provider.calls[3][1]["pageToken"] == "p2"
    assert provider.calls[6][1] == {"startHistoryId": "10", "maxResults": 500, "pageToken": "h2"}
    assert not provider.steps


@pytest.mark.asyncio
async def test_incremental_labels_and_deleted_messages_fetch_current_state():
    provider = Provider(
        [
            (
                "/history",
                {
                    "historyId": "21",
                    "history": [
                        {
                            "messagesDeleted": [{"message": {"id": "gone"}}],
                            "labelsAdded": [{"message": {"id": "a"}}],
                            "messages": [{"id": "a"}],
                        }
                    ],
                },
            ),
            ("/messages/a", message("a", labelIds=["STARRED"])),
            ("/messages/gone", missing()),
        ]
    )
    state = cursor(history_id="20", canonical_query="")
    results = [r async for r in GmailCapture(provider.get, None).generate(state)]
    assert len(results) == 2
    assert results[1].kind == "delete"
    assert results[1].removal_reason == "provider_deleted"
    assert state.data["history_id"] == "21"


@pytest.mark.asyncio
async def test_expired_incremental_boundary_bootstraps_and_reconciles():
    provider = Provider(
        [
            ("/history", missing()),
            ("/profile", {"historyId": "30"}),
            ("/messages", {}),
            ("/history", {"historyId": "31"}),
        ]
    )
    state = cursor(history_id="20", canonical_query="")
    results = [r async for r in GmailCapture(provider.get, None).generate(state)]
    assert results == [StartedScope(record_type="message"), CompletedScope(record_type="message")]
    assert state.data["history_id"] == "31"


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [RuntimeError("network"), missing()])
async def test_failure_after_full_crawl_does_not_finish_scope_or_advance_checkpoint(error):
    provider = Provider(
        [
            ("/profile", {"historyId": "30"}),
            ("/messages", {"messages": [{"id": "a"}]}),
            ("/messages/a", message("a")),
            ("/history", error),
        ]
    )
    state = cursor()
    before = state.data
    observed = []
    with pytest.raises(type(error)):
        async for record in GmailCapture(provider.get, None).generate(state):
            observed.append(record)
    assert len(observed) == 2
    assert observed[0] == StartedScope(record_type="message")
    assert state.data == before


@pytest.mark.asyncio
async def test_filtered_query_always_reconciles_and_external_mime_bytes_are_partial():
    raw = message("a", payload={"parts": [{"body": {"attachmentId": "external", "size": 3}}]})
    provider = Provider(
        [
            ("/messages", {"messages": [{"id": "a"}]}),
            ("/messages/a", raw),
        ]
    )
    state = cursor(history_id="20", canonical_query="")
    results = [
        r async for r in GmailCapture(provider.get, "from:person@example.com").generate(state)
    ]
    assert results[0] == StartedScope(record_type="message")
    assert results[1].payload == raw
    assert results[1].completeness == "partial"
    assert results[1].blobs == ()
    assert results[-1] == CompletedScope(record_type="message")
    assert provider.calls[0][1]["q"] == "from:person@example.com"
    assert state.data["history_id"] == ""


@pytest.mark.asyncio
async def test_legacy_entity_cursor_cannot_skip_canonical_bootstrap():
    provider = Provider(
        [
            ("/profile", {"historyId": "30"}),
            ("/messages", {}),
            ("/history", {"historyId": "31"}),
        ]
    )
    results = [r async for r in GmailCapture(provider.get, None).generate(cursor(history_id="20"))]
    assert results == [StartedScope(record_type="message"), CompletedScope(record_type="message")]


@pytest.mark.asyncio
async def test_repeated_page_token_fails_without_advancing_cursor():
    provider = Provider(
        [
            ("/history", {"nextPageToken": "same"}),
            ("/history", {"nextPageToken": "same"}),
        ]
    )
    state = cursor(history_id="20", canonical_query="")
    with pytest.raises(ValueError, match="repeated"):
        _ = [r async for r in GmailCapture(provider.get, None).generate(state)]
    assert state.data["history_id"] == "20"


@pytest.mark.asyncio
async def test_incremental_detail_failure_retains_old_boundary():
    provider = Provider(
        [
            ("/history", {"historyId": "21", "history": [{"messages": [{"id": "a"}]}]}),
            ("/messages/a", RuntimeError("upstream unavailable")),
        ]
    )
    state = cursor(history_id="20", canonical_query="")
    with pytest.raises(RuntimeError):
        _ = [r async for r in GmailCapture(provider.get, None).generate(state)]
    assert state.data["history_id"] == "20"


@pytest.mark.asyncio
async def test_filtered_page_failure_never_claims_completed_scope():
    provider = Provider(
        [
            ("/messages", {"messages": [{"id": "a"}], "nextPageToken": "p2"}),
            ("/messages/a", message("a")),
            ("/messages", RuntimeError("upstream unavailable")),
        ]
    )
    state = cursor(history_id="20", canonical_query="")
    results = []
    with pytest.raises(RuntimeError):
        async for record in GmailCapture(provider.get, "in:inbox").generate(state):
            results.append(record)
    assert len(results) == 2
    assert results[0] == StartedScope(record_type="message")
    assert not any(type(r) is CompletedScope for r in results)
    assert state.data["history_id"] == "20"


@pytest.mark.asyncio
async def test_external_mime_bytes_are_stored_before_complete_record_without_payload_mutation():
    from unittest.mock import AsyncMock

    from airweave.domains.storage.file_service import FileService

    storage = AsyncMock()
    files = FileService(uuid4(), storage, sync_id=uuid4())
    raw = message(
        "a",
        payload={
            "parts": [
                {
                    "mimeType": "text/plain",
                    "body": {
                        "attachmentId": "body-id",
                        "size": 3,
                    },
                }
            ]
        },
    )
    provider = Provider([("/messages/a", raw)])
    attachment_get = AsyncMock(return_value={"data": "YWJj", "size": 3})
    record = await GmailCapture(
        provider.get, None, files=files, attachment_get=attachment_get
    ).message("a")
    assert record.completeness == "complete"
    assert record.payload == raw
    assert record.blobs[0].source_path == "/payload/parts/0/body"
    assert record.blobs[0].size_bytes == 3
    storage.write_file.assert_awaited_once_with(record.blobs[0].key, b"abc")


@pytest.mark.asyncio
async def test_blob_storage_failure_propagates_and_prevents_record_checkpoint():
    from unittest.mock import AsyncMock

    from airweave.domains.storage.file_service import FileService

    storage = AsyncMock()
    storage.write_file.side_effect = OSError("storage unavailable")
    files = FileService(uuid4(), storage, sync_id=uuid4())
    provider = Provider(
        [
            ("/history", {"historyId": "21", "history": [{"messages": [{"id": "a"}]}]}),
            ("/messages/a", message("a", payload={"body": {"attachmentId": "body-id", "size": 3}})),
        ]
    )
    state = cursor(history_id="20", canonical_query="")
    attachment_get = AsyncMock(return_value={"data": "YWJj", "size": 3})
    capture = GmailCapture(provider.get, None, files=files, attachment_get=attachment_get)
    with pytest.raises(OSError):
        _ = [r async for r in capture.generate(state)]
    assert state.data["history_id"] == "20"


@pytest.mark.asyncio
async def test_oversize_declared_mime_is_partial_without_fetching_or_writing():
    from unittest.mock import AsyncMock

    from airweave.domains.storage.file_service import FileService

    storage = AsyncMock()
    files = FileService(uuid4(), storage, sync_id=uuid4())
    files.MAX_FILE_SIZE_BYTES = 2
    provider = Provider(
        [
            (
                "/messages/a",
                message(
                    "a",
                    payload={
                        "body": {
                            "attachmentId": "body-id",
                            "size": 3,
                        }
                    },
                ),
            )
        ]
    )
    attachment_get = AsyncMock()
    record = await GmailCapture(
        provider.get, None, files=files, attachment_get=attachment_get
    ).message("a")
    assert record.completeness == "partial"
    attachment_get.assert_not_called()
    storage.write_file.assert_not_called()


@pytest.mark.asyncio
async def test_mime_size_mismatch_does_not_store_unverified_bytes():
    from unittest.mock import AsyncMock

    from airweave.domains.storage.file_service import FileService

    storage = AsyncMock()
    files = FileService(uuid4(), storage, sync_id=uuid4())
    provider = Provider(
        [
            (
                "/messages/a",
                message(
                    "a",
                    payload={
                        "body": {
                            "attachmentId": "body-id",
                            "size": 4,
                        }
                    },
                ),
            )
        ]
    )
    attachment_get = AsyncMock(return_value={"data": "YWJj", "size": 3})
    with pytest.raises(ValueError, match="size"):
        await GmailCapture(provider.get, None, files=files, attachment_get=attachment_get).message(
            "a"
        )
    storage.write_file.assert_not_called()


@pytest.mark.asyncio
async def test_mime_json_stream_size_limit_closes_response():
    import logging

    from airweave.domains.sources.token_providers.protocol import ManagedAuthProvider
    from airweave.domains.storage.exceptions import FileSkippedException
    from airweave.platform.configs.config import GmailConfig
    from airweave.platform.http_client.airweave_client import AirweaveHttpClient
    from airweave.platform.sources.gmail import GmailSource

    response = httpx.Response(200, json={"data": "YWJj", "size": 3})
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: response)) as client:
        source = await GmailSource.create(
            auth=ManagedAuthProvider(
                api_key="not-used",
                connected_account_id="test",
                allowed_hosts=frozenset({"gmail.googleapis.com"}),
            ),
            logger=logging.getLogger("test"),
            http_client=AirweaveHttpClient(client, uuid4(), "gmail", feature_flag_enabled=False),
            config=GmailConfig(),
        )
        with pytest.raises(FileSkippedException):
            await source._get_mime_json(BASE + "/messages/a/attachments/b", max_bytes=5)
    assert response.is_closed
