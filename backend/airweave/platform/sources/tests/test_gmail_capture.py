"""Gmail native pages and MIME retention; SQL recovery is tested separately."""

from uuid import uuid4

import httpx
import pytest

from airweave.domains.entities.canonical.cycle_models import (
    CaptureCycle,
    CycleConfiguration,
    CycleVersion,
)
from airweave.domains.entities.canonical.page_source import (
    InvalidCaptureCheckpoint,
    InvalidScanContinuation,
)
from airweave.platform.sources.gmail_capture import BASE, GmailCapture
from airweave.platform.sources.gmail_pages import GmailPages


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


async def initial(pages):
    config = CycleConfiguration(fingerprint="a" * 64, parents={"message": (None,)})
    plan = await pages.prepare(None, config)
    cycle = CaptureCycle(
        version=CycleVersion(cycle_id=uuid4(), revision=1),
        configuration=config,
        mode=plan.mode,
        starting_checkpoint=plan.starting_checkpoint,
    )
    return pages.initial(cycle)


@pytest.mark.asyncio
async def test_baseline_interleaves_history_before_next_list_page():
    provider = Provider(
        [
            ("/profile", {"historyId": "10"}),
            ("/messages", {"messages": [{"id": "a"}], "nextPageToken": "p2"}),
            ("/messages/a", message("a")),
            (
                "/history",
                {"historyId": "11", "history": [{"labelsRemoved": [{"message": {"id": "a"}}]}]},
            ),
            ("/messages/a", message("a", labelIds=[])),
            ("/messages", {"messages": [{"id": "b"}]}),
            ("/messages/b", message("b")),
            (
                "/history",
                {"historyId": "12", "history": [{"messagesAdded": [{"message": {"id": "c"}}]}]},
            ),
            ("/messages/c", message("c")),
        ]
    )
    pages = GmailPages(GmailCapture(provider.get, None))
    continuation = await initial(pages)
    results = []
    for _ in range(4):
        page = await pages.page(continuation)
        results.append(page)
        continuation = page.continuation
    assert [r.identity.native_id for p in results for r in p.records] == ["a", "a", "b", "c"]
    assert [p.final for p in results] == [False, False, False, True]
    assert all(p.provider_checkpoint is None for p in results[:-1])
    assert results[-1].provider_checkpoint.value == {"history_id": "12"}
    assert provider.calls[3][1]["startHistoryId"] == "10"
    assert provider.calls[5][1]["pageToken"] == "p2"
    assert provider.calls[7][1]["startHistoryId"] == "11"
    assert provider.calls[1][1]["includeSpamTrash"] == "true"
    assert "q" not in provider.calls[1][1]


@pytest.mark.asyncio
async def test_filtered_query_is_list_only_without_provider_checkpoint():
    provider = Provider([("/messages", {"messages": [{"id": "a"}]}), ("/messages/a", message("a"))])
    pages = GmailPages(GmailCapture(provider.get, "in:inbox"))
    page = await pages.page(await initial(pages))
    assert page.final and page.provider_checkpoint is None
    assert provider.calls[0][1]["q"] == "in:inbox"
    assert not provider.steps


@pytest.mark.asyncio
async def test_expired_history_requires_whole_cycle_recovery():
    provider = Provider(
        [("/profile", {"historyId": "10"}), ("/messages", {}), ("/history", missing())]
    )
    pages = GmailPages(GmailCapture(provider.get, None))
    first = await pages.page(await initial(pages))
    with pytest.raises(InvalidCaptureCheckpoint):
        await pages.page(first.continuation)
    assert first.provider_checkpoint is None


@pytest.mark.asyncio
async def test_history_replay_hydrates_bounded_offsets_and_rejects_changed_page():
    raw = {"historyId": "11", "history": [{"messages": [{"id": str(i)} for i in range(51)]}]}
    steps = [("/profile", {"historyId": "10"}), ("/messages", {}), ("/history", raw)]
    steps.extend((f"/messages/{i}", message(str(i))) for i in range(50))
    steps.append(("/history", {**raw, "historyId": "12"}))
    provider = Provider(steps)
    pages = GmailPages(GmailCapture(provider.get, None))
    listing = await pages.page(await initial(pages))
    half = await pages.page(listing.continuation)
    assert len(half.records) == 50 and not half.final and half.provider_checkpoint is None
    assert half.continuation.value["history_offset"] == 50
    with pytest.raises(InvalidScanContinuation):
        await pages.page(half.continuation)
    assert provider.calls[-1][0].endswith("/history")


@pytest.mark.asyncio
async def test_history_pagination_keeps_start_boundary_until_terminal():
    provider = Provider(
        [
            ("/profile", {"historyId": "10"}),
            ("/messages", {}),
            ("/history", {"historyId": "999", "nextPageToken": "next"}),
            ("/history", {"historyId": "1000"}),
        ]
    )
    pages = GmailPages(GmailCapture(provider.get, None))
    listing = await pages.page(await initial(pages))
    intermediate = await pages.page(listing.continuation)
    assert intermediate.continuation.value["history_boundary"] == "10"
    assert intermediate.provider_checkpoint is None
    terminal = await pages.page(intermediate.continuation)
    assert provider.calls[-1][1]["startHistoryId"] == "10"
    assert terminal.provider_checkpoint.value == {"history_id": "1000"}


@pytest.mark.asyncio
async def test_repeated_listing_cursor_fails_without_a_completed_page():
    provider = Provider(
        [("/messages", {"nextPageToken": "same"}), ("/messages", {"nextPageToken": "same"})]
    )
    pages = GmailPages(GmailCapture(provider.get, "in:inbox"))
    first = await pages.page(await initial(pages))
    with pytest.raises(ValueError, match="repeated"):
        await pages.page(first.continuation)
    assert not first.final


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
    attachment_get = AsyncMock(return_value={"data": "YWJj", "size": 3})
    capture = GmailCapture(provider.get, None, files=files, attachment_get=attachment_get)
    page = await capture.history_page("20")
    with pytest.raises(OSError):
        await capture.hydrate_history_page(page)


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


@pytest.mark.asyncio
async def test_large_originals_split_batch_without_relisting_or_truncation():
    raw = message("large", payload={"body": {"data": "x" * (9 * 1024 * 1024)}})
    provider = Provider(
        [
            ("/messages", {"messages": [{"id": "large"}, {"id": "small"}]}),
            ("/messages/large", raw),
            ("/messages/small", message("small")),
        ]
    )
    pages = GmailPages(GmailCapture(provider.get, "in:inbox"))
    first = await pages.page(await initial(pages))
    assert not first.final and first.records[0].payload == raw
    assert first.continuation.value["pending_ids"] == ["small"]
    # A new adapter instance models loss of all in-process state.
    final = await GmailPages(GmailCapture(provider.get, "in:inbox")).page(first.continuation)
    assert final.final and final.records[0].identity.native_id == "small"
    assert len([url for url, _ in provider.calls if url.endswith("/messages")]) == 1


def test_combined_cursor_rings_fit_durable_continuation_and_recent_loops_fail():
    import hashlib

    from airweave.platform.sources.gmail_pages import _next_token, _Progress

    values = tuple(hashlib.sha256(str(i).encode()).hexdigest() for i in range(128))
    state = _Progress(
        mode="full",
        phase="history",
        history_boundary="h" * 1024,
        list_token="l" * 8192,
        history_token="h" * 8192,
        list_tokens=values,
        history_tokens=values,
    )
    assert len(state.continuation().model_dump_json().encode()) < 65536
    rotated = values
    for i in range(128, 1000):
        rotated = _next_token(str(i), rotated)
    assert len(rotated) == 128
    with pytest.raises(ValueError, match="repeated"):
        _next_token("999", rotated)


def test_only_explicit_page_token_parameter_errors_authorize_reset():
    from airweave.platform.sources.gmail_pages import invalid_page_token

    assert invalid_page_token(
        {
            "error": {
                "errors": [
                    {"reason": "badRequest", "location": "pageToken", "locationType": "parameter"}
                ]
            }
        }
    )
    assert not invalid_page_token(
        {
            "error": {
                "errors": [{"reason": "badRequest", "location": "q", "locationType": "parameter"}]
            }
        }
    )
    assert not invalid_page_token({"error": {"message": "Invalid request"}})


@pytest.mark.asyncio
async def test_bounded_native_reads_refresh_once_and_identify_only_explicit_bad_token():
    from unittest.mock import MagicMock

    from airweave.domains.sources.token_providers.static import StaticTokenProvider
    from airweave.platform.configs.config import GmailConfig
    from airweave.platform.sources.gmail import GmailSource

    class RefreshingToken(StaticTokenProvider):
        refreshes = 0

        @property
        def supports_refresh(self):
            return True

        async def force_refresh(self):
            self.refreshes += 1
            self._token = "refreshed"
            return self._token

    auth = RefreshingToken("initial")
    requests = []
    responses = [
        httpx.Response(401, json={"error": {"message": "expired"}}),
        httpx.Response(200, json={"historyId": "current"}),
        httpx.Response(
            400,
            json={
                "error": {
                    "errors": [
                        {
                            "reason": "badRequest",
                            "location": "pageToken",
                            "locationType": "parameter",
                        }
                    ]
                }
            },
        ),
    ]

    async def handler(request):
        requests.append(request)
        return responses.pop(0)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        source = await GmailSource.create(
            auth=auth, logger=MagicMock(), http_client=client, config=GmailConfig()
        )
        assert await source._get_capture_json(BASE + "/profile") == {"historyId": "current"}
        with pytest.raises(InvalidScanContinuation):
            await source._get_capture_json(BASE + "/messages", params={"pageToken": "expired"})
    assert auth.refreshes == 1
    assert requests[0].headers["authorization"] == "Bearer initial"
    assert requests[1].headers["authorization"] == "Bearer refreshed"


@pytest.mark.asyncio
async def test_bounded_transport_stops_before_reading_entire_oversized_body():
    from unittest.mock import MagicMock

    from airweave.domains.sources.token_providers.static import StaticTokenProvider
    from airweave.domains.storage.exceptions import FileSkippedException
    from airweave.platform.configs.config import GmailConfig
    from airweave.platform.sources.gmail import GmailSource

    class Chunks(httpx.AsyncByteStream):
        observed = 0

        closed = False

        def __aiter__(self):
            self.iterator = self.chunks()
            return self.iterator

        async def chunks(self):
            for _ in range(10):
                self.observed += 1
                yield b"x" * 5

        async def aclose(self):
            self.closed = True
            await self.iterator.aclose()

    chunks = Chunks()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, stream=chunks))
    ) as client:
        source = await GmailSource.create(
            auth=StaticTokenProvider("synthetic"),
            logger=MagicMock(),
            http_client=client,
            config=GmailConfig(),
        )
        with pytest.raises(FileSkippedException):
            await source._get_mime_json(BASE + "/messages/a", max_bytes=8)
    assert chunks.observed == 2 and chunks.closed


@pytest.mark.asyncio
async def test_managed_proxy_decoded_headers_and_identity_request_match_bounded_reader():
    import json
    from unittest.mock import MagicMock

    from airweave.domains.sources.token_providers.static import StaticTokenProvider
    from airweave.platform.configs.config import GmailConfig
    from airweave.platform.http_client.composio_transport import ComposioTransport
    from airweave.platform.sources.gmail import GmailSource

    async def proxy(request):
        payload = json.loads(request.content)
        assert any(
            item["name"].lower() == "accept-encoding" and item["value"] == "identity"
            for item in payload["parameters"]
        )
        return httpx.Response(
            200,
            json={
                "status": 200,
                "data": {"historyId": "synthetic"},
                "headers": {"content-encoding": "gzip"},
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(proxy)) as upstream:
        async with httpx.AsyncClient(
            transport=ComposioTransport(
                client=upstream,
                api_key="fixture",
                connected_account_id="fixture",
                allowed_hosts={"gmail.googleapis.com"},
            )
        ) as client:
            source = await GmailSource.create(
                auth=StaticTokenProvider("synthetic"),
                logger=MagicMock(),
                http_client=client,
                config=GmailConfig(),
            )
            assert await source._get_capture_json(BASE + "/profile") == {"historyId": "synthetic"}
