"""Synthetic native interpretation; SQL page durability is tested by the shared engine suite."""

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import httpx
import pytest

from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.requests import BlobReference, CompletedScope
from airweave.domains.entities.canonical.scan_models import ScanContinuation
from airweave.domains.sources.exceptions import SourceEntityNotFoundError
from airweave.domains.sources.token_providers.static import StaticTokenProvider
from airweave.platform.configs.config import OutlookMailConfig
from airweave.platform.sources.outlook_graph import OutlookBoundaryError, OutlookGraphClient
from airweave.platform.sources.outlook_mail_capture import OutlookMailCapture
from airweave.platform.sources.outlook_mail_models import OutlookMailContinuation

MIME = b"From: from@example.com\r\nTo: to@example.com\r\nSubject: Original\r\n\r\nFull body\r\n"
MESSAGE = {
    "id": "immutable-message",
    "changeKey": "version-one",
    "parentFolderId": "folder-b",
    "subject": "Original",
    "from": {"emailAddress": {"address": "from@example.com"}},
    "sender": {"emailAddress": {"address": "delegate@example.com"}},
    "body": {"contentType": "text", "content": "Full body"},
    "bodyPreview": "Full",
    "hasAttachments": False,
    "nativeExtension": {"keep": [1, True]},
}
ROOT = CompletedScope(record_type="message")


def files(maximum=10000):
    result = MagicMock()
    result.MAX_FILE_SIZE_BYTES = maximum
    result.store_canonical_blob = AsyncMock(
        return_value=BlobReference(
            key="original", sha256="a" * 64, size_bytes=len(MIME), media_type="message/rfc822"
        )
    )
    return result


async def capture(client, *, config=None):
    return await OutlookMailCapture.create(
        graph=OutlookGraphClient(
            StaticTokenProvider("secret"), client, "outlook_mail", "principal"
        ),
        config=config
        or OutlookMailConfig(
            expected_principal_id="principal", included_folders=[], excluded_folders=[]
        ),
    )


def initial(source):
    return ScanContinuation(
        value=OutlookMailContinuation(fingerprint=source.fingerprint).model_dump(mode="json")
    )


async def first_message_page(source, retained):
    progress = initial(source)
    while True:
        page = await source.capture_page(ROOT, progress, files=retained)
        if page.records or page.final:
            return page
        progress = page.continuation


def topology(request):
    if request.url.path == "/v1.0/me/mailFolders":
        assert request.url.params["includeHiddenFolders"] == "true"
        return httpx.Response(200, json={"value": [{"id": "folder-b"}]})
    if request.url.path.endswith("/childFolders"):
        assert request.url.params["includeHiddenFolders"] == "true"
        return httpx.Response(200, json={"value": []})
    return None


@pytest.mark.asyncio
async def test_full_page_retains_exact_json_and_mime_and_resumes_pending_id():
    requested = []
    fail_second = True

    def graph(request):
        requested.append(request.url.path)
        if request.url.path == "/v1.0/me":
            return httpx.Response(200, json={"id": "principal"})
        assert request.headers["Prefer"] == 'IdType="ImmutableId"'
        folder_page = topology(request)
        if folder_page is not None:
            return folder_page
        if request.url.path.endswith("/messages/delta"):
            return httpx.Response(
                200,
                json={
                    "value": [{"id": "immutable-message"}, {"id": "second"}],
                    "@odata.deltaLink": str(request.url.copy_with(query=b"$deltatoken=end")),
                },
            )
        if request.url.path.endswith("/$value"):
            if fail_second and "/second/" in request.url.path:
                raise httpx.ReadError("private-url-failure")
            return httpx.Response(200, content=MIME, headers={"content-type": "message/rfc822"})
        mid = request.url.path.rsplit("/", 1)[-1]
        return httpx.Response(200, json={**MESSAGE, "id": mid})

    async with httpx.AsyncClient(transport=httpx.MockTransport(graph)) as client:
        source = await capture(client)
        retained = files()
        first = await first_message_page(source, retained)
        assert first.records[0].payload == MESSAGE
        assert first.records[0].parent is None
        assert first.records[0].completeness == "partial"
        assert first.records[0].blobs[0].source_path is None
        assert not first.final
        retained.store_canonical_blob.assert_awaited_once_with(MIME, media_type="message/rfc822")
        saved = first.continuation.model_copy(deep=True)
        with pytest.raises(httpx.RequestError) as error:
            await source.capture_page(ROOT, saved, files=retained)
        assert "private-url-failure" not in str(error.value)
        assert saved == first.continuation
        fail_second = False
        recreated = await capture(client)
        second = await recreated.capture_page(ROOT, saved, files=retained)
        assert second.final and second.records[0].identity.native_id == "second"
        assert requested.count("/v1.0/me/mailFolders/folder-b/messages/delta") == 1
        assert second.provider_checkpoint is not None


@pytest.mark.asyncio
async def test_current_folder_decides_move_membership_and_ambiguous_404_stops():
    message = dict(MESSAGE)
    unavailable = False

    def graph(request):
        if request.url.path == "/v1.0/me":
            return httpx.Response(200, json={"id": "principal"})
        if "/mailFolders/" in request.url.path:
            return httpx.Response(200, json={"id": request.url.path.rsplit("/", 1)[-1]})
        if unavailable:
            return httpx.Response(404, json={"error": {"code": "ErrorItemNotFound"}})
        if request.url.path.endswith("/$value"):
            return httpx.Response(200, content=MIME, headers={"content-type": "text/plain"})
        return httpx.Response(200, json=message)

    async with httpx.AsyncClient(transport=httpx.MockTransport(graph)) as client:
        source = await capture(
            client,
            config=OutlookMailConfig(
                expected_principal_id="principal",
                included_folders=["folder-a", "folder-b"],
                excluded_folders=["excluded"],
            ),
        )
        retained = files()
        # Duplicate discoveries before/after a folder move keep the same root identity.
        first = await source.message(message["id"], files=retained)
        message["parentFolderId"] = "folder-a"
        second = await source.message(message["id"], files=retained)
        assert first.identity == second.identity and first.kind == second.kind == "upsert"
        assert first.parent is second.parent is None
        message["parentFolderId"] = "excluded"
        outside = await source.message(message["id"], files=retained)
        assert outside.removal_reason == "scope_removed"
        assert retained.store_canonical_blob.await_count == 2
        unavailable = True
        previous = SourceRecord(
            id=uuid4(),
            sync_id=uuid4(),
            identity=first.identity,
            revision=1,
            payload=first.payload,
            payload_schema_version=1,
            capture_hash="a" * 64,
            content_hash=None,
            completeness=first.completeness,
            observed_at=first.observed_at,
            source_created_at=None,
            source_updated_at=None,
            deleted_at=None,
            removal_reason=None,
            blobs=first.blobs,
            indexed_revision=None,
            indexed_pipeline_version=None,
        )
        with pytest.raises(SourceEntityNotFoundError):
            await source.refresh_known(previous, files=retained)


@pytest.mark.asyncio
async def test_cursor_binding_and_wrong_collection_fail_before_message_reads():
    requested = []
    next_link = "https://graph.microsoft.com/v1.0/me/mailFolders/folder-b/messages/delta?$skip=next"

    def graph(request):
        requested.append(request.url.path)
        if request.url.path == "/v1.0/me":
            return httpx.Response(200, json={"id": "principal"})
        folder_page = topology(request)
        if folder_page is not None:
            return folder_page
        return httpx.Response(200, json={"value": [], "@odata.nextLink": next_link})

    async with httpx.AsyncClient(transport=httpx.MockTransport(graph)) as client:
        source = await capture(client)
        wrong = initial(source).model_copy(update={"value": {"fingerprint": "a" * 64}})
        with pytest.raises(OutlookBoundaryError, match="another capture"):
            await source.capture_page(ROOT, wrong, files=files())
        assert requested == ["/v1.0/me"]
        next_link = "https://graph.microsoft.com/v1.0/me/events"
        with pytest.raises(OutlookBoundaryError, match="changed the native collection"):
            await first_message_page(source, files())
        next_link = (
            "https://graph.microsoft.com/v1.0/me/mailFolders/folder-b/messages/delta?token="
            + "x" * 66000
        )
        with pytest.raises(OutlookBoundaryError, match="64 KiB"):
            await first_message_page(source, files())


@pytest.mark.asyncio
async def test_mime_size_omission_is_explicit_and_change_key_race_cannot_commit():
    changed = False

    def graph(request):
        if request.url.path == "/v1.0/me":
            return httpx.Response(200, json={"id": "principal"})
        if request.url.path.endswith("/$value"):
            return httpx.Response(
                200, content=MIME, headers={"content-type": "Message/RFC822; charset=utf-8"}
            )
        value = dict(MESSAGE)
        if changed and "$select" in request.url.params:
            value["changeKey"] = "new-version"
        return httpx.Response(200, json=value)

    async with httpx.AsyncClient(transport=httpx.MockTransport(graph)) as client:
        source = await capture(client)
        bounded = files(maximum=8)
        original = await source.message(MESSAGE["id"], files=bounded)
        assert original.completeness == "metadata_only" and original.blobs == ()
        assert original.payload == MESSAGE
        bounded.store_canonical_blob.assert_not_awaited()
        changed = True
        with pytest.raises(OutlookBoundaryError, match="changed during"):
            await source.message(MESSAGE["id"], files=files())


@pytest.mark.asyncio
async def test_missing_received_date_cannot_broaden_date_selection():
    def graph(request):
        if request.url.path == "/v1.0/me":
            return httpx.Response(200, json={"id": "principal"})
        assert not request.url.path.endswith("/$value")
        return httpx.Response(200, json=MESSAGE)

    async with httpx.AsyncClient(transport=httpx.MockTransport(graph)) as client:
        source = await capture(
            client,
            config=OutlookMailConfig(
                expected_principal_id="principal",
                after_date="2026/01/01",
                included_folders=[],
                excluded_folders=[],
            ),
        )
        with pytest.raises(OutlookBoundaryError, match="requires a received timestamp"):
            await source.message(MESSAGE["id"], files=files())


@pytest.mark.parametrize("order", [("old", "new"), ("new", "old")])
async def test_delta_move_orders_hydrate_mailbox_and_changes_preserve_scope_removal(order):
    """Removed events are invalidations, never deletion authority."""
    folder = "new"
    unavailable = False

    def graph(request):
        if request.url.path == "/v1.0/me":
            return httpx.Response(200, json={"id": "principal"})
        assert request.headers["Prefer"] == 'IdType="ImmutableId"'
        if request.url.path.endswith("/messages/delta"):
            event = {"id": MESSAGE["id"]}
            if "/old/" in request.url.path:
                event["@removed"] = {"reason": "deleted"}
            return httpx.Response(
                200,
                json={
                    "value": [event],
                    "@odata.deltaLink": str(request.url.copy_with(query=b"$deltatoken=next")),
                },
            )
        if unavailable:
            return httpx.Response(404, json={"error": {"code": "ErrorItemNotFound"}})
        if request.url.path.endswith("/$value"):
            return httpx.Response(200, content=MIME, headers={"content-type": "message/rfc822"})
        return httpx.Response(200, json={**MESSAGE, "parentFolderId": folder})

    async with httpx.AsyncClient(transport=httpx.MockTransport(graph)) as client:
        source = await capture(client)
        source.excluded_ids = frozenset({"excluded"})
        state = OutlookMailContinuation(
            fingerprint=source.fingerprint,
            mode="changes",
            phase="messages",
            folders_to_visit=(),
            remaining_folders=order,
        )
        cursor = source._continuation(state)
        first = await source.capture_page(ROOT, cursor, files=files())
        second = await source.capture_page(ROOT, first.continuation, files=files())
        assert first.records[0].identity == second.records[0].identity
        assert first.records[0].kind == second.records[0].kind == "upsert"
        assert first.provider_checkpoint is None and second.provider_checkpoint is not None
        assert second.final
        folder = "excluded"
        withdrawn = await source.capture_page(ROOT, cursor, files=files())
        assert withdrawn.records[0].removal_reason == "scope_removed"
        unavailable = True
        with pytest.raises(SourceEntityNotFoundError):
            await source.capture_page(ROOT, cursor, files=files())
        assert cursor == source._continuation(state)


async def test_recursive_hidden_topology_new_folder_and_disappeared_folder_reset():
    from airweave.domains.entities.canonical.page_source import InvalidCaptureCheckpoint

    calls = []
    unknown_folder = False

    def graph(request):
        path = request.url.path
        calls.append(path)
        if path == "/v1.0/me":
            return httpx.Response(200, json={"id": "principal"})
        assert request.url.params["includeHiddenFolders"] == "true"
        if path.endswith("/mailFolders"):
            value = [{"id": "parent"}]
        elif path.endswith("/parent/childFolders"):
            value = [{"id": "hidden-child", "isHidden": True}]
        else:
            value = []
        if unknown_folder and value:
            value[0]["@odata.type"] = "#microsoft.graph.unknownFolder"
        return httpx.Response(200, json={"value": value})

    async with httpx.AsyncClient(transport=httpx.MockTransport(graph)) as client:
        source = await capture(client)
        cursor = initial(source)
        for _ in range(3):
            page = await source.capture_page(ROOT, cursor, files=files())
            cursor = page.continuation
        state = OutlookMailContinuation.model_validate(cursor.value)
        assert state.phase == "messages"
        assert set(state.remaining_folders) == {"parent", "hidden-child"}
        assert calls[-1].endswith("/hidden-child/childFolders")
        missing = OutlookMailContinuation(
            fingerprint=source.fingerprint,
            mode="changes",
            folder_links={"removed": source._delta_url("removed") + "?$deltatoken=old"},
        )
        cursor = source._continuation(missing)
        for _ in range(2):
            cursor = (await source.capture_page(ROOT, cursor, files=files())).continuation
        with pytest.raises(InvalidCaptureCheckpoint, match="disappeared"):
            await source.capture_page(ROOT, cursor, files=files())
        unknown_folder = True
        with pytest.raises(OutlookBoundaryError, match="not qualified"):
            await source.capture_page(ROOT, initial(source), files=files())


async def test_expired_delta_restarts_without_treating_other_errors_as_empty():
    from airweave.domains.entities.canonical.page_source import InvalidCaptureCheckpoint
    from airweave.domains.sources.exceptions import SourceError

    code = "syncStateNotFound"

    def graph(request):
        if request.url.path == "/v1.0/me":
            return httpx.Response(200, json={"id": "principal"})
        return httpx.Response(400, json={"error": {"code": code, "message": "private-error"}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(graph)) as client:
        source = await capture(client)
        cursor = source._continuation(
            OutlookMailContinuation(
                fingerprint=source.fingerprint,
                mode="changes",
                phase="messages",
                folders_to_visit=(),
                remaining_folders=("folder",),
                folder_links={"folder": source._delta_url("folder") + "?$deltatoken=old"},
            )
        )
        with pytest.raises(InvalidCaptureCheckpoint):
            await source.capture_page(ROOT, cursor, files=files())
        code = "ErrorInvalidRequest"
        with pytest.raises(SourceError) as error:
            await source.capture_page(ROOT, cursor, files=files())
        assert "private-error" not in str(error.value)


@pytest.mark.parametrize("selected_view", [False, True])
async def test_virtual_folder_traverses_physical_child_but_never_owns_message_delta(selected_view):
    requests = []

    def graph(request):
        path = request.url.path
        requests.append(path)
        assert request.headers["Accept"] == "application/json;odata.metadata=minimal"
        if path == "/v1.0/me":
            return httpx.Response(200, json={"id": "principal"})
        if path.endswith("/mailFolders/view"):
            return httpx.Response(200, json={"id": "view"})
        if path.endswith("/messages/delta"):
            assert "/view/" not in path
            return httpx.Response(
                200,
                json={
                    "value": [],
                    "@odata.deltaLink": str(request.url.copy_with(query=b"$deltatoken=end")),
                },
            )
        assert request.url.params["includeHiddenFolders"] == "true"
        if path.endswith("/mailFolders"):
            value = [
                {"id": "physical"},
                {"id": "view", "@odata.type": "#microsoft.graph.mailSearchFolder"},
            ]
        elif path.endswith("/view/childFolders"):
            value = [{"id": "nested-physical", "isHidden": True}]
        else:
            value = []
        return httpx.Response(200, json={"value": value})

    async with httpx.AsyncClient(transport=httpx.MockTransport(graph)) as client:
        source = await capture(
            client,
            config=OutlookMailConfig(
                expected_principal_id="principal",
                included_folders=["view"] if selected_view else [],
                excluded_folders=[],
            ),
        )
        if selected_view:
            with pytest.raises(OutlookBoundaryError, match="search-folder selections"):
                await source.capture_page(ROOT, initial(source), files=files())
            return
        terminal = await first_message_page(source, files())
        assert terminal.final and terminal.records == ()
        assert set(terminal.provider_checkpoint.value["folder_links"]) == {
            "physical",
            "nested-physical",
        }
        assert "/v1.0/me/mailFolders/view/childFolders" in requests
        assert "/v1.0/me/mailFolders/view/messages/delta" not in requests
