"""Outlook native HTTP fixtures through real canonical SQL and retained blob storage."""

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import select

from airweave.adapters.storage.filesystem import FilesystemBackend
from airweave.domains.entities.canonical.blob_materializer import read_blob
from airweave.domains.entities.canonical.cycle_models import CompleteCycle, TerminalCheckpoint
from airweave.domains.entities.canonical.projection_mappers import map_record
from airweave.domains.entities.canonical.requests import CompletedScope
from airweave.domains.sources.token_providers.static import StaticTokenProvider
from airweave.domains.storage.file_service import FileService
from airweave.domains.sync_pipeline.canonical_scan import CanonicalScanDriver
from airweave.models.capture_scan import CaptureScan
from airweave.models.entity import Entity
from airweave.models.sync_job import SyncJob
from airweave.platform.configs.config import OutlookMailConfig
from airweave.platform.sources.outlook_graph import OutlookGraphClient
from airweave.platform.sources.outlook_mail_capture import OutlookMailCapture

BASE = "https://graph.microsoft.com/v1.0/me"
MIME = (
    b"Subject: Original\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n"
    b"Original retained bytes\r\n"
)


class Mailbox:
    """One interrupted list page, followed by a move of the same immutable message."""

    def __init__(self):
        self.fail_next_page = True
        self.calls = []
        self.moved = False
        self.incremental = False
        self.fail_incremental = True

    def incremental_page(self, request):
        """One new delta round interrupted after the first folder."""
        assert request.url.params.get("$deltatoken") == "one"
        if "/sent/" in request.url.path and self.fail_incremental:
            raise httpx.ConnectError("Synthetic incremental interruption", request=request)
        return httpx.Response(
            200,
            json={
                "value": [{"id": "a", "@removed": {"reason": "deleted"}}],
                "@odata.deltaLink": str(request.url.copy_with(query=b"$deltatoken=two")),
            },
        )

    async def handle(self, request):
        path = request.url.path
        if path == "/v1.0/me":
            return httpx.Response(200, json={"id": "mailbox"})
        assert request.headers["Prefer"] == 'IdType="ImmutableId"'
        self.calls.append((path, str(request.url.query)))
        if path == "/v1.0/me/mailFolders":
            return httpx.Response(200, json={"value": [{"id": "inbox"}, {"id": "sent"}]})
        if path.endswith("/childFolders"):
            return httpx.Response(200, json={"value": []})
        if self.incremental and path.endswith("/messages/delta"):
            return self.incremental_page(request)
        if path == "/v1.0/me/mailFolders/sent/messages/delta":
            return httpx.Response(
                200,
                json={
                    "value": [],
                    "@odata.deltaLink": BASE + "/mailFolders/sent/messages/delta?$deltatoken=one",
                },
            )
        if path == "/v1.0/me/mailFolders/inbox/messages/delta":
            if request.url.params.get("$skiptoken") == "second":
                if self.fail_next_page:
                    raise httpx.ConnectError("Synthetic page interruption", request=request)
                self.moved = True
                return httpx.Response(
                    200,
                    json={
                        "value": [{"id": "a"}, {"id": "b"}],
                        "@odata.deltaLink": BASE
                        + "/mailFolders/inbox/messages/delta?$deltatoken=one",
                    },
                )
            return httpx.Response(
                200,
                json={
                    "value": [{"id": "a"}],
                    "@odata.nextLink": BASE + "/mailFolders/inbox/messages/delta?$skiptoken=second",
                },
            )
        if path.endswith("/$value"):
            return httpx.Response(200, content=MIME, headers={"Content-Type": "message/rfc822"})
        identity = path.rsplit("/", 1)[-1]
        assert identity in {"a", "b"}
        return httpx.Response(
            200,
            json={
                "id": identity,
                "changeKey": "returned"
                if self.incremental
                else ("moved" if self.moved and identity == "a" else "original"),
                "parentFolderId": "sent"
                if self.moved and identity == "a" and not self.incremental
                else "inbox",
                "subject": "Original",
                "body": {"contentType": "text", "content": "Original retained bytes"},
                "createdDateTime": "2026-09-01T00:00:00Z",
                "lastModifiedDateTime": "2026-09-02T00:00:00Z",
                "unknownNativeField": {"retained": True},
            },
        )


async def run_capture(database, service, fence, mailbox, storage):
    async with httpx.AsyncClient(transport=httpx.MockTransport(mailbox.handle)) as client:
        graph = OutlookGraphClient(
            StaticTokenProvider("synthetic"), client, "outlook_mail", "mailbox"
        )
        connector = await OutlookMailCapture.create(
            graph=graph,
            config=OutlookMailConfig(
                expected_principal_id="mailbox", included_folders=[], excluded_folders=[]
            ),
        )
        files = FileService(fence.job_id, storage, sync_id=fence.sync_id)
        try:
            return await CanonicalScanDriver(
                service, database, fence, connector, AsyncMock(), AsyncMock(), files
            ).run()
        finally:
            await files.cleanup_sync_directory(MagicMock())


async def complete(database, service, fence, cycle):
    """Match production terminal publication using the exact committed root scan."""
    async with database() as db:
        scan = await service.read_scan(db, fence, CompletedScope(record_type="message"))
        return await service.complete_cycle(
            db,
            CompleteCycle(
                fence=fence,
                expected=cycle.version,
                terminal_checkpoint=TerminalCheckpoint(
                    record_type="message",
                    expected=scan.version,
                    checkpoint=scan.provider_checkpoint,
                ),
            ),
        )


async def test_resumed_mailbox_page_preserves_original_and_updates_move_in_place(
    database, source, tmp_path
):
    service, fence = source
    mailbox = Mailbox()
    storage = FilesystemBackend(tmp_path)
    with pytest.raises(httpx.RequestError):
        await run_capture(database, service, fence, mailbox, storage)
    async with database() as db:
        rows = list((await db.scalars(select(Entity))).all())
        assert len(rows) == 1 and rows[0].native_id == "a" and rows[0].deleted_at is None
        initial_id = rows[0].id
        initial_revision = rows[0].record_revision
        scan = await db.scalar(select(CaptureScan))
        assert scan.phase == "collecting"
        before = await service.read_cycle(db, fence)
        renewed = await service.activate_writer(
            db,
            fence.organization_id,
            fence.sync_id,
            fence.job_id,
            attempt_id=uuid4(),
            attempt_number=2,
        )
    mailbox.fail_next_page = False
    split = len(mailbox.calls)
    after = await run_capture(database, service, renewed, mailbox, storage)
    after = await complete(database, service, renewed, after)
    assert after.phase == "complete"
    assert after.last_full_capture.discovery == "incomplete"
    assert after.promoted_checkpoint is not None
    assert after.version.cycle_id == before.version.cycle_id
    listing = [
        query
        for path, query in mailbox.calls[split:]
        if path == "/v1.0/me/mailFolders/inbox/messages/delta"
    ]
    assert len(listing) == 1 and "second" in listing[0]
    async with database() as db:
        rows = list((await db.scalars(select(Entity))).all())
        assert len(rows) == 2 and all(row.deleted_at is None for row in rows)
        moved = next(row for row in rows if row.native_id == "a")
        assert moved.id == initial_id and moved.record_revision == initial_revision + 1
        assert moved.source_payload["parentFolderId"] == "sent"
        assert moved.source_payload["unknownNativeField"] == {"retained": True}
        retained = await service.store.read(db, fence.organization_id, fence.sync_id, moved.id)
        assert retained.parent is None and retained.identity.container_id is None
        assert retained.completeness == "partial" and len(retained.blobs) == 1
        assert await read_blob(retained, retained.blobs[0], storage) == MIME
    # A new job consumes the promoted folder tokens, never re-enumerates messages.
    async with database() as db:
        previous_job = await db.get(SyncJob, renewed.job_id)
        previous_job.status = "completed"
        job_id = uuid4()
        db.add(
            SyncJob(
                id=job_id,
                organization_id=fence.organization_id,
                sync_id=fence.sync_id,
                status="running",
            )
        )
        await db.commit()
        next_fence = await service.activate_writer(
            db,
            fence.organization_id,
            fence.sync_id,
            job_id,
            attempt_id=uuid4(),
            attempt_number=1,
        )
    mailbox.incremental = True
    with pytest.raises(httpx.RequestError):
        await run_capture(database, service, next_fence, mailbox, storage)
    async with database() as db:
        partial = await service.read_cycle(db, next_fence)
        assert partial.mode == "changes" and partial.phase == "active"
        assert partial.promoted_checkpoint == after.promoted_checkpoint
        rows = list((await db.scalars(select(Entity))).all())
        assert len(rows) == 2 and all(row.deleted_at is None for row in rows)
    mailbox.fail_incremental = False
    split = len(mailbox.calls)
    changed = await run_capture(database, service, next_fence, mailbox, storage)
    changed = await complete(database, service, next_fence, changed)
    async with database() as db:
        rows = list((await db.scalars(select(Entity))).all())
        moved = next(row for row in rows if row.native_id == "a")
        assert moved.id == initial_id and moved.deleted_at is None
        assert moved.source_payload["parentFolderId"] == "inbox"
        retained = await service.store.read(db, fence.organization_id, fence.sync_id, moved.id)
        assert await read_blob(retained, retained.blobs[0], storage) == MIME
    assert changed.mode == "changes"
    assert all(
        "two" in value
        for value in changed.promoted_checkpoint.checkpoint.value["folder_links"].values()
    )
    delta_requests = [path for path, _ in mailbox.calls[split:] if path.endswith("/delta")]
    assert delta_requests == ["/v1.0/me/mailFolders/sent/messages/delta"]
    # Actual mapper dispatch consumes retained bytes after the provider client has closed.
    async with map_record(retained, "outlook_mail", storage) as projected:
        assert len(projected.entities) == 1
        assert projected.entities[0].id == "a"
        assert projected.parts[-1].part.key == "/attachment_inventory"
        assert projected.parts[-1].entity is None


@pytest.mark.parametrize(
    "provider,original_config",
    [
        ("outlook_mail", {"capture_originals": True, "expected_principal_id": "mailbox"}),
        (
            "stripe",
            {
                "original_capture": {
                    "expected_account_id": "acct_selected",
                    "livemode": False,
                    "api_version": "2025-06-30.basil",
                }
            },
        ),
    ],
)
async def test_factory_rejects_mode_conflict_before_auth_or_cursor_reset(
    database, source, provider, original_config
):
    """A full sync or skip-load cannot reinterpret a persisted Outlook cursor."""
    from airweave.domains.sync_pipeline.config import SyncConfig
    from airweave.domains.sync_pipeline.factory import SyncFactory
    from airweave.models.sync_cursor import SyncCursor
    from airweave.platform.sources.outlook_mail import OutlookMailSource
    from airweave.platform.sources.stripe import StripeSource

    _, fence = source
    factory = object.__new__(SyncFactory)
    factory._source_lifecycle_service = MagicMock(create=AsyncMock())
    factory._source_registry = MagicMock()
    factory._source_registry.get.return_value.source_class_ref = (
        OutlookMailSource if provider == "outlook_mail" else StripeSource
    )
    factory._build_arf_replay_source = AsyncMock(return_value="offline legacy replay")
    ctx = MagicMock()
    ctx.organization.id = fence.organization_id
    sc = MagicMock(short_name=provider, config_fields={})
    sync = MagicMock(id=fence.sync_id)
    job = MagicMock(id=fence.job_id)
    replay = SyncConfig.model_validate({"behavior": {"replay_from_arf": True}})
    async with database() as db:
        cursor = await db.scalar(select(SyncCursor).where(SyncCursor.sync_id == fence.sync_id))
        if cursor is None:
            cursor = SyncCursor(
                sync_id=fence.sync_id, organization_id=fence.organization_id, cursor_data={}
            )
            db.add(cursor)
        cursor.cursor_data = {}
        await db.flush()
        assert (
            await factory._build_source(db, sync, job, ctx, MagicMock(), sc, False, replay)
            == "offline legacy replay"
        )
        factory._build_arf_replay_source.reset_mock()
        sc.config_fields = original_config
        with pytest.raises(ValueError, match="not mutable ARF"):
            await factory._build_source(db, sync, job, ctx, MagicMock(), sc, False, replay)
        cursor.cursor_data = {"delta_link": "legacy continuation"}
        await db.flush()
        for force_full_sync in (False, True):
            with pytest.raises(ValueError, match="retained state"):
                await factory._build_source(
                    db, sync, job, ctx, MagicMock(), sc, force_full_sync, replay
                )
        sc.config_fields = {}
        cursor.cursor_data = {"canonical_cycle": {"phase": "active"}}
        await db.flush()
        with pytest.raises(ValueError, match="retained state"):
            await factory._build_source(db, sync, job, ctx, MagicMock(), sc, True, replay)
        factory._source_lifecycle_service.create.assert_not_called()
        factory._build_arf_replay_source.assert_not_called()
