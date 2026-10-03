"""Synthetic WhatsApp HTTP, real PostgreSQL commits and retained original bytes."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import select

from airweave.adapters.storage.filesystem import FilesystemBackend
from airweave.domains.entities.canonical.cycle_models import CompleteCycle
from airweave.domains.entities.canonical.query import CanonicalQueryService, RecordNotFound
from airweave.domains.entities.canonical.query_store import CanonicalQueryStore
from airweave.domains.entities.canonical.requests import CaptureBatch, CompletedScope
from airweave.domains.entities.canonical.scan_models import ScanContinuation
from airweave.domains.entities.canonical.store import CanonicalRecordStore
from airweave.domains.entities.canonical.tests.helpers import bind_projection
from airweave.domains.sources.exceptions import SourceServerError
from airweave.domains.storage.file_service import FileService
from airweave.domains.sync_pipeline.canonical_scan import CanonicalScanDriver
from airweave.models.capture_scan import CaptureScan
from airweave.models.entity import Entity
from airweave.models.sync_job import SyncJob
from airweave.platform.http_client.airweave_client import AirweaveHttpClient
from airweave.platform.http_client.unipile_transport import UnipileWhatsAppClient
from airweave.platform.sources.records.whatsapp_models import WhatsAppMessage
from airweave.platform.sources.whatsapp_capture import WhatsAppCapture, WhatsAppCaptureConfig

CHAT = {
    "object": "Chat",
    "provider": "whatsapp",
    "id": "group@lid",
    "is_group": True,
    "is_1to1": False,
    "is_channel": False,
}
MEDIA = b"synthetic-original-audio"


def message(identity, *, changed=False):
    payload = {
        "object": "Message",
        "provider": "whatsapp",
        "id": identity,
        "chat_id": "group@lid",
        "sender_id": "person@lid",
        "timestamp": "2026-10-02T00:00:00Z",
        "is_sender": False,
        "text": "Changed 引用" if changed else "Original مرحباً",
        "native_unknown": {"retained": [None, 3]},
    }
    if identity == "m1":
        payload["attachments"] = [
            {
                "object": "Attachment",
                "id": "audio",
                "type": "audio",
                "mimetype": "audio/ogg",
                "voice_note": True,
            }
        ]
        if changed:
            payload["is_edited"] = True
    elif changed:
        payload["is_deleted"] = True
    return payload


def connector(raw):
    http = AirweaveHttpClient(raw, uuid4(), "whatsapp", feature_flag_enabled=False)
    return WhatsAppCapture(
        UnipileWhatsAppClient(http, account_id="acc_bound", api_key="synthetic"),
        WhatsAppCaptureConfig(
            account_id="acc_bound",
            account_user_id="self@lid",
            native_user_id="self@lid",
            pagination="cursor",
            page_size=20,
            max_pages_per_scope=10,
            maximum_attachment_bytes=1000,
            participant_pagination="offset",
            maximum_participant_pages=10,
            maximum_participant_items=100,
            maximum_participant_bytes=100000,
            reaction_pagination="offset",
            maximum_reaction_pages=10,
            maximum_reaction_items=100,
            maximum_reaction_bytes=100000,
        ),
    )


def responder(calls, *, fail=False, changed=False):  # noqa: C901 -- synthetic native API routes
    def respond(request):  # noqa: C901 -- synthetic native API routes
        calls.append((request.url.path, request.url.params.get("cursor")))
        path = request.url.path
        if path.endswith("/accounts/acc_bound"):
            data = {
                "object": "Account",
                "id": "acc_bound",
                "provider": "whatsapp",
                "user_id": "self@lid",
                "status": "running",
                "is_locked": False,
            }
        elif path.endswith("/users/self@lid"):
            data = {"object": "UserProfile", "id": "self@lid", "display_name": "Self"}
        elif path.endswith("/attachments/audio"):
            return httpx.Response(200, content=MEDIA, headers={"content-type": "audio/ogg"})
        elif path.endswith("/reactions"):
            data = {"data": [], "native_extra": {"retained": None}}
        elif path.endswith("/participants"):
            data = {"data": [], "native_extra": {"retained": None}}
        elif path.endswith("/messages"):
            second = request.url.params.get("cursor") == "p2"
            if fail and second:
                return httpx.Response(
                    503,
                    json={
                        "object": "Error",
                        "status": 503,
                        "type": "server_error",
                        "title": "Synthetic temporary failure",
                    },
                )
            data = {"data": [message("m2" if second else "m1", changed=changed)]}
            if not second:
                data["next_cursor"] = "p2"
        elif path.endswith("/chats/group@lid"):
            data = CHAT
        elif path.endswith("/chats"):
            data = {"data": [CHAT]}
        else:
            raise AssertionError(f"Unexpected synthetic API route: {path}")
        return httpx.Response(200, json=data)

    return respond


async def run(database, service, fence, raw, files):
    return await CanonicalScanDriver(
        service,
        database,
        fence,
        connector(raw),
        AsyncMock(),
        AsyncMock(),
        files,
    ).run()


async def test_whatsapp_sql_restart_replay_edit_delete_and_original_media(
    database, source, tmp_path
):
    service, fence = source
    await bind_projection(database, fence, source_name="whatsapp")
    storage = FilesystemBackend(tmp_path)
    files = FileService(uuid4(), storage, sync_id=fence.sync_id)
    async with httpx.AsyncClient(transport=httpx.MockTransport(responder([], fail=True))) as raw:
        with pytest.raises(SourceServerError):
            await run(database, service, fence, raw, files)
    async with database() as db:
        scan = await db.scalar(
            select(CaptureScan).where(CaptureScan.record_type == "whatsapp_message")
        )
        assert scan.phase == "collecting" and scan.continuation["cursor"] == "p2"
        rows = list(
            (
                await db.scalars(
                    select(Entity).where(Entity.entity_definition_short_name == "whatsapp_message")
                )
            ).all()
        )
        assert len(rows) == 1 and rows[0].native_id == "m1"
        assert rows[0].record_revision == 1
        queries = CanonicalQueryService(
            CanonicalRecordStore(), CanonicalQueryStore(), "synthetic-test-key"
        )
        own = await queries.read(db, fence.organization_id, fence.sync_id, rows[0].id)
        assert own.payload["text"] == "Original مرحباً"
        with pytest.raises(RecordNotFound):
            await queries.read(db, uuid4(), fence.sync_id, rows[0].id)
        blob = rows[0].blob_references[0]
        assert blob["source_path"] == "/attachments/0"
        before = await service.read_cycle(db, fence)
        # A crash/retry within the same writer attempt resumes the committed cursor.
        # A distinct writer attempt refreshes message inventory now that it owns reactions.
        newer = fence
    assert await storage.read_file(blob["key"], max_bytes=1000) == MEDIA

    calls = []
    async with httpx.AsyncClient(transport=httpx.MockTransport(responder(calls))) as raw:
        after = await run(database, service, newer, raw, files)
        async with database() as db:
            after = await service.complete_cycle(
                db, CompleteCycle(fence=newer, expected=after.version)
            )
        count = len(calls)
        replay = await run(database, service, newer, raw, files)
        assert len(calls) == count and replay.version == after.version
    assert after.version.cycle_id == before.version.cycle_id
    assert after.last_full_capture.discovery == "incomplete"
    assert [cursor for path, cursor in calls if path.endswith("/messages")] == ["p2"]

    async with database() as db:
        rows = list(
            (
                await db.scalars(
                    select(Entity).where(Entity.entity_definition_short_name == "whatsapp_message")
                )
            ).all()
        )
        assert len(rows) == 2 and all(row.record_revision == 1 for row in rows)
        assert all(row.source_payload["native_unknown"] == {"retained": [None, 3]} for row in rows)
        (await db.get(SyncJob, newer.job_id)).status = "completed"
        job = SyncJob(
            id=uuid4(),
            sync_id=newer.sync_id,
            organization_id=newer.organization_id,
            status="running",
        )
        db.add(job)
        await db.commit()
        fresh = await service.activate_writer(
            db,
            newer.organization_id,
            newer.sync_id,
            job.id,
            attempt_id=uuid4(),
            attempt_number=1,
        )
    async with httpx.AsyncClient(transport=httpx.MockTransport(responder([], changed=True))) as raw:
        changed = await run(database, service, fresh, raw, files)
        async with database() as db:
            await service.complete_cycle(db, CompleteCycle(fence=fresh, expected=changed.version))
            rows = {
                row.native_id: row
                for row in (
                    await db.scalars(
                        select(Entity).where(
                            Entity.entity_definition_short_name == "whatsapp_message"
                        )
                    )
                ).all()
            }
            assert rows["m1"].record_revision == 2
            assert rows["m1"].source_payload["text"] == "Changed 引用"
            assert rows["m2"].record_revision == 2
            assert rows["m2"].deleted_at is not None
            assert rows["m2"].removal_reason == "provider_deleted"
    assert await storage.read_file(blob["key"], max_bytes=1000) == MEDIA


async def test_whatsapp_reactions_restricted_parent_requires_fresh_child_attestation(
    database, source, tmp_path
):
    """Actual native capture records and SQL access gate, without a mocked visibility store."""
    service, fence = source
    await bind_projection(database, fence, source_name="whatsapp")
    queries = CanonicalQueryService(
        CanonicalRecordStore(), CanonicalQueryStore(), "synthetic-test-key"
    )
    files = FileService(uuid4(), FilesystemBackend(tmp_path), sync_id=fence.sync_id)
    async with httpx.AsyncClient(transport=httpx.MockTransport(responder([]))) as raw:
        capture = connector(raw)
        chats = await capture.capture_page(
            CompletedScope(record_type="whatsapp_chat"), ScanContinuation(), files=files
        )
        chat = chats.records[0]

        async def native_parent(hidden):
            return await capture._message(
                WhatsAppMessage.model_validate(message("m1") | {"is_hidden": hidden}),
                chat.identity,
                files,
                datetime.now(timezone.utc),
            )

        visible = await native_parent(False)
        async with database() as db:
            saved = await service.capture(db, CaptureBatch(fence=fence, records=(chat, visible)))
            parent_id = next(
                change.record.id
                for change in saved.changes
                if change.record.identity == visible.identity
            )
            parent_record = await queries.read(db, fence.organization_id, fence.sync_id, parent_id)
        scope = capture.child_scope(parent_record, "whatsapp_message_reactions")
        page = await capture.capture_page(
            scope, ScanContinuation(), parent=parent_record, files=files
        )
        async with database() as db:
            saved = await service.capture(db, CaptureBatch(fence=fence, records=page.records))
            reaction_id = saved.changes[0].record.id
            assert (
                await queries.read(db, fence.organization_id, fence.sync_id, reaction_id)
            ).content_access == "available"
        hidden = await native_parent(True)
        async with database() as db:
            await service.capture(db, CaptureBatch(fence=fence, records=(hidden,)))
            blocked = await queries.read(db, fence.organization_id, fence.sync_id, reaction_id)
            assert (
                blocked.content_access == "unavailable"
                and blocked.payload == {}
                and not blocked.blobs
            )
        restored = await native_parent(False)
        async with database() as db:
            await service.capture(db, CaptureBatch(fence=fence, records=(restored,)))
            blocked = await queries.read(db, fence.organization_id, fence.sync_id, reaction_id)
            assert blocked.content_access == "unavailable" and blocked.payload == {}
            current = await queries.read(db, fence.organization_id, fence.sync_id, parent_id)
        fresh = await capture.capture_page(scope, ScanContinuation(), parent=current, files=files)
        async with database() as db:
            await service.capture(db, CaptureBatch(fence=fence, records=fresh.records))
            readable = await queries.read(db, fence.organization_id, fence.sync_id, reaction_id)
            assert readable.content_access == "available"
            assert readable.payload == page.records[0].payload


async def test_whatsapp_new_attempt_refreshes_message_inventory_without_duplicate_revisions(
    database, source, tmp_path
):
    """Inventory parents deliberately resweep on a distinct writer attempt."""
    service, fence = source
    await bind_projection(database, fence, source_name="whatsapp")
    files = FileService(uuid4(), FilesystemBackend(tmp_path), sync_id=fence.sync_id)
    async with httpx.AsyncClient(transport=httpx.MockTransport(responder([], fail=True))) as raw:
        with pytest.raises(SourceServerError):
            await run(database, service, fence, raw, files)
    async with database() as db:
        before = await db.scalar(
            select(CaptureScan).where(CaptureScan.record_type == "whatsapp_message")
        )
        old_sweep = before.sweep_id
        assert before.continuation["cursor"] == "p2"
        first = await db.scalar(
            select(Entity).where(Entity.entity_definition_short_name == "whatsapp_message")
        )
        first_id = first.id
        assert first.native_id == "m1" and first.record_revision == 1
        newer = await service.activate_writer(
            db,
            fence.organization_id,
            fence.sync_id,
            fence.job_id,
            attempt_id=uuid4(),
            attempt_number=2,
        )
    calls = []
    async with httpx.AsyncClient(transport=httpx.MockTransport(responder(calls))) as raw:
        cycle = await run(database, service, newer, raw, files)
    assert [cursor for path, cursor in calls if path.endswith("/messages")] == [None, "p2"]
    queries = CanonicalQueryService(
        CanonicalRecordStore(), CanonicalQueryStore(), "synthetic-test-key"
    )
    async with database() as db:
        await service.complete_cycle(db, CompleteCycle(fence=newer, expected=cycle.version))
        after = await db.scalar(
            select(CaptureScan).where(CaptureScan.record_type == "whatsapp_message")
        )
        assert after.sweep_id != old_sweep and after.phase == "complete"
        assert (await db.get(Entity, first_id)).record_revision == 1
        messages = list(
            (
                await db.scalars(
                    select(Entity).where(Entity.entity_definition_short_name == "whatsapp_message")
                )
            ).all()
        )
        assert len(messages) == 2 and all(row.record_revision == 1 for row in messages)
        reactions = list(
            (
                await db.scalars(
                    select(Entity).where(
                        Entity.entity_definition_short_name == "whatsapp_message_reactions"
                    )
                )
            ).all()
        )
        assert len(reactions) == 2
        for reaction in reactions:
            current = await queries.read(db, fence.organization_id, fence.sync_id, reaction.id)
            assert current.content_access == "available"
            assert current.parent.native_id == current.payload["message_id"]
            assert current.parent.container_id == current.payload["chat_id"]
