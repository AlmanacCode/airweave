"""SQL inventory and prepared body boundaries with synthetic native Gmail messages."""

import base64
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select, update

from airweave.adapters.storage.filesystem import FilesystemBackend
from airweave.domains.converters.registry import ConverterRegistry
from airweave.domains.entities.canonical.mail_body import PreparedMailBody
from airweave.domains.entities.canonical.mail_facts_v1 import gmail_metadata_v1
from airweave.domains.entities.canonical.mail_models import MailFilters, MailMessageQuery
from airweave.domains.entities.canonical.mail_query import CanonicalMailQuery, MailChanged
from airweave.domains.entities.canonical.projection_gc import ProjectionGCStore
from airweave.domains.entities.canonical.projection_models import (
    ProjectionDocument,
    ProjectionLocator,
    scope_projection_document_id,
)
from airweave.domains.entities.canonical.projection_store import CanonicalProjectionStore
from airweave.domains.entities.canonical.projector import CanonicalProjector
from airweave.domains.entities.canonical.requests import RecordIdentity
from airweave.domains.entities.canonical.tests.helpers import (
    bind_projection,
    capture,
    observation,
)
from airweave.domains.entities.canonical.tests.test_http import query_app
from airweave.domains.sync_pipeline.processors.chunk_embed import ChunkEmbedProcessor
from airweave.models.entity import Entity
from airweave.models.projection_generation import ProjectionGeneration
from airweave.models.source_connection import SourceConnection
from airweave.models.sync import Sync

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def message(
    native_id, when=NOW, subject="Inventory", sender='"Doe, Jane" <Jane@EXAMPLE.test>', **changes
):
    payload = {
        "id": native_id,
        "threadId": "old-and-new-thread",
        "internalDate": str(int(when.timestamp() * 1000)),
        "labelIds": ["INBOX", "UNREAD"],
        "snippet": "snippet must never satisfy body search",
        "payload": {
            "mimeType": "text/html",
            "headers": [
                {"name": "From", "value": sender},
                {
                    "name": "To",
                    "value": '"Recipient, One" <ONE@example.test>, Two <two@example.test>',
                },
                {"name": "Subject", "value": subject},
                {"name": "Date", "value": "Tue, 01 Jan 2019 00:00:00 +0000"},
            ],
            "body": {
                "data": base64.urlsafe_b64encode(
                    b"<p>Full retained body boundary canary</p>"
                ).decode()
            },
        },
    }
    return observation(
        native_id,
        identity=RecordIdentity(record_type="message", native_id=native_id),
        payload=payload,
        source_created_at=when,
        **changes,
    )


async def read(database, fence, **options):
    async with database() as db:
        return await CanonicalMailQuery("mail-test-key").messages(
            db, fence.organization_id, fence.sync_id, MailMessageQuery(**options)
        )


async def test_metadata_sql_inventory_over_200_and_exact_facets(database, source):
    service, fence = source
    await bind_projection(database, fence)
    records = [
        message(f"match-{index}", NOW + timedelta(milliseconds=index)) for index in range(257)
    ]
    records.extend(
        message(f"unrelated-{index}", NOW + timedelta(days=1), sender="other@example.test")
        for index in range(30)
    )
    await capture(database, service, fence, *records)
    filters = MailFilters(
        from_addresses=("no@example.test", "JANE@example.test"),
        to_addresses=("absent@example.test", "one@example.test"),
        after=NOW,
        before=NOW + timedelta(days=1),
        folder="inbox",
        unread=True,
    )
    seen, cursor = [], None
    while True:
        page = await read(database, fence, filters=filters, limit=41, cursor=cursor)
        seen.extend(page.messages)
        assert page.indexing.text_unavailable == 257
        assert page.indexing.metadata_missing == 0
        assert all(item.sender[0].name == "Doe, Jane" for item in page.messages)
        if not page.has_more:
            assert page.next_cursor is None
            break
        assert page.next_cursor and page.next_cursor != cursor
        cursor = page.next_cursor
    assert len(seen) == len({item.id for item in seen}) == 257
    assert all(item.native_id.startswith("match-") for item in seen)
    assert not (
        await read(
            database,
            fence,
            filters=filters.model_copy(update={"to_addresses": ("other@example.test",)}),
        )
    ).messages
    exact = await read(
        database, fence, filters=MailFilters(after=NOW, before=NOW + timedelta(milliseconds=1))
    )
    assert [item.native_id for item in exact.messages] == ["match-0"]
    assert not (await read(database, fence, filters=MailFilters(unread=False))).messages


async def test_http_scope_filter_cursor_change_revocation_and_gaps(database, source, monkeypatch):
    service, fence = source
    await bind_projection(database, fence)
    invalid = message("invalid").model_copy(update={"source_created_at": None})
    await capture(database, service, fence, message("one"), message("two"), invalid)
    owner = fence.organization_id
    app = query_app(database, lambda: SimpleNamespace(organization=SimpleNamespace(id=owner)))
    async with AsyncClient(transport=ASGITransport(app), base_url="http://fixture") as client:
        path = f"/sync/{fence.sync_id}/mail/messages"
        response = await client.get(
            path, params={"limit": 1, "from_addresses": "jane@example.test"}
        )
        assert response.status_code == 200, response.text
        page = response.json()
        assert page["indexing"]["metadata_missing"] == 1
        assert "payload" not in page["messages"][0] and "body" not in page["messages"][0]
        assert "capture" in page
        cursor = page["next_cursor"]
        assert (await client.get(path, params={"cursor": cursor})).status_code == 400
        assert (
            await client.get(
                path, params={"after": "2026-02-01T00:00:00Z", "before": "2026-01-01T00:00:00Z"}
            )
        ).status_code == 422
        assert (await client.get(path, params={"after": "2026-01-01"})).status_code == 422
        owner = uuid4()
        assert (await client.get(path)).status_code == 404
        owner = fence.organization_id
        await capture(database, service, fence, message("new"))
        changed = await client.get(
            path, params={"cursor": cursor, "from_addresses": "jane@example.test"}
        )
        assert (
            changed.status_code == 409 and changed.json()["error"]["code"] == "mail_changed_restart"
        )
        import airweave.domains.entities.canonical.mail_query as query_module

        original = query_module.capture_coverage

        async def revoke(*args, **kwargs):
            result = await original(*args, **kwargs)
            async with database() as db:
                await db.execute(
                    update(SourceConnection)
                    .where(SourceConnection.sync_id == fence.sync_id)
                    .values(is_authenticated=False)
                )
                await db.commit()
            return result

        monkeypatch.setattr(query_module, "capture_coverage", revoke)
        assert (await client.get(path)).status_code == 404


async def test_body_preparation_cursor_seal_gc_and_cross_pipeline(database, source):
    service, fence = source
    await bind_projection(database, fence)
    await capture(database, service, fence, message("one"), message("two"))
    store = CanonicalProjectionStore()
    async with database() as db:
        work = (await store.pending(db, fence.organization_id, fence.sync_id))[0]
    generation = uuid4()
    body = PreparedMailBody(text="full body across representation boundaries", status="complete")
    async with database() as db:
        assert await store.prepare_mail_body(db, work, generation, body)
        assert await store.prepare_mail_body(db, work, generation, body)
        assert (await db.get(Sync, fence.sync_id)).mail_text_sequence == 1
        assert not await store.publish(db, work, generation, 0)
    page = await read(database, fence, filters=MailFilters(query="BODY ACROSS REPRESENTATION"))
    assert (
        len(page.messages) == 1
        and page.indexing.text_ready == 1
        and page.indexing.text_unavailable == 1
    )
    assert not (
        await read(database, fence, filters=MailFilters(query="snippet must never"))
    ).messages
    subject = await read(database, fence, filters=MailFilters(query="inventory"), limit=1)
    async with database() as db:
        assert await store.prepare_mail_body(db, work, uuid4(), body)
    with pytest.raises(MailChanged):
        await read(
            database,
            fence,
            filters=MailFilters(query="inventory"),
            limit=1,
            cursor=subject.next_cursor,
        )
    async with database() as db:
        generations = tuple(
            await db.scalars(
                select(ProjectionGeneration).order_by(
                    ProjectionGeneration.created_at, ProjectionGeneration.id
                )
            )
        )
        newest = generations[-1].id
    later = datetime.now(timezone.utc) + timedelta(days=2)
    async with database() as db:
        assert await ProjectionGCStore().claim(db, newest, now=later) is None
        assert await ProjectionGCStore().claim(db, generation, now=later) is not None
    async with database() as db:
        # Seal once, permit exact retry, and reject a second distinct feed manifest.
        row = await db.get(ProjectionGeneration, newest)
        assert row.documents is None
        locator = ProjectionLocator(
            record_id=work.record.id,
            revision=work.record.revision,
            pipeline_version=work.pipeline_version,
            generation=newest,
            part_index=0,
        )
        documents = tuple(
            ProjectionDocument(
                schema_name="base_entity",
                document_id=scope_projection_document_id(
                    fence.sync_id,
                    work.binding.collection_id,
                    f"Entity_{locator.encode()}__chunk_{index}",
                ),
            )
            for index in range(2)
        )
        assert await store.prepare(db, work, newest, work.binding.collection_id, documents[:1])
        assert await store.prepare(db, work, newest, work.binding.collection_id, documents[:1])
        with pytest.raises(ValueError, match="immutable"):
            await store.prepare(db, work, newest, work.binding.collection_id, documents)
        assert await store.publish(db, work, newest, 1)
    # The same work snapshot no longer owns the index CAS after publication.
    async with database() as db:
        assert not await store.prepare(db, work, newest, work.binding.collection_id, ())
    async with database() as db:
        await db.execute(
            update(Sync).where(Sync.id == fence.sync_id).values(index_pipeline_version=2)
        )
        await db.commit()
    assert not (await read(database, fence, filters=MailFilters(query="body across"))).messages
    assert (await read(database, fence)).indexing.text_unavailable == 2
    await capture(
        database, service, fence, message(work.record.identity.native_id, subject="Edited")
    )
    assert not (await read(database, fence, filters=MailFilters(query="body across"))).messages


async def test_real_gmail_builder_retains_body_after_embedding_failure(database, source, tmp_path):
    from airweave.domains.embedders.fakes.embedder import FakeDenseEmbedder, FakeSparseEmbedder

    service, fence = source
    binding = await bind_projection(database, fence)
    await capture(database, service, fence, message("one"))
    store = CanonicalProjectionStore()
    async with database() as db:
        work = (await store.pending(db, fence.organization_id, fence.sync_id))[0]
    processor = ChunkEmbedProcessor(ConverterRegistry(), FakeDenseEmbedder(), FakeSparseEmbedder())
    processor.process_built_text = AsyncMock(side_effect=RuntimeError("Embedding unavailable"))
    projector = CanonicalProjector(store, database, processor, FilesystemBackend(tmp_path))
    destination = MagicMock(collection_id=binding.collection_id)
    with pytest.raises(RuntimeError, match="Embedding unavailable"):
        await projector.project_one(work, "gmail", destination, MagicMock())
    destination.feed_prepared.assert_not_called()
    page = await read(database, fence, filters=MailFilters(query="retained body boundary"))
    assert len(page.messages) == 1 and page.indexing.text_ready == 1
    async with database() as db:
        assert (await db.get(Entity, work.record.id)).indexed_generation is None
        assert (await db.scalar(select(ProjectionGeneration))).documents is None


def test_versioned_metadata_parser_native_date_and_casefold():
    item = message("one")
    facts = gmail_metadata_v1(item.payload, "one", NOW)
    assert facts.sender[0].address == "jane@example.test"
    assert [mail.address for mail in facts.to] == ["one@example.test", "two@example.test"]
    assert gmail_metadata_v1(item.payload, "foreign", NOW) is None
    assert gmail_metadata_v1(item.payload, "one", NOW + timedelta(milliseconds=1)) is None
    assert gmail_metadata_v1(message("one", sender="broken address").payload, "one", NOW) is None


async def test_versioned_backfill_restores_facts_without_changing_originals(database, source):
    import importlib.util
    from pathlib import Path

    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    service, fence = source
    await bind_projection(database, fence)
    await capture(
        database,
        service,
        fence,
        message("legacy"),
        message("malformed").model_copy(update={"source_created_at": None}),
    )
    path = Path(__file__).resolve().parents[5] / "alembic/versions/0014_retained_gmail_query.py"
    spec = importlib.util.spec_from_file_location("gmail_backfill_test", path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)

    def migrate(connection):
        with Operations.context(MigrationContext.configure(connection)):
            migration.downgrade()
            migration.upgrade()

    async with database() as db:
        original = tuple(
            (row.native_id, row.record_revision, row.source_payload)
            for row in await db.scalars(select(Entity).order_by(Entity.native_id))
        )
        sequence = (await db.get(Sync, fence.sync_id)).observed_change_sequence
        await (await db.connection()).run_sync(migrate)
        await db.commit()
    page = await read(database, fence)
    assert [item.native_id for item in page.messages] == ["legacy"]
    assert page.indexing.metadata_missing == 1
    async with database() as db:
        current = tuple(
            (row.native_id, row.record_revision, row.source_payload)
            for row in await db.scalars(select(Entity).order_by(Entity.native_id))
        )
        assert current == original
        assert (await db.get(Sync, fence.sync_id)).observed_change_sequence == sequence
