"""Metadata distinguishes owned records from independently indexed entity classes."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from airweave.domains.entities.canonical.query_store import CanonicalQueryStore
from airweave.domains.entities.canonical.requests import RecordIdentity
from airweave.domains.entities.canonical.tests.helpers import capture, observation
from airweave.domains.search.builders.collection_metadata import CollectionMetadataBuilder


async def test_captured_counts_respect_visibility_and_do_not_claim_indexed_counts(database, source):
    service, fence = source
    parent_id = RecordIdentity(record_type="calendar", native_id="cal")
    parent = observation(identity=parent_id)
    child = observation("event", "cal", parent=parent_id)
    await capture(database, service, fence, parent, child)
    query = CanonicalQueryStore()
    async with database() as db:
        assert await query.captured_counts(db, fence.organization_id, fence.sync_id) == {
            "calendar": 1,
            "event": 1,
        }
        assert await query.captured_counts(db, uuid4(), fence.sync_id) == {}
    collection_repo = AsyncMock()
    collection_repo.get_by_readable_id.return_value = SimpleNamespace(id=uuid4())
    sc_repo = AsyncMock()
    sc_repo.get_by_collection_ids.return_value = [
        SimpleNamespace(short_name="google_calendar", sync_id=fence.sync_id, is_authenticated=True)
    ]
    registry = MagicMock()
    registry.get.return_value = SimpleNamespace(
        source_class_ref=SimpleNamespace(canonical_record_types=("calendar", "event")),
        federated_search=False,
    )
    definitions = MagicMock()
    definitions.list_for_source.return_value = [
        SimpleNamespace(
            name="GoogleCalendarEventEntity",
            short_name="GoogleCalendarEventEntity",
            entity_schema={"properties": {"title": {"description": "Event title"}}},
        )
    ]
    legacy_counts = AsyncMock()
    builder = CollectionMetadataBuilder(
        collection_repo,
        sc_repo,
        registry,
        definitions,
        legacy_counts,
    )
    context = SimpleNamespace(organization=SimpleNamespace(id=fence.organization_id))
    async with database() as db:
        metadata = await builder.build(db, context, "calendar")
    assert metadata.sources[0].captured_record_counts == {"calendar": 1, "event": 1}
    assert metadata.sources[0].entity_types[0].count is None
    assert "indexing may lag" in metadata.to_md()
    assert "indexed count unknown" in metadata.to_md()
    assert "(no entities)" not in metadata.to_md()
    legacy_counts.get_counts_per_sync_and_type.assert_not_called()
    await capture(
        database,
        service,
        fence,
        parent.model_copy(
            update={
                "kind": "delete",
                "removal_reason": "access_revoked",
            }
        ),
    )
    async with database() as db:
        assert await query.captured_counts(db, fence.organization_id, fence.sync_id) == {}
