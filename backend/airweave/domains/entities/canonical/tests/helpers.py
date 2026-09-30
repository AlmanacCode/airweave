"""Synthetic capture inputs and transaction invocation for database tests."""

from datetime import datetime, timezone

from airweave.domains.entities.canonical.requests import CaptureBatch, CaptureRecord, RecordIdentity


def observation(native_id="one", container_id=None, **changes):
    values = {
        "identity": RecordIdentity(
            record_type="event", native_id=native_id, container_id=container_id
        ),
        "payload": {"id": native_id, "summary": "Synthetic event"},
        "observed_at": datetime.now(timezone.utc),
        "content_hash": "same-body",
    }
    values.update(changes)
    return CaptureRecord(**values)


async def capture(database, service, fence, *records):
    async with database() as db:
        return await service.capture(db, CaptureBatch(fence=fence, records=records))


async def publish_prepared(store, db, work, generation, count, collection_id=None):
    """Synthetic exact manifests exercise the same mandatory pre-feed transaction."""
    from uuid import uuid4

    from airweave.domains.entities.canonical.projection_models import (
        ProjectionDocument,
        ProjectionLocator,
        scope_projection_document_id,
    )

    locator = ProjectionLocator(
        record_id=work.record.id,
        revision=work.record.revision,
        pipeline_version=work.pipeline_version,
        generation=generation,
        part_index=0,
    ).encode()
    collection_id = collection_id or uuid4()
    prepared = await store.prepare(
        db,
        work,
        generation,
        collection_id,
        tuple(
            ProjectionDocument(
                schema_name="base_entity",
                document_id=scope_projection_document_id(
                    work.record.sync_id, collection_id, f"Entity_{locator}__chunk_{index}"
                ),
            )
            for index in range(count)
        ),
    )
    return prepared and await store.publish(db, work, generation, count)
