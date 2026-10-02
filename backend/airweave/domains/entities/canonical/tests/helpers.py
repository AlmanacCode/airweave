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
    collection_id = collection_id or work.binding.collection_id
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


async def bind_projection(database, fence, source_name="gmail", collection_id=None):
    """Create the real authenticated source/collection required for synthetic projection."""
    from uuid import uuid4

    from sqlalchemy import select

    from airweave.domains.entities.canonical.projection_models import ProjectionBinding
    from airweave.models.collection import Collection
    from airweave.models.source_connection import SourceConnection
    from airweave.models.vector_db_deployment_metadata import VectorDbDeploymentMetadata

    collection_id = collection_id or uuid4()
    readable_id = str(collection_id)
    source_id = uuid4()
    async with database() as db:
        deployment = await db.scalar(select(VectorDbDeploymentMetadata))
        if deployment is None:
            deployment = VectorDbDeploymentMetadata(
                dense_embedder="test", embedding_dimensions=3, sparse_embedder="test"
            )
            db.add(deployment)
            await db.flush()
        db.add(
            Collection(
                id=collection_id,
                name="Synthetic projection",
                readable_id=readable_id,
                organization_id=fence.organization_id,
                vector_db_deployment_metadata_id=deployment.id,
            )
        )
        await db.flush()
        db.add(
            SourceConnection(
                id=source_id,
                name="Synthetic projection",
                short_name=source_name,
                organization_id=fence.organization_id,
                sync_id=fence.sync_id,
                readable_collection_id=readable_id,
                is_authenticated=True,
            )
        )
        await db.commit()
    return ProjectionBinding(
        source_connection_id=source_id, source_name=source_name, collection_id=collection_id
    )
