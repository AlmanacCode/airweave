"""Real source/collection scope resolution; only the remote projector is replaced."""

# ruff: noqa: F811
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID, uuid4

import pytest

from airweave.domains.entities.canonical.projection_models import ProjectionBatchResult
from airweave.domains.entities.canonical.requests import RecordIdentity
from airweave.domains.entities.canonical.tests.conftest import database, source  # noqa: F401
from airweave.domains.entities.canonical.tests.helpers import capture, observation
from airweave.domains.temporal.activities import project_canonical_records as module
from airweave.models import Collection, SourceConnection
from airweave.models.vector_db_deployment_metadata import VectorDbDeploymentMetadata


@pytest.fixture
async def activity_setup(database, source, monkeypatch):
    service, fence = source
    metadata = VectorDbDeploymentMetadata(
        dense_embedder="test", sparse_embedder="test", embedding_dimensions=3
    )
    collection_id, connection_id = uuid4(), uuid4()
    async with database() as db:
        db.add(metadata)
        await db.flush()
        db.add(
            Collection(
                id=collection_id,
                name="Native",
                readable_id="native",
                organization_id=fence.organization_id,
                vector_db_deployment_metadata_id=metadata.id,
            )
        )
        db.add(
            SourceConnection(
                id=connection_id,
                organization_id=fence.organization_id,
                sync_id=fence.sync_id,
                name="Native",
                short_name="almanac",
                readable_collection_id="native",
            )
        )
        await db.commit()
    await capture(
        database,
        service,
        fence,
        observation(
            identity=RecordIdentity(record_type="knowledge", native_id="one"),
            payload={"body": "नमस्ते"},
        ),
    )
    monkeypatch.setattr(module, "get_db_context", database)
    destination = SimpleNamespace(close_connection=AsyncMock())
    create = AsyncMock(return_value=destination)
    monkeypatch.setattr(module.VespaDestination, "create", create)
    projector = SimpleNamespace(batch=AsyncMock(return_value=ProjectionBatchResult(published=1)))
    registry = Mock()
    registry.get.side_effect = AssertionError("Native sources must not enter provider registry")
    activity = module.ProjectCanonicalRecordsActivity(projector, registry)
    return activity, fence, connection_id, collection_id, destination, create


@pytest.mark.parametrize("provider", ["almanac", "gmail", "legacy"])
async def test_capability_routes_native_and_provider(activity_setup, database, provider):
    activity, fence, connection_id, collection_id, destination, create = activity_setup
    if provider != "almanac":
        async with database() as db:
            connection = await db.get(SourceConnection, connection_id)
            connection.short_name = provider
            await db.commit()
        activity.source_registry.get.side_effect = None
        activity.source_registry.get.return_value = SimpleNamespace(
            source_class_ref=SimpleNamespace(
                canonical_record_types=("message",) if provider == "gmail" else ()
            )
        )
    result = await activity.run(str(fence.organization_id), str(fence.sync_id), str(UUID(int=0)))
    if provider == "legacy":
        assert result == ProjectionBatchResult().model_dump(mode="json")
        create.assert_not_awaited()
        activity.projector.batch.assert_not_awaited()
        return
    assert result["published"] == 1
    create.assert_awaited_once_with(
        collection_id=collection_id,
        organization_id=fence.organization_id,
        logger=module.logger,
        soft_fail=False,
    )
    activity.projector.batch.assert_awaited_once_with(
        fence.organization_id,
        fence.sync_id,
        provider,
        destination,
        module.logger,
        after_id=UUID(int=0),
    )
    destination.close_connection.assert_awaited_once()
    if provider == "almanac":
        activity.source_registry.get.assert_not_called()
    else:
        activity.source_registry.get.assert_called_once_with(provider)


async def test_foreign_organization_and_empty_page_do_not_connect(activity_setup):
    activity, fence, _, _, destination, create = activity_setup
    assert await activity.run(
        str(uuid4()), str(fence.sync_id)
    ) == ProjectionBatchResult().model_dump(mode="json")
    # This valid source has no pending records beyond the maximum UUID.
    assert await activity.run(
        str(fence.organization_id), str(fence.sync_id), str(UUID(int=2**128 - 1))
    ) == ProjectionBatchResult().model_dump(mode="json")
    activity.source_registry.get.assert_not_called()
    activity.projector.batch.assert_not_awaited()
    create.assert_not_awaited()
    destination.close_connection.assert_not_awaited()


async def test_projector_failure_propagates_and_closes_destination(activity_setup):
    activity, fence, _, _, destination, _ = activity_setup
    activity.projector.batch.side_effect = RuntimeError("synthetic projection failure")
    with pytest.raises(RuntimeError, match="synthetic projection failure"):
        await activity.run(str(fence.organization_id), str(fence.sync_id))
    destination.close_connection.assert_awaited_once()
