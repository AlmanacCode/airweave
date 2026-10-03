"""Immutable runtime provenance with real SQL fences and synthetic processing failures."""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from sqlalchemy import select

from airweave.adapters.storage.filesystem import FilesystemBackend
from airweave.core.container.preparation import preparation_recipe
from airweave.core.logging import logger
from airweave.domains.converters.registry import ConverterRegistry
from airweave.domains.embedders.fakes.embedder import FakeDenseEmbedder, FakeSparseEmbedder
from airweave.domains.embedders.registry import DenseEmbedderRegistry, SparseEmbedderRegistry
from airweave.domains.entities.canonical.mail_body import PreparedMailBody, current_mail_body
from airweave.domains.entities.canonical.preparation_recipe import PreparationRecipe
from airweave.domains.entities.canonical.projection_gc import ProjectionGCStore
from airweave.domains.entities.canonical.projection_store import CanonicalProjectionStore
from airweave.domains.entities.canonical.projector import CanonicalProjector
from airweave.domains.entities.canonical.reprojection import plan_reprojection
from airweave.domains.entities.canonical.tests.helpers import bind_projection, capture
from airweave.domains.entities.canonical.tests.test_mail_query import message
from airweave.domains.entities.canonical.tests.test_owned_search import (
    indexed as search_indexed,  # noqa: F401, F811
)
from airweave.domains.sync_pipeline.processors.chunk_embed import ChunkEmbedProcessor
from airweave.models.entity import Entity
from airweave.models.projection_generation import ProjectionGeneration
from airweave.models.sync import Sync


def test_composition_facts_separate_embedding_from_extraction_without_loading_models():
    dense, sparse = DenseEmbedderRegistry(), SparseEmbedderRegistry()
    dense.build()
    sparse.build()
    recipe = preparation_recipe(
        artifact_sha256=None,
        converter_extensions=ConverterRegistry().supported_extensions(),
        configured_ocr=(),
        dense=dense.get("openai_text_embedding_3_small"),
        sparse=sparse.get("fastembed_bm25"),
        dimensions=1536,
    )
    assert recipe.artifact_sha256 is None and ".png" not in recipe.extraction.converter_extensions
    assert recipe.extraction.actual_ocr_outcome == "unknown"
    assert recipe.chunking.semantic.boundary_model.sha256 is None
    assert recipe.embedding.model_revision is None
    changed = recipe.model_copy(
        update={"embedding": recipe.embedding.model_copy(update={"dimensions": 512})}
    )
    assert recipe.component_digest("extraction") == changed.component_digest("extraction")
    assert recipe.component_digest("chunking") == changed.component_digest("chunking")
    assert recipe.component_digest("embedding") != changed.component_digest("embedding")


@pytest.mark.parametrize("stage", ["conversion", "embedding"])
async def test_failed_processing_keeps_recipe_before_calls_and_originals(
    database, source, tmp_path, stage
):
    service, fence = source
    binding = await bind_projection(database, fence)
    original = message("one")
    await capture(database, service, fence, original)
    recipe = PreparationRecipe(artifact_sha256="a" * 64)
    store = CanonicalProjectionStore()
    processor = ChunkEmbedProcessor(ConverterRegistry(), FakeDenseEmbedder(), FakeSparseEmbedder())

    async def fail(*args, **kwargs):
        async with database() as db:
            attempt = (await db.scalars(select(ProjectionGeneration))).one()
            assert attempt.preparation_recipe == recipe.model_dump(mode="json")
            assert attempt.documents is None
            assert (attempt.mail_body_text is not None) == (stage == "embedding")
        raise RuntimeError("synthetic preparation outage")

    method = "build_text" if stage == "conversion" else "process_built_text"
    setattr(processor, method, AsyncMock(side_effect=fail))
    project = CanonicalProjector(
        store,
        lambda _organization: database(),
        processor,
        FilesystemBackend(tmp_path),
        recipe=recipe,
    )
    target = MagicMock(collection_id=binding.collection_id, feed_prepared=AsyncMock())
    result = await project.batch(fence.organization_id, fence.sync_id, "gmail", target, logger)
    assert result.failed == 1 and result.published == 0
    target.feed_prepared.assert_not_awaited()
    async with database() as db:
        row = (await db.scalars(select(Entity))).one()
        attempt = (await db.scalars(select(ProjectionGeneration))).one()
        assert row.source_payload == original.payload and row.record_revision == 1
        assert row.indexed_generation is None and row.projection_error == "RuntimeError"
        assert attempt.preparation_recipe == recipe.model_dump(mode="json")
        if stage == "embedding":
            assert "retained body boundary" in attempt.mail_body_text


async def test_attempt_recipe_cannot_change_at_begin_body_or_seal(database, source):
    service, fence = source
    binding = await bind_projection(database, fence)
    await capture(database, service, fence, message("one"))
    store = CanonicalProjectionStore()
    async with database() as db:
        work = (await store.pending(db, fence.organization_id, fence.sync_id))[0]
    generation = uuid4()
    recipe, other = PreparationRecipe(artifact_sha256="a" * 64), PreparationRecipe()
    body = PreparedMailBody(text="complete retained body", status="complete")
    async with database() as db:
        assert await store.begin_attempt(db, work, generation, recipe)
        assert await store.begin_attempt(db, work, generation, recipe)
        assert not await store.publish(db, work, generation, 0)
        with pytest.raises(ValueError, match="immutable"):
            await store.begin_attempt(db, work, generation, other)
        with pytest.raises(ValueError, match="immutable"):
            await store.prepare_mail_body(db, work, generation, body, recipe=other)
        assert await store.prepare_mail_body(db, work, generation, body, recipe=recipe)
        assert await store.prepare_mail_body(db, work, generation, body, recipe=recipe)
        with pytest.raises(ValueError, match="immutable"):
            await store.prepare(db, work, generation, binding.collection_id, (), recipe=other)
        assert await store.prepare(db, work, generation, binding.collection_id, (), recipe=recipe)
        assert await store.prepare(db, work, generation, binding.collection_id, (), recipe=recipe)
        row = await db.get(ProjectionGeneration, generation)
        assert row.mail_body_text == body.text and row.preparation_recipe == recipe.model_dump()


async def test_failed_attempt_gc_preserves_provenance_and_does_not_hide_mail_body(database, source):
    service, fence = source
    await bind_projection(database, fence)
    await capture(database, service, fence, message("one"))
    store = CanonicalProjectionStore()
    async with database() as db:
        work = (await store.pending(db, fence.organization_id, fence.sync_id))[0]
    good, failed = uuid4(), uuid4()
    recipe = PreparationRecipe()
    async with database() as db:
        assert await store.begin_attempt(db, work, good, recipe)
        assert await store.prepare_mail_body(
            db,
            work,
            good,
            PreparedMailBody(text="valid early body", status="complete"),
            recipe=recipe,
        )
        assert await store.begin_attempt(db, work, failed, recipe)
        designated = await db.scalar(
            select(current_mail_body().with_only_columns(ProjectionGeneration.id).scalar_subquery())
            .select_from(Entity)
            .join(Sync, Sync.id == Entity.sync_id)
            .where(Entity.id == work.record.id)
        )
        assert designated == good
    now = datetime.now(timezone.utc) + timedelta(hours=2)
    async with database() as db:
        assert await ProjectionGCStore().claim(db, good, now=now) is None
        page = await ProjectionGCStore().claim(db, failed, now=now)
        assert page.documents == () and page.artifact_keys == ()
        row = await db.get(ProjectionGeneration, failed)
        assert row.retired_at is not None and row.preparation_recipe == recipe.model_dump()
        assert not await store.begin_attempt(db, work, failed, recipe)
        assert not await store.publish(db, work, failed, 0)


async def test_reprojection_retry_preserves_unknown_old_recipe_and_original(
    database, search_indexed  # noqa: F811
):
    fence, locator, _ = search_indexed
    store = CanonicalProjectionStore()
    async with database() as db:
        old = await db.get(ProjectionGeneration, locator.generation)
        assert old.preparation_recipe is None
        original = (await db.get(Entity, locator.record_id)).source_payload
    for _ in range(2):
        async with database() as db:
            await plan_reprojection(
                db,
                fence.organization_id,
                fence.sync_id,
                expected_version=2,
                target_version=3,
                apply=True,
            )
            await db.commit()
    async with database() as db:
        work = (await store.pending(db, fence.organization_id, fence.sync_id))[0]
        assert work.pipeline_version == 3
        generation = uuid4()
        recipe = PreparationRecipe(artifact_sha256="b" * 64)
        assert await store.begin_attempt(db, work, generation, recipe)
        assert (await db.get(ProjectionGeneration, locator.generation)).preparation_recipe is None
        assert (await db.get(Entity, locator.record_id)).source_payload == original
        assert (
            await db.get(ProjectionGeneration, generation)
        ).preparation_recipe == recipe.model_dump()
