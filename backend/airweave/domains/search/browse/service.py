"""Browse service — paginated tabular listing of a collection.

No query, no embeddings, no ranking. Hits Vespa's filter_search() and count()
in parallel. Forces chunk_index = 0 so each source entity shows up as a single row.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.api.context import ApiContext
from airweave.domains.collections.protocols import CollectionRepositoryProtocol
from airweave.domains.search.adapters.vector_db.protocol import VectorDBProtocol
from airweave.domains.search.protocols import BrowseServiceProtocol
from airweave.domains.search.types.filters import (
    FilterableField,
    FilterCondition,
    FilterGroup,
    FilterOperator,
)
from airweave.domains.search.visible_vector_db import UnavailableExactCount, VisibleVectorDB
from airweave.domains.sources.protocols import SourceRegistryProtocol
from airweave.schemas.search_v2 import BrowseResponse

if TYPE_CHECKING:
    from airweave.schemas.search_v2 import BrowseRequest


class BrowseService(BrowseServiceProtocol):
    """Browse a collection as paginated rows."""

    def __init__(
        self,
        vector_db: VectorDBProtocol,
        collection_repo: CollectionRepositoryProtocol,
        source_registry: SourceRegistryProtocol,
    ) -> None:
        """Initialize with vector DB and collection repository."""
        self._vector_db = vector_db
        self._source_registry = source_registry
        self._collection_repo = collection_repo

    async def browse(
        self,
        db: AsyncSession,
        ctx: ApiContext,
        readable_id: str,
        request: BrowseRequest,
    ) -> BrowseResponse:
        """Run a paginated unranked listing for the given collection."""
        start = time.monotonic()
        ctx.logger.info(
            f"Browse started collection={readable_id} limit={request.limit} offset={request.offset}"
        )

        collection = await self._collection_repo.get_by_readable_id(db, readable_id, ctx)
        if not collection:
            raise HTTPException(status_code=404, detail=f"Collection '{readable_id}' not found")

        filter_groups = self._build_filter_groups(request)
        collection_id = str(collection.id)
        name_substring = (request.name_query or "").strip() or None

        visible_db = VisibleVectorDB(
            self._vector_db, db, ctx, readable_id, collection_id, self._source_registry
        )
        # One SQL session is never used concurrently by validation/count.
        retrieved = await self._vector_db.filter_search(
            filter_groups=filter_groups, collection_id=collection_id,
            limit=request.limit, offset=request.offset, name_substring=name_substring,
        )
        results = await visible_db.validate(retrieved)
        try:
            total = await visible_db.count(filter_groups, collection_id, name_substring)
        except UnavailableExactCount:
            total = None

        duration_ms = int((time.monotonic() - start) * 1000)
        ctx.logger.info(
            f"Browse completed collection={readable_id} "
            f"rows={len(results)} total={total} duration_ms={duration_ms}"
        )

        return BrowseResponse(
            results=results,
            total=total,
            retrieval_incomplete=len(results) != len(retrieved),
            limit=request.limit,
            offset=request.offset,
        )

    def _build_filter_groups(self, request: BrowseRequest) -> list[FilterGroup]:
        """Combine user filters with the chunk-index=0 row anchor and convenience filters.

        chunk_index=0 collapses Vespa's per-chunk documents to one row per source entity.
        Each FilterGroup is AND-internal; multiple groups are OR'd together. To preserve
        AND semantics across user filters AND our forced conditions, we extend each user
        group's conditions instead of appending a new group.
        """
        chunk_anchor = FilterCondition(
            field=FilterableField.SYSTEM_METADATA_CHUNK_INDEX,
            operator=FilterOperator.EQUALS,
            value=0,
        )

        extra_conditions: list[FilterCondition] = [chunk_anchor]

        if request.sync_ids:
            extra_conditions.append(
                FilterCondition(
                    field=FilterableField.SYSTEM_METADATA_SYNC_ID,
                    operator=FilterOperator.IN,
                    value=list(request.sync_ids),
                )
            )

        if request.entity_types:
            extra_conditions.append(
                FilterCondition(
                    field=FilterableField.SYSTEM_METADATA_ENTITY_TYPE,
                    operator=FilterOperator.IN,
                    value=list(request.entity_types),
                )
            )

        if not request.filter:
            return [FilterGroup(conditions=extra_conditions)]

        return [
            FilterGroup(conditions=[*group.conditions, *extra_conditions])
            for group in request.filter
        ]
