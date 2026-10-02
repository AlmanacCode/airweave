"""Request-scoped vector reads validated against the authoritative publication store."""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.api.context import ApiContext
from airweave.domains.entities.canonical.read_authority import source_is_readable
from airweave.domains.search.adapters.vector_db.exceptions import VectorDBError
from airweave.domains.search.adapters.vector_db.protocol import VectorDBProtocol
from airweave.domains.search.canonical_visibility import visible_results
from airweave.domains.search.types import QueryEmbeddings, SearchPlan, SearchResults
from airweave.domains.search.types.filters import (
    FilterableField,
    FilterCondition,
    FilterGroup,
    FilterOperator,
)
from airweave.domains.search.types.results import CompiledQuery, SearchResult
from airweave.domains.sources.protocols import SourceRegistryProtocol
from airweave.models.source_connection import SourceConnection


class UnavailableExactCount(VectorDBError):
    """The index cannot prove an exact currently visible record count."""


class VisibleVectorDB:
    """Same adapter contract with current publication checks on every content retrieval."""

    def __init__(
        self,
        delegate: VectorDBProtocol,
        db: AsyncSession,
        ctx: ApiContext,
        readable_id: str,
        collection_id: str,
        registry: SourceRegistryProtocol,
    ) -> None:
        """Bind one authenticated collection; callers cannot widen that scope."""
        self._delegate = delegate
        self._db = db
        self._ctx = ctx
        self._readable_id = readable_id
        self._collection_id = collection_id
        self._registry = registry

    def _check_scope(self, collection_id: str) -> None:
        if collection_id != self._collection_id:
            raise VectorDBError("Collection does not match this search request")

    async def validate(self, results: list[SearchResult]) -> list[SearchResult]:
        """Revalidate even previously cached results before further consumption."""
        return await visible_results(
            self._db, self._ctx.organization.id, self._readable_id, results, self._registry
        )

    async def compile_query(
        self,
        plan: SearchPlan,
        embeddings: QueryEmbeddings,
        collection_id: str,
        acl_principals: list[str] | None = None,
    ) -> CompiledQuery:
        """Compile only within the bound collection."""
        self._check_scope(collection_id)
        return await self._delegate.compile_query(plan, embeddings, collection_id, acl_principals)

    async def execute_query(self, compiled_query: CompiledQuery) -> SearchResults:
        """Validate returned content regardless of query fields."""
        result = await self._delegate.execute_query(compiled_query)
        visible = await self.validate(result.results)
        excluded = len(result.results) - len(visible)
        return result.model_copy(
            update={
                "results": visible,
                "retrieval_incomplete": result.retrieval_incomplete or excluded > 0,
                "excluded_candidates": result.excluded_candidates + excluded,
            }
        )

    async def filter_search(
        self,
        filter_groups: list[FilterGroup],
        collection_id: str,
        limit: int = 50,
        offset: int = 0,
        name_substring: str | None = None,
    ) -> list[SearchResult]:
        """Validate read and navigation candidates before returning any text."""
        self._check_scope(collection_id)
        results = await self._delegate.filter_search(
            filter_groups, collection_id, limit, offset, name_substring
        )
        return await self.validate(results)

    async def count(
        self,
        filter_groups: list[FilterGroup],
        collection_id: str,
        name_substring: str | None = None,
    ) -> int:
        """Legacy index counts remain available; canonical counts need database semantics."""
        self._check_scope(collection_id)
        sources = list(
            await self._db.scalars(
                select(SourceConnection).where(
                    SourceConnection.organization_id == self._ctx.organization.id,
                    SourceConnection.readable_collection_id == self._readable_id,
                    SourceConnection.is_authenticated.is_(True),
                    source_is_readable(SourceConnection.organization_id, SourceConnection.sync_id),
                    SourceConnection.sync_id.is_not(None),
                )
            )
        )
        for source in sources:
            if getattr(
                self._registry.get(source.short_name).source_class_ref,
                "canonical_record_types",
                (),
            ):
                raise UnavailableExactCount(
                    "Exact current count is unavailable for this indexed collection; "
                    "enumerate committed source records instead."
                )
        if not sources:
            return 0
        allowed = FilterCondition(
            field=FilterableField.SYSTEM_METADATA_SYNC_ID,
            operator=FilterOperator.IN,
            value=[str(source.sync_id) for source in sources],
        )
        scoped = (
            [FilterGroup(conditions=[*group.conditions, allowed]) for group in filter_groups]
            if filter_groups
            else [FilterGroup(conditions=[allowed])]
        )
        return await self._delegate.count(scoped, collection_id, name_substring)

    async def close(self) -> None:
        """The shared adapter is owned by the application, not this request."""
