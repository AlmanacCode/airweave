"""Original-record indexed retrieval using the existing executor and SQL authority."""

import re
from collections import defaultdict
from uuid import UUID

from fastapi import HTTPException
from pydantic import AwareDatetime, BaseModel, ConfigDict
from sqlalchemy import and_, case, func, or_, select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from airweave.api.context import ApiContext
from airweave.domains.entities.canonical.coverage import capture_coverage
from airweave.domains.entities.canonical.extraction_models import ExtractionCoverage
from airweave.domains.entities.canonical.projection_models import ProjectionLocator
from airweave.domains.entities.canonical.projection_store import publications_match
from airweave.domains.entities.canonical.requests import RecordIdentity
from airweave.domains.entities.canonical.search_metadata import (
    SEARCH_METADATA_PIPELINE_VERSION,
    epoch_microseconds,
)
from airweave.domains.entities.canonical.source import indexed_record_types
from airweave.domains.entities.canonical.store import content_is_available
from airweave.domains.search.owned_models import (
    OwnedSearchCoverage,
    OwnedSearchHit,
    OwnedSearchRequest,
    OwnedSearchResponse,
)
from airweave.domains.search.protocols import SearchPlanExecutorProtocol
from airweave.domains.search.types import FilterCondition, FilterGroup, SearchPlan, SearchQuery
from airweave.domains.search.types.results import SearchResult, SearchResults
from airweave.domains.sources.protocols import SourceRegistryProtocol
from airweave.models.collection import Collection
from airweave.models.entity import Entity
from airweave.models.projection_generation import ProjectionGeneration
from airweave.models.source_connection import SourceConnection
from airweave.models.sync import Sync


class _SourceScope(BaseModel):
    """Detached authorization/index snapshot; revalidated before returning any hit."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    id: UUID
    sync_id: UUID
    short_name: str
    readable_collection_id: str
    collection_id: UUID
    index_pipeline_version: int


class _EnrichmentRecord(BaseModel):
    """Detached search-card fields; original payloads and blob manifests stay in the store."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    id: UUID
    sync_id: UUID
    record_revision: int
    indexed_generation: UUID
    indexed_pipeline_version: int
    entity_definition_short_name: str
    native_id: str
    container_id: str | None
    observed_at: AwareDatetime
    source_created_at: AwareDatetime | None
    source_updated_at: AwareDatetime | None
    completeness: str
    email_thread_id: str | None


class OwnedSearchService:
    """No provider lifecycle, planner, answer generator or reranker dependency."""

    def __init__(self, executor: SearchPlanExecutorProtocol, registry: SourceRegistryProtocol):
        """Reuse the executor and registry without another retrieval stack."""
        self._executor = executor
        self._registry = registry

    async def search(
        self,
        sessions: async_sessionmaker[AsyncSession],
        ctx: ApiContext,
        request: OwnedSearchRequest,
    ) -> OwnedSearchResponse:
        """Resolve exact authorized scopes before any embedding/index request."""
        async with sessions() as db:
            scopes, groups = await self._resolve_scopes(db, ctx, request)
        if self._filtered(request) and any(
            scope.index_pipeline_version < SEARCH_METADATA_PIPELINE_VERSION
            for scope in scopes.values()
        ):
            raise HTTPException(
                409,
                {
                    "code": "reindex_required",
                    "message": "Selected sources need canonical search metadata re-projection",
                },
            )
        scope_snapshot = self._scope_identity(scopes)
        hits, scores, exclusions, postfiltered = {}, {}, 0, 0
        engine_partial, full = False, False
        plan = SearchPlan(
            query=SearchQuery(primary=request.query),
            limit=200,
            offset=0,
            retrieval_strategy=request.mode,
        )
        prepared_query = await self._executor.prepare_query(plan) if groups else None
        for (collection_id, readable_id), sync_ids in groups.items():
            # A new session has no checked-out connection before indexed retrieval:
            # this path has neither principal ACL discovery nor provider federation.
            async with sessions() as db:
                results = await self._executor.execute(
                    plan=plan,
                    prepared_query=prepared_query,
                    user_filter=[FilterGroup(conditions=self._prefilters(request, sync_ids))],
                    collection_id=str(collection_id),
                    db=db,
                    ctx=ctx,
                    collection_readable_id=readable_id,
                    indexed_only=True,
                )
                group_hits, group_scores, rejected, filtered = await self._enrich(
                    db, ctx, request, sync_ids, scopes, results
                )
            engine_partial |= results.engine_partial
            exclusions += results.excluded_candidates
            full |= len(results.results) + results.excluded_candidates >= 200
            hits.update(group_hits)
            scores.update(group_scores)
            exclusions += rejected
            postfiltered += filtered
        async with sessions() as db:
            sources = await self._coverage(db, ctx, request)
            fresh_scopes, _ = await self._resolve_scopes(db, ctx, request)
            if self._scope_identity(fresh_scopes) != scope_snapshot:
                raise HTTPException(404, "Requested indexed sources changed during retrieval")
            eligible = await self._final_publications(db, ctx, request, scores, scope_snapshot)
        exclusions += len(hits.keys() - eligible)
        ranked = sorted(hits.keys() & eligible, key=lambda key: (-scores[key][0], str(key)))
        return OwnedSearchResponse(
            items=tuple(hits[key] for key in ranked[: request.limit]),
            sources=sources,
            candidate_window_full=full,
            engine_partial=engine_partial,
            excluded_candidates=exclusions,
            postfilter_excluded=postfiltered,
            retrieval_incomplete=engine_partial
            or full
            or exclusions > 0
            or postfiltered > 0
            or len(ranked) > request.limit
            or any(
                row.pending_records
                or row.partially_indexed_records
                or row.extraction_unavailable_records
                or row.extraction_unknown_records
                for row in sources
            ),
        )

    @staticmethod
    def _filtered(request: OwnedSearchRequest) -> bool:
        return bool(
            request.record_types
            or request.created_after
            or request.created_before
            or request.updated_after
            or request.updated_before
        )

    @staticmethod
    def _prefilters(request: OwnedSearchRequest, sync_ids: list[UUID]) -> list[FilterCondition]:
        conditions = [
            FilterCondition(
                field="airweave_system_metadata.sync_id",
                operator="in",
                value=[str(item) for item in sync_ids],
            )
        ]
        if request.record_types:
            conditions.append(
                FilterCondition(
                    field="airweave_system_metadata.canonical_record_type",
                    operator="in",
                    value=list(request.record_types),
                )
            )
        for name, after, before in (
            ("created", request.created_after, request.created_before),
            ("updated", request.updated_after, request.updated_before),
        ):
            if after is not None or before is not None:
                conditions.append(
                    FilterCondition(
                        field=f"airweave_system_metadata.source_{name}_known",
                        operator="equals",
                        value=1,
                    )
                )
            for boundary, operator in ((after, "greater_than_or_equal"), (before, "less_than")):
                if boundary is not None:
                    conditions.append(
                        FilterCondition(
                            field=f"airweave_system_metadata.source_{name}_us",
                            operator=operator,
                            value=epoch_microseconds(boundary),
                        )
                    )
        return conditions

    async def _resolve_scopes(
        self, db: AsyncSession, ctx: ApiContext, request: OwnedSearchRequest
    ) -> tuple[dict[UUID, _SourceScope], dict[tuple[UUID, str], list[UUID]]]:
        rows = (
            (
                await db.execute(
                    select(
                        SourceConnection.id,
                        SourceConnection.sync_id,
                        SourceConnection.short_name,
                        SourceConnection.readable_collection_id,
                        Collection.id.label("collection_id"),
                        Sync.index_pipeline_version,
                    )
                    .join(
                        Collection,
                        and_(
                            Collection.readable_id == SourceConnection.readable_collection_id,
                            Collection.organization_id == ctx.organization.id,
                        ),
                    )
                    .join(
                        Sync,
                        and_(
                            Sync.id == SourceConnection.sync_id,
                            Sync.organization_id == ctx.organization.id,
                        ),
                    )
                    .where(
                        SourceConnection.organization_id == ctx.organization.id,
                        SourceConnection.is_authenticated.is_(True),
                        SourceConnection.sync_id.in_(request.sync_ids),
                    )
                )
            )
            .mappings()
            .all()
        )
        snapshots = [_SourceScope.model_validate(row) for row in rows]
        if len(snapshots) != len(request.sync_ids) or {row.sync_id for row in snapshots} != set(
            request.sync_ids
        ):
            raise HTTPException(404, "Requested indexed sources are unavailable")
        scopes = {row.sync_id: row for row in snapshots}
        allowed_types = set()
        groups = defaultdict(list)
        for connection in snapshots:
            types = indexed_record_types(connection.short_name, self._registry)
            if not types:
                raise HTTPException(422, "Source does not support owned indexed records")
            allowed_types.update(types)
            groups[(connection.collection_id, connection.readable_collection_id)].append(
                connection.sync_id
            )
        if set(request.record_types) - allowed_types:
            raise HTTPException(422, "Record type is unsupported by selected sources")
        return scopes, groups

    async def _enrich(
        self,
        db: AsyncSession,
        ctx: ApiContext,
        request: OwnedSearchRequest,
        sync_ids: list[UUID],
        scopes: dict[UUID, _SourceScope],
        results: SearchResults,
    ) -> tuple[dict[UUID, OwnedSearchHit], dict[UUID, tuple[float, ProjectionLocator]], int, int]:
        locators = [value for result in results.results if (value := self._locator(result))]
        if not locators:
            return {}, {}, len(results.results), 0
        hits, scores, exclusions, postfiltered = {}, {}, 0, 0
        # Recheck current publication while obtaining native identity. Never enrich
        # from an unvalidated cached hit after a concurrent capture/permission change.
        rows = await db.execute(
            select(
                Entity.id,
                Entity.sync_id,
                Entity.record_revision,
                Entity.indexed_generation,
                Entity.indexed_pipeline_version,
                Entity.entity_definition_short_name,
                Entity.native_id,
                Entity.container_id,
                Entity.observed_at,
                Entity.source_created_at,
                Entity.source_updated_at,
                Entity.completeness,
                case(
                    (
                        func.jsonb_typeof(Entity.source_payload["threadId"]) == "string",
                        Entity.source_payload["threadId"].astext,
                    ),
                    else_=None,
                ).label("email_thread_id"),
            )
            .join(Sync, Sync.id == Entity.sync_id)
            .where(
                Entity.organization_id == ctx.organization.id,
                Sync.organization_id == ctx.organization.id,
                Entity.sync_id.in_(sync_ids),
                publications_match(locators),
            )
        )
        records = [_EnrichmentRecord.model_validate(row) for row in rows.mappings()]
        by_id = {record.id: record for record in records}
        extraction_rows = await db.execute(
            select(ProjectionGeneration.id, ProjectionGeneration.extraction_coverage).where(
                ProjectionGeneration.id.in_([r.indexed_generation for r in records])
            )
        )
        extraction = {
            generation: ExtractionCoverage.model_validate(raw) if raw is not None else None
            for generation, raw in extraction_rows
        }
        for rank, result in enumerate(results.results, 1):
            locator = self._locator(result)
            row = by_id.get(locator.record_id) if locator else None
            if (
                row is None
                or row.record_revision != locator.revision
                or row.indexed_generation != locator.generation
                or row.indexed_pipeline_version != locator.pipeline_version
                or str(row.sync_id) != result.airweave_system_metadata.sync_id
                or scopes[row.sync_id].short_name != result.airweave_system_metadata.source_name
            ):
                exclusions += 1
                continue
            coverage = extraction.get(row.indexed_generation)
            if coverage is not None and not any(
                part.part_index == locator.part_index and part.outcome == "indexed"
                for part in coverage.parts
            ):
                exclusions += 1
                continue
            if not self._matches(row, request):
                postfiltered += 1
                continue
            if row.id not in hits:
                hits[row.id] = OwnedSearchHit(
                    record_id=row.id,
                    revision=row.record_revision,
                    sync_id=row.sync_id,
                    source_connection_id=scopes[row.sync_id].id,
                    provider=scopes[row.sync_id].short_name,
                    identity=RecordIdentity(
                        record_type=row.entity_definition_short_name,
                        native_id=row.native_id,
                        container_id=row.container_id,
                    ),
                    title=result.name,
                    excerpts=(),
                    observed_at=row.observed_at,
                    source_created_at=row.source_created_at,
                    source_updated_at=row.source_updated_at,
                    completeness=row.completeness,
                    extraction=extraction.get(row.indexed_generation),
                    email_thread_id=(
                        thread_id
                        if scopes[row.sync_id].short_name == "gmail"
                        and row.entity_definition_short_name == "message"
                        and (thread_id := row.email_thread_id) is not None
                        and re.fullmatch(r"[A-Za-z0-9_-]{1,512}", thread_id)
                        else None
                    ),
                )
                scores[row.id] = (1 / (60 + rank), locator)
            excerpt = result.textual_representation[:2000]
            current = hits[row.id]
            if excerpt and excerpt not in current.excerpts and len(current.excerpts) < 3:
                hits[row.id] = current.model_copy(update={"excerpts": (*current.excerpts, excerpt)})
        return hits, scores, exclusions, postfiltered

    @staticmethod
    def _scope_identity(scopes: dict[UUID, _SourceScope]) -> set[tuple]:
        return {
            (
                sync,
                row.id,
                row.short_name,
                row.readable_collection_id,
                row.collection_id,
                row.index_pipeline_version,
            )
            for sync, row in scopes.items()
        }

    @staticmethod
    async def _final_publications(
        db: AsyncSession,
        ctx: ApiContext,
        request: OwnedSearchRequest,
        scores: dict[UUID, tuple[float, ProjectionLocator]],
        scope_snapshot: set[tuple],
    ) -> set[UUID]:
        # Later collection retrieval may outlive an edit/revocation of earlier hits.
        # Recheck the exact publication, including parent visibility, at the final
        # SQL read boundary. No network I/O occurs after this authorization check.
        if not scores:
            return set()
        # One statement snapshots all candidates, including current authentication.
        # The 20*200 bound keeps these tuple parameters below PostgreSQL's limit.
        return set(
            await db.scalars(
                select(Entity.id)
                .join(Sync, Sync.id == Entity.sync_id)
                .join(
                    SourceConnection,
                    and_(
                        SourceConnection.sync_id == Sync.id,
                        SourceConnection.organization_id == ctx.organization.id,
                        SourceConnection.is_authenticated.is_(True),
                    ),
                )
                .join(
                    Collection,
                    and_(
                        Collection.readable_id == SourceConnection.readable_collection_id,
                        Collection.organization_id == ctx.organization.id,
                    ),
                )
                .where(
                    Entity.organization_id == ctx.organization.id,
                    Sync.organization_id == ctx.organization.id,
                    Entity.sync_id.in_(request.sync_ids),
                    tuple_(
                        SourceConnection.sync_id,
                        SourceConnection.id,
                        SourceConnection.short_name,
                        SourceConnection.readable_collection_id,
                        Collection.id,
                        Sync.index_pipeline_version,
                    ).in_(list(scope_snapshot)),
                    publications_match(locator for _, locator in scores.values()),
                )
            )
        )

    @staticmethod
    def _locator(result: SearchResult) -> ProjectionLocator | None:
        try:
            return ProjectionLocator.parse(result.airweave_system_metadata.original_entity_id)
        except ValueError:
            return None

    @staticmethod
    def _matches(row: _EnrichmentRecord, request: OwnedSearchRequest) -> bool:
        if request.record_types and row.entity_definition_short_name not in request.record_types:
            return False
        for value, after, before in (
            (row.source_created_at, request.created_after, request.created_before),
            (row.source_updated_at, request.updated_after, request.updated_before),
        ):
            if (after is not None and (value is None or value < after)) or (
                before is not None and (value is None or value >= before)
            ):
                return False
        return True

    @staticmethod
    async def _coverage(
        db: AsyncSession, ctx: ApiContext, request: OwnedSearchRequest
    ) -> tuple[OwnedSearchCoverage, ...]:
        pending = or_(
            Entity.indexed_revision.is_distinct_from(Entity.record_revision),
            Entity.indexed_pipeline_version.is_distinct_from(Sync.index_pipeline_version),
            Entity.indexed_generation.is_(None),
        )
        has_indexed = ProjectionGeneration.extraction_coverage.contains(
            {"parts": [{"outcome": "indexed"}]}
        )
        has_omitted = ProjectionGeneration.extraction_coverage.contains(
            {"parts": [{"outcome": "unsupported"}]}
        ) | ProjectionGeneration.extraction_coverage.contains(
            {"parts": [{"outcome": "unavailable_original"}]}
        )
        rows = await db.execute(
            select(
                Entity.sync_id,
                func.count(),
                func.count().filter(pending),
                func.count().filter(~pending & has_indexed & has_omitted),
                func.count().filter(~pending & ~has_indexed & has_omitted),
                func.count().filter(~pending & ProjectionGeneration.extraction_coverage.is_(None)),
            )
            .join(Sync, Sync.id == Entity.sync_id)
            .outerjoin(ProjectionGeneration, ProjectionGeneration.id == Entity.indexed_generation)
            .where(
                Entity.organization_id == ctx.organization.id,
                Sync.organization_id == ctx.organization.id,
                Entity.sync_id.in_(request.sync_ids),
                Entity.record_revision > 0,
                Entity.deleted_at.is_(None),
                content_is_available(),
            )
            .group_by(Entity.sync_id)
        )
        counts = {row[0]: tuple(row[1:]) for row in rows}
        captures = await capture_coverage(db, ctx.organization.id, tuple(request.sync_ids))
        return tuple(
            OwnedSearchCoverage(
                sync_id=sync,
                capture=captures.get(sync),
                active_records=counts.get(sync, (0, 0))[0],
                pending_records=counts.get(sync, (0, 0))[1],
                partially_indexed_records=counts.get(sync, (0, 0, 0, 0, 0))[2],
                extraction_unavailable_records=counts.get(sync, (0, 0, 0, 0, 0))[3],
                extraction_unknown_records=counts.get(sync, (0, 0, 0, 0, 0))[4],
            )
            for sync in request.sync_ids
        )
