"""Original-record indexed retrieval using the existing executor and SQL authority."""

import asyncio
import math
import re
from collections import defaultdict
from uuid import UUID

from anyio import CapacityLimiter, create_task_group
from fastapi import HTTPException
from pydantic import AwareDatetime, BaseModel, ConfigDict, ValidationError
from sqlalchemy import and_, case, func, or_, select, true, tuple_
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from airweave.api.context import ApiContext
from airweave.core.protocols.reranker import RerankerProtocol, RerankerResult
from airweave.core.protocols.tokenizer import TokenizerProtocol
from airweave.domains.entities.canonical.coverage import capture_coverage
from airweave.domains.entities.canonical.extraction_models import ExtractionCoverage
from airweave.domains.entities.canonical.projection_models import ProjectionLocator
from airweave.domains.entities.canonical.projection_store import publications_match
from airweave.domains.entities.canonical.read_authority import source_is_readable
from airweave.domains.entities.canonical.requests import RecordIdentity
from airweave.domains.entities.canonical.search_metadata import (
    NATIVE_TYPE_PIPELINE_VERSION,
    SEARCH_METADATA_PIPELINE_VERSION,
    epoch_microseconds,
)
from airweave.domains.entities.canonical.source import indexed_record_types
from airweave.domains.entities.canonical.store import content_is_available
from airweave.domains.native_ingestion.models import NativeVersion
from airweave.domains.search.owned_models import (
    OwnedCandidate,
    OwnedCandidatesResponse,
    OwnedRanking,
    OwnedRankRequest,
    OwnedRankResponse,
    OwnedSearchCoverage,
    OwnedSearchGroup,
    OwnedSearchHit,
    OwnedSearchMatch,
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
    parent_record_type: str | None
    parent_native_id: str | None
    parent_container_id: str | None
    observed_at: AwareDatetime
    source_created_at: AwareDatetime | None
    source_updated_at: AwareDatetime | None
    completeness: str
    email_thread_id: str | None
    native_version: NativeVersion | None
    native_type: str | None


class _CollectionResult(BaseModel):
    """Detached collection outcome; SQL sessions never cross concurrent tasks."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    hits: dict[UUID, OwnedSearchHit]
    scores: dict[UUID, tuple[float, ProjectionLocator]]
    text: dict[UUID, str]
    excluded: int
    postfiltered: int
    engine_partial: bool
    window_full: bool


class _RetrievedSearch(_CollectionResult):
    """One retrieval's detached candidates and original source scope."""

    scopes: dict[UUID, _SourceScope]


class OwnedSearchService:
    """Canonical retrieval and optional shared ranking, without source provider calls."""

    def __init__(
        self,
        executor: SearchPlanExecutorProtocol,
        registry: SourceRegistryProtocol,
        *,
        reranker: RerankerProtocol | None = None,
        tokenizer: TokenizerProtocol | None = None,
    ):
        """Reuse the executor and registry without another retrieval stack."""
        self._executor = executor
        self._registry = registry
        self._reranker = reranker
        self._tokenizer = tokenizer

    async def _retrieve(
        self,
        sessions: async_sessionmaker[AsyncSession],
        ctx: ApiContext,
        request: OwnedSearchRequest,
    ) -> _RetrievedSearch:
        """Resolve exact authorized scopes before any embedding/index request."""
        async with sessions() as db:
            scopes, groups = await self._resolve_scopes(db, ctx, request)
        if self._filtered(request) and any(
            scope.index_pipeline_version
            < (
                NATIVE_TYPE_PIPELINE_VERSION
                if request.native_types and scope.short_name == "almanac"
                else SEARCH_METADATA_PIPELINE_VERSION
            )
            for scope in scopes.values()
        ):
            raise HTTPException(
                409,
                {
                    "code": "reindex_required",
                    "message": "Selected sources need canonical search metadata re-projection",
                },
            )
        hits, scores, matched_text, exclusions, postfiltered = {}, {}, {}, 0, 0
        engine_partial, full = False, False
        plan = SearchPlan(
            query=SearchQuery(primary=request.query),
            limit=200,
            offset=0,
            retrieval_strategy=request.mode,
        )
        prepared_query = await self._executor.prepare_query(plan) if groups else None
        limiter = CapacityLimiter(4)
        outcomes: dict[UUID, _CollectionResult | HTTPException] = {}

        async def retrieve(collection_id: UUID, readable_id: str, sync_ids: list[UUID]) -> None:
            async with limiter:
                try:
                    # Each task owns a session. No SQL connection is checked out
                    # before the executor's index request; federation stays disabled.
                    async with sessions() as db:
                        results = await self._executor.execute(
                            plan=plan,
                            prepared_query=prepared_query,
                            user_filter=[
                                FilterGroup(conditions=self._prefilters(request, sync_ids))
                            ],
                            collection_id=str(collection_id),
                            db=db,
                            ctx=ctx,
                            collection_readable_id=readable_id,
                            indexed_only=True,
                        )
                        found, ranks, text, rejected, filtered = await self._enrich(
                            db, ctx, request, sync_ids, scopes, results
                        )
                    outcomes[collection_id] = _CollectionResult(
                        hits=found,
                        scores=ranks,
                        text=text,
                        excluded=results.excluded_candidates + rejected,
                        postfiltered=filtered,
                        engine_partial=results.engine_partial,
                        window_full=len(results.results) + results.excluded_candidates >= 200,
                    )
                except HTTPException as error:
                    outcomes[collection_id] = error

        async with create_task_group() as tasks:
            for (collection_id, readable_id), sync_ids in groups.items():
                tasks.start_soon(retrieve, collection_id, readable_id, sync_ids)
        for collection_id, _ in groups:
            outcome = outcomes[collection_id]
            if isinstance(outcome, HTTPException):
                raise outcome
            engine_partial |= outcome.engine_partial
            exclusions += outcome.excluded
            full |= outcome.window_full
            hits.update(outcome.hits)
            scores.update(outcome.scores)
            matched_text.update(outcome.text)
            postfiltered += outcome.postfiltered
        return _RetrievedSearch(
            hits=hits,
            scores=scores,
            text=matched_text,
            excluded=exclusions,
            postfiltered=postfiltered,
            engine_partial=engine_partial,
            window_full=full,
            scopes=scopes,
        )

    async def candidates(
        self,
        sessions: async_sessionmaker[AsyncSession],
        ctx: ApiContext,
        request: OwnedSearchRequest,
    ) -> OwnedCandidatesResponse:
        """Return one bounded shortlist for product authority checks, without reranking."""
        found = await self._retrieve(sessions, ctx, request)
        scope_snapshot = self._scope_identity(found.scopes)
        async with sessions() as db:
            sources = await self._coverage(db, ctx, request)
            fresh_scopes, _ = await self._resolve_scopes(db, ctx, request)
            if self._scope_identity(fresh_scopes) != scope_snapshot:
                raise HTTPException(404, "Requested indexed sources changed during retrieval")
            eligible = await self._final_publications(
                db, ctx, request, found.scores, scope_snapshot
            )
        ordered = sorted(
            found.hits.keys() & eligible, key=lambda key: (-found.scores[key][0], str(key))
        )
        # Keep original matched text separate from presentation snippets. The
        # product may discard candidates before any external model sees them.
        candidates = tuple(
            OwnedCandidate(
                hit=found.hits[key],
                projection=found.scores[key][1],
                retrieval_score=found.scores[key][0],
                text=found.text[key][:32000],
                text_truncated=len(found.text[key]) > 32000,
            )
            for key in ordered[: request.limit]
        )
        return OwnedCandidatesResponse(
            candidates=candidates,
            sources=sources,
            candidate_window_full=found.window_full,
            engine_partial=found.engine_partial,
            excluded_candidates=found.excluded + len(found.hits.keys() - eligible),
            postfilter_excluded=found.postfiltered,
            shortlist_truncated=len(ordered) > request.limit,
        )

    async def rank_candidates(
        self,
        sessions: async_sessionmaker[AsyncSession],
        ctx: ApiContext,
        request: OwnedRankRequest,
    ) -> OwnedRankResponse:
        """Recheck canonical authority around ranking a backend-approved shortlist."""
        scope_request = OwnedSearchRequest(query=request.query, sync_ids=request.sync_ids)
        async with sessions() as db:
            scopes, _ = await self._resolve_scopes(db, ctx, scope_request)
        snapshot = self._scope_identity(scopes)
        hits, scores, text, expected_syncs = {}, {}, {}, {}
        for candidate in request.candidates:
            hit = candidate.hit
            scope = scopes.get(hit.sync_id)
            if (
                scope is None
                or hit.source_connection_id != scope.id
                or hit.provider != scope.short_name
                or (
                    hit.group is not None
                    and (hit.group.matched_records != 1 or hit.group.additional_matches)
                )
                or (hit.provider == "almanac" and hit.native_version is None)
            ):
                raise HTTPException(422, "Candidate does not match its declared source scope")
            hits[hit.record_id] = hit
            scores[hit.record_id] = (candidate.retrieval_score, candidate.projection)
            text[hit.record_id] = candidate.text
            expected_syncs[hit.record_id] = hit.sync_id
        async with sessions() as db:
            eligible = await self._final_publications(
                db, ctx, scope_request, scores, snapshot, expected_syncs
            )
        candidates = sorted(eligible, key=lambda key: (-scores[key][0], str(key)))
        ranked, ranking = await self._rank(request.query, candidates, hits, text)
        async with sessions() as db:
            fresh, _ = await self._resolve_scopes(db, ctx, scope_request)
            if self._scope_identity(fresh) != snapshot:
                raise HTTPException(404, "Requested indexed sources changed during ranking")
            final = await self._final_publications(
                db, ctx, scope_request, scores, snapshot, expected_syncs
            )
        approved = tuple(key for key in ranked if key in final)
        return OwnedRankResponse(
            record_ids=approved,
            ranking=ranking,
            excluded_candidates=len(request.candidates) - len(approved),
        )

    async def search(
        self,
        sessions: async_sessionmaker[AsyncSession],
        ctx: ApiContext,
        request: OwnedSearchRequest,
    ) -> OwnedSearchResponse:
        """Retrieve and rank provider content within the owned canonical authority."""
        found = await self._retrieve(sessions, ctx, request)
        scope_snapshot = self._scope_identity(found.scopes)
        hits, scores, matched_text = found.hits, found.scores, found.text
        exclusions, postfiltered = found.excluded, found.postfiltered
        engine_partial, full = found.engine_partial, found.window_full
        # Revalidate the whole union before any remote text disclosure: earlier
        # collections may have changed while later collections were retrieved.
        async with sessions() as db:
            fresh_scopes, _ = await self._resolve_scopes(db, ctx, request)
            if self._scope_identity(fresh_scopes) != scope_snapshot:
                raise HTTPException(404, "Requested indexed sources changed during retrieval")
            eligible = await self._final_publications(db, ctx, request, scores, scope_snapshot)
        exclusions += len(hits.keys() - eligible)
        candidates = sorted(hits.keys() & eligible, key=lambda key: (-scores[key][0], str(key)))
        ranked, ranking = await self._rank(request.query, candidates, hits, matched_text)
        # No SQL connection is held across the reranker await. No network I/O
        # follows this final authorization/publication read boundary.
        async with sessions() as db:
            sources = await self._coverage(db, ctx, request)
            fresh_scopes, _ = await self._resolve_scopes(db, ctx, request)
            if self._scope_identity(fresh_scopes) != scope_snapshot:
                raise HTTPException(404, "Requested indexed sources changed during retrieval")
            final = await self._final_publications(db, ctx, request, scores, scope_snapshot)
        exclusions += len(set(candidates) - final)
        ranked = [key for key in ranked if key in final]
        grouped = self._group_hits([hits[key] for key in ranked])
        return OwnedSearchResponse(
            items=tuple(grouped[: request.limit]),
            ranking=ranking,
            sources=sources,
            candidate_window_full=full,
            engine_partial=engine_partial,
            excluded_candidates=exclusions,
            postfilter_excluded=postfiltered,
            retrieval_incomplete=ranking.shortlist_truncated
            or engine_partial
            or full
            or exclusions > 0
            or postfiltered > 0
            or len(grouped) > request.limit
            or any(
                row.pending_records
                or row.partially_indexed_records
                or row.extraction_unavailable_records
                or row.extraction_unknown_records
                for row in sources
            ),
        )

    @staticmethod
    def _conversation(row: _EnrichmentRecord, hit: OwnedSearchHit) -> OwnedSearchGroup | None:
        """Derive membership only from admitted canonical parent or validated Gmail identity."""
        if hit.provider == "gmail" and hit.email_thread_id is not None:
            return OwnedSearchGroup(
                kind="email_thread", native_id=hit.email_thread_id, matched_records=1
            )
        if hit.provider != "almanac":
            return None
        if (
            row.entity_definition_short_name == "session"
            and row.container_id is None
            and row.parent_record_type is None
            and row.parent_native_id is None
            and row.parent_container_id is None
        ):
            return OwnedSearchGroup(kind="session", native_id=row.native_id, matched_records=1)
        if (
            row.entity_definition_short_name == "message"
            and row.parent_record_type == "session"
            and row.parent_native_id
            and row.parent_container_id is None
            and row.container_id == row.parent_native_id
        ):
            return OwnedSearchGroup(
                kind="session", native_id=row.parent_native_id, matched_records=1
            )
        return None

    @staticmethod
    def _group_hits(ranked: list[OwnedSearchHit]) -> list[OwnedSearchHit]:
        """Group final eligible matches in rank order, preserving each original."""
        output: list[OwnedSearchHit] = []
        positions: dict[tuple[UUID, str, str, str], int] = {}
        for hit in ranked:
            group = hit.group
            if group is None:
                output.append(hit)
                continue
            key = (hit.sync_id, hit.provider, group.kind, group.native_id)
            if key not in positions:
                positions[key] = len(output)
                output.append(
                    hit.model_copy(
                        update={
                            "group": group.model_copy(
                                update={"matched_records": 1, "additional_matches": ()}
                            )
                        }
                    )
                )
                continue
            index = positions[key]
            representative = output[index]
            previous = representative.group
            assert previous is not None
            additional = previous.additional_matches
            if len(additional) < 3:
                exact = OwnedSearchMatch.model_validate(
                    hit.model_dump(exclude={"group"}, round_trip=True)
                )
                additional = (
                    *additional,
                    exact.model_copy(update={"excerpts": exact.excerpts[:1]}),
                )
            output[index] = representative.model_copy(
                update={
                    "group": previous.model_copy(
                        update={
                            "matched_records": previous.matched_records + 1,
                            "additional_matches": additional,
                        }
                    )
                }
            )
        return output

    async def _rank(
        self,
        query: str,
        candidates: list[UUID],
        hits: dict[UUID, OwnedSearchHit],
        matched_text: dict[UUID, str],
    ) -> tuple[list[UUID], OwnedRanking]:
        """One model call over a bounded mixed shortlist; failures keep retrieval order."""
        shortlist = candidates[:200]
        ranking = OwnedRanking(
            candidates_considered=len(candidates),
            shortlisted_candidates=len(shortlist),
            shortlist_truncated=len(candidates) > len(shortlist),
        )
        if not shortlist:
            return [], ranking.model_copy(update={"fallback_reason": None})
        if self._reranker is None or self._tokenizer is None:
            return shortlist, ranking
        documents, truncated = [], 0
        for key in shortlist:
            title = hits[key].title[:256]
            text = f"{title}\n\n{matched_text[key]}"
            document = self._bounded_document(text)
            documents.append(document)
            truncated += int(document != text or title != hits[key].title)
        ranking = ranking.model_copy(update={"input_truncated_documents": truncated})
        try:
            async with asyncio.timeout(10):
                results = await self._reranker.rerank(query, documents, top_n=len(documents))
        except TimeoutError:
            return shortlist, ranking.model_copy(update={"fallback_reason": "timeout"})
        except Exception:
            # Never log provider errors: SDK messages can contain submitted text.
            return shortlist, ranking.model_copy(update={"fallback_reason": "provider_error"})
        try:
            invalid = (
                not isinstance(results, list)
                or len(results) != len(shortlist)
                or any(
                    not isinstance(result, RerankerResult)
                    or type(result.index) is not int
                    or not 0 <= result.index < len(shortlist)
                    or type(result.relevance_score) not in (int, float)
                    or not math.isfinite(result.relevance_score)
                    for result in results
                )
                or {result.index for result in results} != set(range(len(shortlist)))
            )
        except (TypeError, ValueError, OverflowError):
            invalid = True
        if invalid:
            return shortlist, ranking.model_copy(update={"fallback_reason": "invalid_output"})
        ordered = sorted(results, key=lambda result: (-result.relevance_score, result.index))
        return [shortlist[result.index] for result in ordered], ranking.model_copy(
            update={
                "method": "shared_rerank",
                "fallback_reason": None,
                "candidates_reranked": len(shortlist),
            }
        )

    def _bounded_document(self, text: str) -> str:
        """Bound local tokenizer counts; provider tokenization and billing may differ."""
        if self._tokenizer.count_tokens(text) <= 2048:
            return text
        accepted, rejected = 0, len(text)
        while rejected - accepted > 1:
            midpoint = (accepted + rejected) // 2
            if self._tokenizer.count_tokens(text[:midpoint]) <= 2048:
                accepted = midpoint
            else:
                rejected = midpoint
        # Token count need not be monotonic in characters. The accepted prefix
        # was measured explicitly, so this remains a safe bound, not a max-length claim.
        return text[:accepted]

    @staticmethod
    def _filtered(request: OwnedSearchRequest) -> bool:
        return bool(
            request.record_types
            or request.native_types
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
        if request.native_types:
            conditions.append(
                FilterCondition(
                    field="airweave_system_metadata.native_type",
                    operator="in",
                    value=list(request.native_types),
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
                        source_is_readable(
                            SourceConnection.organization_id, SourceConnection.sync_id
                        ),
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
    ) -> tuple[
        dict[UUID, OwnedSearchHit],
        dict[UUID, tuple[float, ProjectionLocator]],
        dict[UUID, str],
        int,
        int,
    ]:
        locators = [value for result in results.results if (value := self._locator(result))]
        if not locators:
            return {}, {}, {}, len(results.results), 0
        hits, scores, matched_text, exclusions, postfiltered = {}, {}, {}, 0, 0
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
                Entity.parent_record_type,
                Entity.parent_native_id,
                Entity.parent_container_id,
                Entity.observed_at,
                Entity.source_created_at,
                Entity.source_updated_at,
                Entity.completeness,
                case(
                    (
                        Entity.sync_id.in_(
                            [sync for sync in sync_ids if scopes[sync].short_name == "almanac"]
                        ),
                        Entity.source_payload["version"],
                    ),
                    else_=None,
                ).label("native_version"),
                case(
                    (
                        Entity.sync_id.in_(
                            [sync for sync in sync_ids if scopes[sync].short_name == "almanac"]
                        ),
                        case(
                            (
                                Entity.entity_definition_short_name == "knowledge",
                                Entity.source_payload["original"]["type"].astext,
                            ),
                            else_=Entity.entity_definition_short_name,
                        ),
                    ),
                    else_=None,
                ).label("native_type"),
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
        try:
            records = [_EnrichmentRecord.model_validate(row) for row in rows.mappings()]
        except ValidationError:
            raise HTTPException(
                409,
                {
                    "code": "reindex_required",
                    "message": (
                        "Selected native snapshots need a fresh native export and re-projection"
                    ),
                },
            ) from None
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
                    native_version=row.native_version,
                    email_thread_id=(
                        thread_id
                        if scopes[row.sync_id].short_name == "gmail"
                        and row.entity_definition_short_name == "message"
                        and (thread_id := row.email_thread_id) is not None
                        and re.fullmatch(r"[A-Za-z0-9_-]{1,512}", thread_id)
                        else None
                    ),
                )
                hits[row.id].group = self._conversation(row, hits[row.id])
                scores[row.id] = (1 / (60 + rank), locator)
                matched_text[row.id] = result.textual_representation
            # Dynamic summaries are keyword fragments, not semantic explanations.
            # No-mark and older-schema responses keep the existing chunk fallback.
            snippet = result.query_snippet
            if (
                request.mode.value != "semantic"
                and snippet
                and "<hi>" in snippet
                and "</hi>" in snippet
            ):
                # Remove only Vespa's presentation delimiters. All other markup is
                # ordinary untrusted text, never parsed/rendered as HTML here.
                excerpt = (
                    snippet.replace("<hi>", "")
                    .replace("</hi>", "")
                    .replace("<sep />", " … ")
                    .strip()
                    or result.textual_representation
                )[:2000]
            else:
                excerpt = result.textual_representation[:2000]
            current = hits[row.id]
            if excerpt and excerpt not in current.excerpts and len(current.excerpts) < 3:
                hits[row.id] = current.model_copy(update={"excerpts": (*current.excerpts, excerpt)})
        return hits, scores, matched_text, exclusions, postfiltered

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
        expected_syncs: dict[UUID, UUID] | None = None,
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
                        source_is_readable(
                            SourceConnection.organization_id, SourceConnection.sync_id
                        ),
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
                    true()
                    if expected_syncs is None
                    else tuple_(Entity.id, Entity.sync_id).in_(list(expected_syncs.items())),
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
        if request.native_types and row.native_type not in request.native_types:
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
