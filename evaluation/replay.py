"""Replay frozen queries over HTTP against an already qualified, retained corpus.

No capture, indexing, SQL, provider calls or relevance judgments happen here.
"""

import hashlib
import json
import os
from pathlib import Path
from time import perf_counter
from typing import Literal
from uuid import UUID

import httpx
from pydantic import BaseModel, Field, model_validator

from airweave.domains.entities.canonical.extraction_models import ExtractionCoverage
from airweave.domains.entities.canonical.models import RecordPage
from airweave.domains.entities.canonical.requests import RecordIdentity
from airweave.domains.search.owned_models import (
    OwnedRanking,
    OwnedSearchRequest,
    OwnedSearchResponse,
)
from airweave.domains.search.retrieval_strategy import RetrievalStrategy
from evaluation.owned_retrieval import CorpusRecord, delivered_result
from evaluation.retrieval import (
    Dataset,
    Identifier,
    Result,
    Run,
    Timing,
    Value,
    pool_unjudged,
)


class FrozenRecord(CorpusRecord):
    """Replay-only proof of one qualified locator's capture and extraction revision."""

    capture_hash: str = Field(pattern=r"^[a-f0-9]{64}$")
    indexed_pipeline_version: int = Field(ge=1)
    extraction: ExtractionCoverage
    parent: RecordIdentity | None = None


class FrozenCorpus(Value):
    """Complete eligible record set in each explicitly selected source sync."""

    corpus_id: Identifier
    organization_id: UUID
    sync_ids: tuple[UUID, ...] = Field(min_length=1, max_length=20)
    records: tuple[FrozenRecord, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_scope(self):
        """Require unique locators and stable source scopes without silently filtering."""
        if len(set(self.sync_ids)) != len(self.sync_ids):
            raise ValueError("Duplicate source scope")
        if len({r.record_id for r in self.records}) != len(self.records):
            raise ValueError("Duplicate corpus locator")
        if len({r.evaluation_id for r in self.records}) != len(self.records):
            raise ValueError("Duplicate native source identity")
        scopes: dict[UUID, tuple[str, str]] = {}
        for record in self.records:
            if record.sync_id not in self.sync_ids:
                raise ValueError("Corpus record is outside selected sources")
            identity = (record.source_id, record.provider)
            if scopes.setdefault(record.sync_id, identity) != identity:
                raise ValueError("Source scope has conflicting identity")
        if len(set(scopes.values())) != len(scopes):
            raise ValueError("Native source identity spans multiple destination syncs")
        return self


class ReplayConfiguration(Value):
    """Explicit request settings; system/model description is operator supplied."""

    system: str = Field(min_length=1)
    mode: RetrievalStrategy
    unit: Literal["card", "displayed_original"] = "card"
    limit: int = Field(default=20, ge=1, le=200)


class Observation(Value):
    """One outcome, including failures, with raw HTTP bytes retained separately."""

    query_id: Identifier
    request_id: Identifier
    status: Literal["success", "partial", "error", "timeout"]
    http_status: int | None = None
    ranking: OwnedRanking | None = None
    candidate_window_full: bool | None = None
    engine_partial: bool | None = None
    retrieval_incomplete: bool | None = None


class ReplayEvidence(Value):
    """Evidence and practical limits; invalid corpus checks never produce a scored run."""

    outcome: Literal["verified", "invalid"]
    configuration: ReplayConfiguration
    corpus_sha256: str
    dataset_sha256: str
    census_before: bool
    census_after: bool
    observations: tuple[Observation, ...]
    limitations: tuple[str, ...] = (
        "Before/after census traversals are live and non-atomic; transient changes can escape detection.",
        "Record availability is metadata/permission evidence, not physical blob integrity.",
        "Extraction is checked for delivered matches; other records retain qualification-time proof.",
        "System/model description is supplied by the operator, not independently verified by the API.",
        "Timings cover one HTTP request each, with unknown cache state and no retries.",
    )


class CorpusMismatch(ValueError):
    """The supplied qualified corpus no longer describes the destination."""


def save_bytes(path: Path, data: bytes) -> None:
    """Never overwrite evidence or rely on a permissive caller umask."""
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as output:
        output.write(data)


def save_model(path: Path, model: BaseModel) -> None:
    """Retain validated inputs and results privately."""
    save_bytes(path, (model.model_dump_json(indent=2) + "\n").encode())


async def verify_census(
    client: httpx.AsyncClient, corpus: FrozenCorpus, output: Path
) -> None:
    """Compare paginated active, content-available records with the complete frozen set."""
    expected = {record.record_id: record for record in corpus.records}
    eligible: set[UUID] = set()
    for scope_index, sync_id in enumerate(corpus.sync_ids):
        seen: set[UUID] = set()
        cursors: set[str] = set()
        cursor = None
        previous_id: UUID | None = None
        page_index = 0
        while True:
            params: dict[str, str | int] = {"state": "active", "limit": 500}
            if cursor is not None:
                params["cursor"] = cursor
            response = await client.get(f"sync/{sync_id}/records", params=params)
            save_bytes(
                output / f"scope-{scope_index:02d}-page-{page_index:05d}.json",
                response.content,
            )
            response.raise_for_status()
            page = RecordPage.model_validate_json(response.content)
            for live in page.records:
                if (
                    live.sync_id != sync_id
                    or live.id in seen
                    or (previous_id is not None and live.id <= previous_id)
                ):
                    raise CorpusMismatch(
                        "Census traversal has invalid scope or ordering"
                    )
                seen.add(live.id)
                previous_id = live.id
                if live.deleted_at is not None:
                    raise CorpusMismatch("Active traversal returned a deleted record")
                if live.content_access != "available":
                    continue
                frozen = expected.get(live.id)
                if frozen is None or (
                    frozen.sync_id != live.sync_id
                    or frozen.identity != live.identity
                    or frozen.parent != live.parent
                    or frozen.revision != live.revision
                    or frozen.capture_hash != live.capture_hash
                    or live.indexed_revision != frozen.revision
                    or live.indexed_pipeline_version != frozen.indexed_pipeline_version
                ):
                    raise CorpusMismatch("Destination differs from qualified corpus")
                eligible.add(live.id)
            if page.has_more != (page.next_cursor is not None):
                raise CorpusMismatch("Census continuation is contradictory")
            if not page.has_more:
                break
            if not page.records or page.next_cursor in cursors:
                raise CorpusMismatch("Census continuation did not advance")
            cursor = page.next_cursor
            cursors.add(cursor)
            page_index += 1
    if eligible != set(expected):
        raise CorpusMismatch("Qualified corpus records are missing or unavailable")


def validate_extraction(response: OwnedSearchResponse, corpus: FrozenCorpus) -> None:
    """Check the exact extraction proof of every explicitly delivered original."""
    expected = {record.record_id: record for record in corpus.records}
    for hit in response.items:
        additional = () if hit.group is None else hit.group.additional_matches
        for match in (hit, *additional):
            record = expected.get(match.record_id)
            if record is None or match.extraction != record.extraction:
                raise CorpusMismatch(
                    "Delivered extraction differs from qualified proof"
                )


async def replay(
    client: httpx.AsyncClient,
    dataset: Dataset,
    corpus: FrozenCorpus,
    configuration: ReplayConfiguration,
    output: Path,
) -> Run:
    """One sequential trial, with raw failures retained and no corpus rebuilds."""
    if dataset.corpus_id != corpus.corpus_id:
        raise CorpusMismatch("Dataset and corpus identities differ")
    ids = {
        r.card_id if configuration.unit == "card" else r.evaluation_id
        for r in corpus.records
    }
    for query in dataset.queries:
        units = {tag for tag in query.tags if tag.startswith("unit:")}
        if units != {f"unit:{configuration.unit}"} or any(
            judgment.record_id not in ids for judgment in query.judgments
        ):
            raise CorpusMismatch(
                "Dataset labels do not describe the selected delivery unit"
            )
    requests = tuple(
        OwnedSearchRequest(
            query=q.text,
            sync_ids=corpus.sync_ids,
            mode=configuration.mode,
            limit=configuration.limit,
        )
        for q in dataset.queries
    )
    output.mkdir(mode=0o700, parents=False, exist_ok=False)
    save_model(output / "dataset.json", dataset)
    save_model(output / "corpus.json", corpus)
    before = after = False
    observations: list[Observation] = []
    results: list[Result] = []
    timings: list[Timing] = []
    try:
        (output / "before").mkdir(mode=0o700)
        await verify_census(client, corpus, output / "before")
        before = True
        for index, (query, request) in enumerate(
            zip(dataset.queries, requests, strict=True)
        ):
            request_id = f"request-{index:05d}"
            save_model(output / f"{request_id}-input.json", request)
            started = perf_counter()
            status_code = None
            parsed = None
            abort: CorpusMismatch | None = None
            try:
                response = await client.post(
                    "sync/search", json=request.model_dump(mode="json")
                )
                duration = (perf_counter() - started) * 1000
                status_code = response.status_code
                save_bytes(output / f"{request_id}-response.json", response.content)
                response.raise_for_status()
                parsed = OwnedSearchResponse.model_validate_json(response.content)
                if len(parsed.items) > request.limit:
                    raise ValueError("Delivered card count exceeds requested limit")
                validate_extraction(parsed, corpus)
                try:
                    result = delivered_result(
                        query.id, parsed, corpus.records, unit=configuration.unit
                    )
                except (ValueError, KeyError) as exc:
                    raise CorpusMismatch(
                        "Delivered identity differs from qualified corpus"
                    ) from exc
            except httpx.TimeoutException:
                duration = (perf_counter() - started) * 1000
                result = Result(query_id=query.id, status="timeout")
            except httpx.HTTPError:
                duration = (perf_counter() - started) * 1000
                result = Result(query_id=query.id, status="error")
            except CorpusMismatch as exc:
                abort = exc
                result = Result(query_id=query.id, status="error")
            except ValueError:
                result = Result(query_id=query.id, status="error")
            results.append(result)
            timings.append(
                Timing(
                    request_id=request_id,
                    query_id=query.id,
                    phase="end_to_end",
                    condition="unknown",
                    status=result.status,
                    duration_ms=duration,
                )
            )
            observation = Observation(
                query_id=query.id,
                request_id=request_id,
                status=result.status,
                http_status=status_code,
                ranking=None if parsed is None else parsed.ranking,
                candidate_window_full=None
                if parsed is None
                else parsed.candidate_window_full,
                engine_partial=None if parsed is None else parsed.engine_partial,
                retrieval_incomplete=None
                if parsed is None
                else parsed.retrieval_incomplete,
            )
            observations.append(observation)
            save_model(output / f"{request_id}-outcome.json", observation)
            if abort is not None:
                raise abort
        (output / "after").mkdir(mode=0o700)
        await verify_census(client, corpus, output / "after")
        after = True
    finally:
        save_model(
            output / "evidence.json",
            ReplayEvidence(
                outcome="verified" if after else "invalid",
                configuration=configuration,
                corpus_sha256=hashlib.sha256(
                    corpus.model_dump_json().encode()
                ).hexdigest(),
                dataset_sha256=dataset.fingerprint(),
                census_before=before,
                census_after=after,
                observations=tuple(observations),
            ),
        )
    run = Run(
        system=configuration.system,
        corpus_id=corpus.corpus_id,
        dataset_sha256=dataset.fingerprint(),
        results=tuple(results),
        timings=tuple(timings),
    )
    save_model(output / "run.json", run)
    save_bytes(
        output / "unjudged.json",
        (json.dumps(pool_unjudged(dataset, (run,)), indent=2) + "\n").encode(),
    )
    return run
