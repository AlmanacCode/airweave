# /// script
# requires-python = ">=3.12"
# dependencies = ["ir-measures==0.4.3", "pydantic==2.11.9"]
# ///
"""Offline relevance and latency reports, independent of production credentials."""

import argparse
import hashlib
import json
import math
import os
from collections import defaultdict
from pathlib import Path
from statistics import mean
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

Identifier = Annotated[str, Field(min_length=1, pattern=r"^\S+$")]
Milliseconds = Annotated[float, Field(ge=0, allow_inf_nan=False)]


class Value(BaseModel):
    """Strict immutable file boundary."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class Judgment(Value):
    """Record-level relevance; zero is judged irrelevant, absence is unjudged."""

    record_id: Identifier
    relevance: int = Field(ge=0, le=3, strict=True)
    assessor: Literal["human", "agent", "synthetic"]


class DuplicateGroup(Value):
    """Distinct originals judged redundant for this query's intent and filters."""

    record_ids: tuple[Identifier, ...] = Field(min_length=2)
    assessor: Literal["human", "agent", "synthetic"]


class Query(Value):
    """A fixed intent with explicit known-answer or no-answer expectation."""

    id: Identifier
    text: str = Field(min_length=1)
    tags: tuple[Identifier, ...] = ()
    expectation: Literal["relevant_records", "no_answer"]
    expectation_assessor: Literal["human", "agent", "synthetic"]
    judgments: tuple[Judgment, ...]
    duplicate_groups: tuple[DuplicateGroup, ...] = ()

    @model_validator(mode="after")
    def validate_judgments(self):
        """Never silently treat a query with no positive judgments as an answerable query."""
        ids = [j.record_id for j in self.judgments]
        if len(ids) != len(set(ids)):
            raise ValueError("Duplicate record judgment")
        positive = any(j.relevance > 0 for j in self.judgments)
        if positive != (self.expectation == "relevant_records"):
            raise ValueError("Query expectation conflicts with positive judgments")
        if not self.text.strip() or len(self.tags) != len(set(self.tags)):
            raise ValueError("Query text must be nonblank and tags unique")
        grades = {j.record_id: j.relevance for j in self.judgments}
        assigned = set()
        for group in self.duplicate_groups:
            for record in group.record_ids:
                if record in assigned:
                    raise ValueError(
                        "Duplicate-group members must be disjoint and unique"
                    )
                if record not in grades:
                    raise ValueError(
                        "Duplicate-group members require relevance judgments"
                    )
                assigned.add(record)
            if len({grades[record] for record in group.record_ids}) != 1:
                raise ValueError("Duplicate-group members must have equal relevance")
        return self


class Dataset(Value):
    """Versioned corpus and labels; real content belongs in a private operator directory."""

    corpus_id: Identifier
    version: Identifier
    queries: tuple[Query, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def unique_queries(self):
        """Prevent duplicate intents from silently weighting the aggregate."""
        if len({q.id for q in self.queries}) != len(self.queries):
            raise ValueError("Duplicate query ID")
        return self

    def fingerprint(self) -> str:
        """Bind a run to the exact validated corpus/query/judgment specification."""
        encoded = json.dumps(
            self.model_dump(mode="json"), sort_keys=True, ensure_ascii=False
        )
        return hashlib.sha256(encoded.encode()).hexdigest()


class Result(Value):
    """One delivered ranking per query, before any evaluator-side filtering."""

    query_id: Identifier
    status: Literal["success", "partial", "error", "timeout"]
    record_ids: tuple[Identifier, ...] = ()

    @model_validator(mode="after")
    def validate_ranking(self):
        """Require record-level deduplication and unambiguous failed outcomes."""
        if len(self.record_ids) != len(set(self.record_ids)):
            raise ValueError(
                "Duplicate ranked record: evaluate the record-level response"
            )
        if self.status in {"error", "timeout"} and self.record_ids:
            raise ValueError("Failed requests cannot contain a delivered ranking")
        return self


class Timing(Value):
    """One observed phase, with explicit request identity and cache condition."""

    request_id: Identifier
    query_id: Identifier
    phase: Literal["end_to_end", "retrieval", "reranking", "read"]
    condition: Literal["cold", "warm", "unknown"]
    status: Literal["success", "partial", "error", "timeout"]
    duration_ms: Milliseconds


class Run(Value):
    """A named system configuration and its observed outputs; no model-score assumptions."""

    system: str = Field(min_length=1)
    dataset_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    corpus_id: Identifier
    results: tuple[Result, ...]
    timings: tuple[Timing, ...] = ()

    @model_validator(mode="after")
    def unique_observations(self):
        """Reject accidentally duplicated queries or phase measurements."""
        if len({r.query_id for r in self.results}) != len(self.results):
            raise ValueError("Duplicate query result")
        keys = [(t.request_id, t.phase) for t in self.timings]
        if len(keys) != len(set(keys)):
            raise ValueError("Duplicate request phase timing")
        requests: dict[str, tuple[str, str]] = {}
        for timing in self.timings:
            identity = (timing.query_id, timing.condition)
            if requests.setdefault(timing.request_id, identity) != identity:
                raise ValueError("Request timing identity changed between phases")
        return self


class DuplicateReport(Value):
    """Known redundant slots; unassigned records are not declared unique."""

    cutoff: Literal[20] = 20
    grouped_records: int
    duplicate_slots: int
    unassigned_records: int


class QueryReport(Value):
    """Scores retain failures and label provenance, without copying private query text."""

    query_id: str
    tags: tuple[str, ...]
    status: str
    expectation: str
    assessors: tuple[str, ...]
    scores: dict[str, float]
    returned: int
    judged_in_top_10: int
    unjudged_in_top_10: int
    correctly_empty: bool | None
    duplicates: DuplicateReport | None


class LatencyReport(Value):
    """Percentiles by phase/condition/outcome; failures never disappear into fast successes."""

    phase: str
    condition: str
    status: str
    samples: int
    p50_ms: float
    p95_ms: float
    max_ms: float


class Report(Value):
    """Descriptive evidence, not a production-readiness verdict."""

    system: str
    corpus_id: str
    dataset_sha256: str
    answerable_queries: int
    aggregate: dict[str, float]
    queries: tuple[QueryReport, ...]
    latency: tuple[LatencyReport, ...]
    warnings: tuple[str, ...]


def validate_run(dataset: Dataset, run: Run) -> None:
    """Require every observation to belong to the exact frozen evaluation dataset."""
    if (
        run.corpus_id != dataset.corpus_id
        or run.dataset_sha256 != dataset.fingerprint()
    ):
        raise ValueError("Run does not match the exact dataset and corpus")
    query_ids = {q.id for q in dataset.queries}
    if {r.query_id for r in run.results} != query_ids:
        raise ValueError(
            "Every dataset query needs exactly one result, including failures"
        )
    if any(t.query_id not in query_ids for t in run.timings):
        raise ValueError("Timing references an unknown query")


def pool_unjudged(
    dataset: Dataset, runs: tuple[Run, ...]
) -> dict[str, tuple[str, ...]]:
    """Union delivered candidates needing assessment; never invent negative labels."""
    for run in runs:
        validate_run(dataset, run)
    return {
        query.id: tuple(
            sorted(
                {
                    record
                    for run in runs
                    for result in run.results
                    if result.query_id == query.id
                    for record in result.record_ids
                }
                - {judgment.record_id for judgment in query.judgments}
            )
        )
        for query in dataset.queries
    }


def summarize(dataset: Dataset, run: Run) -> Report:
    """Use established IR measures, preserving API rank order with unique synthetic scores."""
    import ir_measures

    validate_run(dataset, run)
    measures = [
        ir_measures.nDCG @ 10,
        ir_measures.RR @ 10,
        ir_measures.P @ 5,
        ir_measures.R @ 20,
    ]
    names = dict(
        zip(measures, ("nDCG@10", "MRR@10", "Precision@5", "Recall@20"), strict=True)
    )
    reports = []
    results = {r.query_id: r for r in run.results}
    for query in dataset.queries:
        result = results[query.id]
        scores = {}
        if query.expectation == "relevant_records":
            qrels = [
                ir_measures.Qrel(query.id, j.record_id, j.relevance)
                for j in query.judgments
            ]
            ranked = [
                ir_measures.ScoredDoc(query.id, record, float(-rank))
                for rank, record in enumerate(result.record_ids)
            ]
            scores = {
                names[m]: float(v)
                for m, v in ir_measures.calc_aggregate(measures, qrels, ranked).items()
            }
        judged = {j.record_id for j in query.judgments}
        top = result.record_ids[:10]
        membership = {
            record: index
            for index, group in enumerate(query.duplicate_groups)
            for record in group.record_ids
        }
        duplicate_window = result.record_ids[:20]
        assigned = [membership[r] for r in duplicate_window if r in membership]
        reports.append(
            QueryReport(
                query_id=query.id,
                tags=query.tags,
                status=result.status,
                expectation=query.expectation,
                assessors=tuple(
                    sorted(
                        {query.expectation_assessor}
                        | {j.assessor for j in query.judgments}
                        | {g.assessor for g in query.duplicate_groups}
                    )
                ),
                scores=scores,
                returned=len(result.record_ids),
                judged_in_top_10=sum(record in judged for record in top),
                unjudged_in_top_10=sum(record not in judged for record in top),
                correctly_empty=(result.status == "success" and not result.record_ids)
                if query.expectation == "no_answer"
                else None,
                duplicates=DuplicateReport(
                    grouped_records=len(assigned),
                    duplicate_slots=len(assigned) - len(set(assigned)),
                    unassigned_records=len(duplicate_window) - len(assigned),
                )
                if query.duplicate_groups
                else None,
            )
        )
    answerable = [q for q in reports if q.expectation == "relevant_records"]
    groups: dict[tuple[str, str, str], list[float]] = defaultdict(list)
    for timing in run.timings:
        groups[(timing.phase, timing.condition, timing.status)].append(
            timing.duration_ms
        )
    latencies = []
    for (phase, condition, status), samples in sorted(groups.items()):
        samples.sort()
        # Nearest-rank percentiles are defined even for one observation; counts remain visible.
        latencies.append(
            LatencyReport(
                phase=phase,
                condition=condition,
                status=status,
                samples=len(samples),
                p50_ms=samples[math.ceil(0.5 * len(samples)) - 1],
                p95_ms=samples[math.ceil(0.95 * len(samples)) - 1],
                max_ms=samples[-1],
            )
        )
    warnings = [
        "Recall is against judged relevant records, not all connected-account content.",
        "Unjudged records receive no relevance credit; pool and judge them before conclusions.",
        "Latency uses nearest-rank percentiles; small samples do not establish tail reliability.",
        "Duplicate counts use explicit query-specific labels; unassigned records may also repeat.",
        "Relevance metrics score the delivered ranking unchanged, including distinct redundant originals.",
    ]
    if any(assessor != "human" for q in reports for assessor in q.assessors):
        warnings.append(
            "Dataset includes agent or synthetic judgments, not exclusively human labels."
        )
    if not run.timings:
        warnings.append("No latency measurements supplied.")
    return Report(
        system=run.system,
        corpus_id=run.corpus_id,
        dataset_sha256=run.dataset_sha256,
        answerable_queries=len(answerable),
        aggregate={
            name: mean(q.scores[name] for q in answerable) for name in names.values()
        }
        if answerable
        else {},
        queries=tuple(reports),
        latency=tuple(latencies),
        warnings=tuple(warnings),
    )


def main() -> None:
    """Print a dataset fingerprint or save a report without overwriting existing artifacts."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dataset", type=Path)
    parser.add_argument("--run", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    dataset = Dataset.model_validate_json(args.dataset.read_bytes())
    if args.run is None:
        print(dataset.fingerprint())
        return
    if args.output is None:
        parser.error("--output is required with --run")
    report = summarize(dataset, Run.model_validate_json(args.run.read_bytes()))
    # Private reports can contain record IDs; do not print them or use world-readable defaults.
    fd = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as output:
        output.write(report.model_dump_json(indent=2) + "\n")


if __name__ == "__main__":
    main()
