"""Small adversarial datasets exercise evaluation honesty, not production relevance."""

import pytest
from pydantic import ValidationError

from evaluation.retrieval import (
    Dataset,
    Judgment,
    Query,
    Result,
    Run,
    Timing,
    summarize,
)


def corpus():
    return Dataset(
        corpus_id="fixture-v1",
        version="labels-v1",
        queries=(
            Query(
                id="known",
                text="परियोजना बैठक",
                expectation="relevant_records",
                expectation_assessor="synthetic",
                judgments=(
                    Judgment(record_id="meeting", relevance=3, assessor="synthetic"),
                    Judgment(record_id="spam", relevance=0, assessor="synthetic"),
                ),
            ),
            Query(
                id="failed",
                text="a known document",
                expectation="relevant_records",
                expectation_assessor="synthetic",
                judgments=(
                    Judgment(record_id="document", relevance=1, assessor="synthetic"),
                ),
            ),
            Query(
                id="absent",
                text="nonexistent meeting",
                expectation="no_answer",
                expectation_assessor="synthetic",
                judgments=(),
            ),
        ),
    )


def run(dataset, **changes):
    values = dict(
        system="synthetic",
        corpus_id=dataset.corpus_id,
        dataset_sha256=dataset.fingerprint(),
        results=(
            Result(
                query_id="known",
                status="success",
                record_ids=("spam", "meeting", "unknown"),
            ),
            Result(query_id="failed", status="timeout"),
            Result(query_id="absent", status="error"),
        ),
    )
    return Run(**(values | changes))


def test_failed_query_counts_and_unjudged_is_not_labeled_irrelevant():
    dataset = corpus()
    report = summarize(dataset, run(dataset))
    assert report.answerable_queries == 2
    assert report.aggregate["MRR@10"] == 0.25
    assert report.aggregate["Precision@5"] == 0.1
    assert report.aggregate["Recall@20"] == 0.5
    assert 0 < report.aggregate["nDCG@10"] < 0.5
    assert report.queries[0].judged_in_top_10 == 2
    assert report.queries[0].unjudged_in_top_10 == 1
    assert report.queries[2].correctly_empty is False
    assert any("synthetic" in warning for warning in report.warnings)


def test_latency_conditions_failures_and_tail_are_separate():
    dataset = corpus()
    timings = tuple(
        Timing(
            request_id=f"warm-{i}",
            query_id="known",
            phase="end_to_end",
            condition="warm",
            status="success",
            duration_ms=i,
        )
        for i in range(1, 21)
    ) + (
        Timing(
            request_id="cold",
            query_id="known",
            phase="end_to_end",
            condition="cold",
            status="success",
            duration_ms=500,
        ),
        Timing(
            request_id="timeout",
            query_id="failed",
            phase="end_to_end",
            condition="warm",
            status="timeout",
            duration_ms=10000,
        ),
    )
    report = summarize(dataset, run(dataset, timings=timings))
    warm = next(
        row
        for row in report.latency
        if row.condition == "warm" and row.status == "success"
    )
    assert (warm.samples, warm.p50_ms, warm.p95_ms, warm.max_ms) == (20, 10, 19, 20)
    assert len(report.latency) == 3


def test_missing_query_or_changed_labels_cannot_improve_score_silently():
    dataset = corpus()
    original = run(dataset)
    with pytest.raises(ValueError, match="Every dataset query"):
        summarize(
            dataset, original.model_copy(update={"results": original.results[:1]})
        )
    with pytest.raises(ValueError, match="exact dataset"):
        summarize(dataset.model_copy(update={"version": "new-labels"}), original)
    with pytest.raises(ValidationError, match="Duplicate ranked record"):
        Result(query_id="known", status="success", record_ids=("meeting", "meeting"))
    with pytest.raises(ValidationError):
        Timing(
            request_id="x",
            query_id="known",
            phase="end_to_end",
            condition="warm",
            status="success",
            duration_ms=float("nan"),
        )


def test_no_answer_success_is_separate_from_answerable_metrics():
    dataset = Dataset(corpus_id="empty", version="v1", queries=(corpus().queries[2],))
    observation = Run(
        system="empty",
        corpus_id=dataset.corpus_id,
        dataset_sha256=dataset.fingerprint(),
        results=(Result(query_id="absent", status="success"),),
    )
    report = summarize(dataset, observation)
    assert report.aggregate == {}
    assert report.queries[0].correctly_empty is True
