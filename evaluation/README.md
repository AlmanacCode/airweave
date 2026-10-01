# Retrieval evaluation

Offline evaluation of the **delivered record ranking**, with separately observed
latency samples. This does not run sync, query a provider, or change an index.
The production service does not import this package. `uv` installs the pinned
evaluation dependencies separately; no additional production infrastructure.

```sh
uv run evaluation/retrieval.py /private/path/dataset.json
# Prints the dataset fingerprint. Record it in the corresponding run file.
uv run evaluation/retrieval.py /private/path/dataset.json \
  --run /private/path/run.json --output /private/path/report.json

uv run --no-project --with ir-measures==0.4.3 --with pydantic==2.11.9 \
  --with pytest python -m pytest evaluation/tests -q --confcutdir=evaluation
```

Reports are created with mode 0600 and refuse to overwrite files. Keep real
queries, judgments, record IDs and response captures out of Git. Reports omit
query text but can still reveal sensitive labels through query IDs and tags.
Validation errors can echo input: do not publish raw error logs from private runs.

## Dataset

```json
{
  "corpus_id": "frozen-corpus-manifest-sha256",
  "version": "judgments-v1",
  "queries": [{
    "id": "meeting-hindi",
    "text": "परियोजना बैठक कब है?",
    "tags": ["language:hi", "intent:known-item", "split:holdout"],
    "expectation": "relevant_records",
    "expectation_assessor": "human",
    "judgments": [
      {"record_id": "calendar:meeting-1", "relevance": 3, "assessor": "human"},
      {"record_id": "gmail:promotion-2", "relevance": 0, "assessor": "human"}
    ]
  }]
}
```

Use stable record identities, not chunk IDs. The corpus manifest should pin the
record revisions, permissions/scopes, extraction and index versions. Its content
is the operator's responsibility: this offline scorer verifies the supplied
corpus ID and exact dataset fingerprint, **not the live index's contents**.
Include applied account/date/type filters in the frozen experiment specification;
do not compare runs using different filters as ranking improvements.

Grades: 0 irrelevant, 1 marginally useful, 2 useful, 3 directly satisfies intent.
Binary precision, recall and reciprocal rank count grades >= 1 as relevant.
`ir-measures` supplies nDCG with its default linear graded gain, RR, P and R;
we do not maintain custom implementations of that scoring math.

Every known-answer query needs a positive judgment. `no_answer` queries explicitly
assert no relevant answer in this corpus and have no positive judgments. Their
empty-success outcome is reported separately, not averaged into nDCG/recall.
An error returning nothing is not a correct abstention. No-answer labels need
evidence; merely failing to find an item does not establish absence.

## Run

```json
{
  "system": "commit+embedding+retrieval+reranker+configuration",
  "dataset_sha256": "<fingerprint printed by the first command>",
  "corpus_id": "frozen-corpus-manifest-sha256",
  "results": [{
    "query_id": "meeting-hindi",
    "status": "success",
    "record_ids": ["gmail:promotion-2", "calendar:meeting-1"]
  }],
  "timings": [{
    "request_id": "trial-001",
    "query_id": "meeting-hindi",
    "phase": "end_to_end",
    "condition": "unknown",
    "status": "success",
    "duration_ms": 812.4
  }]
}
```

The array order is authoritative. Unique descending synthetic scores preserve it
when calling the metrics library; scores from different retrieval engines are
not compared. Duplicate record IDs are rejected rather than silently repaired.
Include every query exactly once, recording errors/timeouts with empty results.
Partial responses keep their returned ranking and an explicit `partial` status.
For repeated ranking trials, save separate runs instead of selecting the best.

Latency may have repeated samples per query, with unique request IDs. Supported
phases: end_to_end, retrieval, reranking, read. Measure with a monotonic clock;
record the actual condition as cold/warm/unknown. First request does not prove
cold caches. Timing groups preserve phase, cache condition and outcome, including
errors/timeouts. No timings are inferred from relevance results. End-to-end HTTP
latency does not establish browser paint latency. Percentiles use nearest rank;
sample count and maximum accompany p50/p95. Do not draw tail conclusions from a
handful of measurements or mix concurrency/load conditions in one run.

## What constitutes improvement

Compare the same frozen corpus, scopes, labels and held-out queries. Pool records
from candidate systems and judge previously unjudged records before concluding
one wins. Reports expose per-query top-10 judged/unjudged counts and assessor
provenance. Missing judgments receive no relevance credit; they are not relabeled
irrelevant. Recall means recall of **judged relevant records**, not account-wide
recall. Failed answerable queries contribute zero instead of disappearing from
the denominator. Inspect per-query and tagged regressions alongside aggregates.

Real-corpus collection, request timing instrumentation, reviewed judgments,
snippet evaluation and corpus-freezing automation are still separate work.
The four focused synthetic tests verify evaluator behavior, not search quality.

Reference: [ir-measures interfaces and formats](https://ir-measur.es/en/latest/getting-started.html).
