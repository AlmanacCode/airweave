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

### Duplicate crowding

Distinct record IDs may contain redundant answers. A session trial returned19
identical messages in20 keyword slots; ordinary record-level relevance metrics
can reward every copy. Optionally label redundant originals per query:

```json
"duplicate_groups": [
  {"record_ids": ["session:message-a", "session:message-b"], "assessor": "human"}
]
```

Members must be distinct, disjoint across groups, and have explicit equal
relevance judgments. Judge redundancy for the query's intent and filters:
identical text at different dates need not answer a date-specific query equally.
Sharing a session, mailbox or channel alone does not establish redundancy.
Agent-generated labels must use `assessor: "agent"`, even when computed from
exact original text. Keep authoritative message identities intact.

The per-query `duplicates` report counts labeled records, extra slots after the
first member of each group, and unassigned records in the delivered top20.
Unassigned records are not claimed unique. Without duplicate labels the report
is null, not zero. Counts are descriptive; a failed/empty request is not proof of
good diversity. Existing nDCG/MRR/precision/recall scores use the unchanged ranking;
the evaluator never silently collapses results to improve scores.

Duplicate labels participate in the dataset fingerprint and assessor provenance.
This schema revision also changes fingerprints for previously parsed datasets;
regenerate the fingerprint and bind runs to the validated dataset deliberately.
Do not copy a hash from a different set of labels to make a comparison pass.

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
The focused synthetic tests verify evaluator behavior, not search quality.

Reference: [ir-measures interfaces and formats](https://ir-measur.es/en/latest/getting-started.html).

## Bounded native publishing helper

`evaluation.native_import.publish_native` publishes already-staged `NativeSnapshot`
values through the authenticated native HTTP API; it neither discovers nor fetches
source content. Run with the backend environment and `PYTHONPATH=backend:.` from the
repository root. The injected `httpx.AsyncClient` base URL must target the API root
with a trailing slash. Retain the exact input and request key after a transport error:
reinvoking resumes the durable cursor; changed input with that key conflicts. The
returned terminal summary certifies bounded capture, not search publication.

Tests live in the backend native-ingestion suite and require disposable PostgreSQL.
The standalone retrieval-metrics test environment does not import this helper.

### Operator command

From the repository root, using the backend Python environment:

```sh
PYTHONPATH=backend:. backend/.venv/bin/python -m evaluation.native_import_cli \
  --url http://127.0.0.1:18086/api/v1/ \
  --input staged-knowledge.json --owner OWNER_ID --dataset knowledge \
  --collection COLLECTION_ID --request-key stable-import-key
```

Supply `AIRWEAVE_API_KEY` through the process environment (for example a secret
manager); there is no key argument or dotenv loading. No server database/settings
credentials are needed. Use the backend API key authorized to attest this owner.
The file is a UTF-8 JSON array of `NativeSnapshot` values, at most **32 MiB**.
Use `--dataset sessions` for staged session roots and their original messages.
This command publishes the supplied bounded set, not an account-wide backfill;
it does not infer deletions from records omitted from the file.

External URLs must use HTTPS; HTTP is allowed only for loopback IPs or localhost.
URL credentials, queries and fragments are rejected. Redirects and environment
proxies are disabled. Each HTTP operation has a 10-second connect timeout and
60-second read/write/pool timeout, not a whole-import deadline. No automatic
retry occurs. Keep the exact staged file and request key on failure; repeat the
same command to recover durable progress. Do not change the file under that key.

Success writes one JSON capture summary to stdout, including
`capture_complete: true`, `coverage: "bounded"`, and `indexing: "not_verified"`.
Search publication must be verified separately. Errors use stderr without raw
payloads, keys, file paths or destination URLs. Exit codes: 0 completed capture;
2 invalid configuration/input; 3 unknown network outcome; 4 destination rejection
or inconsistent import progress (including cross-record preflight rejection);
130 interrupted; 1 unexpected failure. Failed commands may have committed earlier
pages: a nonzero exit is not evidence of rollback.
