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

### Importing assessor labels

Assessor rubrics must match those meanings before becoming scorer judgments.
For example, a private rubric may use 1 for "tangential, not useful". Export that
label as **0**, not 1; preserve its original label and the explicit mapping in
the assessment evidence. Partial answers graded 2 and direct answers graded 3
can retain their grades. A topical match that does not help answer the query is
not marginally useful merely because it shares words with the query.

Judge the displayed snippet and the underlying original separately. An unhelpful
snippet does not prove that the original lacks an answer. Read retained content
when needed; otherwise leave document relevance unknown and omit its qrel rather
than inventing a zero. Record the inspected part/revision and assessor provenance.
Report the judged fraction of each result window alongside pooled metrics. Recall
against a judged pool is not recall against every connected account.

Changing labels creates a new dataset fingerprint. Reuse saved rankings for an
explicit offline re-evaluation bound to that new dataset; never overwrite the
original experiment or repeat paid retrieval merely to change its labels.

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

## Grouped conversation results

Judge the unit the user actually sees. A conversation card can be relevant even
when its displayed message is not the answer. Keep these two experiments separate:

- **Card ranking:** stable conversation identity for a session/thread card, and
  original record identity for an ungrouped result. Judge the conversation as a
  whole. Use a separately fingerprinted dataset tagged `unit:card`.
- **Displayed-original ranking:** representative original followed by the explicitly
  returned additional matches, preserving card order and within-card order. Judge
  those exact original identities in a separate dataset tagged `unit:displayed_original`.
  Do not add unreturned members merely because they belong to a returned group.
  This is a ranking of inspectable originals, not the number of visible cards.

Both use the existing evaluator; no second scoring implementation is needed.
A card hit does not prove passage recall, and `matched_records` is not a list of
answers or evidence that every member is relevant. Keep query filters, corpus
revisions, model settings and assessment provenance fixed for both experiments.
The one-session trial in the product worklog is diagnostic, not a judged benchmark:
it found the session in every mode, but the expected passage only in keyword and
hybrid mode. More diverse sessions and independently judged queries are required.

`evaluation.owned_retrieval.delivered_result` exports these two units from the
existing typed `OwnedSearchResponse`. Supply a frozen tuple of `CorpusRecord`
locators with destination record/sync/revision, preserved provider identity, and
a stable original source ID (account or attested owner/dataset). The adapter
checks each returned original against the census and hashes source plus native
identity for rebuild-stable evaluation IDs. Conversation IDs additionally bind
group kind and native ID; accounts with the same native thread ID stay separate.
Attest optional `ConversationIdentity` from canonical parent identity or a
validated retained email thread before retrieval. A returned group or additional
member contradicting that frozen membership is rejected. Use `CorpusRecord.card_id`
for card judgments, independent of which member becomes the representative.
Use `unit="card"` for primary UI/API ranking and `unit="displayed_original"`
for the explicitly flattened diagnostic. Returned match counts never invent
hidden original members. An incomplete response retains its ranking and is
marked partial. This helper requires the backend environment; it does not query
the service or verify publication by itself.

`evaluation.retrieval.pool_unjudged(dataset, runs)` returns each query's sorted
union of delivered IDs without existing judgments. It checks the exact dataset
and corpus binding for every run. Assess that private pool before comparing
metrics; membership in the pool is neither a positive nor a negative label.
If labels change, deliberately regenerate the dataset fingerprint and rebind
the preserved observations; never rerun queries selectively to improve scores.

The delivery adapter's synthetic tests use backend fixtures:

```sh
PYTHONPATH=backend:. backend/.venv/bin/python -m pytest \
  backend/tests/unit/search/test_owned_evaluation.py -q
```

## HTTP replay of a retained corpus

The replay command runs one mode against an **already qualified, retained** corpus.
It does not copy records, publish, fetch providers or rebuild an index. The earlier
mixed-corpus trial was cleaned; its census cannot be replayed against missing records.
Prepare and qualify a retained fixture separately before using this command.

```sh
PYTHONPATH=backend:. backend/.venv/bin/python -m evaluation.replay_cli \
  --url http://127.0.0.1:18086/api/v1/ \
  --dataset /private/path/card-dataset.json \
  --census /private/path/qualified-corpus.json \
  --output /private/path/new-hybrid-run \
  --system 'reviewed-commit+embedding+retrieval+reranker+configuration' \
  --mode hybrid --unit card --limit 20
```

Authentication uses `AIRWEAVE_API_KEY` and the census organization via
`X-Organization-ID`. The native publisher's URL rules also apply here: external
HTTPS, loopback HTTP, no URL credentials, no redirects or environment proxies.
No server database credentials or dotenv files are needed; `--help` needs no key.
Each HTTP operation has a 10-second connect and 60-second read/write/pool timeout.
Requests run once, sequentially, without retries or a whole-run deadline.

Inputs are strict `Dataset` and `evaluation.replay.FrozenCorpus` JSON, each at most
32 MiB. The census contains `corpus_id`, `organization_id`, explicit `sync_ids`
(at most 20) and `records`. Each `FrozenRecord` extends `CorpusRecord` with the
qualified `capture_hash`, `indexed_pipeline_version`, exact `extraction` parts,
and canonical `parent` identity. Preserve source revisions/native versions in the
qualified corpus specification identified by `corpus_id`; destination revisions
are separately checked here. Freeze conversation membership before retrieval.
The census must enumerate **every active, content-available record in each selected
sync**, including intentionally excluded records; it need not describe other
sources in the provider account. Empty selected syncs are allowed. Every query
must carry exactly the matching `unit:card` or `unit:displayed_original` tag and
every judgment must refer to an identity in this corpus.

Before and after querying, the runner paginates the existing record-list endpoint
and checks exact eligible membership, native/parent identities, destination
revisions, capture hashes and current index/pipeline revisions. Missing, extra,
withdrawn or changed records reject the comparison. Every explicitly returned
representative and additional match must also have exactly the qualified
extraction coverage. No per-record fetches are added. Availability is API
metadata/permission evidence, **not physical blob integrity**. These live census
traversals are non-atomic; changes that occur and revert between observations can
escape detection. The API does not attest the supplied model/system descriptor.

The fresh output directory is mode 0700; files are 0600 and never overwritten.
It retains validated inputs, paginated census responses, numbered search request
and raw response files, every query outcome, mode/fallback metadata and HTTP
timings with unknown cache state. Response bytes are saved before decoding.
HTTP failures, timeouts and malformed responses remain failed query outcomes;
corpus or delivered-proof contradictions abort scoring while preserving evidence.
Only successful before/after checks produce `run.json` and an unjudged candidate
pool. Query failures still stay in the metric denominator. No labels are invented.

Use `--report` to invoke the existing offline scorer; install its pinned
`ir-measures==0.4.3` dependency in that environment first. Alternatively, score
the retained `dataset.json` and `run.json` with the standalone command above.
Exit codes: 0 replay retained (inspect failed/partial counts); 2 invalid inputs or
output; 3 failed census HTTP verification; 4 corpus/proof mismatch; 5 unavailable
offline scoring dependency; 130 interrupted; 1 unexpected failure.

`make test-search` runs both the delivery adapter and replay boundary tests.
These synthetic HTTP tests establish runner behavior, not live ranking quality
or qualification of any existing local corpus.
