# Owned search execution and performance evidence

Owned search retrieves indexed originals. It never falls back to provider search.
Each request resolves authorized source connections, uses the unchanged 200-candidate
window per collection, checks exact publications before content leaves retrieval,
and rechecks source authentication and publications before returning results.

Publication eligibility includes record/revision, pipeline version, generation,
non-retirement, parent visibility and indexed-part coverage. Batched tuple checks
share the visibility expression; an indexed body never authorizes an unsupported
attachment. Retained text reads use the same publication predicate.

Query embeddings can be prepared once and reused across collections **within one
owned-search request**. Preparation belongs to the creating executor instance and
pins the primary query, ordered variations and retrieval strategy. A different
executor or changed query/strategy is rejected. Filters, ACL resolution, vector
queries and publication checks still run separately for each collection. Keyword
search never performs dense inference. There is no shared result, authorization,
query or embedding cache and no concurrent use of a database session.

## Topology and limits

Almanac's server-owned `CaptureTarget` chooses organization and collection for each
owner/provider/project. Provisioning does not require an organization or collection
per provider. Sources may share a collection, or use several collections in the
same organization. Organization is an authorization boundary; collection is an
index/deployment boundary. A provisioned source cannot silently change collection.

The September 30 local pilot uses four isolated organizations, each with one
collection. Request-local embedding reuse does **not** reduce that pilot's four
embedding calls. The multi-collection SQL fixture qualifies reuse and preserves the
race fence; it is not evidence of deployed production topology or a new speedup
for the pilot. No corpus ownership or operator binding was remapped for this work.

## Recorded local evidence

Before/after batched publication checks, an actual-router Slack hybrid query for
`meeting` retained 200 candidates and fell from 2222 ms to 569 ms in two local
instrumented samples. Enrichment fell from 625 to 25 ms. Embedding and Vespa latency
varied between samples, so these are diagnostic samples, not a latency guarantee.

A later full browser benchmark had one HTTP 503 whose body and duration were not
retained. The evaluation harness suppresses general logs to avoid private-content
logging, so the failing stage cannot be reconstructed. Both containers remained
running without OOM/restart. No cause-specific retry or timeout increase is justified
by that evidence. Safe status, exception type and elapsed-stage diagnostics are
being qualified separately in the Almanac adapter.

The current request-owned database session also holds its connection across
embedding/index waits after the first SQL query. Separating read phases is a
follow-up requiring explicit auth/session ownership and retained final fences;
it is not included in embedding reuse and is not a proven cause of the 503.


## Repeated actual API qualification after embedding reuse

Six serial product requests each returned HTTP 200 and 25 results. Their wall times
were 1976, 836, 1408, 1202, 1210 and 3013 ms. Each fanned out to four organizations;
[24 sanitized stage samples](search-stage-samples.json) preserve measured evidence.
The slowest organization request per product request was:

| Sample | Airweave total | Dense | Vespa adapter | Visibility | Enrichment | All SQL |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1176 | 410 | 452 | 35 | 230 | 273 |
| 2 | 703 | 163 | 274 | 54 | 176 | 212 |
| 3 | 1334 | 74 | 189 | 883 | 170 | 1031 |
| 4 | 1094 | 176 | 416 | 122 | 331 | 381 |
| 5 | 1019 | 175 | 378 | 70 | 346 | 389 |
| 6 | 2815 | 131 | 2609 | 23 | 19 | 36 |

All durations are milliseconds, measured as nested wall intervals; SQL overlaps
other stages and these columns must not be summed. No errors were recorded.
The sixth sample's delay was in the Vespa adapter await, which includes thread
queueing, HTTP, native engine work and event-loop scheduling. It is **not** evidence
of a PostgreSQL plan switch. Sample 3 independently had an 831 ms SQL fingerprint
interval. Neither measurement reconstructs the previous unlogged 503/13-second tail.

## Proposed next diagnostics and session ownership (not implemented)

The current Vespa adapter invokes the synchronous SDK through `asyncio.to_thread`.
Measure worker start/end alongside the adapter await, and retain only numeric native
response timing fields if available. This separates thread queue delay from SDK wall
time without logging YQL, embeddings, results or identities. Do not change query
ranking, candidate counts, retries or database plans before attributing the tail.

Database phase separation needs an explicit owner. The search route and its auth
dependency currently share FastAPI's cached `get_db` session. API context contains
Pydantic snapshots; Auth0 resolution nevertheless flushes a last-active update into
that session without committing it. Closing an arbitrary borrowed session inside
the executor would be an unsafe general contract.

A bounded redesign would finish this route's authentication session explicitly,
then use existing session-factory read contexts: scopes/version to a typed detached
snapshot; embedding preparation without SQL; each collection's network retrieval
before its session performs visibility/enrichment SQL; finally fresh scopes,
coverage and exact publication checks in one read phase. Generic executor behavior
and other routes stay unchanged. Test auth side-effect semantics and revocation
between phases, including a constrained pool, before claiming reduced occupancy.
No extra service, table, result cache or concurrent `AsyncSession` use is proposed.

The installed PyVespa `Vespa.query` creates a fresh `VespaSync` HTTP session per
call, then retains the full decoded response in `VespaQueryResponse.json`. This is
a source-level finding, not proof that connection setup causes the observed tail.
Vespa's [result-format documentation](https://docs.vespa.ai/en/reference/querying/default-result-format.html)
provides `querytime`, `summaryfetchtime` and `searchtime` in seconds when
`presentation.timing=true` is requested. A temporary diagnostic can copy the query
body and add only that presentation flag, retaining those three numeric values
alongside worker-entry/exit times. No query trace, response body or result content
needs to be logged. Pooling changes should wait for that decomposition.


## Worker and native Vespa decomposition

A second instrumented candidate ran immutable commit
`e37d018de786d3b9446b31a100a8dcd1c4ae9009` on loopback port 18084.
Authentication qualification returned 401 without a key and 200 for an authorized
retained-record read. Almanac then switched its private origin to this candidate;
all six product requests returned 200 with 25 records and identical result-identity
digests. The original 18081 process was stopped only after this success. The candidate
continues serving the same isolated corpus; no provider fetch, schema migration or
operator identity remapping occurred.

Product times were 2439, 922, 1333, 565, 660 and 1171 ms.
[24 sanitized worker samples](search-worker-samples.json) record native Vespa timing
and worker boundaries. The critical organization request per product request was:

| Sample | Airweave total | Dense | Worker queue | SDK worker | Resume delay | Native Vespa search | All SQL |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1702 | 703 | 0.1 | 900 | 0.2 | 509 | 47 |
| 2 | 818 | 161 | 3.3 | 358 | 0.2 | 96 | 226 |
| 3 | 1252 | 78 | 1.2 | 196 | 0.4 | 69 | 946 |
| 4 | 487 | 94 | 0.7 | 181 | 0.4 | 84 | 178 |
| 5 | 535 | 128 | 0.7 | 359 | 0.9 | 172 | 20 |
| 6 | 900 | 248 | 0.2 | 321 | 0.3 | 122 | 286 |

Milliseconds; nested intervals again must not be summed. On these critical requests,
thread queueing and event-loop resumption are small. Native Vespa processing explains
part of SDK wall time; the remainder includes transport/session setup and JSON decode,
which were not measured separately. Sample 3 again contains a SQL delay. This does not
justify blaming thread starvation, choosing a database plan, or changing HTTP pooling
without a targeted test. Six successful samples neither establish a latency percentile
nor reconstruct the earlier unlogged 503 and 13-second response.

Instrumentation lives only in the private launcher, preserving general log suppression.
Its copied query body adds `presentation.timing` only; result selection, ranking,
authorization and the 200-candidate bound remain unchanged.

## Narrow enrichment projection

Search enrichment now selects only the scalar identity/publication/date/completeness
fields used by a result card, plus the potential Gmail thread ID. A frozen typed row
replaces the live `Entity` ORM object. Native payloads and blob manifests are no longer
transferred or decoded into the search process merely to build cards. PostgreSQL may
still read the JSON value to extract the thread ID; this is not a claim of eliminating
all database JSON work or a measured latency improvement.

The thread expression returns text only when the native JSON value is a string.
Numbers, objects, missing keys and JSON null remain unavailable. Existing provider,
record-kind and allowed-ID checks still apply. Exact publication predicates, indexed
part coverage, exclusions and the final fresh authorization check are unchanged.

The phase-session proposal remains separate. Installed FastAPI 0.115.14 does not
provide an early function-scoped yield dependency. A dedicated non-yield owned-search
auth dependency could resolve the same `ContextResolver` inside an owned read context,
close that context, and return the detached `ApiContext`. Generic `get_context` remains
unchanged. The search service could then receive the existing session factory and
own its read contexts, carrying only typed source snapshots between them. Preserve
the current read route's Auth0 last-active rollback semantics explicitly; do not add
an implicit commit or close a caller's borrowed transaction. Qualify a one-connection
pool while embeddings/Vespa are paused, plus revocation and record changes between
phases, before adopting this separate proposal.

Projection qualification: 20 owned-search/visibility PostgreSQL tests passed against
an isolated archive of `a539e40979871f5ba23428b802845b5f1329f6c9` with only the
search implementation and its test file overlaid. Existing card checks now assert
identity, dates, completeness and excerpt parity and inspect actual DBAPI result
columns to reject transfer of full payloads/blob manifests. Thread-ID cases cover
missing/null/numeric/object/invalid strings and non-Gmail records. Existing stale,
foreign-source, unsupported-part and later-collection mutation fences also passed.
This is scoped correctness/resource-boundary qualification, not a measured speedup
or a claim that the candidate runtime has been updated with this change.
