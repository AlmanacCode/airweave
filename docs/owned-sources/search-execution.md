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

Collection requests overlap with a maximum of four active collection tasks per
search. Each task owns its SQL session; the shared query embedding is immutable.
The whole candidate union is still revalidated before reranking and before return.
An unavailable collection fails the request instead of silently returning a partial
success. A seven-collection SQL/HTTP rendezvous test proves overlap, the four-task
bound, distinct sessions and preserved HTTP failure behavior. This is concurrency
qualification, not a measured production latency improvement.

## Topology and limits

`POST /sync/search/candidates` reuses the same request filters, retrieval, canonical
publication gates and disconnect cancellation, but never invokes the reranker.
It returns individual record candidates before conversation grouping, respecting
the requested limit (maximum 200). Each includes the exact projection locator,
retrieval score, native snapshot version when present, presentation excerpts, and
matched text capped at 32,000 characters with explicit truncation. This bounds the
internal HTTP transfer; the existing reranker's token limit remains separate.

This endpoint is for Almanac's server to validate current native DB ownership and
versions before model disclosure. `authority=canonical_snapshot` deliberately
does not certify current wiki/session authority. Product validation and shared
reranking of the eligible mixed shortlist are still required integration work.
No candidate cache, second index lookup or durable search session is introduced.

After validating current native authority, the product backend can submit its
approved shortlist to `POST /sync/search/rank`. It returns ordered record IDs and
ranking diagnostics. It does not embed the query or retrieve the index again;
canonical publications and source bindings are checked before model disclosure
and again after the model returns. A revision changed before ranking never reaches
the reranker; a revision changed during ranking is absent from the response.
The product retains the cards, owns grouping/result limits, and must revalidate
current native authority before returning those cards to its caller.

Both two-stage endpoints require the existing backend API-key gate and reject
browser/system sessions. This fork does **not** distinguish different privileged
organization API keys: deployment must keep these keys server-side, with no user
access to the fork's key-administration surface. API-key authentication is not
proof that product-native authority validation happened; that remains the trusted
Almanac backend's responsibility. No new identity or credential store was added.

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

The runtime measured here still used a request-owned database session that held
its connection across embedding/index waits after the first SQL query. The later
phase-owned implementation below addresses that ownership separately; this was
not established as the cause of the 503.


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

## Diagnostic design

The private instrumentation below measures worker start/end alongside the Vespa
adapter await and retains only numeric native response timing fields. It does not
log YQL, embeddings, results or identities or change ranking and candidate limits.

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

Projection qualification: 20 owned-search/visibility PostgreSQL tests passed against
an isolated archive of `a539e40979871f5ba23428b802845b5f1329f6c9` with only the
search implementation and its test file overlaid. Existing card checks now assert
identity, dates, completeness and excerpt parity and inspect actual DBAPI result
columns to reject transfer of full payloads/blob manifests. Thread-ID cases cover
missing/null/numeric/object/invalid strings and non-Gmail records. Existing stale,
foreign-source, unsupported-part and later-collection mutation fences also passed.
This is scoped correctness/resource-boundary qualification, not a measured speedup
or a claim that the candidate runtime has been updated with this change.


## Phase-owned search reads

Owned search now closes its own authentication and initial source-scope read
transactions before embedding preparation. Each collection gets a fresh session:
it performs no SQL until indexed retrieval returns, then validates publication and
builds cards. That session closes when its collection task finishes. A final fresh read
rechecks authorized sources, coverage and every exact publication. Only frozen typed
source snapshots cross phases. Source collection UUID and pipeline version are part
of both the scope comparison and final SQL fence, alongside source/account identity.
A same-name collection replacement or pipeline change cannot reuse the old routing.

The dedicated non-yield dependency calls the existing context resolver. API-key
checks, Auth0 authentication, rate limiting and caching are unchanged. Closing its
owned session preserves the existing read route's rollback of Auth0's flushed
last-active update; this change does not silently commit it. Generic auth dependencies
and other endpoints remain unchanged. No borrowed session is rolled back or closed.

The session factory is resolved dynamically from the configured database module.
Local evaluation must keep its isolated factory installed (or explicitly override
the new dependency); overriding only `get_db` is insufficient. Test fixtures override
both new dependencies. Runtime activation requires checking this boundary again.

Qualification uses real PostgreSQL with pool size one and no overflow: the sole
connection is available while sparse inference or Vespa is paused, after cancellation
there, and after failure/cancellation once enrichment has acquired the connection.
Actual API-key/Auth0 SQL authentication also releases that pool and leaves persisted
last-active unchanged. Scope revocation, collection replacement and pipeline changes
during retrieval reject the response; existing later-collection record mutation
checks still prevent returning an earlier stale hit. Search/embedding transports are
fakes in these tests. This is a pool-ownership/correctness proof, not a measured
latency gain or a claim that the local browser runtime has been activated.

## Deterministic retained Gmail inventory

`GET /sync/{sync_id}/mail/messages` is a canonical SQL query, separate from ranked
`/sync/search`. It accepts literal `query`, repeated exact `from_addresses` and
`to_addresses`, aware half-open `after`/`before`, `folder`, `unread`, `limit` (1–100)
and a signed `cursor`. Addresses are RFC-parsed and casefolded; OR applies within
each address facet, AND across facets. Dots and plus suffixes retain their meaning.
Dates use native Gmail `internalDate`, checked against canonical `source_created_at`.
The provider Date header is not a range authority.

The existing Entity row holds revision-bound, version-one parsed Gmail metadata.
Capture derives it in the same transaction as the original; migration 0014 uses
the frozen version-one parser to backfill bounded pages. Malformed, missing or
inconsistent facts remain unknown. Discovery selects metadata columns only, without
native payloads, body decoding, provider requests or per-hit blob reads.

The existing projection worker prepares one immutable, full converter body fact on
ProjectionGeneration before embeddings. It excludes generated headers and attachment
content. `documents=NULL` marks this prepared-body-only stage; the complete index
manifest seals once before remote feed. NULL can never publish an index generation.
Literal query matches parsed sender/To names and mailboxes, subject, and prepared body.
Each participant value is casefolded during capture/backfill; missing participant facts
remain metadata gaps. No transport headers or snippets participate. Literal body search selects the latest validated fact for the current canonical
revision and explicit current pipeline, regardless of embedding/feed success.
Failed attempts without a prepared body cannot hide earlier valid text. Existing GC
keeps the designated eligible body and retires superseded attempts. Retired rows currently
retain their SQL body bytes; clearing those bytes is a pending bounded GC follow-up. Sync's derived
`mail_text_sequence` advances with designation/retirement under the existing Sync
lock; no new worker, store or source authority exists.

Pages return lean message previews and explicit retained-only capture/indexing evidence.
Metadata gaps are source-wide because their filter membership is unknown; text-ready,
partial and unavailable counts cover the metadata-filtered candidate inventory.
Bodies never appear in discovery. Keyset order is native time plus canonical ID.
Signed cursors bind organization/source/normalized filters, canonical sequence and
pipeline; text queries also bind the prepared-text sequence. Changes require a
restart. Source withdrawal is checked again before returning each page.

| Native capability/resource | Product name | Access operation | Stored representation | Sync/change guarantee | Proof | Gap |
| --- | --- | --- | --- | --- | --- | --- |
| Gmail messages | Retained email inventory | `/mail/messages` | Original canonical payload plus typed revision-bound metadata | Capture sequence fences traversal; half-open native dates | Synthetic real SQL/HTTP enumeration of 257 matches over seven pages | Retained inventory is not proof of full mailbox capture |
| Gmail message body | Literal retained email search | Same endpoint with `query` | Immutable full converter body on existing projection generation | Revision/pipeline/source gates plus derived text sequence | Actual MIME/HTML builder remains searchable after forced embedding failure | Missing or partial originals are explicit text gaps; attachment content excluded |
| Gmail native thread | Retained email read | `/mail/threads/{thread_id}` | Original messages and revision-bound original blobs | Existing live traversal plus current source/record gates | Existing thread/blob HTTP tests | Native draft IDs and mutations remain separate; no complete-thread claim |

Local verification exercises synthetic SQL/HTTP, not a connected provider or deployed
service. The version-one decoder is intentionally frozen: incompatible rederivation
requires a new version and explicit migration rather than changing old backfill code.


## Retained Wispr meeting workflows

`GET /sync/{sync_id}/wispr/meetings` lists authorized retained meeting bodies by
native meeting start descending, then canonical ID descending. Default limit is five;
limits up to 100 and signed cursors allow complete traversal. Aware `after`/`before`
are half-open meeting-start bounds. The version-one preview validator derives
`Entity.meeting_started_at` during capture; migration 0015 backfills bounded pages.
It preserves canonical source-created/update semantics and original JSON. Missing,
malformed, conflicting native identity/start or preview facts produce an explicit
source-wide metadata gap, because their date membership is unknown.

SQL selects lean title/start/native identity/transcript-availability metadata before
LIMIT and never selects full notes, transcripts or the whole original payload.
`has_transcript=null` preserves unknown; false preserves explicit native absence.
Pages carry retained-only capture evidence and a capture-sequence fence, with source
and ancestor visibility checks and a final source/sequence recheck.

The existing Wispr range mapper now exposes stable `notes` and `transcript` native
text parts through the existing projection/text reader. It preserves native range
content and validates continuation framing. Explicit consistent transcript absence
omits that part; missing text with unknown availability fails extraction instead of
fabricating a complete empty transcript. Original exact JSON reads remain available.

**Reprojection gate:** the two native parts change Wispr projection shape. Existing
published generations remain immutable and generated-only until a deliberate new
pipeline version and retained-original reprojection. No existing corpus has been
reprojected by this implementation. Migration 0015 only rebuilds meeting-start facts.

| Native capability/resource | Product name | Access operation | Stored representation | Sync/change guarantee | Proof | Gap |
| --- | --- | --- | --- | --- | --- | --- |
| Wispr meetings | Latest retained meetings | `/wispr/meetings` | Existing originals plus validated native start on Entity | Capture-sequence-fenced newest-first traversal; current source/ancestor gates | Synthetic real SQL/HTTP 260-meeting traversal, malformed/missing/start-conflict gaps, half-open dates and revocation | Retained list is not proof of provider-wide capture |
| Wispr notes/transcript | Retained meeting text | Existing exact record/text endpoints with named `notes`/`transcript` parts | Full reassembled native ranges in existing text artifacts | Immutable revision/generation binding, explicit absent versus unknown transcript | Real mapper/projector/storage/content pagination preserves >40,000-character native fields | Existing generated-only generations require deliberate reprojection; raw editor JSON remains provider-omitted |
