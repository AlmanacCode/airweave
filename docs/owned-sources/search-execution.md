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
