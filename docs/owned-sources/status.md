# Retained source progress

`GET /sync/{sync_id}/status` is an authenticated tenant/source read over committed
canonical metadata. It never fetches provider data, calls models, or loads record
bodies. `capture` is the existing capture-coverage contract; an active cycle does
not certify that a worker is running or that the source inventory is complete.

Preparation partitions visible retained records into candidates and proven
current exclusions. `current_records + pending_records = candidate_records`;
failed rows are a subset of pending. Candidates include unprepared records whose
policy may later exclude them. Exclusion requires a current zero-part/zero-document
publication; revision, pipeline or collection changes invalidate it. There is no
universal completion percentage.

Extraction counts concern content, ignoring metadata parts: indexed content plus
missing content or OCR gaps is partial; no indexed content with omissions is
unavailable. Unknown coverage or metadata-only content is unknown. A searchable
filename does not mean the file's contents were extracted. Last observed is the
latest successful observation among visible originals, not first import or a
provider event timestamp.

The aggregate joins current revisions, pipeline versions, generation identity and
current authenticated collection. A materialized CTE holds narrow flags/timestamp
only, avoiding repeated generation/collection checks for each counter. Source
readability is checked before and after aggregation/capture coverage. Existing
ancestor visibility applies. No new progress state or schema is introduced.

Qualification: two disposable PostgreSQL tests exercise the actual HTTP route
with synthetic authentication. They cover partial/unavailable/unknown extraction,
exclusions, pipeline invalidation, an unfinished capture, owner/source withdrawal
and hidden descendants. Five-row EXPLAIN reduced collection-attestation loops from
40 to 4, with no temp writes; this is a query-shape check, not a scale benchmark.
Non-mixed status uses four SQL statements; mixed capture adds its existing
scope-summary queries. Consumers should request expanded details lazily, not poll
every account rapidly.
