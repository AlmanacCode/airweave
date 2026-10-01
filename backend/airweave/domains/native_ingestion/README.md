# Native snapshot admission

This internal service retains versioned Almanac snapshots in the existing
canonical store. It does not provision sources, authenticate callers, enumerate
Almanac data, or provide a public import endpoint.
Almanac remains the authority for content and access.

The future trusted publisher must bind a source connection with
`short_name=almanac` and `config_fields={owner_id, dataset}`. Datasets are
`knowledge` and `sessions`. The service checks that binding against the fenced
organization/sync and every snapshot. It does not accept a fabricated Composio
account. A future API must issue the fence after authenticating the publisher;
the internal command itself is not an authorization credential.

`NativeIngestionService` owns one existing `UnitOfWork`.
`NativeIngestionStore` composes `CanonicalRecordStore._fenced_sync` and
`_capture_locked` inside that transaction. Those existing internal methods are
the only locking/capture dependencies. No table, independent journal, or
alternate revision allocator is introduced.

Admission loads the batch's exact record identities and parent attestations in
one query under that writer lock. Canonical capture still owns its per-record
writes; batching admission does not bypass its revision or visibility checks.

Retained payloads explicitly identify their authority and snapshot provenance,
contain the unchanged original JSON, and retain the authoritative version even
for tombstones. Knowledge versions are scalar record revisions. Session and
message snapshots carry metadata and original-content revisions as a partial
order; crossed components are rejected. Replay epochs are not transcript
versions. Changed messages, including tombstones, must match their attested session
version, including a parent submitted in the same batch. A deleted parent permits
matching message tombstones but never message upserts. Submit parents before children.

An equal version is accepted only for an identical snapshot; retries do not
restore withdrawn visibility. Lower versions, conflicting equal versions and
malformed retained state fail closed. Local scope withdrawal already preserves
the canonical payload, so it also preserves native version state. Higher versions
cannot reopen locally withdrawn records; explicit renewal needs a separately
designed access-authority operation. Parent access gates remain canonical-owned.

This is record admission, not proof of a complete session transcript or complete
dataset. Pagination, final snapshot completion, deletion discovery, authoritative
export validation, and source renewal remain work for the publisher boundary.
An empty original JSON tombstone is valid but must carry an authoritative version.
The publisher must not invent a version from capture time or an observation count.

Tests use the existing disposable PostgreSQL fixture, including actual migration
schemas and transaction rollback. They do not contact a provider or production.

## Atomic snapshot pages

`IngestNativePage` composes native admission with the existing canonical scan
transaction. `CanonicalScanStore.page` accepts an internal `LockedPageCapture`
implementation; it is code-owned behavior, never a request-supplied callback.
The scan still owns scope validation, CAS, continuation and final-page state.
Native `admit_locked` returns typed changed observations and unchanged record IDs.
An unchanged record in a new sweep must pass current parent-version and active
visibility checks before its sighting is stamped. This changes neither its
canonical revision nor its payload and never restores withdrawn content.

Ordinary exact `ingest` retries still acknowledge retained data without claiming
fresh membership. Snapshot pages enumerate active upserts only; authoritative
tombstones continue through ordinary ingestion. This page operation is not a
changes-feed contract. Empty pages are valid; only `final=true` begins the existing
reconciliation phase, and source completion remains a separate cycle barrier.
Admission, capture, sightings and continuation either commit together or roll back.

HTTP authentication, request receipts, import provisioning, explicit renewal and
publisher completeness attestation remain unfinished. Cancellation must resolve
the job's actual bound source; a caller-supplied source ID is insufficient evidence
for authorizing a job cancellation.

## Native search projection

`projection.py` supplies offline inputs to the existing canonical projector.
It validates original IDs, revisions and timestamps against their snapshot,
then selects native text without replacing the retained original JSON:

- Knowledge: body, title, description and user notes. Path is a locator; renaming
  it keeps record identity. Other structured knowledge fields are retained but
  do not yet have a curated searchable representation.
- Sessions: title and description. Archived sessions remain included, matching
  current Almanac session search; archived knowledge is excluded.
- Messages: original string content or `type=text` blocks, with session/message
  IDs, ordinals and block anchors. Inactive legacy imports are excluded. API
  replay payloads, reasoning fields and arbitrary metadata are not search text.

Mixed messages can publish their text while retained nontext blocks report
`unsupported_format` and partial extraction coverage. Malformed text fails
explicitly. Retaining an image URL in original JSON does not capture image bytes
or make them downloadable; attachment acquisition is still separate work.

Native indexed sources use the same publication and access validation as provider
sources, without a provider registry entry or fabricated OAuth connection. This
is an index capability, not an authentication grant. The trusted publisher and
owner-binding API must still be implemented before exposing native ingestion.

## Trusted source and import identity

Backend-key `PUT /native/sources` ensures the server-derived organization/owner/
dataset identity in one collection; `GET /native/sources/{source_id}` reads it.
Concurrent requests serialize through the existing Sync primary key. Collection
changes and unexplained partial state conflict; an exact ensure never reopens
withdrawn availability. This setup is not proof of imported or indexed content.

`NativeImportStore` is the internal start/read transaction primitive, not yet an
HTTP publisher API. A bounded request key derives an existing SyncJob primary
key. Exact retries recover the original state, even after cancellation or source
withdrawal; they never activate another writer. A changed body conflicts. New
imports require an available source and cannot replace another active writer.
Job creation, canonical writer activation and cycle creation commit together.
A new key after a terminal job may replace its incomplete cycle only when the
retained cycle is associated with that preceding native import. Retained records
remain until the subsequent declared scans reconcile them.

Publisher-declared bounded snapshots use discovery-only completion policies;
complete dataset snapshots use exhaustive policies. This declaration does not
itself certify completeness: final scopes, stable export boundaries, receipts,
renewal and import completion/cancellation still need their HTTP lifecycle.
