# Native snapshot admission

This service retains versioned Almanac snapshots in the existing canonical
store. Backend-key HTTP endpoints own source provisioning and the import
lifecycle; they do not enumerate Almanac data. Almanac remains the authority
for content and access.

The source API binds `short_name=almanac` and
`config_fields={owner_id, dataset}`. Datasets are `knowledge` and `sessions`.
The service checks that binding against the authenticated organization, stored
writer fence and every snapshot. It does not require a fabricated Composio
account. Internal commands are not authorization credentials; HTTP callers
cannot supply writer fences.

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
cannot reopen locally withdrawn records; use the explicit access observation
operation below. Parent access gates remain canonical-owned.

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

HTTP authentication, receipts, provisioning and completion/cancellation are
implemented below, including explicit record access renewal. Authoritative
publisher completeness and automatic source revalidation remain pending. Native jobs cannot be cancelled through the generic
provider route: the guard checks the actual job's sync identity.

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
is an index capability, not an authentication grant. The source API binds the
owner; a trusted authoritative publisher is still required for real ingestion.

## Trusted source and import identity

Backend-key `PUT /native/sources` ensures the server-derived organization/owner/
dataset identity in one collection; `GET /native/sources/{source_id}` reads it.
Concurrent requests serialize through the existing Sync primary key. Collection
changes and unexplained partial state conflict; an exact ensure never reopens
withdrawn availability. This setup is not proof of imported or indexed content.

`NativeImportStore` owns the start/read transaction behind the import HTTP API. A bounded request key derives an existing SyncJob primary
key. Exact retries recover the original state, even after cancellation or source
withdrawal; they never activate another writer. A changed body conflicts. New
imports require an available source and cannot replace another active writer.
Job creation, canonical writer activation and cycle creation commit together.
A new key after a terminal job may replace its incomplete cycle only when the
retained cycle is associated with that preceding native import. Retained records
remain until the subsequent declared scans reconcile them.

Publisher-declared bounded snapshots use discovery-only completion policies;
complete dataset snapshots use exhaustive policies. This declaration does not
itself certify completeness. The HTTP lifecycle verifies final scopes and
receipts; the publisher must establish stable export boundaries. Explicit record
access renewal is available under an active import as described below; source-wide
credential renewal and automatic source revalidation remain separate.

## Import HTTP and page receipts

Backend-key PUT/GET `/native/sources/{source_id}/imports/{request_key}` expose
start and recovery. Keys are opaque nonsecret identifiers: application logs and
analytics redact native paths/parameters, but infrastructure access logs may
still record request URLs. Validation and unexpected exceptions do not log
native request contents through the application middleware.

`NativePageStore.commit` is the transaction behind PUT `/pages`.
It resolves stored writer authority, admits
the page, advances its cursor and retains one bounded receipt in the existing
scan continuation. No second journal or original-content copy is created. Exact
retries of the current page return its acknowledgement. Changed bodies, old CAS
versions, changed reconciliation state and cancelled writers conflict rather
than reapply writes. Cursors are bounded to32KiB, leaving receipt space inside
the existing64KiB continuation limit. Capture acknowledgement is distinct from
index publication or whole-import completion.

## Scope HTTP workflow

Under an active import, PUT `/scopes` begins or resumes an exact scope;
POST `/scopes/read` reads its composite identity from the body without mutation;
PUT `/pages` commits a page; POST `/scopes/reconcile` advances bounded
reconciliation after the final page. Responses use `bounded`/`complete` coverage
and an independent phase, hide writer credentials and internal receipt digests,
and retain the caller cursor plus last acknowledgement for recovery. Parent
epochs are resolved under the server's writer lock. A new import can replace a
prior-cycle scan using its locked version; same-cycle restarts require explicit CAS.

Bounded native imports use observed membership: only parents seen in their
current completed inventory can admit child scopes or require transcripts for
completion. Unseen retained sessions remain available but are not fresh members.
Default provider membership and checkpoint digests are unchanged. Bounded scope
completion never becomes evidence of exhaustive account discovery. Scope reads
currently require an active import; durable terminal summaries and historical
scope availability are still part of import finalization work.

### Native import terminal outcomes

The backend-only import API now supports `POST .../imports/{request_key}/complete`
and `/cancel`. Completion validates all scopes required by the declared capture
policy under the writer lock, then stores the cycle completion, job status, and a
bounded terminal summary in one transaction. A bounded import completing does not
mean the entire account was captured. Summary `indexing: not_verified` explicitly
separates capture from downstream search publication.

An identical completed retry returns its retained summary even after another import
starts. Cancelling an already-terminal import preserves its outcome and never
cancels the newer writer. A cancelled job immediately fails writer-fence checks.
Terminal scope reads remain available only while the reusable scan row still has
that import's cycle ID; once reused, the API reports unavailable/superseded instead
of showing another import's progress. The import's terminal summary remains durable.
Request keys are opaque, nonsecret identifiers: application diagnostics redact them,
but deployment access logs require their own policy.


## Explicit record access observations

Backend-only GET/POST
`/native/sources/{source_id}/imports/{request_key}/records/{record_id}/access`
read or change current access. GET returns identity, retained revision, current parent
visibility epoch, availability and removal reason, never original content. It works for a terminal import, but is
a current-state read, not historical state at that import. Availability includes
source authentication, active sync, record deletion and ancestor visibility.

POST requires an active server-held writer and `expected_revision` from the retained
record. Renewal of a message also requires `expected_parent_epoch` from the access
read. Parent withdrawal/restoration invalidates old child evidence even if the child
revision did not change. Roots use a null parent epoch. `action: withdraw` takes a
confirmed `access_revoked` or `scope_removed` reason. It preserves original native payload/version and blob references while
journaling withdrawal through canonical capture. A search miss or incomplete scan
is not valid evidence. An ambiguous source404 requires provider/source interpretation.

`action: renew` supplies a freshly read, owner-attested active NativeSnapshot for
the exact record. Native owner, identity, version ordering and parent-version checks
still apply. Equal native version requires identical content; it can now explicitly
renew access at the expected local revision. Ordinary imports/page retries cannot.
Newer ordinary child updates also cannot restore inherited unavailability; they require the same explicit renewal path.
Restore parents first, then freshly attest children; restoration advances parent
visibility so previous child attestations remain hidden until individually renewed.
Source-wide authentication is not changed by this operation.

Read retained revision *before* obtaining fresh source evidence. If another mutation
wins meanwhile, CAS fails. After a lost response, GET current state; do not blindly
change the expected revision and resend old evidence. This operation uses ordinary
CAS recovery rather than a second idempotency journal. An already-active unchanged
renewal may be a no-op. Access observations do not mark records seen in inventory;
normal page capture still owns scan progress/completeness.

No new table, migration, scheduler or source credential exists here. NativeImports
owns the transaction; NativeAccessStore composes native admission and shared
canonical revision/journal/visibility machinery. The trusted publisher remains
responsible for obtaining actual source evidence and scheduling revalidation.
