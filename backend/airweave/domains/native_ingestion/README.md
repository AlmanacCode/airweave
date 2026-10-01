# Native snapshot admission

This internal service retains versioned Almanac snapshots in the existing
canonical store. It does not provision sources, authenticate callers, enumerate
Almanac data, publish a search projection, or provide a public import endpoint.
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
