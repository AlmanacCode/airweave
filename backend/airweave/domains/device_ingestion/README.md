# Device source admission

Backend-only acquisition boundary for Messages, Apple Notes and Contacts. The
Almanac gateway attests organization, owner, logical source/account, device and
explicit cloud-copy authorization. A request containing those IDs is not an
independent credential. Never distribute an engine backend API key to a desktop.
A logical account ID is a device-store enrollment identity; this service does not
infer or prove Apple/iCloud account identity from filenames, names or handles.

One `SourceConnection` and `Sync` bind immutable organization/owner/account/source
kind to one canonical store. Existing `config_fields` retain typed enrollment;
existing `SyncJob.sync_metadata` retains run intent and a server-only writer fence.
No new table, credentials connection, per-source mirror, queue or migration exists.
The three namespace UUIDs in `models.py` define persisted v1 identities and must
not be changed after enrollment.

Source ensure creates an inactive enrollment. Binding is an explicit backend
reauthorization: it rotates enrollment generation, cancels any previous writer and
selects one primary device/local-store generation. An exact retry at the desired
next generation with identical device/store intent returns the original result.
Revocation increments generation, stops the writer and sets source read authority
false, hiding retained originals without deleting them. Ordinary bookmark
restoration is never remote reauthorization. Explicit rebind restores source-wide
read authority, while previously withdrawn/deleted records remain withdrawn.
Fresh observed upserts, not enrollment alone, can restore those individual records.

Binding changes, page capture and revocation take the same existing `Sync` lock,
then source/job/scan locks in established order. A page already holding that lock
may commit before revoke; revoke then hides its retained data and rejects later
old-generation submissions. No operation promises to undo a previously committed
capture. Generation/fence verification occurs before receipt recovery as well as
before new capture.

## Backend HTTP contract

Routes are mounted at `/device/sources`; the deployed API supplies its usual v1
prefix. Authoritative strict DTOs live in `models.py`.

| Method/path | Input | Response |
| --- | --- | --- |
| PUT root | EnsureDeviceSource | DeviceSourceState |
| GET /{source_id} | owner_id query | DeviceSourceState |
| PUT /{source_id}/binding | BindDevice | DeviceSourceState |
| POST /{source_id}/revoke | RevokeDevice | DeviceSourceState |
| PUT /{source_id}/runs/{request_key} | DevicePrincipal | DeviceRunState |
| GET /{source_id}/runs/{request_key} | owner_id query | DeviceRunState |
| PUT /{source_id}/runs/{request_key}/pages | exact CommitDevicePage JSON bytes | DevicePageAck |
| POST /{source_id}/runs/{request_key}/complete | DevicePrincipal | DeviceRunState |

The gateway forwards page bytes unchanged. Native Int64 values must never pass
through JavaScript numeric JSON parsing. `DevicePageAck.sha256` is lowercase SHA256
of the exact incoming UTF-8 JSON bytes, including whitespace; no canonicalization,
model dump or JSON roundtrip participates in the retry digest. It echoes validated
binding ID (=source ID), enrollment generation and local-store generation, plus
the shared canonical acknowledgement. Same page ID with different bytes conflicts,
even if JSON values match. Client checkpoints advance only after this acknowledgement
has durably committed; preserve the exact request on lost/uncertain responses.

Limits are experimental admission caps: 500 observations, 32KiB cursor, 2MiB whole
page, request key 1...128 characters. The page route bounds its request stream before
JSON parsing. Foreign/missing owner or organization returns 404; stale publisher,
CAS or receipt conflict returns 409 with a stable code. Invalid page schema returns
422 without source contents; stream overflow returns 413. Application privacy
middleware applies existing native-content redaction to device paths. Infrastructure
access logs still require their own URL policy: request keys are opaque/nonsecret.

`original` is validated against the versioned source envelope in
`canonical/apple_payloads.py`, then retained unchanged. Native ID must agree with
that envelope. All source rows retain exact tagged Int64/binary values; validation
is not source-fidelity or Apple permission proof. Notes marked locally deleted
require explicit `scope_removed` withdrawal; locked notes require `access_revoked`.
Their bodies cannot be admitted as ordinary upserts. Sparse explicit deletion is
accepted only through this trusted gateway boundary. Unknown schemas fail closed.
No client blob storage references are admitted: opaque staged-upload enrollment and
original attachment delivery are future work. Native relationships are retained
inside original envelopes; this slice exposes one fixed root kind per source and
does not claim normalized group/folder/account relationship queries.

Runs reuse canonical writer activation, bounded discovery-only cycles, scans and
`ScanPageReceipts`. Final page means collection ended; `/complete` reconciles the
bounded scope and finishes that existing cycle/job. Missing inventory never deletes
records. Completion reports `coverage=bounded` and `indexing=not_verified`.
A current page receipt is recoverable until canonical progress advances. Historical
run metadata does not display a newer run's reused scan row. No live scheduler,
Contacts reset replay coordinator, attachment uploader, search consumer or cloud
production activation is included.

## Verification

Use only a disposable PostgreSQL database:

```sh
CANONICAL_TEST_DATABASE_URL=postgresql+asyncpg://apple_capture_test@127.0.0.1:55439/apple_capture_test \
  /tmp/almanac-apple-capture-tests/bin/python -m pytest \
  airweave/domains/device_ingestion/tests/test_admission.py -q -o log_cli=false
```

Tests run the actual migration schemas and SQL transactions. They cover duplicate
requests, exact-byte conflicts, Int64 above 2^53, unchanged original omissions,
owner/org/device/store-generation isolation, idempotent binding/revoke, one primary
publisher, revoke/page serialization under the real Sync lock, source read withdrawal,
retained record withdrawal after reauthorization, receipt-failure rollback, Notes
lock/local-delete admission, explicit schema/blob rejection, bounded completion
and scan reuse. Receipt failure simulates the crash boundary; it is not a killed
process or network-drop test.

The standalone store/service tests and native receipt/page regression suites are
verified locally. Full app HTTP/container runtime, hosted execution, signed-device
permission, real Apple source lifecycle/fidelity, indexing and retained search/read
remain separate proof. No real source data, remote API or user permission is used.

Attachment admission is bounded to8MiB per blob and64 handles/64MiB per run.
`PUT /{source_id}/runs/{request_key}/uploads/{handle}/intent` accepts
`DeviceUploadIntent`: principal, native ID, unchanged versioned original,
attachment array index, expected SHA256/size and optional media type. The UUID
handle is an idempotency key: identical intent recovers, changed intent conflicts.
`PUT .../uploads/{handle}/content` accepts raw binary and the four principal
fields as query parameters. This deliberately avoids multipart filenames and
caller storage keys. Responses are `DeviceUploadHandle`; no storage key is returned.
A page observation's optional `uploads` array references admitted handles. Only
matching native ID, identical original and distinct attachment indexes resolve
into committed canonical blob refs at `/original/attachments/{index}`.

Intent commit and post-storage admission are separate transactions. Both fence
current enrollment/run under the existing Sync lock. Storage I/O uses the existing
FileService content-addressed canonical path without holding that lock. Revocation
or writer replacement during upload fails the second check. Such an interrupted
write can leave unreferenced bytes; existing projection GC does not reclaim these
canonical source blobs, so orphan reclamation remains unimplemented. They cannot
be downloaded through the canonical record API without a committed reference.

Attachment membership proves association to the attested native observation,
not that supplied bytes came from the protected native file. Real local source
permissions/attachment acquisition remain unqualified. Contacts has no attachment
schema and rejects uploads. Locked/deletion-marked Notes reject byte admission.
Retained reads use the existing canonical blob API, which verifies committed
membership, size/SHA256 and rechecks visibility after storage I/O. SQL plus real
temporary-filesystem tests cover exact retry, conflicting intent, wrong hash,
foreign device, changed original, successful byte read, corrupt bytes, revoke
during upload and revoke during download. These are synthetic proofs; full HTTP
container and remote storage behavior are not verified here.


## Endpoint actor alternatives (pipeline 5)

Owned search `actor_any_of` accepts `{role, handles, match}` with `match` equal to
`raw` (default) or `endpoint`. Existing individual AND `ActorFilter` predicates
remain raw. The same-role OR and combined 20-handle limit are unchanged. Endpoint
mode qualifies only explicit international phone spellings using Contacts v2's
whole-input Unicode decimal grammar, phonenumberslite 9.0.40 and exact extensions.
National values, emails and unsupported spellings retain raw exact comparison;
there is no inferred country, name merge, ownership or reachability claim.

Each qualified endpoint contributes a separate `native-actor-endpoint-v1` term
to the existing fast-search `actor_tokens` attribute alongside its unchanged raw
term. Current canonical SQL handles are independently compared under the same
policy after index retrieval, with existing scope/publication/revision fences.
No extra actor database, handle inventory, index field or bounded sampling is used.

New device sources initialize pipeline 5. Existing sources are not silently
upgraded: actor requests against pipeline 4 return `reindex_required`; deliberate
canonical reprojection is required before those sources qualify. Parser/policy
changes require another pipeline/term version review. Actual disposable PostgreSQL
and Vespa fixtures verify endpoint variants, extension isolation, unchanged raw
behavior, old-version rejection and revoked-scope refusal. Fixed vectors verify
filter behavior, not relevance or live people identity.

### Explicit historical retained originals

`GET /api/v1/sync/{sync}/records/{record}/revisions/{revision}` returns
`{record, current_revision, authority: "current_source_record_access"}`. The
separate `/revisions/{revision}/blobs/{sha256}` route downloads a verified blob
belonging to that immutable capture, with private/no-store caching and historical
revision/authority headers. Existing current-record blob downloads still reject
an old revision with 409.

Historical access uses current source and record availability, including a check
after storage I/O. It does not reconstruct the snapshot's original ACL: regrant
or a permitted move can restore history under the existing retained-data policy.
Historical bytes do not prove that today's provider attachment is unchanged.
No live migration, source access, or hosted authentication is qualified by the
synthetic PostgreSQL/filesystem/ASGI tests. A body update with locally unavailable attachment bytes may commit a new partial
capture without current blob references. Prior original bytes stay associated with
their historical revision; they are never carried forward as freshly verified
current bytes. Native Messages/Notes pages explicitly use partial completeness;
this proof does not qualify arbitrary callers claiming complete binary coverage.
