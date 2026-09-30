# Retained text implementation plan

Goal: read complete derived text from retained originals, with the exact revision,
pipeline and publication generation used by search. Original provider records and
bytes remain authoritative. No additional transcript store, source fetch or CLI
vocabulary is introduced.

The text builder will record complete indexing text and its converter-content
boundary at construction. Indexing and artifact persistence consume that same
result, without rerunning conversion or reconstructing text from snippets. Generated
metadata remains distinguishable; content reads default to the converter output.
Existing PDF converters flatten page boundaries, so source page anchors remain
explicitly unavailable. Character ranges identify the declared derived text only.

Generation manifests will retain immutable representation descriptors, not bodies.
The existing object store holds UTF-8 bytes. A manifest commits before external
writes; publication uses the existing compare-and-swap after blob and index writes.
Reads use the same publication/ancestor-visibility predicate before and after
bounded, checksum-verified storage access. Retired-generation garbage collection
owns derived blobs, including abandoned attempts and repeated late writes.

Scope: full PDF/readable text and declared generated record representations.
No transcription, video understanding, visual fidelity or source-page coordinates
are implied. Existing generations without representation metadata stay explicitly
unavailable until reprojected.

Proof required: converter runs once; text beyond a search excerpt remains readable;
generated/content boundary survives marker-like user content; prepared but
unpublished bytes cannot be read; revision/pipeline/revocation changes deny reads;
corrupt blobs fail closed; retirement reclaims artifacts and retries failures.
Synthetic provider/PDF and migrated SQL tests are separate from live qualification.

## Implemented internal read contract

`GET /sync/{sync_id}/records/{record_id}/text-representations?revision=N`
returns `record_id`, `revision`, `status` (`available` or `unavailable`) and
`representations`. Each descriptor contains an opaque `id`, parent `record_id`,
`revision`, `generation`, `pipeline_version`, source-local `part_key`,
`kind` (`extracted_text` or `generated_text`), `media_type: text/markdown`,
`content_characters` (null for generated-only text), `index_characters`, and
`source_anchors: unavailable`. Storage keys and internal part ordinals are omitted.

`GET` of that path plus `/{representation_id}` requires `revision` and `generation`.
It accepts `offset` (default 0), `limit` (default 12000, maximum 100000), and
`view=content|index` (default content). Offsets count Unicode code points in that
view. The response contains `representation`, `view`, `offset`, `total_characters`,
`text`, and nullable `next_offset`. Follow next_offset with the same immutable
reference/view. The default excludes generated metadata; generated-only records
require explicit index view. An unavailable/changed generation returns 409, and
withdrawn content returns 404. Neither falls back to a provider or search excerpt.

These are internal service APIs for Almanac's established read surface, not new
public CLI commands. Existing organization-scoped backend authorization applies.
Almanac must enforce its user/account binding before calling them.

UTF-8 artifacts use the existing per-file storage size bound; publication fails
instead of silently truncating an oversized conversion. Reads verify byte length,
SHA256 and character count. Migration 0011 adds nullable body-free descriptors;
old generations require explicit reprojection before this representation exists.

## Verification evidence

Local synthetic tests exercise an actual three-page PDF converter, migrated
PostgreSQL publication, filesystem blobs and authenticated ASGI read routes.
Chunking/embedding are fixed substitutes: this is a content/authorization proof,
not a search-quality or live-deployment claim. Tests cover unpublished artifacts,
full content versus metadata, corruption, mid-read revision/pipeline changes,
parent withdrawal, failed cleanup and late blob recreation. Strict exact-object
storage deletion propagates failure; cloud version retention remains backend policy.

The canonical/pipeline/wiring regression run passed 376, skipped 2, with one
30-second child-process timeout; that unchanged lifecycle test passed in 9.18s
on isolated rerun. Final focused retained-text/exclusion/worker checks passed 34.
The worker readiness mock now uses the real `/.well-known/ready` path and tests
both healthy 200 and unavailable 503. No private provider calls or deployment ran.

## Gmail size metadata and configured extraction coverage

| Native capability/resource | Product name | Access operation | Stored representation | Sync/change guarantee | Proof | Gap |
| --- | --- | --- | --- | --- | --- | --- |
| Inline MIME body / external body bytes | Email text | Retained record projection | Native JSON unchanged; external blob verified against canonical actual byte size and SHA256 | Projection does not refetch or modify capture | Synthetic declared-size discrepancy succeeds; invalid base64 and corrupt canonical bytes still fail | Live retained inline bodies showed native size one byte smaller than decoded data; no upstream cause established |
| Inline images and attachments | Email extraction parts | Configured converter registry | Available body text plus explicit unsupported part coverage when converter absent | Reprojection follows existing pipeline-version lifecycle | Real SQL partial publication without OCR; configured converter failure still blocks publication | Adding OCR requires pipeline-version change to retry prior unsupported parts; no image understanding claim |

Google documents MIME `size` as byte count, but it is provider metadata rather
than our storage-integrity checksum. Projection preserves the reported value
and consumes the retained base64 data or verified canonical blob without an
additional provider-size equality requirement. External attachment acquisition
keeps its existing checks; this change does not claim that path was requalified.
[Native MessagePartBody contract](https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.messages.attachments#MessagePartBody).

Unsupported means no configured converter for that part. It does not turn an
installed converter's failure into success: malformed PDFs, OCR errors, and
other attempted-conversion failures still leave publication pending. Scanned
PDFs with a configured PDF converter but no OCR fallback remain in that category.

### Drive originals omitted during capture

A non-folder Drive file explicitly captured as `metadata_only` with no blobs
publishes `unavailable_original` / `original_not_captured` extraction evidence,
zero searchable chunks, and unavailable text. It does not index a title as if it
were the document. A complete record missing its required blob remains an error.
The retained evaluation presentation failure was this omission, not a converter
failure: native size exceeded the initial capture limit and no bytes were stored.

| Native capability/resource | Product name | Access operation | Stored representation | Sync/change guarantee | Proof | Gap |
| --- | --- | --- | --- | --- | --- | --- |
| File metadata without captured body | Original / extraction availability | Existing record and extraction reads | Native metadata plus explicit unavailable-original coverage | Later captured bytes form a new revision; loss of body retires previous text eligibility | Real SQL metadata-only → retained text → metadata-only lifecycle; complete/missing-blob remains error | Actual recapture depends on provider access and configured limits; no converter success claimed |
