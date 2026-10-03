# Inert textual attachments and remaining extraction gaps

The retained combined evaluation corpus was audited without provider reads or index
changes. Counts below are attachment/representation occurrences, not unique emails.

| Source | Observed gap | Next useful action |
| --- | --- | --- |
| Gmail | 150 ICS, 5 delivery reports, 5 RFC headers unsupported | Strict inert text conversion implemented in this slice |
| Gmail | 86 JPEG/PNG and 20 PDF representations with unsupported/partial OCR coverage | Qualify the existing LocalOcrProvider; the evaluation fixture omitted OCR configuration |
| Gmail | 29 GIF occurrences, PKPASS/MP3/MP4 one each | Separate format support decisions; do not call binary originals extracted text |
| Drive | 8 PNG, 2 MP4, 1 partial PDF | Existing OCR qualification for images/PDF; video remains unsupported |
| Drive | 3 native Slides files with no retained export | Preserve export failure evidence before claiming the reason; native Slides acquisition is separate work |
| Slack | 124 uncaptured native file IDs | Existing files-on capture lifecycle; metadata-only records are not retained originals |

## Implemented contract

Gmail dispatches exactly `text/calendar`, `application/ics`,
`message/delivery-status`, and `text/rfc822-headers` to strict textual extraction.
MIME charset decoding uses the existing strict decoder, including its explicit
UTF-8 recovery evidence. Derived temporary files use UTF-8 and trusted `.ics`,
`.dsn`, or `.headers` suffixes. Original filenames, MIME metadata, provider payload,
and retained bytes remain unchanged.

The shared converter preserves lines and whitespace. It does not parse events,
unfold fields, normalize timezones or recurrence, execute content, follow links,
or perform calendar actions. It rejects invalid UTF-8 rather than guessing or
inserting replacement characters. Unknown binary formats remain unsupported.

A decoding failure marks that attachment `failed/conversion_failed` using the
existing coverage model; the readable email body can still publish. Missing bytes
remain `unavailable_original`, and blob scope/hash/size failures remain fatal.

This is source code support, not a change to already published generations. The
frozen combined evaluation runtime and corpus are unchanged. A later ordinary
pipeline-version reprojection is required to apply this preparation change.

## Drive limitation retained explicitly

`google_drive_content._download_representation` already computes precise
`ExportState` reasons (`export_size_limit`, `read_size_limit`). The non-Docs/non-native-
Sheets `blob is None` return drops that evidence. Current WorkspaceManifestV1
requires DocsState, and its consumer explicitly rejects non-Docs identities.

The next coherent fix is an export-only manifest variant sharing ExportState,
with source identity/version verification and corresponding reader/mapper dispatch.
Do not fabricate DocsState, put generated coverage inside provider JSON, or infer
historic failure reasons from native file size. The existing three Slides
originals do not establish which export failure occurred.

## Verification

Synthetic MIME fixtures cover multilingual charset recovery, declared Latin-1,
verbatim recurrence/timezone/header lines, and original immutability. A real SQL
projector test publishes a body and valid ICS while retaining a malformed ICS as
an explicit failed part. Existing partial-attachment recovery tests also pass.
These are local tests, not live provider or hosted qualification.

## Activation and retry ownership

`Sync.index_pipeline_version` is the persisted preparation version; converter code
is not automatically fingerprinted. Activation must use `plan_reprojection` with
an explicit expected-current and higher target version before the ordinary
projector runs. Do not reuse an existing current generation as evidence that it
has the new textual preparation. This commit changes no runtime version or corpus.

Malformed attachment bytes retain `projection_error=conversion_failed`. Background
source discovery excludes failed rows. An explicit projection workflow has at
most three failed sweeps, with 30/60-second backoff, then reports pending failure;
`skip_failed=True` does not resweep. Repeated manual `batch()` calls can retry failed
rows, so operators must retain pagination and bound attempts. A persistent decode
failure requires corrected source bytes or a deliberate decoder change, not an
unbounded retry loop.
