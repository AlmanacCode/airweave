# Durable live capture proof

`canonical_capture.py` is opt-in and makes read-only Gmail/Drive/Calendar provider requests.
It commits real captured records and blobs into disposable local storage, then
checks reads over fresh database connections, paginated lists/change feeds, exact
payload equality, blob SHA256/size, and an identical replay with zero new changes.
It does **not** assert a completed account sync or save an upstream checkpoint.

Requirements:

- A separately initialized PostgreSQL database `sync_tests`, user `sync_test`,
  reachable only through an explicitly supplied private Unix-socket directory.
- `CANONICAL_TEST_DATABASE_URL` using `postgresql+asyncpg`, that database/user,
  no network hostname, and `?host=/absolute/private/socket&port=...`.
- `COMPOSIO_API_KEY`, `LIVE_GMAIL_ACCOUNT_ID`, `LIVE_DRIVE_ACCOUNT_ID`, and
  `LIVE_EXPECTED_EMAIL` in the subprocess environment. The two accounts must
  already be connected. The harness checks their actual provider email identity.

Optionally set `LIVE_PROVIDERS=gmail` or `LIVE_PROVIDERS=google_drive` to verify
only one provider; default is both. `LIVE_PROVIDERS=google_calendar` selects Calendar
and requires `LIVE_CALENDAR_ACCOUNT_ID`. It verifies the primary CalendarList ID
against `LIVE_EXPECTED_EMAIL`. Only selected provider account variables are required.

Run from `backend`, using your approved secret injector:

```sh
PYTHONPATH=. .venv/bin/python tests/live/canonical_capture.py
```

The source sample stops after at least three records and one blob, or 25 records.
Gmail uses `newer_than:7d smaller:5M`; capture/download limits are reduced to 10 MiB
for this probe. A Gmail/Drive sample without a blob fails. Calendar samples up to
25 original records and requires an event; its JSON does not require blobs, so
blob verification is null when none exist. Listing includes tombstones so sparse
cancellations are verified alongside ordinary originals. No scope-completion observations
are persisted. The application database hostname is overridden to an unusable
value; only the explicit test URL reaches a database engine.

A random `canonical_live_*` schema receives the real migrations. Files live under
one temporary directory with mode `0700`, and the process uses umask `077`.
Schema and files are removed in `finally`, including on failure. Database WAL may
retain deleted pages until the disposable PostgreSQL cluster is destroyed; use a
private test cluster and remove it after the larger verification session. Do not
run this against a production database or retain/export provider payloads.

Output contains only counts, booleans, safe exception class names, stages, HTTP
status/host and allowlisted provider error codes. The live
script never prints credentials, record contents, account identifiers, filenames,
or SQL parameters. Automated mock/store tests remain separate from this proof.

## Almanac HTTP consumer handoff

For Gmail, optionally set `LIVE_ALMANAC_ROOT` to the Almanac checkout and
`LIVE_ALMANAC_PYTHON` to its Python 3.12 **venv executable path** (do not resolve
its symlink to the base interpreter). Run the same `canonical_capture.py` command
in Airweave's Python 3.13 environment. The callback serves the committed sample
through the actual `api_router` on an ephemeral loopback port, then invokes
Almanac's `backend/tests/live/read_owned_gmail.py`.

The child receives a mode0600 manifest inside the existing mode0700 directory;
its environment contains no provider credentials. It reads an actual thread and
record/blob endpoints using `OwnedMailThreadReader` and `OwnedSourceRecords`,
compares native header/body/reference and payload digests, verifies blob SHA/size,
and checks that an unauthenticated probe request is denied. The server uses a
random-key-guarded **test context override**, not hosted WorkOS or real persisted
API-key authentication. It creates no production Almanac account binding. The
child and server are stopped before the enclosing harness removes schema/files.

Verified 2026-09-30: 7 Gmail originals, 7 journal changes, 2 blobs totaling
1,383,758 bytes, exact replay with 0 new changes and fresh-connection readback.
The real HTTP → Almanac reader returned 1 message with complete body and verified
native fields; both blobs passed HTTP SHA/size verification. That selected body
was inline (`external_body_blobs=0`): external-body retrieval is covered by
synthetic tests, not claimed as live-covered by this sample. After the initial
venv-launch failure and successful rerun, independent checks found 0 retained
`canonical_live_*` schemas and 0 private live directories. No complete-source
coverage, checkpoint, hosted authentication, or deployed product proof is claimed.

## Slack and Wispr durable samples

`LIVE_PROVIDERS=slack,wispr` selects the existing source adapters. Set
`LIVE_SLACK_ACCOUNT_ID`, `LIVE_WISPR_ACCOUNT_ID` and `LIVE_WISPR_USER_ID` for
previously connected, authorized accounts. Slack verifies `auth.test` identity
and the matching `users.info` email against `LIVE_EXPECTED_EMAIL`. Wispr verifies
exact ACTIVE Composio account ID, toolkit and principal; this is weaker than a
provider mailbox/profile check and is not an Almanac user binding. A Wispr-only
run does not require the email variable.

Slack/Wispr stop after three distinct records containing a message/meeting, or
25 observations. No file/blob presence is required. Repeated native identities
are reduced to their latest raw observation in this bounded sample before
capture and replay; Slack history/replies can return the same parent twice.
Declared channel/container parent relationships are retained. Every record's
native JSON is compared after durable readback; identical replay must add no
journal changes. Source markers/checkpoints remain deliberately uncommitted.

Verified 2026-09-30: Slack produced four observations and three distinct records
(channel/message), with three journal changes, exact fresh-connection readback
and zero replay changes. Its provider email matched. No file-bearing message was
in that sample, so attachment bytes/partial file coverage were not live-tested.
Wispr produced three meeting records and three journal changes; all three remain
explicitly partial because the provider omits raw editor data/deletion history.
Its replay added zero changes. Neither sample contained blobs or proved complete
source coverage. Both private schema and files were removed afterward.

## Complete configured Gmail reconciliation lifecycle

`PYTHONPATH=. .venv/bin/python tests/live/provider_lifecycle.py` uses the same private
PostgreSQL, Composio key, `LIVE_GMAIL_ACCOUNT_ID` and `LIVE_EXPECTED_EMAIL` inputs.
It freezes a seven-day `after:<epoch> before:<epoch>` query before either run,
with no size or category filter. Each run is a full reconciliation of this query;
this does **not** verify Gmail history-delta resume or the whole mailbox.

Two fresh Python processes run the production `SyncOrchestrator.run`, source,
canonical pipeline, job state machine/repository and cursor read/write path.
The harness injects private database sessions and disables external telemetry;
billing, non-applicable ACL and sync-pausing collaborators are test doubles.
It does not exercise the hosted factory, Temporal worker, WorkOS, or a product
account binding. The second process loads the persisted cursor; the successful
checkpoint must identify its committed attempt. Native JSON/revision digests
compare the two reads. A real mailbox change may make the second digest differ.

Per-run limits: 250 message observations, 600 provider requests, 10 MiB per MIME
blob, 256 MiB total blob writes, and ten minutes. The record/request/storage
limits raise instead of truncating enumeration; no completed scope or checkpoint
is claimed on an aborted enumeration. Oversized MIME parts retain the source's explicit
partial-content status. The parent reaps the child, removes its schema and private
files on success or failure, and prints only counts, status and digests.

Verified 2026-09-30: both fresh-process seven-day runs completed, each with 239
message observations, 287 provider requests, 239 stored records, zero partial
records, and 5,501,801 MIME blob bytes written. Each emitted one start and one
completion marker and saved a checkpoint; the second loaded the durable cursor.
Its final change sequence was 270, so the two raw-payload/revision digests were
**not identical**. This is not a zero-change second-live-run claim. A separate
bounded diagnostic repeated one attachment-bearing message GET and observed only
`payload.parts[*].body.attachmentId` differences. Gmail can rotate these original
attachment locators; the canonical store preserves them. That diagnostic does
not retrospectively attribute all 31 earlier changes. Duplicate replay of an
identical captured observation remains independently tested.

Both private schema/file cleanup checks returned zero leftovers. Fifteen Gmail
source tests and four real-PostgreSQL pipeline tests passed; failure/recovery
fixtures are simulated provider evidence, not live induced failures. The existing
Gmail source now emits an explicit `StartedScope` before full enumeration,
including filtered reconciliation and expired-history bootstrap.

Calendar verification via `LIVE_LIFECYCLE_PROVIDER=google_calendar`: one verified
primary calendar with a fixed seven-day occurrence window completed both fresh
processes. Initial run: 4 provider requests, 190 observations, 183 active records,
3 completed scopes. Second run: 4 requests, 22 observations, 2 completed scopes,
and one actual event listing with the saved `syncToken`. Both jobs completed and
saved checkpoints; second-run change sequence stayed 190 and the native
payload/revision digest was identical. Zero partial records or blobs. Private
schema/files were removed. No all-day or DST case was observed; those semantics
remain covered separately by synthetic tests. Calendar limits are 100 provider
requests, 10,000 observations and 180 seconds per process.

### Calendar consumer over real HTTP

Set `LIVE_VERIFY_CALENDAR_CONSUMER=1` with the explicit `LIVE_ALMANAC_ROOT` and
`LIVE_ALMANAC_PYTHON` paths when running the selected Calendar lifecycle. The
second process uses the shared `almanac_handoff.run_consumer` loopback server and
the real `/sync` routers. A fresh Almanac process receives a private0600 manifest
and an ephemeral service key; provider credentials are excluded from its environment.
The temporary source connection exists only in the disposable PostgreSQL schema.
This does not verify hosted authentication, production bindings or Temporal workers.

Verified September30:21 actual stored expanded events crossed this boundary in5
pages (limit5), retaining real occurrence/calendar/account identities, title,
description, scheduling times, status, recurring master and coverage metadata.
Wrong-source reads were denied and an uncaptured2001 range returned the explicit
product error. Both capture jobs completed with4 provider requests each,190 initial
observations and22 second-run observations,183 active records, unchanged journal
sequence190 and unchanged native payload/revision digest. The second fresh process
loaded its durable checkpoint and made one actual master `syncToken` request.
No all-day events occurred in this sample; DST/all-day correctness remains fixture
coverage. The private schema and files were removed; an independent PostgreSQL
check found zero remaining `canonical_live_*` schemas. No provider writes occurred.

Drive mode: `LIVE_LIFECYCLE_PROVIDER=google_drive` with `LIVE_DRIVE_ACCOUNT_ID`
uses the source's entire accessible `allDrives` corpus; no query, path selection,
or hidden sampling filter is inserted. The initial verification budget was
2,000 observations, 250 provider requests, 128 MiB total retained blob writes,
10 MiB per blob and 300 seconds. The approved follow-up budget uses the source
production limit of 200 MiB per blob and 512 MiB total; record/request/time
limits stay unchanged. Drive checks for at least 2 GiB free disk before each
process starts. A successful initial run must exhaust enumeration plus changes
replay and commit its canonical page token. A second fresh process must actually
request `/changes` with that saved token and emit no full-rescan markers; an
expired token/reset is not reported as successful incremental proof. Failure
stops the attempt and reports whether the previous checkpoint stayed unchanged.

Drive attempt 2026-09-30: the full accessible scope stopped at the configured
10 MiB per-file transport ceiling (`Proxy download exceeds the file size limit`)
during a binary download. The initial attempt and one sanitized diagnostic retry
each made 11 provider requests and yielded 4 observations, with one start marker
and **no** completed scope. The real job became failed; the checkpoint remained
unchanged. No second incremental process was started. This proves the bounded
abort path, not complete Drive synchronization. Both attempts removed their
private schema/files. Thirteen Drive source/content tests passed; a synthetic
storage-budget check also rejected an over-budget write before retaining it.

The approved production-size Drive retry completed on 2026-09-30. Initial run:
85 provider requests, 55 observed/stored records, three not content-complete,
207,597,309 retained blob bytes, one completed full scope and a saved checkpoint.
A fresh second process made two requests, including `/changes` with the actual
saved page token, observed zero changes and saved its new attempt checkpoint.
Change sequence stayed 55 and payload/revision digests were identical. Both jobs
completed. Free disk before the run was 9,525,235,712 bytes; private schema/files
were removed and independently checked absent. This verifies the current
accessible Drive scope and a quiet real delta run; it does not prove the three
incomplete bodies, a hosted consumer, or live induced deletions/revocations.
The earlier smaller-budget failure evidence remains unchanged. Sanitized result:
`evidence/drive-lifecycle-production-limit-20260930.json`.


### Slack and Wispr full lifecycle attempts

Both modes use the real canonical orchestrator and production `cursor=None`.
Slack reconciles only completed channel/message scopes; Wispr has no deletion
scope or durable provider checkpoint. Neither is described as delta resume.
Actual network requests are metered, including Wispr session/account operations.

The first Slack full-accessible-history attempt stopped on a rate limit after
70 requests and 457 observations, with two started scopes and zero completed.
The job failed, checkpoint stayed unchanged, and private schema/files were removed.
No repeat process started. Retry-After was not retained in that first attempt.
The subsequent approved budget is 600 requests, 10,000 observations and 1,800
seconds per process. Slack uses its five-attempt retry policy and honors the
full Retry-After wait; the test does not shorten waits to fit its total budget.
Only status, numeric retry delay and request count are retained as diagnostics.
The longer attempt reached its 1,800-second deadline after 74 requests and 328
observations. Thirty rate limits each supplied a 60-second Retry-After. The job
became cancelled; two scopes started, none completed, and the checkpoint stayed
unchanged. Cancellation telemetry then accessed missing fixture connection
metadata, masking the timeout with AttributeError. The fixture is corrected;
this was not rerun. No second process started, and private schema/files were
removed. Full Slack coverage remains unverified. A durable per-scope sweep and
continuation design is needed before repeatedly rescanning this history.
See `evidence/slack-lifecycle-backoff-incomplete-20260930.json`.

Wispr enumeration and one diagnostic retry each failed after 18 requests and nine
partial observations. `GET_MEETING` returned an opaque tool error string; the
source stopped instead of skipping that meeting. A targeted read-only diagnostic
found no structured status/code or recognized auth/not-found/rate-limit category.
The underlying provider versus intermediary cause is unknown. No complete source,
checkpoint, repeat process or deletion guarantee is claimed. Both lifecycle
attempts cleaned private storage. This is additional failure evidence alongside
the earlier successful three-record sample, not a replacement for that sample.
See `evidence/slack-lifecycle-incomplete-20260930.json` and
`evidence/wispr-lifecycle-incomplete-20260930.json`.
