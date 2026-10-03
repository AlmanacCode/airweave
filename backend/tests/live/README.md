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

## Historical Slack and Wispr samples

Both samples below used the former observation adapters. Slack and Wispr now
use durable pages; `canonical_capture.py` rejects these sources and directs callers
to the shared `provider_lifecycle.py`. Do not use `LIVE_PROVIDERS=wispr` or
`LIVE_PROVIDERS=slack` with the old sampler.

The current lifecycle selects `LIVE_LIFECYCLE_PROVIDER=wispr`, with
`LIVE_WISPR_ACCOUNT_ID` and `LIVE_WISPR_USER_ID` for the existing connected account.
It verifies exact ACTIVE Composio account ID, toolkit and principal. This is weaker
than a provider mailbox/profile check and is not an Almanac user binding. Wispr
requires no email variable. Slack verifies `auth.test` and matching `users.info`
email against `LIVE_EXPECTED_EMAIL`.

The historical samples stopped after three distinct records or25 observations.
Exact native JSON survived durable readback and identical replay added no journal
changes. Source markers/checkpoints remained deliberately uncommitted. These are
historical sample results, not verification of the new page adapters.

Verified 2026-09-30: Slack produced four observations and three distinct records
(channel/message), with three journal changes, exact fresh-connection readback
and zero replay changes. Its provider email matched. No file-bearing message was
in that sample, so attachment bytes/partial file coverage were not live-tested.
Wispr produced three meeting records and three journal changes; all three remain
explicitly partial because the provider omits raw editor data/deletion history.
Its replay added zero changes. Neither sample contained blobs or proved complete
source coverage. Both private schema and files were removed afterward.

## Current Gmail page lifecycle and bounded recovery

The current Gmail source uses the production durable page/checkpoint protocol.
The existing `provider_lifecycle.py` remains the runner; no live execution of this
new harness is claimed here. The earlier lifecycle results below describe the
previous observation adapter.

With `LIVE_LIFECYCLE_PROVIDER=gmail`, the default still freezes one seven-day
query and runs two complete configured-scope passes in fresh processes. Validation
now reads `canonical_cycle` and the fenced checkpoint; legacy marker counts and
`history_id` are not evidence for the page source. Payload digest differences are
reported honestly rather than requiring a stationary mailbox.

For an explicitly selected unfiltered **partial recovery** probe, additionally set
`LIVE_GMAIL_UNFILTERED=1 LIVE_GMAIL_RESUME=1`. This clears the source's default
label/category filters rather than treating an empty custom query as unfiltered.
The first child exits with code75 only after SQL proves a message page committed.
A second child uses the same running job and a new attempt, resumes the identical
cycle/sweep/version, and exits with code76 after another acknowledged page.
Parent SQL readback independently verifies advancement. No payloads or provider
cursors enter printed evidence. A mailbox completing before interruption reports
`recovery_not_exercised`; it is not successful recovery proof.

The existing600 provider-request,250 observation,600-second and256MiB blob-write
budgets are shared across the interruption/resume pair; each MIME blob remains
limited to10MiB. Exact known-message refreshes consume request, observation and
storage budgets too. Normal identity verification still reads Gmail's profile in
each process. Separate counters require **zero capture-boundary profile reads**
in the resumed process; identity verification is not counted as a boundary reset.

`LIVE_GMAIL_UNFILTERED=1` without the resume flag selects two bounded full/changes
processes. A changes claim requires the first baseline actually to finish, the
next process to load its promoted checkpoint and issue history requests using that
exact boundary, and the second checkpoint to commit. Budget exhaustion remains
incomplete; no sampled records or interrupted baseline establish mailbox completion.
This option does not increase any existing download or process limit.

## Historical configured Gmail reconciliation lifecycle

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

These historical attempts used the real canonical orchestrator and production
`cursor=None`. Current adapters use canonical durable page checkpoints. Slack's
interruption trial is described below. Wispr now resumes individual meeting scopes,
but has no provider delta cursor or deletion evidence; its discovery-only traversal
is not an exhaustive-source claim. Its current lifecycle runs first/second cycles;
the deliberate same-job interruption probe is still Slack-specific.
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
removed. Full Slack coverage remains unverified. The durable per-scope recovery
implementation below addresses this retry limitation; it has only synthetic
HTTP/PostgreSQL and local cross-process verification so far.
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


### Current Slack cross-process recovery trial (not yet run live)

`LIVE_LIFECYCLE_PROVIDER=slack` uses the production page adapter, capture pipeline,
SQL state, and job state machine. It has three stages within **one aggregate cap**:
600 provider requests, 10,000 observed records, and 7,200 seconds including all
Retry-After waits and subprocess startup.

1. Start a new job. After a history page commits pending thread IDs, the test-only
   wrapper verifies the exact SQL continuation, active cycle, current attempt and
   running job. It writes private UUID/version evidence, flushes sanitized counters,
   and exits 75 before another provider request. This simulates process loss without
   marking the job failed or cancelled.
2. Start a fresh process using the same job and attempt 2. The production driver
   refreshes membership, then resumes the saved child state. A rejected cursor is
   reported separately from successful cursor reuse; the normal one-restart bound
   still applies. No complete-scope claim is made until actual finalization.
3. Only after completion, start a genuinely new job and verify its new cycle.

The parent subtracts each child's counters from the remaining limits and enforces
one wall-clock deadline even while a child sleeps. No pending thread found, changed
scope, or exhausted budget leaves the requested resume proof incomplete. A forcibly
killed child's counters are unknown: aggregate exact counts are null and known
counts are reported only as lower bounds. No further stage follows that failure.
After the resumed child exits (including a forced timeout), the parent reads the
saved scan directly from SQL. Same tenant, sync, job, attempt 2, cycle and sweep,
with an advanced revision, proves a resumed page committed. This is reported as
`resumed_page_committed`; `resumed_partial` means that proof exists but the cycle
has not completed. Scope withdrawal and expired-cursor restarts cannot satisfy
this proof. Full-cycle and subsequent-new-cycle completion remain separate checks.
Private schema, downloaded files, and resume evidence are removed by the parent.

`test_resume_probe.py` verifies the actual special process exit and fresh-process
resume using synthetic Slack responses and isolated PostgreSQL. Its budget tests
use fake child processes to check decreasing limits and unknown killed-child usage.
These are harness tests, not evidence of live Slack coverage or throughput.

### Opt-in Wispr same-job recovery proof

From `backend`, with the same private PostgreSQL and existing verified account environment:

```sh
LIVE_LIFECYCLE_PROVIDER=wispr LIVE_WISPR_RESUME=1 PYTHONPATH=. .venv/bin/python tests/live/provider_lifecycle.py
```

Required secret environment names are `COMPOSIO_API_KEY`, `LIVE_WISPR_ACCOUNT_ID`
and `LIVE_WISPR_USER_ID`; PostgreSQL uses `CANONICAL_TEST_DATABASE_URL`. Do not
print these values. This mode shares a hard20 HTTP request,600 observed record,
180 second budget across both processes, including account/session/listing calls.
Set `LIVE_WISPR_REQUEST_LIMIT=17` to reduce the aggregate request budget; values
from1 through20 are validated before setup/network. The default remains20.
No retries or new account grants occur. Errors, including rate failures, stop the trial.

The first process exits before another body call after SQL verifies three completed
meeting children. The second uses the same job/cycle with a new writer attempt,
refreshes listing membership and commits one additional body. Before each body call,
the typed probe rejects a completed sibling identity. It verifies prior scan revisions
remain unchanged and then exits intentionally. Private evidence contains only scan
identities and native-ID SHA256 digests, never meeting content; output contains counts
and booleans. Parent cleanup removes the private schema/files on all normal or failed
exits. `wispr_recovery_verified` is distinct from full traversal or deletion completeness.

A source with fewer than five discoverable meetings, many body ranges, changed inventory,
rate failure or exhausted budget can leave the proof inconclusive. Without the opt-in
flag, Wispr retains its ordinary first/second-cycle trial; that is not a recovery proof.
Offline PostgreSQL/subprocess tests verify the actual process exits and fresh retry,
failed-body stop, and aggregate budget. The subsequent live result is recorded below.

Live checkpoint, 2026-09-30 (`cf14b4e`): the pipeline constructor had discarded
`completion_policies` while checking topology; the fix preserves the source policy
and still rejects incorrect topology. Full orchestrator/subprocess tests now cover
this integration, rather than only the scan driver.

The subsequent14-request trial used12 requests for253 listing observations and
three committed meeting bodies, then intentionally exited75. The fresh process
used its remaining2 requests during account/session setup (last response HTTP201)
and hit the request guard before validation/body capture. Thus storage and intentional
interruption are live-verified; recovery remains **unverified**. Together with the
six prior preflight requests, this exhausted the20-request batch. Private schemas,
files and code archive were removed; independent schema count was zero. See
[evidence](evidence/wispr-recovery-budget-incomplete-20260930.json).

A future independently approved trial with a fresh20-request budget is plausibly
sufficient, not guaranteed: measured first-stage12 + expected fresh setup3 + listing
refresh2 leaves3 GET/range calls for one additional meeting. A one-call body needs18
requests total; a body requiring4 calls needs21 and must stop under the current cap.
Inventory and range sizes can change, and prior rate failures provide no reliable
reset interval. Keep the current20 maximum and180-second deadline; do not automatically
raise limits or rerun after a failure. No further live requests were made for this note.


**Latest live result, 2026-09-30: recovery verified.** A separate fresh20-request,
180-second trial59141 ran immutable `2318901`. First stage:10 HTTP requests,
256 observations, three completed meeting bodies, intentional exit75. Fresh
same-job process:8 HTTP requests,254 observations, one additional body committed,
intentional exit76 with `wispr_recovery_verified=true`. Aggregate:18 requests and
510 observations. These observations include repeated listing inventory, not510
unique meetings. The prior three body scopes retained the same cycle/revisions;
the probe rejected any attempt to fetch their identities again.

This proves per-meeting recovery through the actual source/orchestrator/store,
not a full-source traversal, new-cycle refresh, exhaustive discovery, deletion
handling or Almanac owner attestation. A failed meeting may repeat its own ranges;
all bodies remain partial because provider raw editor data is unavailable.
Parent independently verified zero live schemas and private-live directories and
removed the immutable code archive. The earlier20-request failed batch remains
historical evidence; this successful trial had its own20-request cap. Safe results:
[evidence](evidence/wispr-recovery-20260930.json), committed `78e7f37`.

## Retained browser evaluation (explicit opt-in)

`evaluation.py capture --directory /absolute/new/private/directory` retains a
bounded Gmail + Drive sample for the local UI. It reuses `provider_lifecycle.child` and its production durable-page driver,
the production projector/converters, real local MiniLM/BM25 embeddings, and Vespa.
The default `canonical_capture.py` still always removes its schema/files. This
separate evaluation command requires the same private `sync_tests` Unix socket,
provider environment, and additionally `LIVE_DRIVE_PERMISSION_ID`. Each provider
trial is bounded to 180 requests, 500 records, 180 seconds and 50 MiB of blob writes;
one file is bounded to 10 MiB. Gmail uses `newer_than:1d smaller:100K`. Committed
pages remain searchable if a later page hits a bound; interrupted capture is explicit
in the printed lifecycle evidence and must not be represented as a complete sync.

Run from `backend` with `PYTHONPATH=.`. The named directory must not already exist;
it is created mode0700 with a mode0600 `manifest.json`. This contains service keys
and source identities and must never be committed or copied into public logs.
Failed evaluations also retain their explicitly requested directory/schema for
inspection and cleanup. API keys expire after one day. Native identity verification
happens before retained binding metadata is created.

Local dependencies are isolated Vespa on `127.0.0.1:8081/19071`, the repository's
MiniLM inference image on `127.0.0.1:9878`, and Redis on `127.0.0.1:16379`. Deploy
`vespa/app` with dimension384 before capture. Model inference is real; no model API
key or paid model call is necessary. PDF text extraction uses the existing local
extractor; scanned PDFs needing OCR are not claimed covered without an OCR service.

`evaluation.py serve --directory SAME_DIRECTORY` serves the actual Airweave API
router on `127.0.0.1:18081`, with its production container and persisted encrypted
organization API keys. It does not override authentication. Only DB session ownership
is redirected to the explicit isolated schema. No Temporal schedules or worker are
started. This is a bounded capture/search/read evaluation, **not full synchronization
or automatic Connect qualification**. Almanac must use its own disposable database,
explicit development actor, real source bindings and existing hosted HTTP proxy.
Development-token authentication is not proof of WorkOS login.

Cleanup after stopping the API: delete each evaluation sync's Vespa documents
through `VespaDestination.delete_by_sync_id`, drop only the manifest's validated
`canonical_eval_<32 hex>` schema from the explicitly private `sync_tests` database,
and remove the exact private evaluation directory and provider-environment file.
The disposable evaluation containers can then be removed by their exact names;
never run global Docker pruning or alter the product database. PostgreSQL WAL and
Docker filesystem blocks may retain deleted bytes until those disposable resources
are destroyed. The private corpus remains on this machine until that cleanup.


Verified local evaluation, 2026-09-30 (before subsequent extraction repairs):

- Real MiniLM 384-dimensional and BM25 inference passed; Vespa schema was deployed.
- Actual API-key middleware denied absent keys (401), invalid keys (403), and
  admitted the persisted organization keys (200). No context override was used.
- Gmail: 50 originals from the explicit one-day/100 KiB filter, 47 published text
  representations, 6 original blobs verified by HTTP SHA256. Three projections
  remain pending: two native MIME size discrepancies and one image-conversion failure.
- Drive: 4 committed originals before a file exceeded the 10 MiB trial limit;
  3 published representations, 5 original blobs verified by HTTP SHA256. Content
  includes native Google Doc data and DOCX/PPTX/XLSX; no PDF was present in this sample.
- Exact record reads and keyword search returned actual retained records for both
  providers. This does not establish full-account coverage, scheduling recovery,
  WorkOS login, cloud deployment, or overall search quality.

Use `evaluation.py project --directory SAME_DIRECTORY` to process pending stored
records without provider access. It traverses all pending pages once; failures stay
pending and are reported, rather than being silently marked indexed. The service
reads current SQL publication state, so reprojection becomes visible without an API
restart.


`evaluation.py extend --directory SAME_DIRECTORY --provider google_calendar|slack`
adds one native-attested source while preserving existing keys and source identities.
Extensions run sequentially because the private manifest has one owner. Calendar
uses its verified primary ID and a seven-day occurrence materialization window;
original masters are also retained by the production connector. Slack uses the
verified workspace/user pair, with `external_user_id` retained for the Almanac binding.
Source construction starts at the current canonical search metadata pipeline version;
older retained trials upgrade using `plan_reprojection` followed by `project`.

Prepared chunks now prepend up to 128 tokens of exact preceding source context,
reserving space inside the 8192-token limit. Overlap stays within one prepared part;
character offsets still address its unchanged original text. Chonkie 1.5.5's
`OverlapRefinery` was inspected: merged context leaves start/end offsets unchanged,
and token-mode decoding can cut Unicode. The existing Unicode-safe tokenizer owns
the bounded suffix instead. There is no new parser, storage or index field.

Existing published generations do **not** gain overlap from a code update. Use the
existing operator to advance that sync's current pipeline version before retained-only
reprojection; never silently replace a published generation under its old version.
No corpus was reprojected for this change. The real Wispr boundary diagnostic and
focused tests show source-faithful short-phrase coverage, not arbitrary-length phrase
guarantees or improved hybrid ranking. See
[`chunk-overlap-preparation-20261002.json`](evidence/chunk-overlap-preparation-20261002.json).

The Calendar trial retained 183 visible originals (190 observations including removals)
and projected every record. The Slack trial stopped at its record budget after 56
requests: 460 committed originals were all projected, with capture still explicitly
active/incomplete. Wispr's separate private broker-attested trial stopped on an upstream
tool error with a rate signal after 18 requests/284 observations; no native account
identity or Almanac binding is claimed for it.

Drive's second bounded trial resumed the same unfinished full-scan cursor with a
200 MiB file/512 MiB total blob limit after checking at least 10 GiB free disk. It
retained 14 PDFs among the captured originals. The source job completed and SQL showed
a complete full cycle with a promoted checkpoint. The former live-helper assertion
incorrectly assumed any loaded cursor implies a changes pass; it now distinguishes
unfinished full-scan continuation from subsequent/resumed changes while still requiring
checkpoint promotion. This was a harness assertion failure, separately recorded from
the actual completed source job. PDF text/read verification remains a separate check.

## Fixed fourteen-day retained Gmail qualification

`gmail_declared_range.py` runs one explicitly authorized fixed fourteen-day query
in a new private schema and blob directory, preserving other corpora. Run with the
fork Python environment, `PYTHONPATH=.` from `backend`, and the existing private
Unix-socket `CANONICAL_TEST_DATABASE_URL`. Capture requires `--inputs` (existing
private provider credential file), `--attestation` (independent saved native binding
attestation), and `--mailbox` (the explicitly authorized principal). Both files must
be current-user-owned and private. The requested principal must match both saved
sources; the existing auth provider rechecks the exact account/user/auth-config,
then Gmail profile verification precedes source authentication and capture.

The fixed epoch query is created once from runtime UTC, with no rolling query,
size, or category filter. Gmail documents epoch seconds as the timezone-precise
alternative to calendar dates, which use PST midnight
([Gmail filtering](https://developers.google.com/workspace/gmail/api/guides/filtering)).
Completeness means the provider query's observed enumeration completed; this is
not a whole-mailbox or exact snapshot/boundary guarantee. Runtime caps are 2,000
records, 4,000 native requests, 30 minutes, and 512 MiB aggregate blob writes. The
existing production MIME limit remains 200 MiB and native JSON remains 32 MiB.
Exceeding a bound stops partial capture; it does not narrow the coverage filter.

The operator retains a mode0700 directory with mode0600 manifests and evidence;
these include original identities and local service routing and must never be
committed. No encrypted production credentials, AWS resources, hosted schedules,
or real product account rows are created. `--verify PRIVATE_MANIFEST` hashes all
immutable blobs and traverses the actual mail SQL pages without provider access.
`--prepare-text PRIVATE_MANIFEST` uses the existing mapper, MIME conversion, strict
text builder, and revision/pipeline-bound prepared-body CAS. It stops before
chunking, embedding, or index feed; failures remain explicit. This is a manual
runtime-stage qualification, not automatic capture-to-preparation activation.

The [2026-10-02 evidence](evidence/gmail-declared-range-20261002.json) records 456
captured messages, 570 native requests plus one binding metadata read, and 72
verified blobs. Capture alone had 456 unavailable bodies. Manual preparation made
453 bodies ready in 4.691 seconds; the other three contain no visible text in any
retained MIME alternative and remain explicitly unavailable. Through an isolated
installed CLI and the actual Almanac/fork HTTP readers, five date pages returned
456 unique messages, `kushagra` returned 68, and a returned thread read completed.
These calls made no provider requests. Match counts are observed, not a relevance
score. The companion actor and binding were synthetic; production auth is not
qualified.

Allocated PostgreSQL tables/indexes/TOAST totalled 19,578,880 bytes, including
empty-schema overhead and change history; filesystem blobs used 5,671,657 bytes.
Original JSON text (13,274,324 bytes) and prepared-body payload (1,465,083 bytes)
are separate payload measurements, not allocated database sizes. This fixed
filtered scan has no Gmail history checkpoint: its cost is not the future
unfiltered delta-sync cost model. All private corpus/schema data remains available
for subsequent relevance checks and must be cleaned only by exact manifest scope.

The [real retained Wispr latest-five proof](evidence/wispr-latest-five-real-cli-20261002.json)
uses the preserved 260 meeting bodies in a fresh local schema/blob copy and fresh
Vespa collection. Current additive migrations backfill native starts in the copy;
original payload/revision/blob-reference digests stay unchanged. Only the actual
five latest bodies were projected: five publications, 31 documents, 262,790 bytes
of retained text artifacts including metadata, and 11.122 seconds including clone,
backfill, and existing local MiniLM/BM25 inference. No shared schema was redeployed,
provider was called, or paid model was used. This direct bounded projector proof
does not qualify automatic Temporal activation or a native Wispr principal.

The installed candidate CLI listed those same five in native-start order, then
used each returned record reference and text continuation command to read both
parts with matching revision, character count, and SHA. All five native notes
fields are explicitly empty in the saved source ranges; their transcripts contain
153,413 characters in total. Four transcripts required multiple CLI reads, with
up to six ranges. The 27 CLI commands took 0.486–0.800 seconds each including
startup. The owner/account binding is synthetic, coverage is the captured subset,
and no account-wide recall or released-product claim follows from this proof.

### Forty-two local dates and native year-zero creation values (October 2)

`evidence/calendar-42day-capture-20261002.json` records one real selected primary
calendar, September 27 through November 8 in America/Los_Angeles: 42 local dates,
1009 elapsed hours across DST. The retained corpus contains one calendar, 169
master/event records and 107 expanded occurrences; all three capture scopes and
the durable cycle were independently verified complete. No embeddings ran.

The wider window exposed 37 native occurrences with creation value
`0000-12-31T00:00:00.000Z`. Capture now retains that exact value in original JSON
and sets only the normalized creation timestamp to unknown; event start/end and
update times are unchanged. Other invalid timestamps still fail validation.
The interrupted occurrence capture resumed in the same private corpus after the
fix. The original post-run harness incorrectly assumed that loading any cursor
meant a delta cycle; it now distinguishes resuming incomplete full capture from
starting a changes pass after a completed cycle. Final SQL verification did not
recapture the provider. Actual installed-CLI month-grid qualification is separate.

## Real retained Gmail Temporal projection

The [2026-10-02 workflow evidence](evidence/gmail-real-temporal-projection-20261002.json)
uses a current-migration clone of the preserved 456-message corpus, a fresh Vespa
collection, the actual `ProjectCanonicalRecordsWorkflow` and activity, the actual
Gmail source capability, real mapper/converter/chunker, and existing local
MiniLM/BM25 inference. A local Temporal test server time-skips retry waits; SQL,
model execution, immutable artifacts, and Vespa feed remain real. No provider or
paid model was called, no shared schema was redeployed, and original digests stayed
unchanged. This qualifies pending drain and retries, not production worker
activation or automatic capture-to-child-workflow dispatch.

The first execution published nothing: offline model resolution still attempted
remote Hugging Face asset lookups, all blocked by the application network guard.
After diagnosis, the approved retry used the exact already-cached chunker weights
through their explicit local URL. A new execution drained the same captured clone
without recapture. It published 453 messages / 965 documents in 181.742 seconds,
then failed truthfully after retrying the three known empty bodies. The 21 activity
pages include two failed-only retry sweeps. Prepared bodies remain ready for 453;
three remain explicitly unavailable. Final stdout reporting encountered Decimal
serialization after saving evidence; the workflow was not rerun for reporting.

Actual candidate retrieval and current-revision reads then exercised local keyword,
semantic, and hybrid search. `kushagra` returned 74, 100, and 100 candidates. A plain
prose substring independently verified to occur in exactly one published body found
that message at ranks 1, 38, and 2. An earlier punctuation/numeric substring returned
zero keyword candidates; its cause remains an explicit query UX follow-up. The
100-candidate windows are bounded and these observations do not establish general
relevance or recall. No ranking was tuned against this fixture and no reranker ran.

The retry made 965 local dense requests and 965 Vespa feeds, with 21,919,353 bytes
of document payload and 1,887,252 bytes of retained text artifacts. One dense call
per chunk is measured cost/latency evidence for future batching work. Allocated
PostgreSQL tables/indexes/TOAST totalled 25,239,552 bytes including failed-attempt
history; filesystem storage was 7,558,909 bytes including 5,671,657 copied original
blob bytes. Published prepared-body payload was 1,465,083 bytes. These measurements
are distinct and must not be extrapolated to whole-mailbox or delta-sync costs.


### Known-empty Gmail bodies: completed pending drain

The [empty-body follow-up](evidence/gmail-empty-body-projection-20261002.json)
keeps failed/unavailable HTML conversion distinct from successful conversion with
no visible text. Metadata stays searchable; the retained content boundary excludes
that metadata from email body reads. Independent review verified this boundary.
The actual Temporal workflow drained only the remaining three records in 2.115
seconds: all 456 records are now published as 968 documents, with zero pending
conversion failures. Three real retained reads returned empty text, zero content
characters and no continuation; SQL prepared bodies were empty strings, not NULL.
Original payload/revision/blob-reference digests remained unchanged. Three local
dense calls and three Vespa feeds ran; no provider or paid model calls occurred.
The final operator stdout serialization failed after durable evidence was saved;
this is a reporting defect, not a workflow failure. No recapture or repeated
projection was performed to fix reporting. Production worker activation and
capture child-workflow dispatch remain unqualified.


### Slack checkpoint progress, 2026-10-02

A surviving retained evaluation had 79 channels, 381 messages and an active
checkpoint; its failed job had hit the record budget. Its pinned native
workspace/user fingerprint matched current capture, but its schema lacked current
columns. One isolated current-schema copy preserved originals, scans and cursor.
A new writer used the actual canonical capture driver against the same active
cycle. The original corpus digest remained unchanged.

The bounded 600-second pass retained **80 channels and 1,376 messages**, including
**995 new message IDs**. It made 136 capture proxy requests, including identity
and retries, plus one separate broker metadata read. Nine observed HTTP 429
responses each supplied a 60-second Retry-After. The deadline cancelled the job;
the cycle remains active with 23 complete scans and one collecting scan. SQL
proved that the prior collecting message scan kept its scan and sweep IDs,
advanced its revision and continuation, and completed. The isolated copy and
advanced checkpoint are preserved. Another pass needs a new writer job because
the cancelled job is terminal; it must reuse this copy and its current checkpoint.

File capture stayed disabled to preserve the immutable cycle configuration.
Ninety-one messages contain metadata for 101 distinct native file IDs; these
references are **not downloaded assets**. The 995 new messages are captured only:
zero new projection generations or publication rows were created. The 381 prior
message publication rows were copied; their remote index was not requalified.
No model, index feed, provider write,
auth change or scheduling activation ran. Full source completion and separate
file retention remain unqualified. The copy retains original index routing IDs;
future projection must first isolate its collection namespace.

See `evidence/slack-checkpoint-progress-20261002.json`.

## Bounded full Gmail OCR repair

[Sanitized repair evidence](evidence/gmail-full-ocr-repair-20261002.json) records
real existing Temporal workflow/activity/projector execution on the exact sixteen
pending messages from a preserved full capture. It stopped at the 600-second cap
after fourteen new publications: 2,032 complete bodies, two explicit gaps, and
4,202 current Vespa documents independently matched to SQL manifests. Earlier
2,018 generation identities and the frozen baseline census remained unchanged;
original SQL/blob integrity was verified. Provider and paid-model calls were zero.
The private post-repair census is a separate immutable artifact, not an overwrite
of the baseline. This gate does not qualify automatic capture-child dispatch or
retrieval relevance. Remaining conversion failures and local OCR language/scope
limits are described in [local OCR](../../docs/local_ocr.md).

## Exact-two Gmail body publication with explicit attachment gaps

[Sanitized runtime evidence](evidence/gmail-exact-two-partial-publication-20261002.json)
records the existing Temporal workflow/activity/projector on only the two
remaining original messages, using commit `f6c89e4`. One bounded sweep published
both in 124.84 seconds: all 2,034 bodies are now available, with one complete
parent and one partial parent whose vector-only PDF explicitly reports
`failed` / `conversion_failed`. The workflow truthfully ended `FAILED` for that
attachment; automatic `skip_failed` recovery has no admitted pending work.

Independent verification checked 2,125 current text artifacts and 4,224 Vespa
documents against SQL manifests, plus actual body literal queries, original
thread reads, derived content reads, publication CAS gates, and current-body
Vespa keyword hits for both messages. All four original PDFs remain downloadable
and hash-valid. The earlier 2,032 publication pointers, body provenance and bytes,
source SQL/blob digest, and frozen comparative censuses are unchanged. The new
2,034-body census is separate; no earlier relevance population was regraded.
Provider and paid-model calls were zero. Frozen comparative files and source archives were unchanged. After projection,
only the owned loopback fork/product readers were refreshed to separate archived
`f6c89e4`/`0a192fd9d` code, preserving the same synthetic database, account and
authorization token. Actual HTTP reads, literal/shared keyword search membership,
and bad-credential/wrong-account rejection passed for the new outcomes.
The standalone browser generated contract has a separate companion-owned refresh.
This gate does not claim retrieval relevance or automatic capture-child dispatch.
