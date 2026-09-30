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
