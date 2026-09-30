# Durable live capture proof

`canonical_capture.py` is opt-in and makes read-only Gmail/Drive provider requests.
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
only one provider; default is both. Only selected provider account variables are required.

Run from `backend`, using your approved secret injector:

```sh
PYTHONPATH=. .venv/bin/python tests/live/canonical_capture.py
```

The source sample stops after at least three records and one blob, or 25 records.
Gmail uses `newer_than:7d smaller:5M`; capture/download limits are reduced to 10 MiB
for this probe. A sample without a blob fails. No scope-completion observations
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
