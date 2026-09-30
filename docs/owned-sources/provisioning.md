# Owned account provisioning

This backend-only API is implemented; it has not been deployed or qualified against live Temporal/provider services. Almanac still needs durable delivery from its account authority before Connect automatically provisions capture.

`PUT /owned-sources/{account_uuid}` commits a desired account generation and reconciles it. `GET` reads the committed delivery state. Both require an organization API key, whose existing broad source-management authority remains unchanged. Never expose this key or endpoint directly to browsers. The server fixes the namespace to `almanac`.

An active request contains `generation`, `state: "active"`, and `source` with `provider`, `expected_identity`, `collection`, `auth_provider`, `connected_account_id`, `auth_config_id`, `user_id`, `config`, and five-field `cron`. Composio selectors contain no provider tokens. The configured auth-provider connection must belong to the request organization. Native validation checks the expected identity before admitting capture. Supported providers are Gmail (mailbox), Google Calendar (primary calendar ID), and Google Drive (permission ID). Slack and Wispr identity contracts remain future work.

A disconnect contains only `generation` and `state: "disconnected"`. Active credential rotation preserves source/sync IDs and retained originals. Disconnect is a tombstone for this account UUID, matching Almanac's account lifecycle; a subsequent new Connect uses a new UUID. Deletion of retained records is not part of disconnect.

Same generation and identical payload reuse the same source, sync and initial job. Changed payload at the same generation or an older generation returns 409. A higher generation cannot change provider, native identity or collection. Ordinary source update/delete routes reject provisioned sources to prevent bypassing this lifecycle.

The response includes account/organization IDs, current/observed generations, source/sync IDs, expected identity and `pending`, `ready`, or `disconnected`. `ready` means native identity was verified and scheduling/start was acknowledged; it does **not** mean records were captured or indexed. Read source coverage for that evidence. Do not publish an account binding merely because IDs exist in a pending response.

## Recovery ownership

Database source creation and desired-generation changes commit atomically before external execution. Provider verification occurs outside the relationship lock and must pass a generation comparison afterward. Jobs copy the current generation under the sync lock; unverified, paused and stale managed generations cannot enter capture. Canonical write fences prevent already-running older jobs from committing after rotation/disconnect.

The bounded 20-second execution phase serializes cancellation and scheduler changes under the relationship lock. Timeouts release the lock without acknowledging cleanup; cancellation job IDs and the desired generation remain committed for retry. Deterministic schedules are removed even when their database link is missing, then recreated with current generation arguments. The initial workflow uses its durable job UUID and rejects duplicate workflow reuse.

After an uncertain response, Almanac must retry the identical PUT until acknowledged, or deliver a newer generation. GET does not retry side effects. This API introduces no second queue or autonomous reconciliation worker: the planned Almanac durable account intent owns delivery/retry. Native verification and infrastructure errors may leave a pending intent, and live operational retry/alert behavior remains to be integrated there.

## Local verification

The provisioning tests use migrated PostgreSQL and actual source/sync creation, with synthetic identities and mocked provider/Temporal I/O. They cover lost replies, transaction rollback, rotation cleanup timeout, disconnect racing native verification, organization/API-key boundaries, and legacy mutation rejection. A separate actual TemporalScheduleService test uses fake remote handles to prove orphan schedule replacement and relinking. These tests are not live deployment qualification.

## Reversible pause

`state: paused` accepts no source specification, fences existing jobs and removes
schedules using the same durable cleanup as disconnect. It retains the source/sync
IDs and original protected SourceConnection config. A higher-generation active
request revalidates that exact provider/native identity/collection before rotating
credentials and resuming. The stop request does not replace that authority.
Inactive responses return the preserved expected identity when a source exists.
An initially paused account may create its first source later; disconnected is
terminal even if no source was ever created. Pause is for configuration disable;
disconnect is for account removal.

Pause qualification: seven real PostgreSQL provisioning tests passed (including
active → paused → same-ID active, mismatched native identity rejected, terminal
existing/initial disconnect, and initially paused → first active). Provider native
validation and Temporal calls are simulated; no live account operations occurred.
Log: `/tmp/provisioning-pause-tests.log`. Migration 0010's state constraint was
extended before deployment; deployed schemas would require a new migration.
