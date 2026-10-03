# Owned source store staging

Status: application manifest prepared; dependencies and a running cluster are
still required. Local manifest validation is not a deployment or capacity test.

The target is Porter project 19566, cluster 6108, AWS us-east-1. No production
cutover. Keep the full incremental bill below $500/month before credits. The
manifest requests 1.5 CPU and 3 GiB for application processes only; PostgreSQL,
Vespa, Temporal, embedding inference, Redis, Svix, disks and networking are
additional. Confirm the total node allocation and price before provisioning.

## One release image, two processes

`porter.yaml` builds the backend once and runs the API and Temporal worker.
The API is cluster-private, with no public load balancer. Initially verify it
through an authenticated operator port-forward; the Almanac bridge needs an
explicit reachable, authenticated network path before it can use this endpoint.
Do not mistake `private: true` for a private load balancer reachable from other
clouds. Do not expose PostgreSQL, Vespa or Temporal publicly.

The entrypoint executes Uvicorn directly so termination reaches the application.
It never prints database URLs, runs migrations, or creates organizations. Worker
shutdown gets 90 seconds inside a 120-second pod grace period. Interrupted jobs
must recover through Temporal and the existing writer fencing, not an assumption
that shutdown always finishes. Test termination under actual capture before
claiming recovery is verified in this deployment.

## Prerequisites

Provision isolated PostgreSQL databases for application and Temporal state;
private Redis, Temporal and Svix; persistent Vespa with the matching application
package; configured Cohere inference; and a private S3 bucket for canonical bytes.
Use the repository's existing dependency configurations as inputs, not its local
Compose file as a production deployment: that file enables local auth and exposes
ports. Dependency image pins, persistent volumes, backups and a restore exercise
remain deployment work. This manifest gates API readiness on PostgreSQL, Redis
and Temporal reporting up. Missing/skipped critical probes do not pass. Verify
index, worker polling and capture readiness separately; API dependency health
does not prove those workflows work.

When Temporal is critical, failure to initialize its system schedules aborts API
startup so the existing process supervisor can retry. Optional Temporal setups
retain their permissive startup behavior. This avoids an API stuck unready with
no initialized Temporal client after a transient startup outage.

Create the Porter environment group `almanac-source-store-staging` with only the
owned service's values. Supply database connection settings, Redis endpoint,
Temporal endpoint/namespace, Vespa endpoint, COHERE_API_KEY,
STORAGE_AWS_BUCKET, SVIX_URL, SVIX_JWT_SECRET and the application's required
encryption/signing secrets. Inspect `backend/airweave/core/config/settings.py`
for exact current fields. Do not copy the main product database from Doppler.
Use a least-privilege workload IAM role for this bucket and configure the Porter
AWS-role connection once that role exists. Do not add static root AWS keys.

OCR is optional. The worker starts without MISTRAL_API_KEY or DOCLING_BASE_URL.
Text extraction and source capture continue; a scanned document that needs OCR
retains its captured bytes and a failed, retryable projection instead of entering
search with missing content. Configure an OCR backend to index those documents.
This does not mean every PDF requires OCR: the PDF converter first tries local
text extraction. Image conversion requires an OCR backend.

The selected embedding recipe is Cohere Embed 5 Pro (`cohere_embed_v5_pro`),
1,024 dimensions, matching the retained-corpus qualification. Supply the key via
secret management; no local MiniLM service is part of this staging template.
FastEmbed and the semantic chunker still load public model artifacts; prewarm a
writable persistent model cache and qualify startup under the intended network
policy. Removing MiniLM does not remove those local preparation dependencies.

Cohere is metered separately from AWS. The configured account's $50/month cap is
not a credit-eligibility claim or an application-enforced per-import budget.
Reranking remains opt-in at the product query contract, not implicitly enabled
by supplying an embedding key. No generative-search capability is implied.

This template is for a fresh isolated index configured with the same model and
1,024-dimensional schema. Do not point it at an existing 384-dimensional index or
silently relabel stored vectors; changing models requires an explicit new index/
reprojection and verified cutover. Existing MiniLM CI fixtures remain unchanged.

## Local image build

`make build` builds the same backend Dockerfile used by CI, without publishing or
starting services. Use `make build IMAGE=almanac-source-store:COMMIT` to choose a
tag. CI separately checks packaged Python and entrypoint syntax; a successful
build does not establish dependency startup or a deployment.

## Database setup and deployment

Validate locally from the repository root:

```sh
make validate-deploy
```

Against the explicitly verified isolated database, run the committed image as a
one-off operator job with migration credentials:

```sh
alembic upgrade head
python -m airweave.db.bootstrap
```

See `backend/SERVICE_AUTH.md` for organization/key provisioning. Store each key in
secret management. Runtime database credentials should not have migration
privileges. Migrations are intentionally not automatic predeploy hooks: an image
rollback does not reverse database changes.

After dependencies, IAM, secrets, cluster quota and the reviewed dev commit are
verified, use the existing Porter credentials without printing them:

```sh
doppler run -p almanac -c dev -- porter apply --project 19566 --cluster 6108 --dry-run -f porter.yaml
doppler run -p almanac -c dev -- porter apply --project 19566 --cluster 6108 --wait -f porter.yaml
```

The Doppler invocation authenticates the operator CLI; it does not authorize
injecting every local variable into the application environment group. Build and
deploy from a clean reviewed dev checkout, not a feature branch with dirty work.
Record the resulting image digest and deployed commit.

Before calling staging working, prove unauthorized requests fail; verify account
scope; capture actual provider records and bytes; read them from a new process;
query the live index; edit/delete disposable source fixtures where permitted;
interrupt and resume a worker; and restore isolated database/blob backups. Record
provider limitations separately from successful checks. Never infer full source
coverage from one healthy request.

Manifest syntax reference: https://docs.porter.run/applications/configuration-as-code/reference
Private service semantics: https://docs.porter.run/applications/configuration-as-code/services/web-service

## Private local worker listeners

Set `WORKER_BIND_HOST=127.0.0.1` for a host-local worker. This binds both its
health/metrics/drain control server and Temporal SDK metrics exporter to loopback.
The default remains `0.0.0.0` for container networking. Binding limits network
reachability; it does not authenticate these endpoints, including `POST /drain`.
Keep container worker ports private. API listener and API metrics bindings are
configured separately.

## Shared process capacity

The checked-in manifest explicitly sets `DB_POOL_SIZE=8`,
`DB_POOL_MAX_OVERFLOW=0`, `TEMPORAL_MAX_CONCURRENT_ACTIVITIES=4`, and
`TEMPORAL_MAX_CONCURRENT_WORKFLOW_TASKS=8`. These are initial bounded budgets,
not measured throughput or freshness guarantees. SQL pool capacity no longer
comes from `SYNC_MAX_WORKERS`; that setting remains per-sync record/batch
processing concurrency (two in this manifest), not worker replicas or whole
pipeline concurrency.

Each process can expose eight application SQL connections plus one independent
health connection. In tenant mode the application budget splits into seven
content connections and one control connection, with no overflow. One API plus
one worker therefore has a configured ceiling of **18**; one API plus two worker
replicas has **27**. Count each additional Uvicorn process, replica and rollout
surge separately, and add migrations, operators, Temporal persistence and other
clients to the database's total budget. Pools grow lazily: ceilings are not
observed connection usage. Connections wait at most the existing 30-second pool
checkout timeout; this is not a whole-operation deadline.

Each worker admits at most four remote Temporal activities at once. Two worker
replicas admit eight total, independently of the API process. Eight workflow
**tasks** per worker bounds execution of workflow decisions, not the number of
waiting durable workflows. The existing eight workflow pollers and sixteen
activity pollers control long polling, not admission. A real installed-SDK test
with those poller values admitted four blocked activities and left the fifth
queued until a slot was released. No local activities or Nexus handlers are
registered; those unused SDK slot kinds are not an additional processing path.

Activities can perform multiple internal operations. Canonical capture uses
ordered durable batches; preparation uses its existing projector; legacy syncs
have their own record semaphore, provider/model limits and executor. Four
activity slots do not establish a whole-pipeline four/eight-record bound.
Retain provider backoff, rate limits and record bounds; measure SQL checkout
waits, backlog and per-stage memory before increasing capacity.

Sync admission now uses one short transaction under the existing sync-row lock.
The internal job UUID is stable for organization, sync, Temporal namespace/run/activity
identity. Exact retries reuse the current-generation job; manual and scheduled
callers cannot both admit a new active job. Failed/cancelled operations are failures,
not successful skips. Ordinary busy schedules skip; forced busy schedules defer
through SDK backoff without retaining an activity slot or SQL session.

New workflow histories use thirty-second attempts and constant thirty-second retry
intervals. Ordinary admissions have at most three attempts within two minutes;
forced admissions have a sixty-five-minute schedule-to-close bound. Explicit
admission errors are nonretryable; SDK timeout uncertainty can retry the same ID.
The workflow patch preserves previous command policy when replaying old histories.
Event publication remains best effort and cannot fail an admitted job's reply.

Exhausting retries after commit can leave PENDING until existing recovery runs:
API startup ensures the singleton cleanup schedule every 150 seconds, which finds
provider PENDING jobs older than three minutes and transitions them to CANCELLED.
Admission failure is outside the workflow's execution transition handler. Cleanup
availability is therefore a real deployment prerequisite, not an unconditional
latency guarantee; a failed cleanup leaves the pending job visible and blocking.
No new recovery queue or job metadata was introduced.

Temporal Python SDK 1.22.0 / Python 3.13 qualification used its actual default
sandbox and callable result decoder. The production typed activity initially
failed decoding postponed dataclass fields (`NameError: Any`); eager annotations
in `activity_results.py` resolve it without widening sandbox passthrough or changing
the wire result. The four-slot test uses production admission and retry policy:
four busy attempts close SQL contexts, a queued completion activity runs, then
all four retry with unchanged distinct operation IDs. A separate ordinary run
commits a pending job, loses its reply through the real thirty-second SDK timeout,
and recovers the same job on attempt two. PostgreSQL qualification uses an isolated
schema for committed replay, concurrent admission, and current authority fences.
Focused results: 23 admission/activity/workflow checks passed (2.39s); the
four-slot SDK case passed (3.10s), ordinary real-time timeout case passed (32.71s),
and 51 existing sync service checks passed (2.97s). An additional actual SQL
cleanup-entrypoint test passed (3.29s): it cancels an eligible stable-UUID pending
job, permits a subsequent admission, and leaves a recent running job untouched.
The cleanup fixture supplies the absent external cancellation response; SQL
selection and state-machine updates are real. These isolated-schema tests do not
requalify deployed role grants or schedules. These are local SDK/SQL proofs,
not a deployed restart or provider throughput test.

Local verification: twenty-one configuration/wiring/shutdown checks passed in
focused runs; the real
SDK admission test passed against a disposable Temporal test server. No provider,
model, production database, running fixture configuration or deployment changed.
The SDK capacity knobs are documented by
[Temporal's Python worker](https://github.com/temporalio/sdk-python/blob/main/temporalio/worker/_worker.py).
