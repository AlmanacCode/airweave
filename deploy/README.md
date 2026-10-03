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
package; local MiniLM inference; and a private S3 bucket for canonical bytes.
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
Temporal endpoint/namespace, Vespa endpoint, TEXT2VEC_INFERENCE_URL,
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

The selected local MiniLM service is a startup dependency for both API and worker:
composition checks its health endpoint. FastEmbed and the semantic chunker also
load public model artifacts; provide a writable persistent model cache and verify
startup with the intended network policy. OCR, generative search credentials,
Cohere reranking, frontend and Temporal UI are optional for owned indexed search.

The manifest uses local embeddings to avoid assuming third-party model credits.
Semantic quality and resource use still need measurement. Do not claim generative
search is operational without separately configured model credentials and a live
search test.

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

A remaining lifecycle gate is forced full sync job creation:
`CreateSyncJobActivity` currently holds an activity slot and an outer SQL
transaction while polling active jobs for up to an hour, opening another session
for each poll. After restart enough waiting cleanup activities can occupy all
slots needed by queued run activities. Raising the pool/slot limits cannot prove
fair recovery. Replace that wait with a short, identity-checked admission attempt
and Temporal retry/backoff only after job creation has a stable idempotent
identity; broad retries today can duplicate committed jobs after lost replies.
The configuration slice does not claim that gate is closed.

Local verification: twenty-one configuration/wiring/shutdown checks passed in
focused runs; the real
SDK admission test passed against a disposable Temporal test server. No provider,
model, production database, running fixture configuration or deployment changed.
The SDK capacity knobs are documented by
[Temporal's Python worker](https://github.com/temporalio/sdk-python/blob/main/temporalio/worker/_worker.py).
