# Private staging review templates

**Prepared, not deployed.** These files deliberately contain `REQUIRED_*` values.
Do not apply this directory. Registry retrieval and Helm rendering were checked on
2026-09-30; no Kubernetes admission, service startup, restore, capacity or cost
claim follows from that check. The $500 monthly cap and quota approval remain open.

## What is pinned

`../versions.lock.json` records retrieved chart archives and SHA-256, image indexes
and architecture-specific digests. These manifests choose **linux/amd64**; confirm
actual nodes before use. Multi-platform indexes/arm64 alternatives are evidence,
not a reason to silently change the deployed platform.

| Component | Published artifact |
| --- | --- |
| Temporal | Official chart 1.7.0, server/admin-tools 1.32.0 |
| PostgreSQL | CNPG chart 0.29.1 / operator 1.30.1; PG 16.15-standard-trixie |
| Redis | 7.4.11-alpine3.21 |
| Svix | Server v1.101.0, not the unrelated SDK version |
| Vespa | Existing CI-tested image index, pinned amd64 child |
| MiniLM | all-MiniLM-L6-v2 inference image, pinned amd64 child |

Primary evidence: [Temporal chart index](https://temporalio.github.io/helm-charts/index.yaml),
[CNPG chart index](https://cloudnative-pg.github.io/charts/index.yaml),
[CNPG image requirements](https://cloudnative-pg.io/documentation/current/image_requirements/),
[Svix pinned CLI](https://github.com/svix/svix-webhooks/blob/server-v1.101.0/server/svix-server/src/main.rs).
Registry manifests were retrieved from Docker Hub/GHCR by the exact references in
the lockfile (`reference` may be a tag or an index digest). Image bytes were not pulled or executed during preparation.

## Required facts before deployment

- Dedicated dependency namespace, actual Kubernetes version/CNPG compatibility,
  existing operator/add-on ownership, node architecture and quota. Reuse a maintained
  PostgreSQL add-on if suitable; do not install a second CNPG operator by accident.
- Verified persistent storage class, encryption, reclaim policy and volume/AZ
  placement; working backup destination/IAM and a restore drill. `postgres.yaml`
  has **no backup policy yet** and provisions only the initial source-store DB.
- Provision separate `source_store`, `temporal`, `temporal_visibility`, `svix`
  databases and schema/runtime roles. Existing-secret references are names, not
  credentials. Grant runtime DML/sequence access and default privileges from the
  schema owner; no runtime role owns schemas. Review grants against actual migrations.
- Secrets: `source-store-schema` (CNPG basic-auth username/password matching owner),
  Temporal `*-schema` / `*-runtime` (password), `source-redis` (password), Svix
  `source-svix-schema` / `source-svix-runtime` (environment described below).
  App credentials stay in its separately scoped staging environment group.
- Actual application namespace/pod identity, CNI NetworkPolicy enforcement, DNS and
  private Almanac-to-service connectivity. Policies deny incoming traffic then allow
  dedicated-namespace peers and explicitly selected application callers. They do
  **not** restrict egress or supply cross-cloud connectivity. Operator management,
  monitoring and bootstrap callers require reviewed ingress rules before use.
- S3 canonical bucket/workload identity, separate backup bucket, restricted egress
  and model cache. No secret values, IAM policies or public ingress are invented here.
- Full cost estimate including nodes, disks, load balancers, NAT/egress and backups;
  $500 is the cap, not an estimate already proved by these resource requests.

`ClusterIP` is not authentication. Redis has a password; Temporal and Vespa rely
on the reviewed private network boundary in this initial topology. Do not expose
those ports publicly. Stronger client isolation/TLS must be chosen if the cluster
is shared with untrusted workloads. The default-deny policy is not a substitute
for verifying the CNI enforces it.

## Bootstrap ownership

1. Resolve infrastructure facts above. Create secrets out of band; keep them out
   of rendered artifacts/version control. Provision DBs/roles before schema jobs.
2. Render Temporal schema **only** with `temporal-schema.values.yaml` overlay and
   `--show-only templates/server-job.yaml`. Never install the entire chart with
   that overlay: it would give server pods schema credentials. Use a distinct job
   name per release; the chart's runtime no-op schema job otherwise has the same
   generated name. Runtime values have `manageSchema: false` and DML credentials.
   History shards are deliberately 128 for this fresh staging proposal and become
   immutable at initialization. Review before the first bootstrap.
3. Run Svix `svix-server migrate` using schema secret, wait for successful exit.
   Runtime explicitly runs `svix-server` without `--run-migrations`; its upstream
   default launch script would run migrations on every startup. Both secrets need
   appropriate `SVIX_DB_DSN`, `SVIX_REDIS_DSN`, `SVIX_JWT_SECRET`,
   `SVIX_QUEUE_TYPE=redis`, `SVIX_CACHE_TYPE=none`. Same JWT secret; distinct DB roles.
   Do not copy Compose's allow-all webhook subnet whitelist into staging.
4. Start Temporal; create the dedicated Temporal namespace chosen for the app and
   the `SyncId` Keyword search attribute, then verify both. Do not mask errors with
   `|| true`. Application settings must match this namespace and private frontend.
5. Deploy the committed `vespa/app` package after replacing `{{EMBEDDING_DIM}}`
   with 384, `{{VERSION}}` with the release identifier, and the host in `hosts.xml`
   with the stable StatefulSet hostname. Wait for prepare/activate **and convergence**.
   `vespa/init-vespa.sh` is a Compose helper whose convergence timeout only warns;
   it is not a reviewed staging bootstrap job. The application package artifact
   and bootstrap job remain a required next step after hostname/release resolution.
6. Explicit application jobs own `alembic upgrade head`, then
   `python -m airweave.db.bootstrap` from the same immutable app image. Create the
   scoped service-key organization separately per `backend/SERVICE_AUTH.md`.
   API/worker entrypoints do not migrate or seed. Do not put schema credentials in
   `porter.yaml` runtime configuration. Existing maintenance schedule ownership stays.
7. Verify Redis persistence, inference `/.well-known/ready` (HTTP 204), Vespa feed/query, Temporal worker
   polling, source-store API authorization, scoped capture and restore. Probe and
   resource tuning remains necessary for Svix/Vespa; these templates don't claim it.

## Local rendering performed

Helm 3.22.0 was downloaded into a temporary directory from the official release,
verified against its published SHA-256 and used without changing the system PATH.
Chart archives were verified against the chart-index checksums in the lockfile.
With extracted chart paths and an isolated `HELM` executable:

```sh
"$HELM" template source-temporal "$TEMPORAL_CHART" --namespace validation-only \
  -f deploy/staging/temporal.values.yaml > /tmp/temporal-review.yaml
"$HELM" template source-temporal "$TEMPORAL_CHART" --namespace validation-only \
  -f deploy/staging/temporal.values.yaml \
  -f deploy/staging/temporal-schema.values.yaml \
  --show-only templates/server-job.yaml > /tmp/temporal-schema-review.yaml
"$HELM" template source-cnpg "$CNPG_CHART" --namespace validation-only \
  -f deploy/staging/cnpg-operator.values.yaml > /tmp/cnpg-review.yaml
```

Results: 13 Temporal objects, 1 schema-only job, 22 CNPG objects; YAML parsed and
all rendered workload images carried digests. Temporal emits four single-replica
server deployments, no UI/admin deployment or public service. Schema overlay
renders two schema init containers using separate schema secrets. Runtime render
contains no schema setup command. Initial rendering rejected obsolete top-level
`cassandra`/`elasticsearch`/`prometheus`/`grafana` keys; they were removed because
chart 1.7 no longer bundles these subcharts. This is why copied older values were
not accepted on faith.

Static service files were parsed and checked for image digests, private Services
and explicit unresolved namespace/storage inputs. These checks do not validate
CRD admission, deployment ordering, credentials or live vendor interoperability.

Parent review corrected the Vespa reference from an invalid digest-as-tag form
to repository@digest. Six static workload image references were independently
checked for syntax and exact agreement with the lockfile amd64 digests. This is
still not image pull, startup or Kubernetes admission proof.

### Readiness correction from actual-model CI

Run36725698424 reached the real inference container but its old `/health` probe
returned404, so model retrieval assertions did not run. Startup validation,
Compose, staging readiness and the integration probe now use
`/.well-known/ready`, matching the [upstream inference API](https://docs.weaviate.io/weaviate/modules/custom-modules).
The integration test requires204 and still verifies actual384-dimensional vectors
and retrieval afterward. Later CI36744440692 at2bb84cf passed the real-model
retrieval lane and image build. That verifies the disposable CI topology; it
does not establish Kubernetes admission, deployed readiness, capacity or restore
for these staging templates.
