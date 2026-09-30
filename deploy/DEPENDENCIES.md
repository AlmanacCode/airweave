# Private staging dependency plan

Reviewed against current code on 2026-09-30. No resources were provisioned by this
review. This is a proposed single-replica staging topology, not a capacity proof
or production availability design. `porter.yaml` deploys only API and worker.

## Decision

Keep one private dependency stack in the existing Porter cluster, with durable
PostgreSQL, Vespa and Redis volumes and canonical bytes in S3. Reuse the official
Temporal Helm chart and a supported PostgreSQL operator/add-on already available
in the cluster. Do not add a second scheduler, managed search product, GPU,
Docling service or paid OCR just to start the worker.

Prefer an existing maintained Porter PostgreSQL add-on if it supplies backups,
restore and version pinning. Otherwise use CloudNativePG's official chart and
single-instance Cluster resource. Do not create a bespoke database controller.
Use official service images for Redis, Svix and MiniLM with small private service
manifests. A single Vespa StatefulSet with its application package and PVC is
adequate for this bounded staging trial; a new Vespa operator and multi-node
production topology are unnecessary until capacity or availability requires them.

## Actual dependency boundary

| Component | Why it is needed | Initial sizing proposal, not measured |
| --- | --- | --- |
| PostgreSQL | Canonical authority, auth, projection ledger, Temporal state and Svix state. Separate databases/roles; do not share the product database. | 0.5 CPU / 2 GiB; 30 GiB durable volume |
| Temporal server | Existing capture/projection/maintenance workflows. Worker cannot poll without it. | One frontend/history/matching/internal worker each; combined 1 CPU / 2 GiB initial requests |
| Redis | Auth context cache, rate limits, pub/sub; Svix delivery queue. Enable persistence for queued delivery. | 0.1 CPU / 256 MiB; 5 GiB volume |
| Vespa | Derived searchable chunks; canonical SQL remains authoritative. | 2 CPU / 8 GiB; 60 GiB volume |
| MiniLM inference | Selected dense embedder. API and worker both check `/health` during composition. | 0.5 CPU / 1.5 GiB; no GPU |
| API | Existing Porter process. | 0.5 CPU / 1 GiB |
| Application worker | Capture, converters, chunker and sparse model share this process. | 1 CPU / 3–4 GiB; current manifest is only 2 GiB and needs measurement |
| Svix | External webhook delivery. Not a startup network prerequisite, but omission loses those events after retries. | 0.1 CPU / 256 MiB; state in PostgreSQL/Redis |
| S3 | Immutable captured blobs and separate database backups. | Private bucket, workload IAM, bounded retention for backups |

The proposed requests total about 5.7 CPU and 18–19 GiB before Kubernetes and
operator overhead. They are allocation hypotheses, not minimum vendor promises.
Use bounded capture concurrency and observe peak RSS, CPU, disk and retry lag.
Do not claim the current 3 GiB application manifest covers the dependency stack.

OCR is optional after the worker startup correction. A scanned PDF without OCR
is captured but its projection fails and stays pending; no partial text is fed.
Text-bearing PDF extraction still works locally. Images need OCR. Generative
model keys, Cohere, frontend, Connect UI and Temporal UI are also unnecessary for
owned indexed retrieval. Do not enable their product endpoints as verified.

Svix details matter: `SvixAdapter.__init__` constructs a client without a probe;
`InMemoryEventBus.publish` logs subscriber exceptions rather than propagating
failure to capture. This is not a durable outbox. Keep Svix for faithful existing
behavior, or separately design an explicit disabled-webhooks mode. Do not point
the service at a nonexistent Svix and describe delivery as reliable.

## Versions and bootstrap

Use PostgreSQL **16.15**, already exercised by integration CI. Pin the actual
operator-supported image digest before deployment. Vespa CI used:

```
vespaengine/vespa@sha256:5c30f5c41e7563498c4f925db6a837a3848f04726a3ed26aed4a7c8ab69f18fd
```

Deploy the repository's 384-dimensional application schema with that image before
API/worker traffic. The current Compose file's `vespa:8`, `redis:7-alpine`, Svix,
Docling and inference image tags are floating and are not a release lockfile.
Resolve and record their architecture-specific digests before applying manifests.

The official Temporal chart's current main source identifies chart **1.7.0** and
server **1.32.0**. This is a candidate requiring published-chart retrieval and
compatibility verification, not an already tested upgrade. The repository's
Compose `auto-setup:1.24.2` is old development configuration; do not reuse it as
production bootstrap. For a fresh isolated namespace, prefer current official
server/chart with PostgreSQL default and visibility stores, no Cassandra,
Elasticsearch, Grafana, public ingress or UI. Validate rendered chart values:
current chart layout uses `server.config.persistence.datastores`, which differs
from older copied examples. Freeze `numHistoryShards` at initial creation.

Bootstrap in this order:

1. Private networking, storage class, node quota and workload IAM; verify the
   actual total monthly cost before allocating nodes.
2. PostgreSQL with separate application, Temporal default, Temporal visibility
   and Svix databases/users. Keep migration/schema credentials out of runtime
   roles. Prove a backup restoration into a disposable database.
3. Redis persistence, Svix, Temporal schema jobs and namespace. Add the `SyncId`
   Keyword search attribute explicitly and verify it exists. Never retain the
   Compose init command's `|| true`, which can hide failed setup.
4. Vespa application deployment, inference health, model artifact cache. The
   Python sparse model and semantic chunker download public artifacts; prewarm a
   writable cache or verify restricted-egress startup. This is independent of OCR.
5. Application migration/bootstrap jobs, then API and worker from the same image.
   Existing system maintenance schedules remain owned by existing app startup.
6. Set critical health probes to `postgres,redis,temporal`; separately exercise
   Vespa query/feed and worker polling. Private service reachability from Almanac
   must be explicitly arranged; cluster-private is not cross-cloud connectivity.

No executable Helm values are included yet: cluster storage class, database
operator availability, published chart pin and IAM secret references are not
verified. Inventing them would produce an apparently complete but unusable
manifest. The next deliverable is a small dependency version lock and rendered
values against those facts, not a new infrastructure framework.

## Cost envelope and tradeoff

The supplied Porter baseline is **$201.22/month**, but its component breakdown
has not been verified here. Do not assume it includes dependency compute, disks,
NAT, an external load balancer or Porter usage-based fees.

An illustrative additional t3a.2xlarge is $0.3008/hour in AWS's published us-east-1
Linux table: **$219.58 for 730 hours**. Adding 120 GiB gp3 at $0.08/GiB-month gives
**$430.40 combined with that baseline**, before remaining charges. Reserving $40
for object storage, backup snapshots, logs and traffic yields **$470.40**. This
is a conditional envelope, not a quote or a $500 guarantee. Root disks, NAT,
Porter fees and autoscaling must fit the remaining allowance or replace this
proposal. Do not add this node if the supplied base already includes adequate
capacity. The account quota currently reported by the parent is 8 vCPU versus
16 required by its selected cluster configuration; no quota increase or node
creation was performed here.

T3 Unlimited is the default and can add CPU-credit charges. Standard mode avoids
that particular overrun by throttling after credits; the 40% baseline gives only
3.2 sustained vCPU on an 8-vCPU instance. Therefore this option favors modest
staging with bounded backfills, not sustained throughput. Compare a fixed-CPU
node quote using the actual existing cluster capacity before choosing. Keep
node autoscaling bounded and an AWS budget alert, while remembering an alert is
not a spending cap.

Separate managed PostgreSQL/Redis/Temporal/search would reduce some operations,
but fitting all of them plus the existing base under $500 has not been shown.
A separate VM running Compose is cheaper to understand initially but adds another
host, backup owner and network boundary while duplicating the existing Porter
platform. It is a fallback only if the cluster's fixed charges make the reviewed
budget impossible. Single-replica stateful services accept downtime; neither
option is production HA.

### Existing Porter configuration readback

On 2026-09-30, the existing cluster6108 form showed Karpenter cost optimization
enabled, max8 application vCPU, on-demand capacity,50GiB node disks, no instance
family restriction and no per-instance size restriction. No settings were saved.
This does **not** establish a dollar ceiling or the proposed t3a.2xlarge price:
the configured pool can select other instance families and sizes. The Porter
[node-group documentation](https://docs.porter.run/cloud-accounts/node-groups)
also describes fixed system and monitoring groups alongside the autoscaled
application group, and private-node egress through NAT by default. Their actual
sizes, node count and NAT charges have not been verified for this cluster.

Before retrying provisioning, reconcile the original$201.22 estimate against all
three groups, control plane, disks and networking. Select an explicitly priced
bounded instance set or fixed node shape that fits the full$500 cap. Max CPU8
alone is insufficient evidence. The quota preflight remains a separate gate.

## Evidence

Local implementation: `porter.yaml`, `docker/docker-compose.yml`,
`backend/airweave/core/container/factory.py`, `core/config/settings.py`,
`domains/embedders/config.py`, `domains/converters/registry.py`,
`domains/temporal/worker/__init__.py`, `adapters/event_bus/in_memory.py`,
`adapters/webhooks/svix.py`, `.github/workflows/owned-source-store.yml`.

Primary external references:

- [Temporal official Helm chart](https://github.com/temporalio/helm-charts)
- [Current Temporal chart metadata](https://raw.githubusercontent.com/temporalio/helm-charts/main/charts/temporal/Chart.yaml)
- [CloudNativePG bootstrap](https://cloudnative-pg.io/documentation/1.27/bootstrap/)
- [Vespa Kubernetes installation](https://docs.vespa.ai/en/operations/kubernetes/operations/installation.html)
- [Svix official server](https://github.com/svix/svix-webhooks)
- [AWS T3 pricing and credit behavior](https://aws.amazon.com/ec2/instance-types/t3/)
- [AWS EBS pricing](https://aws.amazon.com/ebs/pricing/)
