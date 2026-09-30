# Canonical search metadata upgrade

This is an operator action against an explicitly selected private source store,
not a product CLI or a capture command. It performs no provider reads/writes.

Deploy the expanded Vespa application schema **and only compatible API/workers**
before changing versions. An old worker can publish a new numeric version without
the new metadata; the number alone cannot prove compatible binaries. Stop/drain
old workers through the normal release procedure, never mid-capture ad hoc.

Dry-run (default), from `backend/` with the intended staging database configuration:

```
python -m scripts.reproject_canonical --organization ORGANIZATION_UUID \
  --sync SYNC_UUID --expect 1 --target 2
```

Inspect the exact authenticated source binding, current version, captured count
(including tombstones), and target-pending count. These counts describe captured
rows, not search hits or active visible rows. Add `--apply` only after review.
No command was applied while preparing this feature.

Apply locks the existing Sync row, verifies organization/source/collection scope,
and changes its existing `index_pipeline_version`. That immediately invalidates
older publications through the existing SQL visibility fence. Existing pending
projection selection now finds the records; capture checkpoints remain unchanged.
The script starts the existing `ProjectCanonicalRecordsWorkflow` from its first
page. Its normal page/retry/publication behavior remains authoritative.

The database commit and Temporal launch are **not atomic**. If launch fails,
pending records remain durable. Retry the identical expected/target arguments:
current version must equal exactly expected or target, never an arbitrary later
version. An already running deterministic workflow ID is reused; a completed or
failed run can be restarted to drain remaining pending rows without another bump.
An accepted workflow is not proof that all content became indexed. Unsupported
conversions may remain pending and must be reported separately.

Date/type-filtered searches reject versions below 2 with `reindex_required` before
retrieval. Following the bump, pending publication coverage remains explicit;
older generations cannot pass SQL validation. Relevance search is still bounded,
not exhaustive, and this change introduces no chronological ordering or cursor.

New Sync creation still defaults to version 1 in this isolated implementation.
Until integrated initialization is changed, newly captured sources also need the
explicit upgrade. The next coherent creation change is in the existing sync
creation transaction (`domains/syncs/service.py::_create_sync_records`): initialize
new canonical sources to the current projection contract after source capability
resolution, without resetting any existing Sync version. That change is deferred
while the independent capture engine/live lifecycle is active; it must be finished
before presenting new-source date/type search as automatically ready.
