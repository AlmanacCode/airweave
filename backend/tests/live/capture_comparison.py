"""Aggregate retained observations before disposable trials remove private evidence."""

from collections import Counter
from collections.abc import Mapping
from uuid import UUID

from pydantic import JsonValue

from airweave.domains.entities.canonical.models import SourceRecord

FieldPath = tuple[str | int, ...]


def _changed_paths(before: JsonValue, after: JsonValue, path: FieldPath = ()) -> list[FieldPath]:
    if type(before) is not type(after):
        return [path]
    if isinstance(before, dict) and isinstance(after, dict):
        paths = []
        for key in before.keys() | after.keys():
            if key not in before or key not in after:
                paths.append((*path, key))
            else:
                paths.extend(_changed_paths(before[key], after[key], (*path, key)))
        return paths
    if isinstance(before, list) and isinstance(after, list):
        if len(before) != len(after):
            return [path]
        return [
            changed
            for index, (left, right) in enumerate(zip(before, after, strict=True))
            for changed in _changed_paths(left, right, (*path, index))
        ]
    return [] if before == after else [path]


def _category(provider: str, path: FieldPath) -> str:
    # Never emit a native key/path: providers may put private text in JSON keys.
    if provider == "gmail":
        if path and path[0] == "labelIds":
            return "gmail_labels"
        if path == ("historyId",):
            return "gmail_history_id"
        if path[:1] == ("payload",) and path[-2:] == ("body", "attachmentId"):
            middle = path[1:-2]
            if len(middle) % 2 == 0 and all(
                middle[index] == "parts" and type(middle[index + 1]) is int
                for index in range(0, len(middle), 2)
            ):
                return "gmail_attachment_locator"
    return "other_payload"


def compare_observations(
    before: Mapping[UUID, SourceRecord], after: Mapping[UUID, SourceRecord], provider: str
) -> dict[str, int | dict[str, int]]:
    """Return counts only; raw snapshots, identifiers and SHA values stay in memory.

    Full snapshots remain in memory only for the bounded disposable harness.
    Manifest comparison aligns part paths, sizes and SHA; a changed path alone is
    not evidence of changed bytes. It concerns external blobs, not all bodies. Inline
    bytes remain part of raw payload comparison. No inference from equal byte totals.
    """
    counts = Counter(
        matched_records=0,
        changed_revision_records=0,
        changed_payload_records=0,
        locator_only_payload_records=0,
        unchanged_records=0,
        changed_revision_without_payload_or_blob_change=0,
        records_with_retained_blobs=0,
        equal_blob_manifest_records=0,
        different_blob_manifest_records=0,
        equal_blob_size_different_sha_records=0,
    )
    categories = Counter()
    for identity in before.keys() & after.keys():
        left, right = before[identity], after[identity]
        counts["matched_records"] += 1
        changed_revision = left.revision != right.revision
        counts["changed_revision_records"] += changed_revision
        counts["unchanged_records"] += not changed_revision
        paths = _changed_paths(left.payload, right.payload)
        record_categories = [_category(provider, path) for path in paths]
        categories.update(record_categories)
        counts["changed_payload_records"] += bool(paths)
        counts["locator_only_payload_records"] += bool(paths) and set(record_categories) == {
            "gmail_attachment_locator"
        }
        if changed_revision and not paths and left.blobs == right.blobs:
            counts["changed_revision_without_payload_or_blob_change"] += 1
        if left.blobs or right.blobs:
            counts["records_with_retained_blobs"] += 1

            # Native part paths align bytes without persisting or publishing those paths.
            def manifest(record: SourceRecord, include_sha: bool) -> list[tuple[str, int, str]]:
                return sorted(
                    (blob.source_path or "", blob.size_bytes, blob.sha256 if include_sha else "")
                    for blob in record.blobs
                )

            same_sha = manifest(left, True) == manifest(right, True)
            counts["equal_blob_manifest_records"] += same_sha
            counts["different_blob_manifest_records"] += not same_sha
            counts["equal_blob_size_different_sha_records"] += not same_sha and manifest(
                left, False
            ) == manifest(right, False)
    return {
        "schema_version": 1,
        "baseline_records": len(before),
        "added_records": len(after.keys() - before.keys()),
        "removed_rows": len(before.keys() - after.keys()),
        **counts,
        "changed_field_categories": dict(sorted(categories.items())),
    }
