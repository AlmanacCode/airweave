"""Canonical-owned search attributes, independent of provider entity date fallbacks."""

from datetime import datetime, timezone

from pydantic import BaseModel, Field

from airweave.domains.entities.canonical.actors import committed_actor_handles
from airweave.domains.entities.canonical.models import SourceRecord
from airweave.platform.entities._base import AirweaveSystemMetadata

SEARCH_METADATA_PIPELINE_VERSION = 2
NATIVE_TYPE_PIPELINE_VERSION = 3
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


class _NativeOriginalType(BaseModel):
    """Read one field without revalidating large original bodies for every chunk."""

    type: str = Field(min_length=1)


class _NativeTypePayload(BaseModel):
    original: _NativeOriginalType


def epoch_microseconds(value: datetime) -> int:
    """Preserve source precision without float timestamp rounding."""
    if value.tzinfo is None:
        raise ValueError("Canonical search timestamp must be timezone-aware")
    delta = value - _EPOCH
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


def stamp_search_metadata(meta: AirweaveSystemMetadata, record: SourceRecord) -> None:
    """Use only the committed source record, never mapper or observation timestamps."""
    meta.canonical_record_type = record.identity.record_type
    meta.native_type = None
    meta.actor_tokens = list(committed_actor_handles(meta.source_name, record.payload).tokens)
    if meta.source_name == "almanac":
        meta.native_type = (
            _NativeTypePayload.model_validate(record.payload).original.type
            if record.identity.record_type == "knowledge"
            else record.identity.record_type
        )
    meta.source_created_known = int(record.source_created_at is not None)
    meta.source_updated_known = int(record.source_updated_at is not None)
    meta.source_created_us = (
        epoch_microseconds(record.source_created_at) if record.source_created_at else None
    )
    meta.source_updated_us = (
        epoch_microseconds(record.source_updated_at) if record.source_updated_at else None
    )
