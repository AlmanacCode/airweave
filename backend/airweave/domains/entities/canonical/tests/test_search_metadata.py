"""Canonical dates keep source ownership and microsecond precision through feed."""

from datetime import datetime, timezone
from uuid import uuid4

from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.search_metadata import stamp_search_metadata
from airweave.domains.entities.canonical.tests.helpers import observation
from airweave.platform.destinations.vespa.transformer import EntityTransformer
from airweave.platform.entities._base import AirweaveSystemMetadata
from airweave.platform.entities.slack import SlackChannelEntity


def test_metadata_never_uses_mapper_dates():
    native = datetime(2026, 1, 1, microsecond=1, tzinfo=timezone.utc)
    record = SourceRecord(
        id=uuid4(),
        sync_id=uuid4(),
        identity=observation().identity,
        revision=1,
        payload={},
        payload_schema_version=1,
        capture_hash="hash",
        content_hash=None,
        completeness="complete",
        observed_at=native,
        source_created_at=native,
        source_updated_at=None,
        deleted_at=None,
        removal_reason=None,
        blobs=(),
        indexed_revision=None,
        indexed_pipeline_version=None,
    )
    entity = SlackChannelEntity(channel_id="one", title="One", purpose="", topic="", breadcrumbs=[])
    entity.created_at = entity.updated_at = datetime(2030, 1, 1, tzinfo=timezone.utc)
    entity.airweave_system_metadata = AirweaveSystemMetadata()
    stamp_search_metadata(entity.airweave_system_metadata, record)
    fields = EntityTransformer()._build_system_metadata(entity)
    assert fields["source_created_us"] == 1767225600000001
    assert fields["source_created_known"] == 1
    assert fields["source_updated_us"] is None
    assert fields["source_updated_known"] == 0
    assert fields["canonical_record_type"] == "event"
