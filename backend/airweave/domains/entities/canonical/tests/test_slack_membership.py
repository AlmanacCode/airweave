"""Lost membership hides records captured before the first durable checkpoint."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from airweave.domains.entities.canonical.requests import RecordIdentity
from airweave.domains.entities.canonical.tests.helpers import capture, observation
from airweave.domains.entities.canonical.tests.test_capture_pipeline import components
from airweave.domains.sources.token_providers.static import StaticTokenProvider
from airweave.domains.sync_pipeline.canonical_capture import CanonicalCapturePipeline
from airweave.domains.sync_pipeline.capture_attempt import CaptureAttempt
from airweave.platform.sources.slack import SlackApiError, SlackSource

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("listing_fails", [False, True])
async def test_first_completed_membership_hides_prior_partial_channel(
    database, source, listing_fails
):
    service, fence = source
    parent = RecordIdentity(record_type="channel", native_id="lost-channel")
    seeded = await capture(
        database,
        service,
        fence,
        observation(identity=parent),
        observation(
            identity=RecordIdentity(
                record_type="message", native_id="1", container_id="lost-channel"
            ),
            parent=parent,
        ),
    )
    child_id = seeded.changes[1].record.id
    # The failed prior capture saved records but no channel_ids checkpoint.
    ctx, _, runtime, bus = components(database, source)
    connector = SlackSource(
        auth=StaticTokenProvider("fixture"), logger=MagicMock(), http_client=MagicMock()
    )
    connector._get = AsyncMock(
        side_effect=(
            ConnectionError("membership page failed")
            if listing_fails
            else [{"channels": []}, SlackApiError("channel_not_found")]
        ),
    )
    pipeline = CanonicalCapturePipeline(
        service,
        database,
        bus,
        SlackSource.canonical_record_types,
        CaptureAttempt(id=fence.attempt_id, number=fence.attempt_number),
        SlackSource.canonical_container_parents,
        page_source=connector,
    )
    await pipeline.start(ctx)

    async def scan():
        await pipeline.run_scans(ctx, runtime, AsyncMock())

    if listing_fails:
        with pytest.raises(ConnectionError, match="membership page failed"):
            await scan()
    else:
        await scan()
    await pipeline.cleanup_orphaned_entities(ctx, runtime)
    async with database() as db:
        record = await service.store.read(db, fence.organization_id, fence.sync_id, child_id)
        if listing_fails:
            assert record.content_access == "available" and record.payload
            return
        assert record.content_access == "unavailable"
        assert record.payload == {} and record.blobs == ()
        parent_record = await service.store.read(
            db, fence.organization_id, fence.sync_id, seeded.changes[0].record.id
        )
        assert parent_record.removal_reason == "scope_removed"
        assert parent_record.content_access == "unavailable" and parent_record.payload == {}
        changes = await service.store.changes(db, fence.organization_id, fence.sync_id)
        assert all(change.record.payload == {} for change in changes.changes)
