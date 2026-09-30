"""Synthetic capture inputs and transaction invocation for database tests."""

from datetime import datetime, timezone

from airweave.domains.entities.canonical.requests import CaptureBatch, CaptureRecord, RecordIdentity


def observation(native_id="one", container_id=None, **changes):
    values = {
        "identity": RecordIdentity(
            record_type="event", native_id=native_id, container_id=container_id
        ),
        "payload": {"id": native_id, "summary": "Synthetic event"},
        "observed_at": datetime.now(timezone.utc),
        "content_hash": "same-body",
    }
    values.update(changes)
    return CaptureRecord(**values)


async def capture(database, service, fence, *records):
    async with database() as db:
        return await service.capture(db, CaptureBatch(fence=fence, records=records))
