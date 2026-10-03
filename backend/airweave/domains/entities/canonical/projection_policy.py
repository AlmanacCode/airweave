"""Pure, provider-scoped publication policy shared by mapping and store validation."""

from airweave.domains.entities.canonical.calendar import is_cancelled_recurring_event
from airweave.domains.entities.canonical.models import SourceRecord


def excluded_from_search(record: SourceRecord, source_name: str) -> bool:
    """Records retained for authority/navigation that intentionally have no search text."""
    if source_name == "whatsapp":
        from airweave.domains.entities.canonical.whatsapp_projection import (
            excluded_whatsapp,  # noqa: PLC0415
        )

        return excluded_whatsapp(record)
    if source_name == "wispr":
        return record.identity.record_type in {"meeting_listing", "scratchpad_listing"}
    if source_name == "almanac":
        from airweave.domains.native_ingestion.projection import excluded_native

        return excluded_native(record)
    return source_name == "google_calendar" and (
        record.identity.record_type == "event_occurrence"
        or (record.identity.record_type == "event" and is_cancelled_recurring_event(record.payload))
    )
