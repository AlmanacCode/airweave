"""Bounded Google event identity resolution over already captured observations."""

from datetime import date as Date
from typing import Literal, Self
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, ValidationError, model_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.domains.entities.canonical.calendar import is_cancelled_recurring_event
from airweave.domains.entities.canonical.calendar_query import CalendarChanged
from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.query import RecordNotFound
from airweave.domains.entities.canonical.requests import RecordIdentity
from airweave.domains.entities.canonical.store import (
    CanonicalStoreError,
    content_is_available,
    parent_is_visible,
    source_record,
)
from airweave.models.entity import Entity
from airweave.models.sync import Sync


class CalendarReadIncomplete(CanonicalStoreError):
    """Existing observations cannot safely establish one scheduling state."""

    code = "calendar_read_incomplete"


class Version(BaseModel):
    """Only provider version evidence, never capture wall time."""

    model_config = ConfigDict(extra="ignore")
    etag: str | None = None
    updated: AwareDatetime | None = None
    status: str | None = None


class CancellationStart(BaseModel):
    """Only the original occurrence identity may survive a tombstone read."""

    model_config = ConfigDict(extra="ignore")
    date: Date | None = None
    dateTime: AwareDatetime | None = None
    timeZone: str | None = None

    @model_validator(mode="after")
    def exact_start(self) -> Self:
        """Retain exactly one date or instant from the provider identity."""
        if (self.date is None) == (self.dateTime is None):
            raise ValueError("Exactly one original start is required")
        return self


class CancellationIdentity(BaseModel):
    """Safe recurrence exclusion fields retained when a body is deleted."""

    model_config = ConfigDict(extra="ignore")
    id: str = Field(min_length=1)
    status: Literal["cancelled"]
    recurringEventId: str = Field(min_length=1)
    originalStartTime: CancellationStart


def choose(records: tuple[SourceRecord, ...]) -> SourceRecord:
    """Resolve at most one raw and one expanded record; denied content never falls through."""
    if any(
        r.content_access != "available" or r.removal_reason == "access_revoked" for r in records
    ):
        raise RecordNotFound("Calendar event is unavailable")
    if any(
        r.identity.record_type == "event" and r.removal_reason == "scope_removed" for r in records
    ):
        raise RecordNotFound("Calendar event is unavailable")
    candidates = tuple(r for r in records if r.removal_reason != "scope_removed")
    if not candidates:
        raise RecordNotFound("Calendar event has not been captured or is unavailable")
    if len(candidates) == 1:
        return candidates[0]
    raw, expanded = sorted(candidates, key=lambda r: r.identity.record_type != "event")
    try:
        a, b = Version.model_validate(raw.payload), Version.model_validate(expanded.payload)
    except ValidationError:
        raise CalendarReadIncomplete("Calendar observations need a refresh") from None
    same_state = (raw.deleted_at is not None or a.status == "cancelled") == (
        expanded.deleted_at is not None or b.status == "cancelled"
    ) and a.status == b.status
    fields = ("start", "end", "recurringEventId", "originalStartTime")
    same_schedule = all(raw.payload.get(key) == expanded.payload.get(key) for key in fields)
    if a.etag and a.etag == b.etag and not (same_state and same_schedule):
        raise CalendarReadIncomplete("Calendar observations disagree; refresh this calendar")
    if a.updated and b.updated and a.updated != b.updated:
        return raw if a.updated > b.updated else expanded
    if (
        same_state
        and same_schedule
        and ((a.etag and a.etag == b.etag) or raw.payload == expanded.payload)
    ):
        return raw
    raise CalendarReadIncomplete("Calendar observations disagree; refresh this calendar")


async def read_event(
    db: AsyncSession, organization_id: UUID, sync_id: UUID, calendar_id: str, event_id: str
) -> SourceRecord:
    """Use the existing compound identity index and current calendar visibility."""
    scope = select(Sync.observed_change_sequence).where(
        Sync.organization_id == organization_id, Sync.id == sync_id
    )
    sequence = await db.scalar(scope)
    parent = await db.scalar(
        select(Entity.id).where(
            Entity.organization_id == organization_id,
            Entity.sync_id == sync_id,
            Entity.entity_definition_short_name == "calendar",
            Entity.native_id == calendar_id,
            Entity.record_revision > 0,
            Entity.deleted_at.is_(None),
            content_is_available(),
        )
    )
    if sequence is None or parent is None:
        raise RecordNotFound("Calendar is unavailable")
    identity = RecordIdentity(record_type="event", container_id=calendar_id, native_id=event_id)
    rows = (
        await db.scalars(
            select(Entity)
            .where(
                Entity.organization_id == organization_id,
                Entity.sync_id == sync_id,
                Entity.entity_id == identity.entity_key,
                Entity.entity_definition_short_name.in_(("event", "event_occurrence")),
                Entity.record_revision > 0,
                parent_is_visible(),
            )
            .limit(2)
        )
    ).all()
    result = choose(tuple(source_record(row) for row in rows))
    db.expire_all()
    if await db.scalar(scope) != sequence:
        raise CalendarChanged("Calendar changed while reading; try again")
    if result.deleted_at is not None:
        try:
            identity = (
                CancellationIdentity.model_validate(result.payload).model_dump(
                    mode="json", exclude_none=True
                )
                if is_cancelled_recurring_event(result.payload)
                else {}
            )
        except ValidationError:
            raise CalendarReadIncomplete("Calendar cancellation identity needs a refresh") from None
        return result.model_copy(update={"payload": identity, "blobs": ()})
    return result
