"""Bounded live Contacts traversal; ranking never establishes number ownership."""

from typing import Literal
from uuid import UUID

from jose import JWTError, jwt
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.domains.entities.canonical.apple_payloads import NativeContactHandle
from airweave.domains.entities.canonical.contact_preparation import (
    PreparedContact,
    PreparedPhone,
    prepare_contact,
    prepare_phone,
)
from airweave.domains.entities.canonical.coverage import capture_coverage
from airweave.domains.entities.canonical.coverage_models import CaptureCoverage
from airweave.domains.entities.canonical.query import CanonicalQueryService, InvalidRecordCursor
from airweave.domains.entities.canonical.query_models import RecordFilters
from airweave.domains.entities.canonical.store import SourceNotFound


class ContactLookup(BaseModel):
    """Exact input and bounded scan window; continuation retains the same query."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    mode: Literal["raw_handle", "international_phone"]
    value: str = Field(min_length=1, max_length=256)
    limit: int = Field(default=100, ge=1, le=100)
    cursor: str | None = Field(default=None, max_length=16384)


class ContactMatch(BaseModel):
    """One observed handle and its qualified matching reason."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    handle: NativeContactHandle
    reason: Literal["raw_exact", "international_endpoint"]


class ContactCandidate(BaseModel):
    """One individual retained card, never a merged person."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    contact: PreparedContact
    matches: tuple[ContactMatch, ...]


class ContactCandidatePage(BaseModel):
    """Matching cards from a live bounded window, including empty windows."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    candidates: tuple[ContactCandidate, ...]
    scanned: int = Field(ge=0, le=100)
    next_cursor: str | None
    has_more: bool
    interpretation: Literal[
        "raw_exact", "international", "region_required", "unsupported", "malformed"
    ]
    consistency: Literal["live"] = "live"
    coverage: Literal["eligible_retained_cards_only"] = "eligible_retained_cards_only"
    ambiguity: Literal["ownership_not_resolved"] = "ownership_not_resolved"
    preparation_version: Literal["contacts-fields-v2"] = "contacts-fields-v2"
    capture: CaptureCoverage | None = None


class ContactCursor(BaseModel):
    """Signed last-scanned identity with immutable scope and interpretation."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    purpose: Literal["contacts_last_scanned"] = "contacts_last_scanned"
    version: Literal[1] = 1
    organization_id: UUID
    sync_id: UUID
    mode: Literal["raw_handle", "international_phone"]
    value: str
    preparation_version: Literal["contacts-fields-v2"] = "contacts-fields-v2"
    last_scanned_id: UUID


async def lookup_contacts(
    service: CanonicalQueryService,
    db: AsyncSession,
    organization_id: UUID,
    sync_id: UUID,
    query: ContactLookup,
) -> ContactCandidatePage:
    """Return every matching eligible card in this window without ranked truncation."""
    after = None
    if query.cursor:
        try:
            cursor = ContactCursor.model_validate(
                jwt.decode(query.cursor, service.signing_key, algorithms=["HS256"])
            )
        except (JWTError, ValidationError) as exc:
            raise InvalidRecordCursor(
                "Invalid Contacts last-scanned cursor; restart lookup"
            ) from exc
        if (cursor.organization_id, cursor.sync_id, cursor.mode, cursor.value) != (
            organization_id,
            sync_id,
            query.mode,
            query.value,
        ):
            raise InvalidRecordCursor("Contacts cursor belongs to another scope or query")
        after = cursor.last_scanned_id
    phone = (
        prepare_phone(NativeContactHandle(nativeLabelID="query", label=None, rawValue=query.value))
        if query.mode == "international_phone"
        else None
    )
    interpretation = phone.status if phone else "raw_exact"
    rows = await service.queries.list_records(
        db,
        organization_id,
        sync_id,
        RecordFilters(record_type="apple_contact"),
        after_id=after,
        limit=query.limit,
        contact_raw_handle=query.value if query.mode == "raw_handle" else None,
    )
    more = len(rows) > query.limit
    window = rows[: query.limit]
    candidates = []
    for row in window:
        if row.content_access != "available" or row.deleted_at is not None:
            continue
        contact = prepare_contact(row)
        matches = matching_handles(contact, query.value, phone)
        if matches:
            candidates.append(ContactCandidate(contact=contact, matches=tuple(matches)))
    continuation = (
        jwt.encode(
            ContactCursor(
                organization_id=organization_id,
                sync_id=sync_id,
                mode=query.mode,
                value=query.value,
                last_scanned_id=window[-1].id,
            ).model_dump(mode="json"),
            service.signing_key,
            algorithm="HS256",
        )
        if more
        else None
    )
    coverage = await capture_coverage(db, organization_id, (sync_id,))
    if not await service.queries.source_readable(db, organization_id, sync_id):
        raise SourceNotFound("Source is unavailable in this organization")
    return ContactCandidatePage(
        candidates=tuple(candidates),
        scanned=len(window),
        next_cursor=continuation,
        has_more=more,
        interpretation=interpretation,
        capture=coverage.get(sync_id),
    )


def matching_handles(
    contact: PreparedContact, value: str, phone: PreparedPhone | None
) -> tuple[ContactMatch, ...]:
    """Keep exact endpoint extensions and native spellings separate."""
    matches = []
    for index, handle in enumerate(contact.source.contact.phones):
        prepared = contact.phones[index]
        if (
            phone
            and phone.e164
            and prepared.e164 == phone.e164
            and prepared.extension == phone.extension
        ):
            matches.append(ContactMatch(handle=handle, reason="international_endpoint"))
        elif phone is None and handle.raw_value == value:
            matches.append(ContactMatch(handle=handle, reason="raw_exact"))
    if phone is None:
        matches.extend(
            ContactMatch(handle=h, reason="raw_exact")
            for h in contact.source.contact.emails
            if h.raw_value == value
        )
    return tuple(matches)
