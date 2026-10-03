"""Bounded page retry receipts in the existing locked canonical scan continuation."""

import hashlib
import json
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, JsonValue
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.domains.entities.canonical.cycle_models import TERMINAL_CHECKPOINT_KEY
from airweave.domains.entities.canonical.requests import CompletedScope, ScanVersion, WriterFence
from airweave.domains.entities.canonical.scan_models import ScanContinuation
from airweave.domains.entities.canonical.scan_store import CanonicalScanStore, ScanConflict
from airweave.domains.entities.canonical.store import CanonicalStoreError

# Keep existing scans readable; the historical key is storage format, not provider authority.
PAGE_RECEIPT_KEY = "native_page_receipt"


class PageAcknowledgement(BaseModel):
    """Durable page capture acknowledgement, not index publication or cycle completion."""

    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    page_id: UUID
    version: ScanVersion
    phase: Literal["collecting", "reconciling", "complete"]
    sequence: int
    changed: int
    unchanged: int


class PageReceipt(BaseModel):
    """Only the most recently committed page is recoverable with identical success."""

    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    schema_version: Literal[1] = 1
    digest: str = Field(pattern=r"^[a-f0-9]{64}$")
    acknowledgement: PageAcknowledgement


class PageReceiptError(CanonicalStoreError):
    """Missing or malformed canonical page progress cannot acknowledge a retry."""

    code = "page_receipt_failed"


def page_digest(request: BaseModel) -> str:
    """Hash the complete typed request, preserving native retry serialization exactly."""
    return hashlib.sha256(
        json.dumps(
            request.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode()
    ).hexdigest()


def read_page_receipt(continuation: ScanContinuation) -> PageReceipt | None:
    """Validate existing bounded receipt state; never infer success from unknown data."""
    stored = continuation.value.get(PAGE_RECEIPT_KEY)
    return PageReceipt.model_validate(stored) if stored is not None else None


class ScanPageReceipts:
    """Reuse canonical writer/scan locks; caller owns the surrounding transaction.

    Admission wrappers must resolve their own authority before calling recovery.
    Both recovery and persistence independently verify the canonical writer fence.
    No journal, content copy, commit, provider callback or alternate scan authority.
    """

    def __init__(self, scans: CanonicalScanStore):
        """The canonical scan remains the sole owner of progress and row identity."""
        self.scans = scans

    async def recover(
        self,
        db: AsyncSession,
        fence: WriterFence,
        scope: CompletedScope,
        cycle_id: UUID,
        *,
        page_id: UUID,
        digest: str,
        expected: ScanVersion,
    ) -> PageAcknowledgement | None:
        """Only identical current-version replay succeeds, under the existing writer lock."""
        current = await self.scans.read(db, fence, scope)
        if current is None:
            raise PageReceiptError("Scope has not been started")
        if current.cycle_id != cycle_id:
            raise PageReceiptError("Scope belongs to another capture cycle")
        previous = read_page_receipt(current.continuation)
        if previous is not None and previous.acknowledgement.page_id == page_id:
            if previous.digest != digest or previous.acknowledgement.version != current.version:
                raise ScanConflict("Page retry conflicts with committed progress")
            return previous.acknowledgement
        if current.version != expected:
            raise ScanConflict("Page changed; read committed scope progress before retrying")
        return None

    async def persist(
        self,
        db: AsyncSession,
        fence: WriterFence,
        scope: CompletedScope,
        cycle_id: UUID,
        *,
        digest: str,
        acknowledgement: PageAcknowledgement,
        cursor: dict[str, JsonValue],
    ) -> None:
        """Receipt and captured page share the caller's outer commit or rollback."""
        await self.scans.records._fenced_sync(db, fence)
        row = await self.scans._row(db, fence, scope)
        if row is None or row.cycle_id != cycle_id:
            raise ScanConflict("Receipt scope is unavailable or belongs to another capture cycle")
        if acknowledgement.version != ScanVersion(sweep_id=row.sweep_id, revision=row.revision):
            raise ScanConflict("Receipt acknowledgement conflicts with committed progress")
        continuation = ScanContinuation(
            value={
                **{
                    key: value
                    for key, value in row.continuation.items()
                    if key != TERMINAL_CHECKPOINT_KEY
                },
                "cursor": cursor,
                PAGE_RECEIPT_KEY: PageReceipt(
                    digest=digest, acknowledgement=acknowledgement
                ).model_dump(mode="json"),
            }
        )
        row.continuation = {**row.continuation, **continuation.value}
        await db.flush()
