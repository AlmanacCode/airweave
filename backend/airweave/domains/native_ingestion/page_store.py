"""Atomic page admission and bounded lost-response recovery on existing scan state."""

import hashlib
import json
from datetime import datetime, timezone
from uuid import UUID

from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.domains.entities.canonical.scan_models import ScanContinuation
from airweave.domains.entities.canonical.scan_store import CanonicalScanStore, ScanConflict
from airweave.domains.native_ingestion.errors import NativeAdmissionError
from airweave.domains.native_ingestion.import_store import NativeImportStore
from airweave.domains.native_ingestion.models import IngestNativePage
from airweave.domains.native_ingestion.page_models import (
    CommitNativePage,
    NativePageAck,
    NativePageReceipt,
)
from airweave.domains.native_ingestion.store import NativeIngestionStore

_RECEIPT = "native_page_receipt"


class NativePageStore:
    """Caller owns one transaction; no side journal and no retained response payload copies."""

    def __init__(self, imports: NativeImportStore, ingestion: NativeIngestionStore):
        """Share the existing canonical writer and scan boundary."""
        self.imports = imports
        self.ingestion = ingestion
        self.scans = CanonicalScanStore(ingestion.canonical)

    async def commit(
        self,
        db: AsyncSession,
        organization_id: UUID,
        source_id: UUID,
        request_key: str,
        request: CommitNativePage,
    ) -> NativePageAck:
        """A committed current page retry returns its receipt without another capture."""
        imported = await self.imports.active(db, organization_id, source_id, request_key)
        current = await self.scans.read(db, imported.fence, request.scope)
        if current is None:
            raise NativeAdmissionError("Native scope has not been started")
        if current.cycle_id != imported.cycle_id:
            raise NativeAdmissionError("Scope belongs to another native import")
        digest = hashlib.sha256(
            json.dumps(
                request.model_dump(mode="json"),
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).encode()
        ).hexdigest()
        stored = current.continuation.value.get(_RECEIPT)
        if stored is not None:
            try:
                previous = NativePageReceipt.model_validate(stored)
            except ValidationError as error:
                raise NativeAdmissionError("Native page receipt is malformed") from error
            if previous.acknowledgement.page_id == request.page_id:
                if previous.digest != digest or previous.acknowledgement.version != current.version:
                    raise ScanConflict("Page retry conflicts with committed progress")
                return previous.acknowledgement
        if current.version != request.expected:
            raise ScanConflict("Page changed; read committed scope progress before retrying")
        result = await self.ingestion.page(
            db,
            IngestNativePage(
                fence=imported.fence,
                observed_at=datetime.now(timezone.utc),
                snapshots=request.snapshots,
                scope=request.scope,
                cycle_id=imported.cycle_id,
                expected=request.expected,
                continuation=ScanContinuation(value={"cursor": request.cursor}),
                final=request.final,
            ),
        )
        ack = NativePageAck(
            page_id=request.page_id,
            version=result.state.version,
            phase=result.state.phase,
            sequence=result.capture.sequence,
            changed=len(result.capture.changes),
            unchanged=result.capture.unchanged,
        )
        # The scan owns this row and its lock; receipt and page share the outer commit.
        row = await self.scans._row(db, imported.fence, request.scope)
        assert row is not None  # Existing scan remains locked through this transaction.
        row.continuation = {
            "cursor": request.cursor,
            _RECEIPT: NativePageReceipt(digest=digest, acknowledgement=ack).model_dump(mode="json"),
        }
        await db.flush()
        return ack
