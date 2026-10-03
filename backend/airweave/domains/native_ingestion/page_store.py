"""Atomic page admission and bounded lost-response recovery on existing scan state."""

from datetime import datetime, timezone
from uuid import UUID

from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.domains.entities.canonical.page_receipts import (
    PageAcknowledgement,
    PageReceiptError,
    ScanPageReceipts,
    page_digest,
)
from airweave.domains.entities.canonical.scan_models import ScanContinuation
from airweave.domains.entities.canonical.scan_store import CanonicalScanStore
from airweave.domains.native_ingestion.errors import NativeAdmissionError
from airweave.domains.native_ingestion.import_store import NativeImportStore
from airweave.domains.native_ingestion.models import IngestNativePage
from airweave.domains.native_ingestion.page_models import CommitNativePage
from airweave.domains.native_ingestion.store import NativeIngestionStore


class NativePageStore:
    """Caller owns one transaction; no side journal and no retained response payload copies."""

    def __init__(self, imports: NativeImportStore, ingestion: NativeIngestionStore):
        """Share the existing canonical writer and scan boundary."""
        self.imports = imports
        self.ingestion = ingestion
        self.scans = CanonicalScanStore(ingestion.canonical)
        self.receipts = ScanPageReceipts(self.scans)

    async def commit(
        self,
        db: AsyncSession,
        organization_id: UUID,
        source_id: UUID,
        request_key: str,
        request: CommitNativePage,
    ) -> PageAcknowledgement:
        """A committed current page retry returns its receipt without another capture."""
        imported = await self.imports.active(db, organization_id, source_id, request_key)
        digest = page_digest(request)
        try:
            recovered = await self.receipts.recover(
                db,
                imported.fence,
                request.scope,
                imported.cycle_id,
                page_id=request.page_id,
                digest=digest,
                expected=request.expected,
            )
        except ValidationError as error:
            raise NativeAdmissionError("Native page receipt is malformed") from error
        except PageReceiptError as error:
            raise NativeAdmissionError(str(error)) from error
        if recovered is not None:
            return recovered
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
        ack = PageAcknowledgement(
            page_id=request.page_id,
            version=result.state.version,
            phase=result.state.phase,
            sequence=result.capture.sequence,
            changed=len(result.capture.changes),
            unchanged=result.capture.unchanged,
        )
        await self.receipts.persist(
            db,
            imported.fence,
            request.scope,
            imported.cycle_id,
            digest=digest,
            acknowledgement=ack,
            cursor=request.cursor,
        )
        return ack
