"""Publisher recovery and lean retained inventory; neither proves source completeness."""

from typing import Literal
from uuid import UUID

from airweave.domains.native_ingestion.access_models import NativeRecordAccess
from airweave.domains.native_ingestion.import_models import NativeImportState
from airweave.domains.native_ingestion.models import NativeModel, NativeVersion
from airweave.domains.native_ingestion.source_models import NativeSource


class NativePublication(NativeModel):
    """Current writer receipt survives an uncertain terminal acknowledgement."""

    source: NativeSource
    current: NativeImportState | None


class NativeInventoryRecord(NativeRecordAccess):
    """Retained source version and access CAS, without original or projection content."""

    version: NativeVersion


class NativeInventoryPage(NativeModel):
    """Live ID traversal is not evidence that absent source objects were deleted."""

    source: NativeSource
    records: tuple[NativeInventoryRecord, ...]
    next_after: UUID | None
    has_more: bool
    consistency: Literal["live"] = "live"
    order: Literal["id_asc"] = "id_asc"
