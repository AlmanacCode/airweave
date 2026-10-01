"""Native import intent and durable identity, without client-controlled writer fences."""

from typing import Literal
from uuid import UUID

from pydantic import Field

from airweave.core.shared_models import SyncJobStatus
from airweave.domains.entities.canonical.requests import WriterFence
from airweave.domains.native_ingestion.models import NativeModel


class StartNativeImport(NativeModel):
    """Publisher attestation; bounded imports never authorize absence deletion."""

    snapshot_id: str = Field(min_length=1, max_length=256)
    coverage: Literal["bounded", "complete"]


class NativeImportReceipt(NativeModel):
    """Server-only job metadata; an identical request never reactivates a writer."""

    schema_version: Literal[1] = 1
    source_id: UUID
    request_key: str = Field(min_length=1, max_length=128)
    request: StartNativeImport
    fence: WriterFence
    cycle_id: UUID


class NativeImportState(NativeModel):
    """Public identity and execution state; running is not evidence of full capture."""

    source_id: UUID
    import_id: UUID
    request_key: str
    request: StartNativeImport
    status: SyncJobStatus
    cycle_id: UUID
