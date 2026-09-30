"""Opted-in scope pages; the existing capture pipeline owns durable progress."""

from typing import Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from airweave.domains.entities.canonical.cycle_models import (
    CaptureCycle,
    CaptureMode,
    CycleConfiguration,
    ProviderCheckpoint,
)
from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.requests import (
    CaptureRecord,
    CompletedScope,
    ScopeRemovalReason,
)
from airweave.domains.entities.canonical.scan_models import ScanContinuation
from airweave.domains.storage.file_service import FileService


class CapturePage(BaseModel):
    """One provider response and the continuation committed with its original records."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    records: tuple[CaptureRecord, ...] = Field(max_length=500)
    discovered_records: tuple[CaptureRecord, ...] = Field(
        default=(),
        max_length=500,
        description="Verified independent originals from the bound source; never placeholders",
    )
    continuation: ScanContinuation
    final: bool = False
    provider_checkpoint: ProviderCheckpoint | None = None


class CapturePlan(BaseModel):
    """Immutable source plan selected once before engine cycle creation."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    mode: CaptureMode = "full"
    starting_checkpoint: ProviderCheckpoint | None = None


class InvalidCaptureCheckpoint(Exception):
    """Native evidence invalidates a whole cycle boundary, not merely a page token."""


@runtime_checkable
class CheckpointedPageSource(Protocol):
    """Sources with a native changes API reuse the same cycle and page authority."""

    async def prepare_cycle(self, previous: CaptureCycle | None) -> CapturePlan:
        """Provider I/O occurs before the fenced cycle CAS; None requests a fresh full pass."""
        ...

    def initial_continuation(self, cycle: CaptureCycle) -> ScanContinuation:
        """Derive resumable page state only from the persisted immutable plan."""
        ...


class InvalidScanContinuation(Exception):
    """The provider rejected a saved cursor; restart the whole scope, never reconcile it."""


class ScopeAccessLost(Exception):
    """The source's audited contract says this child container is no longer available."""

    def __init__(
        self,
        message: str,
        *,
        removal_reason: ScopeRemovalReason = "access_revoked",
    ):
        """Slack keeps its confirmed-access default; other sources must choose explicitly."""
        super().__init__(message)
        self.removal_reason = TypeAdapter(ScopeRemovalReason).validate_python(removal_reason)


@runtime_checkable
class CanonicalPageSource(Protocol):
    """A source fetches pages; SQL, attempts, scope discovery and checkpointing stay outside it."""

    canonical_record_types: tuple[str, ...]
    canonical_container_parents: dict[str, str | tuple[str | None, ...]]
    capture_cycle_configuration: CycleConfiguration

    async def capture_page(
        self,
        scope: CompletedScope,
        continuation: ScanContinuation,
        *,
        files: FileService,
        parent: SourceRecord | None = None,
    ) -> CapturePage:
        """Fetch one page without advancing durable state or performing writes."""
        ...

    def child_scope(self, parent: SourceRecord, record_type: str) -> CompletedScope:
        """Source-owned stable container policy; always retain the full parent identity."""
        ...

    async def confirm_absent(self, record: SourceRecord) -> None:
        """Raise unless an omitted inventory record is confirmed outside accessible scope."""
        ...


@runtime_checkable
class KnownObjectSource(Protocol):
    """Sources declaring exact known-object validation implement current reads."""

    async def refresh_known(self, record: SourceRecord, *, files: FileService) -> CaptureRecord:
        """Return exact fresh state or explicit unavailability; errors never mean absence."""
        ...
