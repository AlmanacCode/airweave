"""Explicit source capability for original provider observations from one crawl."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from airweave.domains.entities.canonical.requests import (
    CaptureRecord,
    CompletedScope,
    RemovedScope,
    StartedScope,
)

if TYPE_CHECKING:
    from airweave.domains.browse_tree.types import NodeSelectionData
    from airweave.domains.storage.file_service import FileService
    from airweave.domains.syncs.cursors.cursor import SyncCursor

SourceObservation = CaptureRecord | CompletedScope | RemovedScope | StartedScope


@runtime_checkable
class CanonicalSource(Protocol):
    """Opt-in contract; declared types are audited operational records, not search chunks."""

    canonical_record_types: tuple[str, ...]

    def generate_observations(
        self,
        *,
        cursor: SyncCursor | None = None,
        files: FileService | None = None,
        node_selections: list[NodeSelectionData] | None = None,
    ) -> AsyncGenerator[SourceObservation, None]:
        """Yield native records and only successfully completed exact enumeration scopes."""
        ...


@runtime_checkable
class ContainerScopedSource(Protocol):
    """Audited child record types whose visibility depends on a root container record."""

    canonical_container_parents: dict[str, str]
