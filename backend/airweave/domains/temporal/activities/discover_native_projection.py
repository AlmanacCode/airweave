"""Discover committed canonical work without a second dispatch journal."""

from dataclasses import dataclass
from uuid import UUID

from temporalio import activity

from airweave.db.session import get_db_context
from airweave.domains.entities.canonical.projection_store import CanonicalProjectionStore
from airweave.domains.entities.canonical.source import indexed_record_types
from airweave.domains.sources.protocols import SourceRegistryProtocol


@dataclass
class DiscoverNativeProjectionActivity:
    """Read the existing pending state; no credentials or content cross Temporal."""

    source_registry: SourceRegistryProtocol

    @activity.defn(name="discover_native_projection_activity")
    async def run(self, after_id: str | None = None) -> dict:
        """Discover native imports and provider records even while capture is active."""
        source_names = ("almanac",) + tuple(
            entry.short_name
            for entry in self.source_registry.list_all()
            if entry.short_name != "almanac"
            and indexed_record_types(entry.short_name, self.source_registry)
        )
        async with get_db_context() as db:
            page = await CanonicalProjectionStore().pending_sources(
                db,
                source_names=source_names,
                after_id=UUID(after_id) if after_id else None,
                limit=20,
            )
        return page.model_dump(mode="json")
