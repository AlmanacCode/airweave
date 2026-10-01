"""Discover committed native work without a second dispatch journal."""

from uuid import UUID

from temporalio import activity

from airweave.db.session import get_db_context
from airweave.domains.entities.canonical.projection_store import CanonicalProjectionStore


class DiscoverNativeProjectionActivity:
    """Read the existing pending state; no credentials or content cross Temporal."""

    @activity.defn(name="discover_native_projection_activity")
    async def run(self, after_id: str | None = None) -> dict:
        """Native HTTP imports lack the provider workflow's durable projection child."""
        async with get_db_context() as db:
            page = await CanonicalProjectionStore().pending_sources(
                db,
                source_names=("almanac",),
                after_id=UUID(after_id) if after_id else None,
                limit=20,
            )
        return page.model_dump(mode="json")
