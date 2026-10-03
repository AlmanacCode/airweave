"""Bounded reclamation on the existing system maintenance workflow."""

from dataclasses import dataclass
from datetime import datetime, timezone

from temporalio import activity

from airweave.db.session import get_db_context, get_tenant_db_context
from airweave.domains.entities.canonical.projection_gc import ProjectionGCStore
from airweave.domains.storage.protocols import StorageBackend
from airweave.platform.destinations.vespa.client import VespaClient


@dataclass
class CleanupProjectionGenerationsActivity:
    """A tick deletes at most 100 exact document IDs; failures remain durable."""

    storage: StorageBackend

    @activity.defn(name="cleanup_projection_generations_activity")
    async def run(self) -> None:
        """Use existing SQL/Temporal infrastructure, without creating schedules here."""
        store = ProjectionGCStore()
        now = datetime.now(timezone.utc)
        async with get_db_context() as db:
            due = await store.due(db, now=now, limit=20)
        if not due:
            return
        client = await VespaClient.connect()
        remaining = 100
        try:
            for target in due:
                if remaining <= 0:
                    break
                async with get_tenant_db_context(target.organization_id) as db:
                    page = await store.claim(db, target.generation_id, now=now, limit=remaining)
                if page is None:
                    continue
                error = None
                try:
                    await client.delete_documents(
                        [(doc.schema_name, doc.document_id) for doc in page.documents]
                    )
                    for key in page.artifact_keys:
                        await self.storage.delete_file(key)
                except Exception as failure:
                    error = type(failure).__name__
                async with get_tenant_db_context(target.organization_id) as db:
                    await store.acknowledge(db, page, now=datetime.now(timezone.utc), error=error)
                remaining -= len(page.documents) + len(page.artifact_keys)
        finally:
            await client.close()
