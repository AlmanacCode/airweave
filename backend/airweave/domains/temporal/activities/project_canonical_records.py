"""Project owned records without reconnecting or consulting the provider."""

from dataclasses import dataclass
from uuid import UUID

from temporalio import activity

from airweave.core.logging import logger
from airweave.db.session import get_db_context
from airweave.domains.entities.canonical.projection_models import ProjectionBatchResult
from airweave.domains.entities.canonical.projection_store import CanonicalProjectionStore
from airweave.domains.entities.canonical.projector import CanonicalProjector
from airweave.domains.entities.canonical.source import indexed_record_types
from airweave.domains.sources.protocols import SourceRegistryProtocol
from airweave.platform.destinations.vespa.destination import VespaDestination


@dataclass
class ProjectCanonicalRecordsActivity:
    """One bounded page; Temporal retries infrastructure failures independently."""

    projector: CanonicalProjector
    source_registry: SourceRegistryProtocol

    @activity.defn(name="project_canonical_records_activity")
    async def run(
        self,
        organization_id: str,
        sync_id: str,
        after_id: str | None = None,
        skip_failed: bool = False,
    ) -> dict:
        """Resolve tenant collection scope fresh; never receive OAuth credentials."""
        organization = UUID(organization_id)
        sync = UUID(sync_id)
        async with get_db_context() as db:
            binding = await CanonicalProjectionStore().binding(db, organization, sync)
            if binding is None:
                return ProjectionBatchResult().model_dump(mode="json")
            source_name, collection_id = binding.source_name, binding.collection_id
            if not indexed_record_types(source_name, self.source_registry):
                return ProjectionBatchResult().model_dump(mode="json")
            # Do not construct an index client for sources with no pending records.
            pending = await CanonicalProjectionStore().pending(
                db,
                organization,
                sync,
                after_id=UUID(after_id) if after_id else None,
                limit=1,
                skip_failed=skip_failed,
            )
            if not pending:
                return ProjectionBatchResult().model_dump(mode="json")
        destination = await VespaDestination.create(
            collection_id=collection_id,
            organization_id=organization,
            logger=logger,
            soft_fail=False,
        )
        try:
            options = {"skip_failed": True} if skip_failed else {}
            result = await self.projector.batch(
                organization,
                sync,
                source_name,
                destination,
                logger,
                after_id=UUID(after_id) if after_id else None,
                **options,
            )
            return result.model_dump(mode="json")
        finally:
            await destination.close_connection()
