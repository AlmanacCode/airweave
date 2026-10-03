"""Activity and workflow wiring.

This module is the DI wiring point for Temporal.
It connects activities to their dependencies from the container.
"""

from airweave.core.logging import logger


def create_activities() -> list:
    """Create activity instances with dependencies from the container.

    This is the DI wiring point for Temporal activities.
    Each activity class declares its dependencies in __init__.

    Returns:
        List of activity .run methods to register with the worker.

    Future: This will evolve as we add more protocols to the container.
    """
    from airweave import crud
    from airweave.core.container import container
    from airweave.domains.temporal.activities import (
        CheckAndNotifyExpiringKeysActivity,
        CleanupStuckSyncJobsActivity,
        CleanupSyncDataActivity,
        CreateSyncJobActivity,
        RunSyncActivity,
        SelfDestructOrphanedSyncActivity,
        TransitionSyncJobActivity,
    )

    if container is None:
        raise RuntimeError("Container not initialized — cannot wire activities")

    from airweave.db.session import get_tenant_db_context
    from airweave.domains.entities.canonical.projection_store import CanonicalProjectionStore
    from airweave.domains.entities.canonical.projector import CanonicalProjector
    from airweave.domains.sync_pipeline.processors.chunk_embed import ChunkEmbedProcessor
    from airweave.domains.temporal.activities.cleanup_projection_generations import (
        CleanupProjectionGenerationsActivity,
    )
    from airweave.domains.temporal.activities.discover_native_projection import (
        DiscoverNativeProjectionActivity,
    )
    from airweave.domains.temporal.activities.project_canonical_records import (
        ProjectCanonicalRecordsActivity,
    )

    email_service = container.email_service
    event_bus = container.event_bus
    sync_service = container.sync_service
    state_machine = container.sync_job_state_machine
    sync_repo = container.sync_repo
    sync_job_repo = container.sync_job_repo
    entity_repo = container.entity_repo
    sc_repo = container.sc_repo
    conn_repo = container.conn_repo
    collection_repo = container.collection_repo
    temporal_workflow_service = container.temporal_workflow_service
    temporal_schedule_service = container.temporal_schedule_service
    arf_service = container.arf_service

    logger.debug("Wiring activities with container dependencies")

    return [
        DiscoverNativeProjectionActivity(container.source_registry).run,
        CleanupProjectionGenerationsActivity(container.storage_backend).run,
        ProjectCanonicalRecordsActivity(
            projector=CanonicalProjector(
                CanonicalProjectionStore(),
                get_tenant_db_context,
                ChunkEmbedProcessor(
                    container.converter_registry,
                    container.dense_embedder,
                    container.sparse_embedder,
                ),
                container.storage_backend,
                recipe=container.preparation_recipe,
            ),
            source_registry=container.source_registry,
        ).run,
        RunSyncActivity(
            sync_service=sync_service,
            sync_repo=sync_repo,
            sync_job_repo=sync_job_repo,
            collection_repo=collection_repo,
        ).run,
        CreateSyncJobActivity(
            event_bus=event_bus,
            sync_repo=sync_repo,
            sync_job_repo=sync_job_repo,
            sc_repo=sc_repo,
            conn_repo=conn_repo,
            collection_repo=collection_repo,
        ).run,
        TransitionSyncJobActivity(
            state_machine=state_machine,
        ).run,
        CleanupStuckSyncJobsActivity(
            temporal_workflow_service=temporal_workflow_service,
            state_machine=state_machine,
            entity_repo=entity_repo,
            org_repo=crud.organization,
        ).run,
        # Cleanup
        SelfDestructOrphanedSyncActivity(
            temporal_schedule_service=temporal_schedule_service,
        ).run,
        CleanupSyncDataActivity(
            temporal_schedule_service=temporal_schedule_service,
            arf_service=arf_service,
        ).run,
        # Notifications
        CheckAndNotifyExpiringKeysActivity(
            email_service=email_service,
        ).run,
    ]


def get_workflows() -> list:
    """Get workflow classes to register.

    Returns:
        List of workflow classes.
    """
    from airweave.domains.temporal.workflows import (
        APIKeyExpirationCheckWorkflow,
        CleanupStuckSyncJobsWorkflow,
        CleanupSyncDataWorkflow,
        RunSourceConnectionWorkflow,
    )
    from airweave.domains.temporal.workflows.project_canonical_records import (
        ProjectCanonicalRecordsWorkflow,
    )
    from airweave.domains.temporal.workflows.recover_native_projection import (
        RecoverNativeProjectionWorkflow,
    )

    return [
        RecoverNativeProjectionWorkflow,
        ProjectCanonicalRecordsWorkflow,
        RunSourceConnectionWorkflow,
        CleanupStuckSyncJobsWorkflow,
        CleanupSyncDataWorkflow,
        APIKeyExpirationCheckWorkflow,
    ]
