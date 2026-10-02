"""Source read authority; capture scheduling is not retained-content permission."""

from uuid import UUID

from sqlalchemy import and_, exists, not_, select
from sqlalchemy.sql.elements import ColumnElement

from airweave.models.owned_provisioning import OwnedProvisioning
from airweave.models.source_connection import SourceConnection


def source_is_readable(
    organization_id: UUID | ColumnElement[UUID], sync_id: UUID | ColumnElement[UUID]
) -> ColumnElement[bool]:
    """Require attested source availability and deny a withdrawn managed account.

    The authentication flag is also the explicit availability fact for native
    sources. A paused capture never promotes a previously unavailable source.
    Sync execution status and ready-generation fences belong to writers only.
    """
    withdrawn = exists(
        select(OwnedProvisioning.id).where(
            OwnedProvisioning.organization_id == organization_id,
            OwnedProvisioning.sync_id == sync_id,
            OwnedProvisioning.desired_state.in_(("unavailable", "disconnected")),
        )
    ).correlate_except(OwnedProvisioning)
    available = exists(
        select(SourceConnection.id).where(
            SourceConnection.organization_id == organization_id,
            SourceConnection.sync_id == sync_id,
            SourceConnection.is_authenticated.is_(True),
        )
    ).correlate_except(SourceConnection)
    return and_(available, not_(withdrawn))
