"""Explicit tenant sessions over an existing engine; no roles or policies are changed."""

from uuid import UUID

from pydantic import BaseModel, ConfigDict
from sqlalchemy import event, text
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import Session, SessionTransaction


class TenantScope(BaseModel):
    """Validated scope is fixed for the lifetime of one Session identity map."""

    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)
    organization_id: UUID


class TenantSession(Session):
    """Never switch organizations on a Session containing loaded ORM objects."""

    def __init__(self, *, tenant_scope: TenantScope, **kwargs):
        """Require explicit validated scope rather than an ambient request variable."""
        super().__init__(**kwargs)
        self._tenant_scope = tenant_scope

    @property
    def tenant_scope(self) -> TenantScope:
        """Expose immutable scope without a reassignment operation."""
        return self._tenant_scope


@event.listens_for(TenantSession, "after_begin")
def apply_tenant_scope(
    session: TenantSession, transaction: SessionTransaction, connection: Connection
) -> None:
    """Reapply after every commit/rollback, including UnitOfWork's internal commits."""
    connection.execute(
        text("SELECT set_config('airweave.organization_id', :organization, true)"),
        {"organization": str(session.tenant_scope.organization_id)},
    )


def tenant_session_factory(
    engine: AsyncEngine, organization_id: UUID
) -> async_sessionmaker[AsyncSession]:
    """Reuse the supplied pool; ordinary Session context managers own cleanup."""
    scope = TenantScope(organization_id=organization_id)
    return async_sessionmaker(
        engine,
        autoflush=False,
        expire_on_commit=False,
        sync_session_class=TenantSession,
        tenant_scope=scope,
    )
