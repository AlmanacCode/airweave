"""One desired Almanac account/source relationship, not another record store."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import BigInteger, DateTime, ForeignKey, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from airweave.models._base import OrganizationBase


class OwnedProvisioning(OrganizationBase):
    """Durable idempotency and execution intent for the fixed Almanac client."""

    __tablename__ = "owned_provisioning"
    client_namespace: Mapped[str] = mapped_column(String(32), nullable=False)
    account_id: Mapped[UUID] = mapped_column(nullable=False)
    generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    request_payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    desired_state: Mapped[str] = mapped_column(String(20), nullable=False)
    observed_generation: Mapped[int] = mapped_column(BigInteger, default=0, nullable=False)
    source_connection_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("source_connection.id", ondelete="RESTRICT"), nullable=True
    )
    sync_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("sync.id", ondelete="RESTRICT"), nullable=True
    )
    initial_job_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("sync_job.id", ondelete="SET NULL"), nullable=True
    )
    cancellation_job_ids: Mapped[list[str]] = mapped_column(JSONB, default=list, nullable=False)
    verified_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "organization_id",
            "client_namespace",
            "account_id",
            name="uq_owned_provisioning_account",
        ),
        UniqueConstraint("sync_id", name="uq_owned_provisioning_sync"),
    )
