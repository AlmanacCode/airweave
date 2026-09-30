"""One recoverable whole-scope scan, independent of worker attempts."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import BigInteger, CheckConstraint, DateTime, ForeignKey, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from airweave.models._base import OrganizationBase


class CaptureScan(OrganizationBase):
    """Only the canonical capture transaction boundary mutates this state."""

    __tablename__ = "capture_scan"
    sync_id: Mapped[UUID] = mapped_column(ForeignKey("sync.id", ondelete="CASCADE"))
    scope_key: Mapped[str] = mapped_column(String)
    record_type: Mapped[str] = mapped_column(String)
    container_id: Mapped[str | None] = mapped_column(String)
    cycle_id: Mapped[UUID]
    sweep_id: Mapped[UUID]
    revision: Mapped[int] = mapped_column(BigInteger)
    phase: Mapped[str] = mapped_column(String)
    fingerprint: Mapped[str] = mapped_column(String(64))
    continuation: Mapped[dict] = mapped_column(JSONB)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        UniqueConstraint("sync_id", "scope_key", name="uq_capture_scan_scope"),
        CheckConstraint(
            "phase IN ('collecting','reconciling','complete')", name="ck_capture_scan_phase"
        ),
        CheckConstraint("revision >= 1", name="ck_capture_scan_revision"),
    )
