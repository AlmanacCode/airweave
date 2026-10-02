"""Body-free deletion manifest survives removal of the source and its records."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import BigInteger, DateTime, Index, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from airweave.models._base import Base


class ProjectionGeneration(Base):
    """Immutable feed identity; retired generations can never publish again."""

    __tablename__ = "projection_generation"
    organization_id: Mapped[UUID] = mapped_column(index=True)
    sync_id: Mapped[UUID] = mapped_column(index=True)
    collection_id: Mapped[UUID] = mapped_column()
    record_id: Mapped[UUID] = mapped_column(index=True)
    revision: Mapped[int] = mapped_column(BigInteger)
    pipeline_version: Mapped[int] = mapped_column(BigInteger)
    # NULL is a prepared-body-only attempt, never an index publication manifest.
    documents: Mapped[list | None] = mapped_column(JSONB(none_as_null=True), nullable=True)
    mail_body_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    mail_body_status: Mapped[str | None] = mapped_column(String(16), nullable=True)
    extraction_coverage: Mapped[dict | None] = mapped_column(
        JSONB(none_as_null=True), nullable=True
    )
    text_representations: Mapped[list | None] = mapped_column(
        JSONB(none_as_null=True), nullable=True
    )
    retired_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    next_gc_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    last_gc_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    delete_cursor: Mapped[int] = mapped_column(BigInteger, default=0, server_default="0")
    gc_passes: Mapped[int] = mapped_column(BigInteger, default=0, server_default="0")
    gc_attempt: Mapped[UUID | None] = mapped_column(nullable=True)
    gc_error: Mapped[str | None] = mapped_column(String(200), nullable=True)
    __table_args__ = (Index("idx_projection_gc_due", "next_gc_at", "id"),)
