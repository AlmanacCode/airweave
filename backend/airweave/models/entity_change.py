"""Immutable snapshots of observed canonical record revisions."""

from uuid import UUID

from sqlalchemy import BigInteger, ForeignKey, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from airweave.models._base import OrganizationBase


class EntityChange(OrganizationBase):
    """Per-sync commit-ordered journal; snapshot survives later record updates."""

    __tablename__ = "entity_change"

    sync_id: Mapped[UUID] = mapped_column(ForeignKey("sync.id", ondelete="CASCADE"))
    entity_record_id: Mapped[UUID] = mapped_column(ForeignKey("entity.id", ondelete="CASCADE"))
    sequence: Mapped[int] = mapped_column(BigInteger)
    record_revision: Mapped[int] = mapped_column(BigInteger)
    kind: Mapped[str] = mapped_column(String)
    snapshot: Mapped[dict] = mapped_column(JSONB)

    __table_args__ = (UniqueConstraint("sync_id", "sequence", name="uq_entity_change_sequence"),)
