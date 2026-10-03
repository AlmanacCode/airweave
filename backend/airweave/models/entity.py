"""Entity model."""

from datetime import datetime
from typing import TYPE_CHECKING, Optional
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    String,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from airweave.models._base import OrganizationBase

if TYPE_CHECKING:
    from airweave.models.sync import Sync
    from airweave.models.sync_job import SyncJob


class Entity(OrganizationBase):
    """Entity model."""

    __tablename__ = "entity"

    # Override organization_id to disable index (table too large, better indexes exist)
    organization_id: Mapped[UUID] = mapped_column(
        ForeignKey("organization.id", ondelete="CASCADE"),
        nullable=False,
        index=False,  # Disabled: billions of rows, not selective, better indexes on sync_id
    )

    sync_job_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey("sync_job.id", ondelete="SET NULL", name="fk_entity_sync_job_id"), nullable=True
    )
    sync_id: Mapped[UUID] = mapped_column(
        ForeignKey("sync.id", ondelete="CASCADE", name="fk_entity_sync_id"), nullable=False
    )
    entity_id: Mapped[str] = mapped_column(String, nullable=False)
    entity_definition_short_name: Mapped[Optional[str]] = mapped_column(
        String,
        nullable=True,
        comment="Entity definition short_name from the registry (e.g. asana_task_entity)",
    )
    hash: Mapped[str] = mapped_column(String, nullable=False)

    # Canonical source state. Revision zero identifies legacy metadata-only rows.
    native_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    container_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    parent_record_type: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    parent_native_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    parent_container_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    visibility_epoch: Mapped[int] = mapped_column(BigInteger, default=1, server_default="1")
    parent_visibility_epoch: Mapped[Optional[int]] = mapped_column(BigInteger)
    source_payload: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    gmail_metadata: Mapped[Optional[dict]] = mapped_column(JSONB(none_as_null=True), nullable=True)
    gmail_metadata_revision: Mapped[Optional[int]] = mapped_column(BigInteger)
    payload_schema_version: Mapped[int] = mapped_column(default=1, server_default="1")
    record_revision: Mapped[int] = mapped_column(BigInteger, default=0, server_default="0")
    capture_hash: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    content_hash: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    source_created_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    meeting_started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    source_updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    observed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    first_observed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    revision_observed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    first_stored_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    deleted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    removal_reason: Mapped[Optional[str]] = mapped_column(String)
    completeness: Mapped[Optional[str]] = mapped_column(String)
    blob_references: Mapped[Optional[list]] = mapped_column(JSONB)
    last_seen_run_id: Mapped[Optional[UUID]] = mapped_column(nullable=True)
    indexed_generation: Mapped[Optional[UUID]] = mapped_column(nullable=True)
    indexed_chunk_count: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    indexed_revision: Mapped[Optional[int]] = mapped_column(BigInteger)
    indexed_pipeline_version: Mapped[Optional[int]] = mapped_column(BigInteger)
    projection_error: Mapped[Optional[str]] = mapped_column(String)

    # Add back references
    sync_job: Mapped[Optional["SyncJob"]] = relationship(
        "SyncJob",
        back_populates="entities",
        lazy="noload",
    )

    sync: Mapped["Sync"] = relationship(
        "Sync",
        back_populates="entities",
        lazy="noload",
    )

    __table_args__ = (
        CheckConstraint(
            "visibility_epoch >= 1 AND "
            "(parent_visibility_epoch IS NULL OR parent_visibility_epoch >= 1)",
            name="ck_entity_visibility_epoch",
        ),
        Index(
            "idx_entity_canonical_identity",
            "sync_id",
            "entity_definition_short_name",
            "native_id",
            "container_id",
        ),
        Index(
            "idx_entity_canonical_parent",
            "sync_id",
            "parent_record_type",
            "parent_native_id",
            "parent_container_id",
        ),
        Index(
            "idx_entity_canonical_scope", "sync_id", "entity_definition_short_name", "container_id"
        ),
        Index(
            "idx_entity_pending_revision",
            "sync_id",
            "id",
            postgresql_where=text(
                "record_revision > 0 AND indexed_revision IS DISTINCT FROM record_revision"
            ),
        ),
        UniqueConstraint(
            "sync_id",
            "entity_id",
            "entity_definition_short_name",
            name="uq_sync_id_entity_id_entity_def_short_name",
        ),
        Index("idx_entity_sync_id", "sync_id"),
        Index("idx_entity_sync_job_id", "sync_job_id"),
        Index("idx_entity_entity_id", "entity_id"),
        Index("idx_entity_entity_def_short_name", "entity_definition_short_name"),
        Index("idx_entity_entity_id_sync_id", "entity_id", "sync_id"),
        Index(
            "idx_entity_sync_id_entity_def_short_name",
            "sync_id",
            "entity_definition_short_name",
        ),
    )
