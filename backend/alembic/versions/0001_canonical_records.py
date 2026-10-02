"""Canonical source records, fenced writers and observed change snapshots.

Revision ID: 0001
Revises: 0000
"""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "0001"
down_revision = "0000"
branch_labels = None
depends_on = None


def upgrade():
    """Legacy metadata remains revision zero until authoritative recapture."""
    op.drop_constraint("fk_entity_sync_job_id", "entity", type_="foreignkey")
    op.alter_column("entity", "sync_job_id", nullable=True)
    op.create_foreign_key(
        "fk_entity_sync_job_id",
        "entity",
        "sync_job",
        ["sync_job_id"],
        ["id"],
        ondelete="SET NULL",
    )
    for column in (
        sa.Column("native_id", sa.String(), nullable=True),
        sa.Column("container_id", sa.String(), nullable=True),
        sa.Column("parent_record_type", sa.String(), nullable=True),
        sa.Column("parent_native_id", sa.String(), nullable=True),
        sa.Column("parent_container_id", sa.String(), nullable=True),
        sa.Column("source_payload", postgresql.JSONB(), nullable=True),
        sa.Column("payload_schema_version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("record_revision", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("capture_hash", sa.String(), nullable=True),
        sa.Column("content_hash", sa.String(), nullable=True),
        sa.Column("source_created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("source_updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("removal_reason", sa.String(), nullable=True),
        sa.Column("completeness", sa.String(), nullable=True),
        sa.Column("blob_references", postgresql.JSONB(), nullable=True),
        sa.Column("last_seen_run_id", sa.UUID(), nullable=True),
        sa.Column("indexed_revision", sa.BigInteger(), nullable=True),
        sa.Column("indexed_pipeline_version", sa.BigInteger(), nullable=True),
        sa.Column("projection_error", sa.String(), nullable=True),
    ):
        op.add_column("entity", column)
    op.create_index(
        "idx_entity_canonical_scope",
        "entity",
        ["sync_id", "entity_definition_short_name", "container_id"],
    )
    op.create_index(
        "idx_entity_pending_revision",
        "entity",
        ["sync_id", "id"],
        postgresql_where=sa.text(
            "record_revision > 0 AND indexed_revision IS DISTINCT FROM record_revision"
        ),
    )
    for column in (
        sa.Column("observed_change_sequence", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("writer_epoch", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("writer_job_id", sa.UUID(), nullable=True),
        sa.Column("writer_attempt_id", sa.UUID(), nullable=True),
        sa.Column("writer_attempt_number", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("index_pipeline_version", sa.BigInteger(), nullable=False, server_default="1"),
    ):
        op.add_column("sync", column)
    op.create_table(
        "entity_change",
        sa.Column("id", sa.UUID(), nullable=False, primary_key=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("modified_at", sa.DateTime(), nullable=False),
        sa.Column(
            "organization_id",
            sa.UUID(),
            sa.ForeignKey("organization.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "sync_id", sa.UUID(), sa.ForeignKey("sync.id", ondelete="CASCADE"), nullable=False
        ),
        sa.Column(
            "entity_record_id",
            sa.UUID(),
            sa.ForeignKey("entity.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("sequence", sa.BigInteger(), nullable=False),
        sa.Column("record_revision", sa.BigInteger(), nullable=False),
        sa.Column("kind", sa.String(), nullable=False),
        sa.Column("snapshot", postgresql.JSONB(), nullable=False),
        sa.UniqueConstraint("sync_id", "sequence", name="uq_entity_change_sequence"),
    )
    op.create_index("ix_entity_change_organization_id", "entity_change", ["organization_id"])


def downgrade():
    """Refuse to destroy canonical source state through an accidental downgrade."""
    raise RuntimeError("Canonical record migration is forward-only; restore a verified backup")
