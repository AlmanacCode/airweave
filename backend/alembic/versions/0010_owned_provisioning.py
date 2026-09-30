"""Durable account provisioning and generation-fenced capture admission."""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "0010"
down_revision = "0009"
branch_labels = None
depends_on = None


def upgrade():
    """Existing sources/jobs remain unmanaged generation zero."""
    for table, column in (
        ("sync", "provisioning_generation"),
        ("sync", "provisioning_ready_generation"),
        ("sync_job", "provisioning_generation"),
    ):
        op.add_column(table, sa.Column(column, sa.BigInteger(), nullable=False, server_default="0"))
    op.create_table(
        "owned_provisioning",
        sa.Column("id", sa.UUID(), primary_key=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("modified_at", sa.DateTime(), nullable=False),
        sa.Column(
            "organization_id",
            sa.UUID(),
            sa.ForeignKey("organization.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("client_namespace", sa.String(32), nullable=False),
        sa.Column("account_id", sa.UUID(), nullable=False),
        sa.Column("generation", sa.BigInteger(), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("request_payload", postgresql.JSONB(), nullable=False),
        sa.Column("desired_state", sa.String(20), nullable=False),
        sa.Column("observed_generation", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column(
            "source_connection_id",
            sa.UUID(),
            sa.ForeignKey("source_connection.id", ondelete="RESTRICT"),
        ),
        sa.Column("sync_id", sa.UUID(), sa.ForeignKey("sync.id", ondelete="RESTRICT")),
        sa.Column("initial_job_id", sa.UUID(), sa.ForeignKey("sync_job.id", ondelete="SET NULL")),
        sa.Column("cancellation_job_ids", postgresql.JSONB(), nullable=False, server_default="[]"),
        sa.Column("verified_at", sa.DateTime()),
        sa.UniqueConstraint(
            "organization_id",
            "client_namespace",
            "account_id",
            name="uq_owned_provisioning_account",
        ),
        sa.UniqueConstraint("sync_id", name="uq_owned_provisioning_sync"),
        sa.CheckConstraint(
            "generation > 0 AND observed_generation >= 0 AND observed_generation <= generation",
            name="ck_owned_provisioning_generation",
        ),
        sa.CheckConstraint("client_namespace = 'almanac'", name="ck_owned_provisioning_namespace"),
        sa.CheckConstraint(
            "desired_state IN ('active', 'paused', 'disconnected')",
            name="ck_owned_provisioning_state",
        ),
    )


def downgrade():
    """Only explicit rollback removes generation protections."""
    op.drop_table("owned_provisioning")
    op.drop_column("sync_job", "provisioning_generation")
    op.drop_column("sync", "provisioning_ready_generation")
    op.drop_column("sync", "provisioning_generation")
