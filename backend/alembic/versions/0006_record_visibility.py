"""Epoch-attested canonical record-parent visibility.

Revision ID: 0006
Revises: 0005
"""

import sqlalchemy as sa

from alembic import op

revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade():
    """Attest only existing visible flat links; never revive withdrawn content."""
    op.add_column(
        "entity", sa.Column("visibility_epoch", sa.BigInteger(), nullable=False, server_default="1")
    )
    op.add_column("entity", sa.Column("parent_visibility_epoch", sa.BigInteger()))
    op.create_check_constraint(
        "ck_entity_visibility_epoch",
        "entity",
        "visibility_epoch >= 1 AND "
        "(parent_visibility_epoch IS NULL OR parent_visibility_epoch >= 1)",
    )
    op.create_index(
        "idx_entity_canonical_identity",
        "entity",
        ["sync_id", "entity_definition_short_name", "native_id", "container_id"],
    )
    op.create_index(
        "idx_entity_canonical_parent",
        "entity",
        ["sync_id", "parent_record_type", "parent_native_id", "parent_container_id"],
    )
    op.execute("""
        UPDATE entity child SET parent_visibility_epoch = parent.visibility_epoch
        FROM entity parent
        WHERE child.organization_id = parent.organization_id
          AND child.sync_id = parent.sync_id
          AND child.parent_record_type = parent.entity_definition_short_name
          AND child.parent_native_id = parent.native_id
          AND child.parent_container_id IS NOT DISTINCT FROM parent.container_id
          AND child.record_revision > 0 AND parent.record_revision > 0
          AND parent.parent_record_type IS NULL AND parent.deleted_at IS NULL
          AND (parent.removal_reason IS NULL
               OR parent.removal_reason NOT IN ('scope_removed', 'access_revoked'))
          AND (child.removal_reason IS NULL
               OR child.removal_reason NOT IN ('scope_removed', 'access_revoked'))
    """)


def downgrade():
    """Removing visibility attestations requires an explicit operator decision."""
    raise RuntimeError("Record visibility cannot safely downgrade to flat parent checks")
