"""Distinguish reversible read withdrawal from scheduling-only pause."""

from alembic import op

revision = "0013"
down_revision = "0012"
branch_labels = None
depends_on = None


def upgrade():
    """Reuse the existing desired-state authority and retained identity."""
    op.drop_constraint("ck_owned_provisioning_state", "owned_provisioning", type_="check")
    op.create_check_constraint(
        "ck_owned_provisioning_state",
        "owned_provisioning",
        "desired_state IN ('active', 'paused', 'unavailable', 'disconnected')",
    )


def downgrade():
    """Reject rollback while unavailable relationships require the newer contract."""
    op.drop_constraint("ck_owned_provisioning_state", "owned_provisioning", type_="check")
    op.create_check_constraint(
        "ck_owned_provisioning_state",
        "owned_provisioning",
        "desired_state IN ('active', 'paused', 'disconnected')",
    )
