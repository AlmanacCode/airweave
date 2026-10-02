"""Immutable personal owner binding on the existing service organization."""

import sqlalchemy as sa

from alembic import op

revision = "0016"
down_revision = "0015"
branch_labels = None
depends_on = None


def upgrade():
    """Legacy organizations remain unbound; no owner inference or adoption."""
    op.add_column("organization", sa.Column("owned_owner_user_id", sa.String(255), nullable=True))
    op.create_unique_constraint(
        "uq_organization_owned_owner", "organization", ["owned_owner_user_id"]
    )
    op.execute("""
        CREATE FUNCTION immutable_owned_tenant_owner() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF NEW.owned_owner_user_id IS DISTINCT FROM OLD.owned_owner_user_id THEN
                RAISE EXCEPTION 'Owned tenant owner binding is immutable';
            END IF;
            RETURN NEW;
        END;
        $$
    """)
    op.execute("""
        CREATE TRIGGER immutable_owned_tenant_owner
        BEFORE UPDATE OF owned_owner_user_id ON organization
        FOR EACH ROW EXECUTE FUNCTION immutable_owned_tenant_owner()
    """)


def downgrade():
    """Remove only enrollment metadata, never organizations or corpus rows."""
    op.execute("DROP TRIGGER immutable_owned_tenant_owner ON organization")
    op.execute("DROP FUNCTION immutable_owned_tenant_owner()")
    op.drop_constraint("uq_organization_owned_owner", "organization", type_="unique")
    op.drop_column("organization", "owned_owner_user_id")
