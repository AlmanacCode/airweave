"""Tenant-scoped read-only configuration for shared provider request limits."""

import sqlalchemy as sa

from alembic import op

revision = "0020"
down_revision = "0019"
branch_labels = None
depends_on = None

TABLE = "source_rate_limits"
POLICIES = {"owned_limit_admission", "owned_limit_fence"}


def upgrade():
    """Require a known original protection state; grant no control or secret access."""
    bind = op.get_bind()
    namespace = bind.dialect.identifier_preparer.quote(
        bind.execute(sa.text("SELECT current_schema()")).scalar_one()
    )
    protected = bind.execute(
        sa.text(
            "SELECT c.relrowsecurity OR c.relforcerowsecurity OR EXISTS "
            "(SELECT 1 FROM pg_policy p WHERE p.polrelid=c.oid) FROM pg_class c "
            "JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname=current_schema() AND c.relname=:table"
        ),
        {"table": TABLE},
    ).scalar_one()
    if protected:
        raise RuntimeError("Source limit migration requires no preexisting RLS policies")
    table = f'{namespace}."{TABLE}"'
    if bind.execute(
        sa.text("SELECT has_table_privilege('airweave_tenant', :table, 'SELECT')"),
        {"table": table},
    ).scalar_one():
        raise RuntimeError("Source limit migration requires no preexisting tenant SELECT grant")
    scope = (
        "organization_id = NULLIF(current_setting('airweave.organization_id', true), '')::uuid"
    )
    op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY")
    op.execute(
        f"CREATE POLICY owned_limit_admission ON {table} AS PERMISSIVE "
        f"FOR SELECT TO airweave_tenant USING ({scope})"
    )
    op.execute(
        f"CREATE POLICY owned_limit_fence ON {table} AS RESTRICTIVE "
        f"FOR SELECT TO PUBLIC USING ({scope})"
    )
    op.execute(f"GRANT SELECT ON {table} TO airweave_tenant")


def downgrade():
    """Restore only the known unprotected state; refuse later policy additions."""
    bind = op.get_bind()
    namespace = bind.dialect.identifier_preparer.quote(
        bind.execute(sa.text("SELECT current_schema()")).scalar_one()
    )
    policies = set(
        bind.execute(
            sa.text(
                "SELECT policyname FROM pg_policies "
                "WHERE schemaname=current_schema() AND tablename=:table"
            ),
            {"table": TABLE},
        ).scalars()
    )
    if policies != POLICIES:
        raise RuntimeError("Source limit downgrade requires exactly the owned policies")
    table = f'{namespace}."{TABLE}"'
    for policy in sorted(POLICIES):
        op.execute(f'DROP POLICY "{policy}" ON {table}')
    op.execute(f"REVOKE SELECT ON {table} FROM airweave_tenant")
    op.execute(f"ALTER TABLE {table} NO FORCE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE {table} DISABLE ROW LEVEL SECURITY")
