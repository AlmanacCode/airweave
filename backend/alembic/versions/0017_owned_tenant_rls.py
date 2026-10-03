"""Tenant policies and reference fences for the owned retained path.

Runtime credentials must be separate members of the tenant/control grant roles.
This migration creates no login credentials and grants no RLS bypass capability.
"""

import sqlalchemy as sa

from alembic import op

revision = "0017"
down_revision = "0016"
branch_labels = None
depends_on = None

TENANT = "airweave_tenant"
CONTROL = "airweave_control"
DISCOVERY = "airweave_discovery"
TABLES = (
    "entity",
    "entity_change",
    "entity_count",
    "projection_generation",
    "capture_scan",
    "sync_cursor",
    "sync",
    "sync_job",
    "source_connection",
    "connection",
    "owned_provisioning",
    "collection",
    "sync_connection",
    "organization",
    "api_key",
    "integration_credential",
    "connection_init_session",
)
# Existing single-column FKs keep their original CASCADE / SET NULL behavior.
# Deferred composite FKs check the final transaction after those actions finish.
REFERENCES = (
    ("sync_job", ("organization_id", "sync_id"), "sync", ("organization_id", "id")),
    ("entity", ("organization_id", "sync_id"), "sync", ("organization_id", "id")),
    ("entity", ("organization_id", "sync_job_id"), "sync_job", ("organization_id", "id")),
    ("entity_change", ("organization_id", "sync_id"), "sync", ("organization_id", "id")),
    (
        "entity_change",
        ("organization_id", "sync_id", "entity_record_id"),
        "entity",
        ("organization_id", "sync_id", "id"),
    ),
    ("capture_scan", ("organization_id", "sync_id"), "sync", ("organization_id", "id")),
    (
        "capture_scan",
        ("organization_id", "sync_id", "parent_record_id"),
        "entity",
        ("organization_id", "sync_id", "id"),
    ),
    ("sync_cursor", ("organization_id", "sync_id"), "sync", ("organization_id", "id")),
    ("source_connection", ("organization_id", "sync_id"), "sync", ("organization_id", "id")),
    (
        "source_connection",
        ("organization_id", "connection_id"),
        "connection",
        ("organization_id", "id"),
    ),
    (
        "source_connection",
        ("organization_id", "readable_auth_provider_id"),
        "connection",
        ("organization_id", "readable_id"),
    ),
    (
        "source_connection",
        ("organization_id", "readable_collection_id"),
        "collection",
        ("organization_id", "readable_id"),
    ),
    (
        "source_connection",
        ("organization_id", "connection_init_session_id"),
        "connection_init_session",
        ("organization_id", "id"),
    ),
    (
        "connection",
        ("organization_id", "integration_credential_id"),
        "integration_credential",
        ("organization_id", "id"),
    ),
    (
        "connection_init_session",
        ("organization_id", "final_connection_id"),
        "connection",
        ("organization_id", "id"),
    ),
    (
        "owned_provisioning",
        ("organization_id", "source_connection_id"),
        "source_connection",
        ("organization_id", "id"),
    ),
    ("owned_provisioning", ("organization_id", "sync_id"), "sync", ("organization_id", "id")),
    (
        "owned_provisioning",
        ("organization_id", "initial_job_id"),
        "sync_job",
        ("organization_id", "id"),
    ),
)
UNIQUES = (
    ("sync", ("organization_id", "id")),
    ("sync_job", ("organization_id", "id")),
    ("entity", ("organization_id", "id")),
    ("entity", ("organization_id", "sync_id", "id")),
    ("connection", ("organization_id", "id")),
    ("connection", ("organization_id", "readable_id")),
    ("collection", ("organization_id", "readable_id")),
    ("source_connection", ("organization_id", "id")),
    ("integration_credential", ("organization_id", "id")),
    ("connection_init_session", ("organization_id", "id")),
)


def _role(name):
    """Refuse an existing privileged role rather than silently inheriting it."""
    row = (
        op.get_bind()
        .execute(
            sa.text(
                "SELECT rolsuper,rolbypassrls,rolcreatedb,rolcreaterole,rolcanlogin "
                "FROM pg_roles WHERE rolname=:name"
            ),
            {"name": name},
        )
        .one_or_none()
    )
    if row is None:
        op.execute(
            f'CREATE ROLE "{name}" NOLOGIN NOINHERIT NOSUPERUSER NOBYPASSRLS '
            "NOCREATEDB NOCREATEROLE"
        )
    elif (
        any(row)
        or op.get_bind()
        .execute(
            sa.text(
                "SELECT EXISTS (SELECT 1 FROM pg_auth_members m "
                "JOIN pg_roles r ON r.oid=m.member WHERE r.rolname=:name)"
            ),
            {"name": name},
        )
        .scalar_one()
    ):
        raise RuntimeError("Owned database grant role already has forbidden privileges")


def _require_unprotected_schema(schema):
    """Never combine permissive policies or overwrite another deployment's grants."""
    protected = (*TABLES, "entity_definition", "auth_provider")
    bind = op.get_bind()
    for table in protected:
        existing = bind.execute(
            sa.text(
                "SELECT c.relrowsecurity OR c.relforcerowsecurity OR EXISTS "
                "(SELECT 1 FROM pg_policy p WHERE p.polrelid=c.oid) "
                "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname=:schema AND c.relname=:table"
            ),
            {"schema": schema, "table": table},
        ).scalar_one()
        if existing:
            raise RuntimeError("Owned RLS migration requires tables without preexisting policies")
    grants = bind.execute(
        sa.text(
            "SELECT EXISTS (SELECT 1 FROM information_schema.role_table_grants "
            "WHERE table_schema=:schema AND grantee IN "
            "('airweave_tenant','airweave_control','airweave_discovery'))"
        ),
        {"schema": schema},
    ).scalar_one()
    if grants:
        raise RuntimeError("Owned RLS migration requires no preexisting runtime table grants")


def upgrade():
    """Protect real retained tables without changing or inferring any tenant rows."""
    bind = op.get_bind()
    schema = bind.execute(sa.text("SELECT current_schema()")).scalar_one()
    _require_unprotected_schema(schema)
    quote = bind.dialect.identifier_preparer.quote
    namespace = quote(schema)
    for role in (TENANT, CONTROL, DISCOVERY):
        _role(role)
        op.execute(f'GRANT USAGE ON SCHEMA {namespace} TO "{role}"')
    for number, (table, columns) in enumerate(UNIQUES):
        op.create_unique_constraint(f"uq_owned_tenant_{number}", table, list(columns))
    for number, (table, columns, parent, remote) in enumerate(REFERENCES):
        op.create_foreign_key(
            f"fk_owned_tenant_{number}",
            table,
            parent,
            list(columns),
            list(remote),
            deferrable=True,
            initially="DEFERRED",
        )
    organization = "NULLIF(current_setting('airweave.organization_id',true),'')::uuid"
    for table in TABLES:
        qualified = f"{namespace}.{quote(table)}"
        if table == "organization":
            predicate = f"id = {organization}"
        elif table == "entity_count":
            predicate = (
                f"EXISTS (SELECT 1 FROM {namespace}.sync s "
                f"WHERE s.id=entity_count.sync_id AND s.organization_id={organization})"
            )
        elif table == "sync_connection":
            # No organization column exists here. Both existing parents must be
            # in the current tenant; no NULL/global connection exception.
            predicate = (
                f"EXISTS (SELECT 1 FROM {namespace}.sync s "
                f"JOIN {namespace}.connection c ON c.id=sync_connection.connection_id "
                f"WHERE s.id=sync_connection.sync_id AND s.organization_id={organization} "
                f"AND c.organization_id={organization})"
            )
        else:
            predicate = f"organization_id = {organization}"
        op.execute(f"ALTER TABLE {qualified} ENABLE ROW LEVEL SECURITY")
        op.execute(f"ALTER TABLE {qualified} FORCE ROW LEVEL SECURITY")
        op.execute(
            f'CREATE POLICY owned_tenant ON {qualified} TO "{TENANT}" '
            f"USING ({predicate}) WITH CHECK ({predicate})"
        )
        op.execute(f'GRANT SELECT,INSERT,UPDATE,DELETE ON {qualified} TO "{TENANT}"')
    # Only enrollment/authentication authority crosses organizations here.
    for table in ("organization", "api_key", "collection"):
        qualified = f"{namespace}.{quote(table)}"
        op.execute(
            f'CREATE POLICY owned_control ON {qualified} TO "{CONTROL}" '
            "USING (true) WITH CHECK (true)"
        )
        op.execute(f'GRANT SELECT,INSERT,UPDATE ON {qualified} TO "{CONTROL}"')
    _registry_grants(namespace, quote, organization)


def _registry_grants(namespace, quote, organization):
    # Nullable-organization registry definitions expose only global/own schemas,
    # never another tenant's customization. These are not connection secrets.
    for table in ("entity_definition", "auth_provider"):
        qualified = f"{namespace}.{quote(table)}"
        op.execute(f"ALTER TABLE {qualified} ENABLE ROW LEVEL SECURITY")
        op.execute(f"ALTER TABLE {qualified} FORCE ROW LEVEL SECURITY")
        op.execute(
            f'CREATE POLICY owned_tenant ON {qualified} TO "{TENANT}" '
            f"USING (organization_id IS NULL OR organization_id={organization})"
        )
        op.execute(
            f'CREATE POLICY owned_control ON {qualified} TO "{CONTROL}" '
            "USING (organization_id IS NULL)"
        )
    # Registry definitions are shared deployment metadata, never credentials.
    for table in (
        "vector_db_deployment_metadata",
        "entity_definition",
        "auth_provider",
    ):
        op.execute(f'GRANT SELECT ON {namespace}.{quote(table)} TO "{TENANT}","{CONTROL}"')
    # Existing authenticated identity bootstrap uses these relations; no content
    # table access or global credential permission is granted to control.
    for table in ("user", "user_organization", "feature_flag"):
        op.execute(f'GRANT SELECT ON {namespace}.{quote(table)} TO "{CONTROL}"')


def downgrade():
    """Remove this schema's policies/grants; never drop shared grant roles or data."""
    bind = op.get_bind()
    namespace = bind.dialect.identifier_preparer.quote(
        bind.execute(sa.text("SELECT current_schema()")).scalar_one()
    )
    for table in (*TABLES, "entity_definition", "auth_provider"):
        qualified = f"{namespace}.{bind.dialect.identifier_preparer.quote(table)}"
        op.execute(f"DROP POLICY IF EXISTS owned_control ON {qualified}")
        op.execute(f"DROP POLICY owned_tenant ON {qualified}")
        op.execute(f"ALTER TABLE {qualified} NO FORCE ROW LEVEL SECURITY")
        op.execute(f"ALTER TABLE {qualified} DISABLE ROW LEVEL SECURITY")
        op.execute(f'REVOKE ALL ON {qualified} FROM "{TENANT}","{CONTROL}"')
    for table in ("vector_db_deployment_metadata", "user", "user_organization", "feature_flag"):
        qualified = f"{namespace}.{bind.dialect.identifier_preparer.quote(table)}"
        op.execute(f'REVOKE ALL ON {qualified} FROM "{TENANT}","{CONTROL}"')
    for number, (table, *_rest) in reversed(tuple(enumerate(REFERENCES))):
        op.drop_constraint(f"fk_owned_tenant_{number}", table, type_="foreignkey")
    for number, (table, _columns) in reversed(tuple(enumerate(UNIQUES))):
        op.drop_constraint(f"uq_owned_tenant_{number}", table, type_="unique")
