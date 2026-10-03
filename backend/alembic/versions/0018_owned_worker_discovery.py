"""ID-only cross-tenant discovery; payload access always requires tenant scope."""

import sqlalchemy as sa

from alembic import op

revision = "0018"
down_revision = "0017"
branch_labels = None
depends_on = None

# Definer receives only eligibility/identity columns, never originals or manifests.
COLUMNS = {
    "sync": "id,organization_id,index_pipeline_version",
    "sync_job": "id,organization_id,sync_id,status,modified_at,started_at",
    "source_connection": (
        "id,organization_id,sync_id,is_authenticated,short_name,readable_collection_id"
    ),
    "collection": "organization_id,readable_id",
    "owned_provisioning": "organization_id,sync_id,desired_state",
    "entity": (
        "id,organization_id,sync_id,record_revision,indexed_revision,indexed_pipeline_version,"
        "indexed_generation,projection_error,deleted_at,removal_reason,parent_record_type,"
        "parent_native_id,parent_container_id,parent_visibility_epoch,entity_definition_short_name,"
        "native_id,container_id,visibility_epoch"
    ),
    "projection_generation": "id,organization_id,next_gc_at",
}


PENDING = """
SELECT s.organization_id,s.id FROM sync s
WHERE (p_after IS NULL OR s.id>p_after)
AND EXISTS (
    SELECT 1 FROM source_connection sc JOIN collection c
      ON c.organization_id=sc.organization_id AND c.readable_id=sc.readable_collection_id
    WHERE sc.organization_id=s.organization_id AND sc.sync_id=s.id
      AND sc.is_authenticated AND sc.short_name=ANY(p_sources)
      AND NOT EXISTS (SELECT 1 FROM owned_provisioning op
        WHERE op.organization_id=s.organization_id AND op.sync_id=s.id
          AND op.desired_state IN ('unavailable','disconnected'))
)
AND EXISTS (
    SELECT 1 FROM entity e WHERE e.organization_id=s.organization_id AND e.sync_id=s.id
      AND e.record_revision>0 AND e.projection_error IS NULL
      AND (e.indexed_revision IS DISTINCT FROM e.record_revision
        OR e.indexed_pipeline_version IS DISTINCT FROM s.index_pipeline_version
        OR e.indexed_generation IS NULL)
      AND (e.deleted_at IS NOT NULL OR (
        (e.removal_reason IS NULL OR e.removal_reason NOT IN ('scope_removed','access_revoked'))
        AND (e.parent_record_type IS NULL OR EXISTS (
          WITH RECURSIVE ancestors AS (
            SELECT p.id,p.organization_id,p.sync_id,p.parent_record_type,p.parent_native_id,
              p.parent_container_id,p.parent_visibility_epoch,ARRAY[p.id] AS path,false AS cycle,
              (p.record_revision>0 AND p.deleted_at IS NULL
                AND (p.removal_reason IS NULL
                  OR p.removal_reason NOT IN ('scope_removed','access_revoked'))
                AND p.visibility_epoch=e.parent_visibility_epoch) AS valid
            FROM entity p WHERE p.organization_id=e.organization_id AND p.sync_id=e.sync_id
              AND p.entity_definition_short_name=e.parent_record_type
              AND p.native_id=e.parent_native_id
              AND p.container_id IS NOT DISTINCT FROM e.parent_container_id
            UNION ALL
            SELECT p.id,p.organization_id,p.sync_id,p.parent_record_type,p.parent_native_id,
              p.parent_container_id,p.parent_visibility_epoch,a.path||p.id,p.id=ANY(a.path),
              (a.valid AND p.record_revision>0 AND p.deleted_at IS NULL
                AND (p.removal_reason IS NULL
                  OR p.removal_reason NOT IN ('scope_removed','access_revoked'))
                AND p.visibility_epoch=a.parent_visibility_epoch)
            FROM entity p JOIN ancestors a ON p.organization_id=a.organization_id
              AND p.sync_id=a.sync_id
              AND p.entity_definition_short_name=a.parent_record_type
              AND p.native_id=a.parent_native_id
              AND p.container_id IS NOT DISTINCT FROM a.parent_container_id WHERE NOT a.cycle
          ) SELECT 1 FROM ancestors WHERE parent_record_type IS NULL AND valid AND NOT cycle
        ))
      ))
)
ORDER BY s.id LIMIT p_limit+1
"""


def upgrade():
    """Grant fixed bounded functions, not a cross-tenant Session or bypass role."""
    bind = op.get_bind()
    namespace = bind.dialect.identifier_preparer.quote(
        bind.execute(sa.text("SELECT current_schema()")).scalar_one()
    )
    existing = bind.execute(
        sa.text(
            "SELECT c.relrowsecurity OR c.relforcerowsecurity OR EXISTS "
            "(SELECT 1 FROM pg_policy p WHERE p.polrelid=c.oid) FROM pg_class c "
            "JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname=current_schema() AND c.relname='feature_flag'"
        )
    ).scalar_one()
    if existing:
        raise RuntimeError(
            "Owned worker migration requires feature flags without preexisting policies"
        )
    # Workers retain current feature authority without reading billing data.
    op.execute(f"ALTER TABLE {namespace}.feature_flag ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE {namespace}.feature_flag FORCE ROW LEVEL SECURITY")
    op.execute(f"""
        CREATE POLICY owned_tenant ON {namespace}.feature_flag FOR SELECT TO "airweave_tenant"
        USING (organization_id=NULLIF(current_setting('airweave.organization_id',true),'')::uuid)
    """)
    op.execute(f"""
        CREATE POLICY owned_control ON {namespace}.feature_flag FOR SELECT
        TO "airweave_control" USING (true)
    """)
    op.execute(f'GRANT SELECT ON {namespace}.feature_flag TO "airweave_tenant"')
    for table, columns in COLUMNS.items():
        op.execute(
            f"CREATE POLICY owned_discovery ON {namespace}.{table} FOR SELECT "
            'TO "airweave_discovery" USING (true)'
        )
        op.execute(f'GRANT SELECT ({columns}) ON {namespace}.{table} TO "airweave_discovery"')
    # Explicit trusted search path prevents caller-controlled temporary relations
    # or objects from replacing the fixed SQL's authority relations/functions.
    path = f"pg_catalog,{namespace},pg_temp"
    op.execute(f"""
        CREATE FUNCTION {namespace}.owned_pending_sources(
          p_sources text[],p_after uuid,p_limit integer)
        RETURNS TABLE(organization_id uuid,sync_id uuid) LANGUAGE plpgsql SECURITY DEFINER
        SET search_path={path} AS $function$
        BEGIN
          IF p_limit IS NULL OR p_limit<1 OR p_limit>100 THEN
            RAISE EXCEPTION 'Discovery page size must be between 1 and 100';
          END IF;
          RETURN QUERY {PENDING};
        END $function$;
    """)
    op.execute(f"""
        CREATE FUNCTION {namespace}.owned_due_generations(p_now timestamptz,p_limit integer)
        RETURNS TABLE(organization_id uuid,generation_id uuid) LANGUAGE plpgsql SECURITY DEFINER
        SET search_path={path} AS $function$
        BEGIN
          IF p_limit IS NULL OR p_limit<1 OR p_limit>100 THEN
            RAISE EXCEPTION 'Cleanup page size must be between 1 and 100';
          END IF;
          RETURN QUERY SELECT g.organization_id,g.id FROM projection_generation g
            WHERE g.next_gc_at<=p_now ORDER BY g.next_gc_at,g.id LIMIT p_limit;
        END $function$;
    """)
    op.execute(f"""
        CREATE FUNCTION {namespace}.owned_stale_jobs(
          p_pending timestamp,p_running timestamp,p_after uuid,p_limit integer)
        RETURNS TABLE(organization_id uuid,job_id uuid) LANGUAGE plpgsql SECURITY DEFINER
        SET search_path={path} AS $function$
        BEGIN
          IF p_limit IS NULL OR p_limit<1 OR p_limit>100 THEN
            RAISE EXCEPTION 'Job discovery page size must be between 1 and 100';
          END IF;
          RETURN QUERY SELECT j.organization_id,j.id FROM sync_job j
            WHERE (p_after IS NULL OR j.id>p_after) AND (
              (j.status IN ('pending','cancelling') AND j.modified_at<p_pending)
              OR (j.status='running' AND j.started_at<p_running))
            AND NOT EXISTS (SELECT 1 FROM source_connection sc
              WHERE sc.organization_id=j.organization_id AND sc.sync_id=j.sync_id
                AND sc.short_name='almanac')
            ORDER BY j.id LIMIT p_limit;
        END $function$;
    """)
    for signature in (
        "owned_pending_sources(text[],uuid,integer)",
        "owned_due_generations(timestamptz,integer)",
        "owned_stale_jobs(timestamp,timestamp,uuid,integer)",
    ):
        op.execute(f'ALTER FUNCTION {namespace}.{signature} OWNER TO "airweave_discovery"')
        op.execute(f"REVOKE ALL ON FUNCTION {namespace}.{signature} FROM PUBLIC")
        op.execute(f'GRANT EXECUTE ON FUNCTION {namespace}.{signature} TO "airweave_control"')


def downgrade():
    """Remove only this migration's fixed capabilities."""
    bind = op.get_bind()
    namespace = bind.dialect.identifier_preparer.quote(
        bind.execute(sa.text("SELECT current_schema()")).scalar_one()
    )
    op.execute(f"DROP POLICY owned_tenant ON {namespace}.feature_flag")
    op.execute(f"DROP POLICY owned_control ON {namespace}.feature_flag")
    op.execute(f"ALTER TABLE {namespace}.feature_flag NO FORCE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE {namespace}.feature_flag DISABLE ROW LEVEL SECURITY")
    op.execute(f'REVOKE SELECT ON {namespace}.feature_flag FROM "airweave_tenant"')
    for signature in (
        "owned_pending_sources(text[],uuid,integer)",
        "owned_due_generations(timestamptz,integer)",
        "owned_stale_jobs(timestamp,timestamp,uuid,integer)",
    ):
        op.execute(f"DROP FUNCTION {namespace}.{signature}")
    for table, columns in COLUMNS.items():
        op.execute(f"DROP POLICY owned_discovery ON {namespace}.{table}")
        op.execute(f'REVOKE SELECT ({columns}) ON {namespace}.{table} FROM "airweave_discovery"')
