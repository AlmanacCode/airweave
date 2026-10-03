# Tenant transaction foundation

`tenant_session_factory(existing_engine, validated_uuid)` creates a new Session
with immutable public scope and the existing engine's pool. The `after_begin`
event sets the organization GUC transaction-locally on each transaction's actual
connection, including transactions started after UnitOfWork commits. `get_tenant_db_context` requires the explicit `TENANT_DATABASE_URI`; it never
falls back to the control or migration-owner credential. When configured, the
existing application pool ceiling is divided into one control slot and the
remaining tenant slots. Connection guards reject admin/owner credentials and
membership in the other runtime lane.

Resolve persisted authenticated organization authority before opening it. Never
switch the tenant of a loaded Session; create another Session. Public scope
immutability prevents accidental switching and ORM identity-map reuse. It does
not stop malicious trusted Python from rewriting private attributes, or a SQL
caller from changing a custom GUC. Normal Session context managers own
rollback/close; SQLAlchemy invalidates interrupted connections when required.

Three focused PostgreSQL tests use only one new synthetic schema/table and a
random actual LOGIN role with no owner/admin membership, no superuser or RLS
bypass privilege. They verify missing context, two-tenant read/write filtering,
repeated UnitOfWork commits, rollback, immutable scope, physical connection
reuse, statement timeout and asynchronous task cancellation. The runtime login
cannot SET ROLE to the migration account. The fixture drops only its own schema
and role; it applies no policies to real tables.

Migration 0017 adds FORCE RLS on 17 retained/authority tables and two nullable
registry tables. `sync_connection` and the trigger-maintained `entity_count`
use their existing parent relations; no inferred tenant columns are added.
Composite deferred references reject cross-tenant IDs while preserving existing
CASCADE/SET NULL actions. NULL/global connection credentials are denied. The
migration refuses preexisting policies/RLS and runtime table grants rather than
combining permissive policies or overwriting another protection scheme.

Three further actual-schema PostgreSQL checks capture two tenants through the
normal capture service using distinct tenant/control LOGIN DSNs; omitted WHERE
predicates return only the current tenant, foreign references/writes fail,
unscoped tenant sessions return no protected rows, and control cannot select
originals. They also verify refusal of preexisting PUBLIC policies and owner
runtime credentials. These checks apply migration 0017 only to fresh disposable
schemas, never the retained qualification corpus.

This is **not yet a fully integrated or deployed RLS boundary**. Post-auth HTTP,
source-worker and narrow cross-tenant discovery integration is a separate slice.
Real HTTP revocation, worker/GC boundaries, deployed least-privilege login grants,
legacy-row migration validation and PgBouncer qualification remain release gates.
The shared auth-provider name versus globally unique Connection.readable_id is
also an enrollment blocker for multiple owners; RLS does not resolve it or permit
global credentials as a workaround.
