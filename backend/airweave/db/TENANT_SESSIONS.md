# Tenant transaction foundation

`tenant_session_factory(existing_engine, validated_uuid)` creates a new Session
with immutable public scope and the existing engine's pool. The `after_begin`
event sets the organization GUC transaction-locally on each transaction's actual
connection, including transactions started after UnitOfWork commits. The opt-in
`get_tenant_db_context` exposes this through the existing database composition.

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

This is **not an integrated RLS boundary**. Existing HTTP authentication, capture,
native, projection and control sessions remain unchanged. Real table policies,
least-privilege deployed grants, separate bootstrap/control capabilities,
composite tenant-reference fences, worker discovery and PgBouncer qualification
are required before advertising hosted tenant database isolation.
