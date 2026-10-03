# Owned tenant SQL boundary

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

Actual-schema PostgreSQL checks capture two tenants through the
normal capture service using distinct tenant/control LOGIN DSNs; omitted WHERE
predicates return only the current tenant, foreign references/writes fail,
unscoped tenant sessions return no protected rows, and control cannot select
originals. They also verify refusal of preexisting PUBLIC policies and owner
runtime credentials. These checks apply migrations 0017 and 0018 to fresh disposable
schemas, never the retained qualification corpus.

Owned HTTP authenticates with a short-lived control Session, closes it, and opens
a separate tenant Session for retained reads, native imports and provisioning.
Source capture, projection, job transitions and cleanup reopen tenant Sessions
using their trusted workflow or discovered organization IDs. Migration 0018 adds
three bounded, fixed discovery functions for pending projection sources, due
generation IDs and stale job IDs. Their NOLOGIN definer has column-only access
to eligibility metadata; control can execute the functions but cannot assume the
definer role or select originals. Native jobs remain excluded from provider cleanup.
Feature flags remain readable under tenant scope without subscription tables.

Five actual-schema checks cover capture filtering and foreign references, policy
and credential rejection, concurrent two-tenant HTTP reads, forged organization
headers, cached-key revocation, source withdrawal and shared stale-job discovery.
The focused PostgreSQL regression batch passes 23 checks, including projection,
native import and service-key boundaries. Worker scope unit checks pass separately.
These are disposable-schema results, **not deployed RLS qualification**. SQL
organization isolation does not replace Almanac's private Account binding ACL.

Deployment requires applying 0017 then 0018 (before dependent 0019), validating
legacy foreign references, and provisioning distinct least-privilege LOGINs with
membership in exactly one runtime grant role. No migration-owner fallback is
supported. Owned composition refuses a missing tenant DSN and makes an actual
tenant-role PostgreSQL probe critical to readiness. Deployed grants, legacy-row
migration validation, worker deployment and PgBouncer qualification remain gates.
The shared auth-provider name versus globally unique Connection.readable_id is
also an enrollment blocker for multiple owners; RLS does not resolve it or permit
global credentials as a workaround.

Owned composition recognizes the complete existing enrollment-control pair
(organization plus allowed control API key IDs). Independently of local mode,
that service selects existing null subscription accounting and payment adapters;
it does not initialize Stripe or grant access to Airweave billing tables. Three
composition checks verify partial configuration rejection and retention of the
nonlocal Redis API rate limiter. Canonical/entity counters, provider backoff and
bounded shared processing remain separate from subscription accounting.
