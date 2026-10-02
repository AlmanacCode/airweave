# Server-only personal tenant enrollment

This local implementation replaces manual tenant creation as an enrollment primitive.
It does not activate product onboarding, capture, native publication or deployment.
WorkOS and Almanac's authenticated backend remain the human-owner authority.

`POST /owned-tenants/ensure` accepts:

```json
{"owner_user_id":"user_opaque_subject","existing_only":false}
```

The private response contains `owner_user_id`, `organization_id`, `collection`,
`api_key_id`, `api_key` (the usable scoped credential), and aware UTC `expires_at`.
`Cache-Control: no-store` applies. The key is masked in Python representations;
its plaintext JSON serialization is deliberate for this authorized backend boundary.
Do not log requests/responses, keys or human subjects. Product must take the subject
from its authenticated actor/current database authority, never a public owner field.

Configure one **dedicated empty control organization**, not a user's destination:

- `AUTH_MODE=api_key`, using existing fresh stored-key validation.
- `OWNED_TENANT_CONTROL_ORGANIZATION_ID`: control organization's UUID.
- `OWNED_TENANT_CONTROL_API_KEY_IDS`: JSON array of its allowed API-key UUIDs.

Both are required to admit enrollment. A missing configuration returns503. Exact
verified control org and allowlisted key ID must both match; no Auth0/local user
session can enroll. Every key belonging to the configured control org is restricted
to POST of the actual ensure endpoint. Mounted prefixes and both trailing-slash
variants work; other methods/endpoints cannot obtain authenticated control access.
The control key cannot authenticate read, search, download, key-management or admin
operations, even if `api_key_admin_sync` is accidentally enabled. Public health
routes do not authenticate any credential: perform anonymous health checks rather
than treating a health response as control-key validation.

For control credential rotation, allowlist old and new IDs during the deliberate
transition. Revoke/delete the old key after switching the backend, then remove its
ID. A de-allowlisted key still cannot gain ordinary/admin access in the control org.
Before removing/changing the control-org setting itself, revoke all old control
keys explicitly; that setting change otherwise removes their org-level restriction.
No hidden durable control scope or extra authentication system is introduced.

The stored nullable unique `Organization.owned_owner_user_id` is immutable in SQL,
including NULL→owner changes. Migration0016 leaves every legacy organization unbound.
The ensure operation never adopts an existing unbound organization. Versioned
namespace IDs make identical attempts recoverable; stored owner equality, unique
subject and exact org/collection/key identities are mandatory authority checks.
Email, labels, UUID derivation and generic metadata are not ownership evidence.
Each owner has a globally unique readable collection, because the existing Collection
schema makes readable IDs unique across the deployment. Existing shared embedding
metadata must contain its single deployment row; absence or ambiguity returns503
and rolls back new enrollment rather than initializing services or model configuration.

The organization row lock and designated APIKey row lock serialize first creation,
retries, renewal and concurrent revocation. Organization, Collection and APIKey
creation commit atomically. A lost response returns those same rows/key on retry.
A valid key is unchanged. An existing decryptable expired key is renewed in its same
row with a new opaque key and **90-day validity**; the old key immediately ceases to
validate, and a lost renewal reply recovers the new key. There is no force-rotation
API in this slice. A missing/deleted/malformed designated key on an existing tenant
returns409 requiring operator recovery; ensure cannot undo explicit key withdrawal.
Missing/conflicting collection or owner binding also returns409 rather than repair.
Disconnecting one account must not delete this owner binding or reroute other sources.
Deleted WorkOS owners are never reactivated by product enrollment calls.

`existing_only=true` returns404 with `detail.code=not_enrolled` when no tenant exists,
without creating any rows. An existing tenant can recover/renew its credential under
the same withdrawal checks, allowing the backend to deliver deletion/unavailability
without allocating tenants for never-enrolled deleted users. This is not a list API.

Verification on2026-10-02 uses freshly created, fully migrated disposable PostgreSQL
databases, then removes only those test databases. SQL and ASGI HTTP qualify concurrent
creation, response-loss retry, expired renewal, withdrawn/malformed keys, owner
conflicts/immutability, lookup-only absence, missing deployment metadata, actual issued
key canonical reads, counterfeit/cross-owner rejection, mounted ensure routes and
control-key denial despite the admin flag/de-allowlisting. Existing service-auth
regressions are included. No production configuration, existing corpus migration,
provider call, Auth0 user, billing customer, worker or cloud resource was created.

Product follow-up must replace manual owner target/key arrays with this enrollment
resolver and explicit global provider policies, while retaining existing binding
intent/ACK and native receipt authorities. Four interactive providers and Wispr's
trusted initial broker admission remain distinct. This endpoint alone does not
establish ten-user automatic capture or freshness capacity.
