# Standalone service authentication and database setup

Use `AUTH_MODE=api_key` for the owned service. Every ordinary API request needs
an existing organization-scoped `X-API-Key`. The default is `api_key`: omitting
credentials does not select a demo user. This mode makes no Auth0 network calls
and does not create users from service credentials. User-only routes still
require user authentication; existing Connect sessions retain their own token
and scope checks. Auth0 mode preserves the existing user-first authentication
precedence when both user and API-key credentials are supplied.

`AUTH_MODE=auth0` retains hosted Auth0 user authentication and API-key access.
The existing Auth0 configuration remains required in that mode.
`AUTH_MODE=local` explicitly enables insecure demo authentication and is allowed
only for `ENVIRONMENT=local` or `test`. Never deploy local mode. The deprecated
`AUTH_ENABLED=true/false` is accepted as a migration input mapping to auth0/local;
contradicting it with AUTH_MODE is an error. Remove AUTH_ENABLED when adopting
AUTH_MODE. The environment label is operator-controlled, not an isolation boundary.

Keys are checked against persisted credentials on every request; expiry and
revocation take effect without waiting for a key-to-organization cache. All
record and job authorization still uses the key's organization. Authenticated
sync SSE subscriptions authorize job ownership before subscribing.

## Explicit database setup

Neither API startup nor the container entrypoint migrates the database or seeds
users, organizations, or keys. `RUN_ALEMBIC_MIGRATIONS` is no longer a runtime
switch. Run setup as a separately authorized deployment job with reviewed,
isolated database credentials:

```sh
# From backend/, against the intended isolated target only:
poetry run alembic upgrade head
poetry run python -m airweave.db.bootstrap
```

The second command installs native destination definitions and the initial embedding
metadata. Ordinary startup validates that metadata without inserting it. Service
mode does not require FIRST_SUPERUSER or FIRST_SUPERUSER_PASSWORD; these are
local bootstrap inputs only. Provision the
intended service organization and key through the existing model/CRUD operator
workflow, store the key in secret management, then start the API and workers.
Do not print keys in logs. Runtime replicas should not need migration privileges.
Temporal database setup is separate and must also target isolated resources.

For a disposable local demo only:

```sh
AUTH_MODE=local poetry run python -m airweave.db.bootstrap --local-superuser
```

This opts into the legacy demo user/org/key provisioning. It is refused in
api_key or auth0 mode. The production entrypoint starts Uvicorn without reload.
Almanac/WorkOS remains end-user identity authority; this service does not
introduce an independent customer login system.
