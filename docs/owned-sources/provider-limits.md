# Configured provider request limits

Source capture always consults its supplied request limiter; inherited subscription
feature flags no longer bypass configured protection. An absent exact organization
and source row remains unconfigured/unlimited. There is no global default row.
Credential validation keeps its existing separate client without a limiter.

DatabaseRateLimitConfigProvider uses a tenant-scoped transaction. A database error
propagates before provider transport and is never cached as an absent limit.
Redis config caching remains five minutes, including legitimate absent rows; an
already cached configuration can be used without a fresh database read. Configured
counting uses the existing Redis sliding-window Lua operation. Redis enforcement
errors propagate. These are request quota bounds, not capture concurrency bounds.

Local migration 0020 requires an initially unprotected source_rate_limits table
and no prior tenant SELECT grant. It adds SELECT-only tenant admission plus a
restrictive PUBLIC tenant fence (missing context denies), forces RLS, and grants
no control access. Downgrade refuses unexpected policies rather than disabling
other protection. No existing corpus or production migration has been applied.
Any operator configuration-write path must be qualified separately under its real
role; this migration does not introduce an operator API or runtime write grant.

Gmail, Drive, Calendar and Slack registry entries declare organization-level
limiting. Wispr does not declare a rate-limit level, so this is not evidence of
Wispr MCP quota enforcement. The existing HTTP 429 Retry-After conversion uses an
integer number of seconds; no subsecond retry behavior is qualified here.

## Local verification

- Seventy existing configuration/lifecycle checks passed, including capture with
  no paid feature flag calling the configured limiter before HTTP.
- Actual PostgreSQL test uses independent nonowner LOGIN tenant/control roles:
  own limit visible, foreign rows and missing tenant context hidden, control
  SELECT denied, absent row returns no configuration.
- Disposable Redis 7.2.7 and the actual limiter/client accept one synthetic
  external HTTP request and reject the next with 429 before its transport.
- After removing the config cache entry, a simulated DB outage rejects the
  request before transport and leaves the cache empty. Only the unavailable DB
  seam and external provider transport are synthetic in this failure case.
- Actual SQL/Redis check: one passed in 0.69 seconds. Its schema, login roles and
  nonpersistent Redis process are removed by fixture cleanup.

This prerequisite does not qualify account provisioning through Temporal to a
new retained original, hosted role deployment, or multiowner broker credentials.
