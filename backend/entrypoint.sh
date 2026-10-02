#!/bin/sh
set -eu

# Database migrations and bootstrap are explicit deployment jobs. Startup checks
# the provisioned schema; the process supervisor handles unavailable dependencies.
# exec delivers termination signals directly to Uvicorn for graceful shutdown.
exec python -m uvicorn airweave.main:app --host 0.0.0.0 --port 8001
