.DEFAULT_GOAL := help
BACKEND := backend
PYTHON := .venv/bin/python
PYTEST := .venv/bin/pytest
IMAGE ?= almanac-source-store:local

.PHONY: help setup check test-store test-provisioning test-capture test-search test-auth test-health test-worker test-ocr test-index validate-deploy build

help:
	@echo 'setup         Install the locked backend and development dependencies'
	@echo 'build         Build the backend image locally; set IMAGE to choose its tag'
	@echo 'test-store    Verify record transactions and publication on disposable PostgreSQL'
	@echo 'test-capture  Verify source capture, transport and blob handling'
	@echo 'test-search   Verify search services and visibility behavior'
	@echo 'test-auth     Verify service-key and source authorization boundaries'
	@echo 'test-health   Verify dependency probes and readiness behavior'
	@echo 'test-worker   Verify owned projection, recovery and maintenance workflows'
	@echo 'test-ocr      Verify local OCR boundaries and per-file fallback'
	@echo 'test-index    Deploy/test schemas on explicitly disposable local Vespa'
	@echo 'check         Run store, capture, search, authentication and health checks'
	@echo 'validate-deploy Validate the Porter application manifest locally (no deployment)'
	@echo ''
	@echo 'test-store requires CANONICAL_TEST_DATABASE_URL; it creates and removes test schemas.'
	@echo 'See deploy/README.md for staging prerequisites and explicit deployment steps.'

build:
	docker build --tag "$(IMAGE)" $(BACKEND)

validate-deploy:
	porter apply validate -f porter.yaml

setup:
	cd $(BACKEND) && POETRY_VIRTUALENVS_IN_PROJECT=true uvx --from poetry==2.3.2 poetry install --with dev,lint --no-root --no-interaction

test-store:
	@test -n "$$CANONICAL_TEST_DATABASE_URL" || (echo 'Set CANONICAL_TEST_DATABASE_URL to a disposable PostgreSQL database.' >&2; exit 1)
	cd $(BACKEND) && $(PYTEST) -q -o log_cli=false airweave/domains/entities/canonical/tests airweave/domains/native_ingestion/tests airweave/domains/syncs/tests

test-capture:
	cd $(BACKEND) && $(PYTEST) -q -o log_cli=false airweave/platform/sources/tests tests/unit/platform/sources/records tests/unit/platform/sources/test_*_capture.py tests/unit/domains/entities tests/unit/platform/http_client/test_composio_transport.py tests/unit/platform/http_client/test_logging_privacy.py airweave/domains/storage/tests/test_file_service.py

test-provisioning:
	@test -n "$$CANONICAL_TEST_DATABASE_URL" || (echo 'Set CANONICAL_TEST_DATABASE_URL to a disposable PostgreSQL database.' >&2; exit 1)
	cd $(BACKEND) && $(PYTEST) -q -o log_cli=false airweave/domains/owned_provisioning/tests airweave/domains/source_connections/tests airweave/domains/temporal/tests airweave/domains/sources/tests/test_lifecycle.py airweave/domains/sync_pipeline/tests/test_factory.py

test-search:
	cd $(BACKEND) && $(PYTEST) -q -o log_cli=false airweave/domains/search airweave/domains/embedders

test-auth:
	cd $(BACKEND) && $(PYTEST) -q -o log_cli=false airweave/api/tests/test_service_auth.py airweave/api/tests/test_sync_authorization.py airweave/api/tests/test_context_resolver_auth.py

test-health:
	cd $(BACKEND) && $(PYTEST) -q -o log_cli=false airweave/core/health/tests airweave/adapters/health/tests airweave/core/container/tests/test_health_wiring.py

test-worker:
	@test -n "$$CANONICAL_TEST_DATABASE_URL" || (echo 'Set CANONICAL_TEST_DATABASE_URL to a disposable PostgreSQL database.' >&2; exit 1)
	cd $(BACKEND) && $(PYTEST) -q -o log_cli=false airweave/domains/temporal/activities/tests/test_project_canonical_records.py airweave/domains/temporal/activities/tests/test_discover_native_projection.py airweave/domains/temporal/activities/tests/test_cleanup_stuck_sync_jobs.py airweave/domains/temporal/workflows/tests/test_native_projection_recovery.py airweave/domains/temporal/workflows/tests/test_source_projection_replay.py airweave/domains/temporal/workflows/tests/test_reproject_command.py airweave/domains/temporal/workflows/tests/test_cleanup_workflows.py airweave/domains/temporal/workflows/tests/test_canonical_projection.py airweave/domains/temporal/worker/tests/test_wiring.py airweave/domains/temporal/worker/tests/test_config.py airweave/domains/temporal/worker/tests/test_worker.py airweave/domains/temporal/worker/tests/test_worker_ocr_guard.py

test-ocr:
	cd $(BACKEND) && $(PYTEST) -q -o log_cli=false airweave/domains/ocr/tests airweave/core/container/tests/test_ocr_wiring.py

test-index:
	@test "$$OWNED_VESPA_TEST" = 1 || (echo 'Set OWNED_VESPA_TEST=1 only for a disposable Vespa at localhost:8081/19071; this deploys schemas.' >&2; exit 1)
	cd $(BACKEND) && $(PYTEST) -q -o log_cli=false tests/integration/test_owned_vespa.py tests/integration/test_canonical_search_prefilters.py airweave/domains/entities/canonical/tests/test_real_vespa_projection.py

check: test-store test-provisioning test-capture test-search test-auth test-health test-worker test-ocr
