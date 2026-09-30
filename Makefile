.DEFAULT_GOAL := help
BACKEND := backend
PYTHON := .venv/bin/python
PYTEST := .venv/bin/pytest
IMAGE ?= almanac-source-store:local

.PHONY: help setup check test-store test-capture test-search test-auth test-health test-index validate-deploy build

help:
	@echo 'setup         Install the locked backend and development dependencies'
	@echo 'build         Build the backend image locally; set IMAGE to choose its tag'
	@echo 'test-store    Verify record transactions and publication on disposable PostgreSQL'
	@echo 'test-capture  Verify source capture, transport and blob handling'
	@echo 'test-search   Verify search services and visibility behavior'
	@echo 'test-auth     Verify service-key and source authorization boundaries'
	@echo 'test-health   Verify dependency probes and readiness behavior'
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
	cd $(BACKEND) && $(PYTEST) -q -o log_cli=false airweave/domains/entities/canonical/tests

test-capture:
	cd $(BACKEND) && $(PYTEST) -q -o log_cli=false airweave/platform/sources/tests tests/unit/platform/sources/records tests/unit/platform/sources/test_slack_capture.py tests/unit/platform/sources/test_wispr_capture.py tests/unit/platform/http_client/test_composio_transport.py airweave/domains/storage/tests/test_file_service.py

test-search:
	cd $(BACKEND) && $(PYTEST) -q -o log_cli=false airweave/domains/search

test-auth:
	cd $(BACKEND) && $(PYTEST) -q -o log_cli=false airweave/api/tests/test_service_auth.py airweave/api/tests/test_sync_authorization.py airweave/api/tests/test_context_resolver_auth.py

test-health:
	cd $(BACKEND) && $(PYTEST) -q -o log_cli=false airweave/core/health/tests airweave/adapters/health/tests airweave/core/container/tests/test_health_wiring.py

test-index:
	@test "$$OWNED_VESPA_TEST" = 1 || (echo 'Set OWNED_VESPA_TEST=1 only for a disposable Vespa at localhost:8081/19071; this deploys schemas.' >&2; exit 1)
	cd $(BACKEND) && $(PYTEST) -q -o log_cli=false tests/integration/test_owned_vespa.py airweave/domains/entities/canonical/tests/test_real_vespa_projection.py

check: test-store test-capture test-search test-auth test-health
