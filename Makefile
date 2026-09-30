.DEFAULT_GOAL := help
BACKEND := backend
PYTHON := .venv/bin/python
PYTEST := .venv/bin/pytest

.PHONY: help setup test-store test-capture test-search validate-deploy

help:
	@echo 'setup         Install the locked backend and development dependencies'
	@echo 'test-store    Verify record transactions and publication on disposable PostgreSQL'
	@echo 'test-capture  Verify source capture, transport and blob handling'
	@echo 'test-search   Verify search services and visibility behavior'
	@echo 'validate-deploy Validate the Porter application manifest locally (no deployment)'
	@echo ''
	@echo 'test-store requires CANONICAL_TEST_DATABASE_URL; it creates and removes test schemas.'
	@echo 'See deploy/README.md for staging prerequisites and explicit deployment steps.'

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
