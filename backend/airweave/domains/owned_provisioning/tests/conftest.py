"""Use isolated PostgreSQL with the production session autoflush policy."""

import pytest

from airweave.domains.entities.canonical.tests.conftest import (
    database as canonical_database,  # noqa: F401
)


@pytest.fixture
async def database(canonical_database):  # noqa: F811 - pytest injects the imported fixture
    """Keep implicit flushes from masking production transaction ordering."""
    canonical_database.configure(autoflush=False)
    return canonical_database
