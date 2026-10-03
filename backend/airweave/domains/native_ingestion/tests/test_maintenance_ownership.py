"""Provider timeout maintenance cannot terminate resumable native imports."""

from datetime import timedelta

import pytest
from sqlalchemy import text, update

from airweave.core.datetime_utils import utc_now_naive
from airweave.domains.entities.canonical.tests.conftest import worker_discovery  # noqa: F401
from airweave.domains.native_ingestion.tests.test_imports import native, start  # noqa: F401
from airweave.models.sync_job import SyncJob


@pytest.mark.parametrize("native", ["knowledge"], indirect=True)
async def test_stuck_provider_query_excludes_native_import(
    database,
    source,
    native,  # noqa: F811
    worker_discovery,  # noqa: F811
):
    imported = await start(database, native)
    _, provider = source
    cutoff = utc_now_naive() - timedelta(minutes=15)
    async with database() as db:
        await db.execute(update(SyncJob).values(started_at=cutoff - timedelta(minutes=1)))
        await db.commit()
        jobs = await db.execute(
            text("SELECT organization_id,job_id FROM owned_stale_jobs(:cutoff,:cutoff,NULL,100)"),
            {"cutoff": cutoff},
        )
        assert {job.job_id for job in jobs} == {provider.job_id}
    # Exclusion does not terminate or replace the durable native import.
    assert await start(database, native) == imported
