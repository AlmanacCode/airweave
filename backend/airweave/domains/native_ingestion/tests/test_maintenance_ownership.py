"""Provider timeout maintenance cannot terminate resumable native imports."""

from datetime import timedelta

import pytest
from sqlalchemy import update

from airweave.core.datetime_utils import utc_now_naive
from airweave.crud.crud_sync_job import sync_job
from airweave.domains.native_ingestion.tests.test_imports import native, start  # noqa: F401
from airweave.models.sync_job import SyncJob


@pytest.mark.parametrize("native", ["knowledge"], indirect=True)
async def test_stuck_provider_query_excludes_native_import(database, source, native):  # noqa: F811
    imported = await start(database, native)
    _, provider = source
    cutoff = utc_now_naive() - timedelta(minutes=15)
    async with database() as db:
        await db.execute(update(SyncJob).values(started_at=cutoff - timedelta(minutes=1)))
        await db.commit()
        jobs = await sync_job.get_stuck_jobs_by_status(
            db, status=["running"], started_before=cutoff
        )
        assert {job.id for job in jobs} == {provider.job_id}
    # Exclusion does not terminate or replace the durable native import.
    assert await start(database, native) == imported
