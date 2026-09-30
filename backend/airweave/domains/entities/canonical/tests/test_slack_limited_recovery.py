"""Retention-limited history cannot erase prior originals or publish a new baseline."""

import pytest

from airweave.domains.entities.canonical.tests.test_mixed_scopes import next_job
from airweave.domains.entities.canonical.tests.test_slack_recovery import ROOT, run, runner, saved
from airweave.domains.sources.exceptions import SourceError


async def test_limited_final_history_preserves_prior_originals_and_committed_progress(
    database, source
):
    baseline, _, _ = runner(database, source, [ROOT, {"messages": [{"ts": "1", "text": "old"}]}])
    await run(baseline)
    _, before, _ = await saved(database)
    source = await next_job(database, source)
    refresh, _, _ = runner(
        database,
        source,
        [
            ROOT,
            {
                "messages": [{"ts": "3", "text": "new"}],
                "response_metadata": {"next_cursor": "older"},
            },
            {"messages": [{"ts": "2", "text": "limited page"}], "is_limited": True},
        ],
    )
    with pytest.raises(SourceError, match="remains incomplete"):
        await run(refresh)
    rows, after, scans = await saved(database)
    assert {row.native_id for row in rows} == {"C1", "1", "3"}
    assert all(row.deleted_at is None for row in rows)
    assert after["canonical_checkpoint"] == before["canonical_checkpoint"]
    assert after["canonical_cycle"]["phase"] != "complete"
    current = next(scan for scan in scans if scan.record_type == "message")
    assert current.phase == "collecting"
    assert current.continuation["history_cursor"] == "older"
    assert current.continuation["history_done"] is False
