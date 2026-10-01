"""Operator retry intent must not disappear behind automatic projection."""

from argparse import Namespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from temporalio.exceptions import WorkflowAlreadyStartedError

from airweave.domains.entities.canonical.reprojection import ReprojectionPlan
from scripts import reproject_canonical


async def test_busy_projection_preserves_committed_version_but_reports_retry(monkeypatch, capsys):
    organization, sync = uuid4(), uuid4()
    args = Namespace(organization=organization, sync=sync, expect=2, target=3, apply=True)
    plan = ReprojectionPlan(
        organization_id=organization,
        sync_id=sync,
        source_connection_id=uuid4(),
        provider="slack",
        current_version=2,
        target_version=3,
        captured_records=1,
        target_pending_records=1,
        workflow_id=f"canonical-projection:{organization}:{sync}",
        applied=True,
    )
    session = AsyncMock()
    context = MagicMock()
    context.__aenter__ = AsyncMock(return_value=session)
    context.__aexit__ = AsyncMock(return_value=False)
    monkeypatch.setattr(reproject_canonical, "AsyncSessionLocal", lambda: context)
    monkeypatch.setattr(reproject_canonical, "plan_reprojection", AsyncMock(return_value=plan))
    client = AsyncMock()
    client.start_workflow.side_effect = WorkflowAlreadyStartedError(plan.workflow_id, "projection")
    monkeypatch.setattr(reproject_canonical, "get_client", AsyncMock(return_value=client))

    with pytest.raises(SystemExit, match="retry this exact command"):
        await reproject_canonical.run(args)

    session.commit.assert_awaited_once()
    assert "accepted" not in capsys.readouterr().out
    call = client.start_workflow.call_args.kwargs
    assert call["id"] == plan.workflow_id
    assert call["args"] == [str(organization), str(sync)]
    assert "id_conflict_policy" not in call
