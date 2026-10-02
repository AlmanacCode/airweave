"""Full child acquisition must admit the exact parent inventory snapshot."""

import pytest

from airweave.domains.entities.canonical.scan_models import BeginScan
from airweave.domains.entities.canonical.scan_store import ScanConflict
from airweave.domains.entities.canonical.tests.helpers import capture
from airweave.domains.entities.canonical.tests.test_cycles import (
    CHILD,
    CONFIG,
    cycle,
    finish,
    page,
    record,
    scan,
)


async def test_full_parent_inventory_epoch_is_required_at_begin_resume_and_restart(
    database, source
):
    service, fence = source
    active = await cycle(database, service, fence)
    root = await scan(database, service, fence, active)
    parent = record().model_copy(
        update={"payload": {"files": ["F1"]}, "descendant_visibility_fields": ("files",)}
    )
    root = await page(database, service, fence, root, parent, final=True)
    await finish(database, service, fence, root)
    async with database() as db:
        work = await service.next_scope_work(db, fence, active.version.cycle_id)
    request = BeginScan(
        fence=fence,
        scope=CHILD,
        cycle_id=active.version.cycle_id,
        fingerprint=CONFIG.fingerprint,
        expected_parent_epoch=work.parent_visibility_epoch,
    )
    # A source may retain this parent while another admitted operation updates it.
    await capture(
        database, service, fence, parent.model_copy(update={"payload": {"files": ["F2"]}})
    )
    async with database() as db:
        with pytest.raises(ScanConflict, match="owner changed"):
            await service.begin_scan(db, request)
    async with database() as db:
        current = await service.next_scope_work(db, fence, active.version.cycle_id)
        request = request.model_copy(
            update={"expected_parent_epoch": current.parent_visibility_epoch}
        )
        admitted = await service.begin_scan(db, request)
    # Full acquisition depends on attachment inventory, not unrelated text revision.
    await capture(
        database,
        service,
        fence,
        parent.model_copy(update={"payload": {"files": ["F2"], "text": "edited"}}),
    )
    async with database() as db:
        resumed = await service.begin_scan(db, request)
        assert resumed.version == admitted.version
    await capture(
        database, service, fence, parent.model_copy(update={"payload": {"files": ["F3"]}})
    )
    for restart in (False, True):
        async with database() as db:
            with pytest.raises(ScanConflict, match="owner changed"):
                await service.begin_scan(
                    db,
                    request.model_copy(update={"expected": admitted.version, "restart": restart}),
                )
