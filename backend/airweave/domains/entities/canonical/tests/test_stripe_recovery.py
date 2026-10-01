"""Stripe source composition and durable recovery; synthetic HTTP, real PostgreSQL."""

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import select

from airweave.domains.entities.canonical.cycle_models import CompleteCycle
from airweave.domains.sources.exceptions import SourceEntityForbiddenError
from airweave.domains.sources.token_providers.protocol import ManagedAuthProvider
from airweave.domains.sync_pipeline.canonical_scan import CanonicalScanDriver
from airweave.models.capture_scan import CaptureScan
from airweave.models.entity import Entity
from airweave.platform.configs.config import StripeCaptureConfig, StripeConfig
from airweave.platform.sources.stripe import StripeSource


async def connector(calls, *, fail=False):
    async def get(url, **kwargs):
        params = kwargs["params"]
        calls.append((url, params))
        status = 200
        if url.endswith("/account"):
            data = {"object": "account", "id": "acct_selected"}
        elif url.endswith("/balance"):
            data = {"object": "balance", "livemode": False, "available": []}
        elif url.endswith("/customers"):
            second = "starting_after" in params
            if second and fail:
                status, data = 403, {"error": {"message": "private provider detail"}}
            else:
                data = {
                    "object": "list",
                    "has_more": not second,
                    "data": [
                        {
                            "id": "cus_second" if second else "cus_first",
                            "object": "customer",
                            "livemode": False,
                            "unknown": {"retained": [1, None]},
                        }
                    ],
                }
        else:
            data = {"object": "list", "data": [], "has_more": False}
        return httpx.Response(status, json=data, request=httpx.Request("GET", url))

    http = AsyncMock()
    http.get.side_effect = get
    source = await StripeSource.create(
        auth=ManagedAuthProvider(
            api_key="fixture",
            connected_account_id="fixture",
            allowed_hosts=frozenset({"api.stripe.com"}),
        ),
        logger=MagicMock(),
        http_client=http,
        config=StripeConfig(
            original_capture=StripeCaptureConfig(
                expected_account_id="acct_selected",
                livemode=False,
                api_version="2025-06-30.basil",
            )
        ),
    )
    return source.capture_page_source


async def test_failed_customer_page_recovers_durable_cursor_and_preserves_original(
    database, source
):
    service, fence = source
    first = await connector([], fail=True)
    with pytest.raises(SourceEntityForbiddenError):
        await CanonicalScanDriver(
            service, database, fence, first, AsyncMock(), AsyncMock(), MagicMock()
        ).run()
    async with database() as db:
        scan = await db.scalar(select(CaptureScan).where(CaptureScan.record_type == "customer"))
        assert scan.phase == "collecting"
        assert scan.continuation["starting_after"] == "cus_first"
        before = await service.read_cycle(db, fence)
        newer = await service.activate_writer(
            db,
            fence.organization_id,
            fence.sync_id,
            fence.job_id,
            attempt_id=uuid4(),
            attempt_number=2,
        )
    calls = []
    resumed = await connector(calls)
    after = await CanonicalScanDriver(
        service, database, newer, resumed, AsyncMock(), AsyncMock(), MagicMock()
    ).run()
    async with database() as db:
        after = await service.complete_cycle(db, CompleteCycle(fence=newer, expected=after.version))
        records = list(
            (
                await db.scalars(
                    select(Entity).where(Entity.entity_definition_short_name == "customer")
                )
            ).all()
        )
    assert after.version.cycle_id == before.version.cycle_id and after.phase == "complete"
    assert after.last_full_capture.discovery == "incomplete"
    assert [
        params.get("starting_after") for url, params in calls if url.endswith("/customers")
    ] == ["cus_first"]
    assert len(records) == 2
    assert all(row.record_revision == 1 and row.deleted_at is None for row in records)
    assert all(row.source_payload["unknown"] == {"retained": [1, None]} for row in records)
