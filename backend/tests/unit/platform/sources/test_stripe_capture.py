"""Stripe provider interpretation only; no live account or SQL qualification."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from airweave.domains.entities.canonical.requests import CompletedScope
from airweave.domains.entities.canonical.scan_models import ScanContinuation
from airweave.platform.sources.stripe_capture import (
    StripeCapture,
    StripeCaptureConfig,
    StripeCaptureError,
)


def config(**overrides):
    return StripeCaptureConfig(
        **{
            "expected_account_id": "acct_selected",
            "connected_account_id": "acct_selected",
            "livemode": False,
            "api_version": "2025-06-30.basil",
            **overrides,
        }
    )


def identity():
    return [
        {"object": "account", "id": "acct_selected"},
        {"object": "balance", "livemode": False, "available": []},
    ]


async def page(source, kind, continuation=None):
    return await source.capture_page(
        CompletedScope(record_type=kind), continuation or ScanContinuation(), files=MagicMock()
    )


@pytest.mark.asyncio
async def test_all_resource_families_preserve_native_json_and_context():
    read = AsyncMock(side_effect=identity())
    source = await StripeCapture.create(read, config())
    assert len(source.canonical_record_types) == 11
    assert all(
        source.capture_cycle_configuration.policy(kind) == "discovery_only"
        for kind in source.canonical_record_types
    )
    for kind in source.canonical_record_types:
        native = {"object": kind, "livemode": False, "unknown": {"nested": [1, None]}}
        if kind != "balance":
            native.update(id="obj_one", created=123)
        if kind == "payment_method":
            native.update(type="us_bank_account", us_bank_account={"last4": "6789"})
        read.side_effect = None
        read.return_value = (
            native
            if kind == "balance"
            else {
                "object": "list",
                "data": [native],
                "has_more": False,
                "url": "https://untrusted.invalid/do-not-follow",
            }
        )
        result = await page(source, kind)
        assert result.final and result.records[0].payload == native
        assert result.records[0].completeness == "partial"
        assert result.records[0].parent is None
        assert result.records[0].kind == "upsert"
        assert result.records[0].source_updated_at is None
        if kind == "subscription":
            assert read.call_args.kwargs["params"]["status"] == "all"
        if kind == "payment_method":
            assert "type" not in read.call_args.kwargs["params"]
            assert result.records[0].payload["type"] == "us_bank_account"
    for call in read.call_args_list:
        assert call.kwargs["headers"] == {
            "Stripe-Version": "2025-06-30.basil",
            "Stripe-Account": "acct_selected",
        }
        assert call.args[0].startswith("/v1/")


@pytest.mark.asyncio
async def test_failed_page_retries_same_cursor_and_binding_cannot_change():
    raw = {"id": "cus_one", "object": "customer", "livemode": False}
    read = AsyncMock(side_effect=identity() + [{"object": "list", "data": [raw], "has_more": True}])
    source = await StripeCapture.create(read, config())
    first = await page(source, "customer")
    failure = RuntimeError("transport failed")
    read.side_effect = failure
    with pytest.raises(RuntimeError) as error:
        await page(source, "customer", first.continuation)
    assert error.value is failure
    assert read.call_args.kwargs["params"]["starting_after"] == "cus_one"
    read.side_effect = None
    read.return_value = {"object": "list", "data": [], "has_more": False}
    final = await page(source, "customer", first.continuation)
    assert final.final and final.records == ()
    calls = read.await_count
    with pytest.raises(StripeCaptureError, match="binding"):
        await page(source, "event", first.continuation)
    other = await StripeCapture.create(
        AsyncMock(side_effect=identity()), config(api_version="2024-06-20")
    )
    with pytest.raises(StripeCaptureError, match="binding"):
        await page(other, "customer", first.continuation)
    assert read.await_count == calls


@pytest.mark.asyncio
async def test_failed_reattest_resets_prior_identity_and_mode():
    read = AsyncMock(side_effect=identity())
    source = await StripeCapture.create(read, config())
    read.side_effect = None
    read.return_value = {"object": "account", "id": "acct_other"}
    with pytest.raises(StripeCaptureError, match="account"):
        await source.verify_principal()
    calls = read.await_count
    with pytest.raises(StripeCaptureError, match="not attested"):
        await page(source, "customer")
    assert read.await_count == calls
    read.side_effect = [identity()[0], {"object": "balance", "livemode": True}]
    with pytest.raises(StripeCaptureError, match="mode"):
        await source.verify_principal()


@pytest.mark.asyncio
async def test_malformed_page_never_becomes_empty_success():
    read = AsyncMock(side_effect=identity())
    source = await StripeCapture.create(read, config())
    read.side_effect = None
    obj = {"id": "cus_one", "object": "customer", "livemode": False}
    for response in (
        {"object": "list", "data": [], "has_more": True},
        {"object": "list", "data": [obj, obj], "has_more": False},
        {"object": "list", "data": [{**obj, "object": "event"}], "has_more": False},
        {"object": "list", "data": [{**obj, "livemode": True}], "has_more": False},
        {"object": "list", "data": [{**obj, "livemode": None}], "has_more": False},
        {"object": "list", "data": [], "has_more": "false"},
        {"object": "list", "has_more": False},
    ):
        read.return_value = response
        with pytest.raises(StripeCaptureError):
            await page(source, "customer")


@pytest.mark.asyncio
async def test_event_snapshot_never_updates_embedded_object_or_confirms_absence():
    event = {
        "id": "evt_one",
        "object": "event",
        "api_version": "2019-02-19",
        "created": 123,
        "livemode": False,
        "type": "customer.deleted",
        "data": {"object": {"id": "cus_deleted", "object": "customer", "deleted": True}},
    }
    read = AsyncMock(
        side_effect=identity() + [{"object": "list", "data": [event], "has_more": False}]
    )
    source = await StripeCapture.create(read, config())
    result = await page(source, "event")
    assert len(result.records) == 1
    assert result.records[0].identity.native_id == "evt_one"
    assert result.records[0].payload == event
    assert result.records[0].kind == "upsert"
    with pytest.raises(StripeCaptureError, match="cannot confirm absence"):
        await source.confirm_absent(MagicMock())


@pytest.mark.asyncio
async def test_managed_source_wires_capture_without_exposing_credentials():
    import httpx

    from airweave.domains.sources.exceptions import SourceEntityForbiddenError, SourceError
    from airweave.domains.sources.token_providers.protocol import ManagedAuthProvider
    from airweave.platform.configs.config import StripeConfig
    from airweave.platform.sources.stripe import StripeSource

    auth = ManagedAuthProvider(
        api_key="test-only",
        connected_account_id="ca_selected",
        allowed_hosts=frozenset({"api.stripe.com"}),
    )
    client = AsyncMock()
    client.get.side_effect = [httpx.Response(200, json=value) for value in identity()]
    source = await StripeSource.create(
        auth=auth,
        logger=MagicMock(),
        http_client=client,
        config=StripeConfig(original_capture=config()),
    )
    assert isinstance(source.capture_page_source, StripeCapture)
    for call in client.get.call_args_list:
        assert call.args[0].startswith("https://api.stripe.com/v1/")
        assert call.kwargs["headers"] == {
            "Stripe-Version": "2025-06-30.basil",
            "Stripe-Account": "acct_selected",
        }
    client.get.side_effect = None
    client.get.return_value = httpx.Response(403, json={"error": {"message": "private-secret"}})
    with pytest.raises(SourceEntityForbiddenError) as exc:
        await page(source.capture_page_source, "customer")
    assert "private-secret" not in str(exc.value)
    with pytest.raises(SourceError, match="explicitly bound"):
        await StripeSource.create(
            auth=auth,
            logger=MagicMock(),
            http_client=client,
            config=StripeConfig(),
        )
