"""Offline synthetic Stripe projection; no provider or live-money claims."""

from datetime import datetime, timezone
from uuid import uuid4

import pytest

from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.requests import RecordIdentity
from airweave.domains.entities.canonical.stripe_projection import map_stripe
from airweave.domains.sync_pipeline.pipeline.text_builder import TextualRepresentationBuilder
from airweave.platform.entities.stripe import (
    StripeEventEntity,
    StripeInvoiceEntity,
    StripePaymentIntentEntity,
)


def original(kind, **values):
    native = "balance" if kind == "balance" else f"{kind}_123"
    payload = {"object": kind, "livemode": False, **values}
    if kind != "balance":
        payload = {"id": native, "created": 1720000000, **payload}
    return SourceRecord(
        id=uuid4(),
        sync_id=uuid4(),
        identity=RecordIdentity(record_type=kind, native_id=native),
        revision=1,
        payload=payload,
        payload_schema_version=1,
        capture_hash="fixture",
        content_hash=None,
        completeness="partial",
        observed_at=datetime(2026, 10, 1, tzinfo=timezone.utc),
        source_created_at=None,
        source_updated_at=None,
        deleted_at=None,
        removal_reason=None,
        blobs=(),
        indexed_revision=None,
        indexed_pipeline_version=None,
    )


def test_invoice_payment_event_keep_separate_facts_and_ids():
    invoice = original(
        "invoice",
        number="INV-42",
        amount_due=1200,
        amount_paid=200,
        amount_remaining=1000,
        currency="jpy",
        status="open",
        customer={"id": "cus_1"},
    )
    payment = original(
        "payment_intent",
        amount=1200,
        currency="jpy",
        status="requires_action",
        client_secret="pi_secret",
    )
    event = original(
        "event",
        type="invoice.paid",
        data={
            "object": {
                "id": invoice.identity.native_id,
                "object": "invoice",
                "paid": True,
                "amount_paid": 1200,
                "metadata": {"secret": "nested_secret"},
            },
            "previous_attributes": {"client_secret": "previous_secret"},
        },
    )
    before = [item.model_dump() for item in (invoice, payment, event)]
    inv, pi, evt = [map_stripe(item)[0] for item in (invoice, payment, event)]
    assert isinstance(inv, StripeInvoiceEntity) and isinstance(pi, StripePaymentIntentEntity)
    assert isinstance(evt, StripeEventEntity)
    assert inv.entity_id == invoice.identity.native_id and inv.amount_remaining == 1000
    assert inv.amount_paid == 200 and inv.currency == "jpy" and inv.paid is None
    assert pi.entity_id == payment.identity.native_id and pi.status == "requires_action"
    assert evt.entity_id == event.identity.native_id and evt.event_type == "invoice.paid"
    assert evt.data == {"object": {"id": invoice.identity.native_id, "object": "invoice"}}
    assert all(entity.updated_at is None for entity in (inv, pi, evt))
    assert inv.updated_time is None and pi.updated_time is None
    assert before == [item.model_dump() for item in (invoice, payment, event)]


def test_secrets_absent_from_both_full_entity_and_search_text():
    examples = [
        original(
            "payment_intent", amount=500, client_secret="secret_a", metadata={"token": "secret_b"}
        ),
        original(
            "payment_method",
            type="card",
            card={"number": "secret_c", "cvc": "secret_d", "fingerprint": "secret_e"},
            billing_details={"name": "Buyer", "email": "buyer@example.com", "token": "secret_f"},
        ),
        original(
            "balance",
            available=[{"amount": 500, "currency": "usd", "token": "secret_g"}],
            pending=[],
        ),
        original(
            "event",
            type="payment_intent.created",
            data={
                "object": {"id": "pi_1", "object": "payment_intent", "client_secret": "secret_h"}
            },
        ),
    ]
    builder = TextualRepresentationBuilder()
    for record in examples:
        entity = map_stripe(record)[0]
        assert "secret_" not in entity.model_dump_json()
        assert "secret_" not in builder.build_metadata_section(entity, "stripe")
        assert "secret_" in str(record.payload)


@pytest.mark.parametrize(
    "kind",
    [
        "balance_transaction",
        "charge",
        "customer",
        "invoice",
        "payment_intent",
        "payment_method",
        "payout",
        "refund",
        "subscription",
    ],
)
def test_resource_breadth_and_unknown_timestamps_flags(kind):
    entity = map_stripe(original(kind))[0]
    assert entity.entity_id == f"{kind}_123"
    assert entity.created_at == datetime.fromtimestamp(1720000000, timezone.utc)
    assert entity.updated_at is None
    values = entity.model_dump()
    for field in (
        "paid",
        "captured",
        "refunded",
        "delinquent",
        "cancel_at_period_end",
        "updated_time",
    ):
        if field in values:
            assert values[field] is None


@pytest.mark.parametrize(
    "change",
    [
        {"id": "other"},
        {"object": "charge"},
        {"created": "1720000000"},
        {"amount_paid": True},
        {"paid": "false"},
    ],
)
def test_identity_and_typed_field_mismatch_fail_without_payload_echo(change):
    record = original("invoice", metadata={"token": "private_secret"})
    record = record.model_copy(update={"payload": {**record.payload, **change}})
    with pytest.raises(ValueError) as error:
        map_stripe(record)
    assert "private_secret" not in str(error.value)


def test_balance_uses_captured_snapshot_and_keeps_currency_buckets_separate():
    record = original(
        "balance",
        available=[{"amount": 500, "currency": "usd"}, {"amount": 1200, "currency": "jpy"}],
        pending=[],
    )
    entity = map_stripe(record)[0]
    assert entity.snapshot_time == record.observed_at
    assert entity.available == [
        {"amount": 500, "currency": "usd"},
        {"amount": 1200, "currency": "jpy"},
    ]
    bad = record.model_copy(
        update={"identity": RecordIdentity(record_type="balance", native_id="other")}
    )
    with pytest.raises(ValueError, match="balance identity"):
        map_stripe(bad)


def test_native_flags_references_and_financial_amounts_are_not_recomputed():
    charge = map_stripe(
        original(
            "charge",
            amount=1000,
            currency="usd",
            paid=True,
            captured=False,
            refunded=False,
            customer={"id": "cus_1", "metadata": {"token": "secret_expansion"}},
        )
    )[0]
    assert charge.paid is True and charge.captured is False and charge.refunded is False
    assert charge.customer_id == "cus_1" and "secret_expansion" not in charge.model_dump_json()
    transaction = map_stripe(
        original(
            "balance_transaction",
            amount=-1000,
            fee=23,
            net=-1023,
            currency="usd",
            source="ch_1",
        )
    )[0]
    assert (transaction.amount, transaction.fee, transaction.net) == (-1000, 23, -1023)
    assert transaction.source == "ch_1"
    refund = map_stripe(
        original("refund", amount=200, charge="ch_1", payment_intent={"id": "pi_1"})
    )[0]
    assert (
        refund.amount == 200 and refund.charge_id == "ch_1" and refund.payment_intent_id == "pi_1"
    )


def test_missing_creation_time_and_wrong_capture_scope_fail():
    record = original("invoice")
    for bad in (
        record.model_copy(update={"payload": {**record.payload, "created": None}}),
        record.model_copy(update={"payload_schema_version": 2}),
        record.model_copy(
            update={
                "identity": RecordIdentity(
                    record_type="invoice",
                    native_id=record.identity.native_id,
                    container_id="cus_1",
                )
            }
        ),
    ):
        with pytest.raises(ValueError):
            map_stripe(bad)


def test_legacy_constructors_keep_explicit_flags_and_timestamps():
    from airweave.platform.entities.stripe import StripeChargeEntity, StripeSubscriptionEntity

    charge = StripeChargeEntity.from_api(
        {"id": "ch_1", "created": 1720000000, "paid": True, "captured": False, "refunded": False},
        web_url=None,
    )
    invoice = StripeInvoiceEntity.from_api(
        {"id": "in_1", "created": 1720000000, "paid": True},
        web_url=None,
    )
    subscription = StripeSubscriptionEntity.from_api(
        {"id": "sub_1", "created": 1720000000, "cancel_at_period_end": False},
        web_url=None,
    )
    assert charge.paid is True and charge.captured is False and charge.refunded is False
    assert invoice.paid is True and subscription.cancel_at_period_end is False
    assert charge.created_time == datetime(2024, 7, 3, 9, 46, 40)


@pytest.mark.asyncio
async def test_shared_projection_dispatch_uses_retained_original_without_storage_reads():
    from unittest.mock import AsyncMock

    from airweave.domains.entities.canonical.projection_mappers import map_record

    storage = AsyncMock()
    record = original("invoice", number="INV-42", currency="usd", amount_due=1200)
    async with map_record(record, "stripe", storage) as projection:
        assert len(projection.parts) == 1
        assert isinstance(projection.parts[0].entity, StripeInvoiceEntity)
        assert projection.parts[0].entity.invoice_id == record.identity.native_id
    assert not storage.mock_calls
