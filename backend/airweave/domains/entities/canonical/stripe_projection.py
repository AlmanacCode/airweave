"""Offline allowlisted Stripe views; amounts retain native currency units.

No calculations, expanded-object recursion, secret-bearing metadata, or provider
fetches. Event objects remain historical events, never current financial records.
"""

from datetime import datetime, timezone
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from airweave.domains.entities.canonical.models import SourceRecord
from airweave.platform.entities._base import BaseEntity
from airweave.platform.entities.stripe import (
    StripeBalanceEntity,
    StripeBalanceTransactionEntity,
    StripeChargeEntity,
    StripeCustomerEntity,
    StripeEventEntity,
    StripeInvoiceEntity,
    StripePaymentIntentEntity,
    StripePaymentMethodEntity,
    StripePayoutEntity,
    StripeRefundEntity,
    StripeSubscriptionEntity,
)

Timestamp = Annotated[int, Field(ge=0, le=253402300799)]


class _Native(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True)


class _Reference(_Native):
    id: str


class _Money(_Native):
    amount: int
    currency: str


class _Billing(_Native):
    name: str | None = None
    email: str | None = None


class _EventObject(_Native):
    id: str | None = None
    object: str


class _EventData(_Native):
    object: _EventObject


class _Original(_Native):
    id: str | None = None
    object: str
    created: Timestamp | None = None
    livemode: bool | None = None
    name: str | None = None
    description: str | None = None
    currency: str | None = None
    status: str | None = None
    type: str | None = None
    amount: int | None = None
    customer: str | _Reference | None = None
    invoice: str | _Reference | None = None
    charge: str | _Reference | None = None
    payment_intent: str | _Reference | None = None
    source: str | _Reference | None = None
    destination: str | _Reference | None = None
    captured: bool | None = None
    paid: bool | None = None
    refunded: bool | None = None
    delinquent: bool | None = None
    cancel_at_period_end: bool | None = None
    email: str | None = None
    phone: str | None = None
    invoice_prefix: str | None = None
    number: str | None = None
    amount_due: int | None = None
    amount_paid: int | None = None
    amount_remaining: int | None = None
    due_date: Timestamp | None = None
    arrival_date: Timestamp | None = None
    current_period_start: Timestamp | None = None
    current_period_end: Timestamp | None = None
    canceled_at: Timestamp | None = None
    fee: int | None = None
    net: int | None = None
    reporting_category: str | None = None
    method: str | None = None
    reason: str | None = None
    api_version: str | None = None
    pending_webhooks: int | None = None
    data: _EventData | None = None
    billing_details: _Billing | None = None
    available: list[_Money] | None = None
    pending: list[_Money] | None = None
    instant_available: list[_Money] | None = None
    connect_reserved: list[_Money] | None = None


def _id(value: str | _Reference | None) -> str | None:
    return value.id if isinstance(value, _Reference) else value


def _time(value: int | None) -> datetime | None:
    return datetime.fromtimestamp(value, timezone.utc) if value is not None else None


def map_stripe(record: SourceRecord) -> tuple[BaseEntity, ...]:
    """Project one retained root; reject identity/schema drift without logging originals."""
    if (
        record.payload_schema_version != 1
        or record.identity.container_id is not None
        or record.parent is not None
        or record.deleted_at is not None
        or record.content_access != "available"
    ):
        raise ValueError("Stripe projection requires an available schema-1 root")
    try:
        native = _Original.model_validate(record.payload)
        if native.object != record.identity.record_type:
            raise ValueError("Stripe original kind differs from canonical identity")
        if native.object == "balance":
            if record.identity.native_id != "balance" or native.id is not None:
                raise ValueError("Stripe balance identity differs from canonical identity")
            return (_balance(record, native),)
        if native.id != record.identity.native_id:
            raise ValueError("Stripe original ID differs from canonical identity")
        return (_entity(record, native),)
    except (ValidationError, OverflowError, OSError):
        raise ValueError("Stripe projection fields are malformed") from None


def _balance(record: SourceRecord, n: _Original) -> StripeBalanceEntity:
    if n.available is None or n.pending is None or n.livemode is None:
        raise ValueError("Stripe balance requires captured amounts and mode")
    return StripeBalanceEntity(
        entity_id="balance",
        name="Account Balance",
        breadcrumbs=[],
        balance_id="balance",
        balance_name="Account Balance",
        snapshot_time=record.observed_at,
        available=[item.model_dump() for item in n.available],
        pending=[item.model_dump() for item in n.pending],
        instant_available=(
            [item.model_dump() for item in n.instant_available]
            if n.instant_available is not None
            else None
        ),
        connect_reserved=(
            [item.model_dump() for item in n.connect_reserved]
            if n.connect_reserved is not None
            else None
        ),
        livemode=n.livemode,
    )


def _entity(record: SourceRecord, n: _Original) -> BaseEntity:  # noqa: C901
    created = _time(n.created)
    if created is None:
        raise ValueError("Stripe original lacks a captured creation timestamp")
    kind = n.object
    name = n.name if kind == "customer" else n.number if kind == "invoice" else None
    name = name or n.description or f"{kind.replace('_', ' ').title()} {n.id}"
    common = {
        "entity_id": n.id,
        "name": name,
        "breadcrumbs": [],
        "created_at": created,
        "updated_at": record.source_updated_at,
        "created_time": created,
    }
    updated = {"updated_time": record.source_updated_at}
    if kind == "balance_transaction":
        return StripeBalanceTransactionEntity(
            **common,
            transaction_id=n.id,
            transaction_name=name,
            amount=n.amount,
            currency=n.currency,
            description=n.description,
            fee=n.fee,
            net=n.net,
            reporting_category=n.reporting_category,
            source=_id(n.source),
            status=n.status,
            type=n.type,
        )
    if kind == "charge":
        return StripeChargeEntity(
            **common,
            **updated,
            charge_id=n.id,
            charge_name=name,
            amount=n.amount,
            currency=n.currency,
            description=n.description,
            captured=n.captured,
            paid=n.paid,
            refunded=n.refunded,
            customer_id=_id(n.customer),
            invoice_id=_id(n.invoice),
        )
    if kind == "customer":
        return StripeCustomerEntity(
            **common,
            **updated,
            customer_id=n.id,
            customer_name=name,
            email=n.email,
            phone=n.phone,
            description=n.description,
            currency=n.currency,
            delinquent=n.delinquent,
            invoice_prefix=n.invoice_prefix,
        )
    if kind == "event":
        if n.data is None or n.livemode is None or n.type is None:
            raise ValueError("Stripe event lacks captured type, mode or object identity")
        return StripeEventEntity(
            **common,
            event_id=n.id,
            event_name=n.type,
            event_type=n.type,
            api_version=n.api_version,
            data=n.data.model_dump(exclude_none=True),
            livemode=n.livemode,
            pending_webhooks=n.pending_webhooks,
        )
    if kind == "invoice":
        return StripeInvoiceEntity(
            **common,
            **updated,
            invoice_id=n.id,
            invoice_name=name,
            customer_id=_id(n.customer),
            number=n.number,
            status=n.status,
            amount_due=n.amount_due,
            amount_paid=n.amount_paid,
            amount_remaining=n.amount_remaining,
            due_date=_time(n.due_date),
            paid=n.paid,
            currency=n.currency,
        )
    if kind == "payment_intent":
        return StripePaymentIntentEntity(
            **common,
            **updated,
            payment_intent_id=n.id,
            payment_intent_name=name,
            amount=n.amount,
            currency=n.currency,
            status=n.status,
            description=n.description,
            customer_id=_id(n.customer),
        )
    if kind == "payment_method":
        return StripePaymentMethodEntity(
            **common,
            payment_method_id=n.id,
            payment_method_name=name,
            type=n.type,
            customer_id=_id(n.customer),
            billing_details=n.billing_details.model_dump(exclude_none=True)
            if n.billing_details
            else {},
        )
    if kind == "payout":
        return StripePayoutEntity(
            **common,
            **updated,
            payout_id=n.id,
            payout_name=name,
            amount=n.amount,
            currency=n.currency,
            arrival_date=_time(n.arrival_date),
            description=n.description,
            destination=_id(n.destination),
            method=n.method,
            status=n.status,
        )
    if kind == "refund":
        return StripeRefundEntity(
            **common,
            refund_id=n.id,
            refund_name=name,
            amount=n.amount,
            currency=n.currency,
            status=n.status,
            reason=n.reason,
            charge_id=_id(n.charge),
            payment_intent_id=_id(n.payment_intent),
        )
    if kind == "subscription":
        return StripeSubscriptionEntity(
            **common,
            **updated,
            subscription_id=n.id,
            subscription_name=name,
            customer_id=_id(n.customer),
            status=n.status,
            current_period_start=_time(n.current_period_start),
            current_period_end=_time(n.current_period_end),
            cancel_at_period_end=n.cancel_at_period_end,
            canceled_at=_time(n.canceled_at),
        )
    raise ValueError("Unsupported Stripe original kind")
