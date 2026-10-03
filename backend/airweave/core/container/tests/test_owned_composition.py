"""Owned nonlocal composition removes inherited subscription accounting only."""

from types import SimpleNamespace
from uuid import uuid4

import pytest

from airweave.adapters.payment.null import NullPaymentGateway
from airweave.adapters.rate_limiter.redis import RedisRateLimiter
from airweave.core.config import settings
from airweave.core.container import factory
from airweave.domains.usage.ledger import NullUsageLedger
from airweave.domains.usage.limit_checker import AlwaysAllowLimitChecker


def test_nonlocal_owned_composition_uses_no_subscription_or_stripe_dependencies(monkeypatch):
    config = settings.model_copy(
        update={
            "LOCAL_DEVELOPMENT": False,
            "DISABLE_RATE_LIMIT": False,
            "STRIPE_ENABLED": True,
            "OWNED_TENANT_CONTROL_ORGANIZATION_ID": uuid4(),
            "OWNED_TENANT_CONTROL_API_KEY_IDS": (uuid4(),),
        }
    )
    assert isinstance(factory._create_payment_gateway(config), NullPaymentGateway)
    assert isinstance(factory._create_usage_checker(config, {}, {}, None), AlwaysAllowLimitChecker)
    assert isinstance(factory._create_usage_ledger(config, {}), NullUsageLedger)
    monkeypatch.setattr(factory, "redis_client", SimpleNamespace(client=object()))
    assert isinstance(factory._create_rate_limiter(config), RedisRateLimiter)


@pytest.mark.parametrize("organization,keys", [(None, (uuid4(),)), (uuid4(), ())])
def test_partial_owned_control_config_is_rejected(organization, keys):
    config = settings.model_copy(
        update={
            "OWNED_TENANT_CONTROL_ORGANIZATION_ID": organization,
            "OWNED_TENANT_CONTROL_API_KEY_IDS": keys,
        }
    )
    with pytest.raises(ValueError, match="both organization and allowed API key IDs"):
        factory._owned_control_configured(config)
