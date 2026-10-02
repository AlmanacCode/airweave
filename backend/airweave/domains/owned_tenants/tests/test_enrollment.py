"""Real migrated PostgreSQL enrollment, credential recovery and authority boundaries."""

import asyncio
from datetime import timedelta, timezone
from uuid import uuid4

import pytest
from fastapi import HTTPException
from sqlalchemy import delete, func, select, update
from sqlalchemy.exc import DBAPIError

from airweave.core import credentials
from airweave.domains.owned_tenants.models import (
    SCOPED_KEY_VALIDITY,
    EnsureOwnedTenant,
    OwnedTenantIdentity,
)
from airweave.domains.owned_tenants.service import OwnedTenantService, utc_now
from airweave.domains.owned_tenants.store import OwnedTenantStore
from airweave.models import APIKey, Collection, Organization
from airweave.models.vector_db_deployment_metadata import VectorDbDeploymentMetadata

pytestmark = pytest.mark.integration
OWNER = "user_synthetic_a"
NOW = utc_now()


@pytest.fixture
async def ready(database):
    async with database() as db:
        db.add(
            VectorDbDeploymentMetadata(
                dense_embedder="test", sparse_embedder="test", embedding_dimensions=3
            )
        )
        await db.commit()
    return OwnedTenantService(OwnedTenantStore(), clock=lambda: NOW)


async def enroll(database, service, owner=OWNER):
    async with database() as db:
        return await service.ensure(db, EnsureOwnedTenant(owner_user_id=owner))


async def test_concurrent_first_ensure_and_lost_response_recover_exact_rows(database, ready):
    results = await asyncio.gather(*(enroll(database, ready) for _ in range(4)))
    retry = await enroll(database, ready)
    assert all(item == retry for item in results)
    assert retry.expires_at == (NOW + SCOPED_KEY_VALIDITY).replace(tzinfo=timezone.utc)
    assert retry.api_key.get_secret_value() not in repr(retry)
    async with database() as db:
        for model in (Organization, Collection, APIKey):
            assert await db.scalar(select(func.count()).select_from(model)) == 1
        assert await db.scalar(select(Organization.owned_owner_user_id)) == OWNER
    other = await enroll(database, ready, "user_synthetic_b")
    assert other.organization_id != retry.organization_id
    assert other.collection != retry.collection
    assert other.api_key != retry.api_key


async def test_expired_key_renewal_is_locked_and_lost_ack_retry_recovers(database, ready):
    initial = await enroll(database, ready)
    identity = OwnedTenantIdentity.for_owner(OWNER)
    async with database() as db:
        key = await db.get(APIKey, identity.api_key_id)
        key.expiration_date = NOW - timedelta(seconds=1)
        await db.commit()
    results = await asyncio.gather(*(enroll(database, ready) for _ in range(3)))
    renewed = results[0]
    assert all(item == renewed for item in results)
    assert renewed.api_key_id == initial.api_key_id
    assert renewed.api_key != initial.api_key
    assert (await enroll(database, ready)) == renewed
    from airweave import crud
    from airweave.core.exceptions import NotFoundException

    async with database() as db:
        stored = await crud.api_key.get_by_key(db, key=renewed.api_key.get_secret_value())
        assert stored is not None and stored.id == renewed.api_key_id
        with pytest.raises(NotFoundException):
            await crud.api_key.get_by_key(db, key=initial.api_key.get_secret_value())


@pytest.mark.parametrize("damage", ["deleted", "malformed"])
async def test_withdrawn_or_invalid_designated_key_never_reenrolls(database, ready, damage):
    first = await enroll(database, ready)
    async with database() as db:
        if damage == "deleted":
            await db.execute(delete(APIKey).where(APIKey.id == first.api_key_id))
        else:
            key = await db.get(APIKey, first.api_key_id)
            key.encrypted_key = credentials.encrypt({"unexpected": "payload"})
            key.expiration_date = NOW - timedelta(days=1)
        await db.commit()
    with pytest.raises(HTTPException) as error:
        await enroll(database, ready)
    assert error.value.status_code == 409
    assert "operator recovery" in error.value.detail


@pytest.mark.parametrize("binding", [None, "user_other", OWNER])
async def test_unbound_collision_and_elsewhere_bound_owner_fail_closed(database, ready, binding):
    identity = OwnedTenantIdentity.for_owner(OWNER)
    async with database() as db:
        db.add(
            Organization(
                id=uuid4() if binding == OWNER else identity.organization_id,
                name="Existing synthetic organization",
                owned_owner_user_id=binding,
            )
        )
        await db.commit()
    with pytest.raises(HTTPException) as error:
        await enroll(database, ready)
    assert error.value.status_code == 409
    async with database() as db:
        assert await db.scalar(select(func.count()).select_from(Collection)) == 0
        assert await db.scalar(select(func.count()).select_from(APIKey)) == 0


async def test_owner_binding_is_immutable_in_actual_migration(database, ready):
    first = await enroll(database, ready)
    legacy = uuid4()
    async with database() as db:
        db.add(Organization(id=legacy, name="Unbound legacy organization"))
        await db.commit()
    async with database() as db:
        with pytest.raises(DBAPIError):
            await db.execute(
                update(Organization)
                .where(Organization.id == first.organization_id)
                .values(owned_owner_user_id="user_other")
            )
        await db.rollback()
        with pytest.raises(DBAPIError):
            await db.execute(
                update(Organization).where(Organization.id == legacy)
                .values(owned_owner_user_id="user_inferred_from_legacy")
            )
        await db.rollback()
    assert (await enroll(database, ready)) == first


async def test_missing_deployment_metadata_rolls_back_enrollment(database):
    with pytest.raises(HTTPException) as error:
        await enroll(database, OwnedTenantService(OwnedTenantStore()))
    assert error.value.status_code == 503
    async with database() as db:
        assert await db.scalar(select(func.count()).select_from(Organization)) == 0
