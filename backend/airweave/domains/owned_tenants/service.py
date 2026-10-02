"""Idempotent owner enrollment using the existing scoped API-key data plane."""

import secrets
from collections.abc import Callable
from datetime import datetime, timezone

from cryptography.fernet import InvalidToken
from fastapi import HTTPException
from pydantic import SecretStr
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.core import credentials
from airweave.db.unit_of_work import UnitOfWork
from airweave.domains.owned_tenants.models import (
    SCOPED_KEY_VALIDITY,
    EnsureOwnedTenant,
    OwnedTenant,
    OwnedTenantIdentity,
    StoredCredential,
)
from airweave.domains.owned_tenants.store import OwnedTenantStore


def utc_now() -> datetime:
    """APIKey currently stores naive UTC; response converts it to aware UTC."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


class OwnedTenantService:
    """No provider, billing, human identity provisioning or workflow dispatch."""

    def __init__(self, store: OwnedTenantStore, clock: Callable[[], datetime] = utc_now) -> None:
        """Inject persistence and UTC clock, including deterministic expiration tests."""
        self.store, self.clock = store, clock

    async def ensure(self, db: AsyncSession, request: EnsureOwnedTenant) -> OwnedTenant:
        """Commit enrollment or recover exactly its existing authority and scoped key."""
        identity = OwnedTenantIdentity.for_owner(request.owner_user_id)
        async with UnitOfWork(db):
            _, new = await self.store.lock(
                db, request.owner_user_id, identity, existing_only=request.existing_only
            )
            await self.store.collection(db, identity, new=new)
            key = await self.store.key(db, identity, new=new)
            retained = None
            if key is not None:
                try:
                    retained = StoredCredential.model_validate(
                        credentials.decrypt(key.encrypted_key)
                    )
                except (InvalidToken, ValueError):
                    raise HTTPException(
                        409, "Owned tenant credential is invalid; operator recovery required"
                    ) from None
            now = self.clock()
            if key is None or key.expiration_date <= now:
                retained = StoredCredential(key=SecretStr(secrets.token_urlsafe(32)))
                key = await self.store.issue(
                    db,
                    identity,
                    credentials.encrypt({"key": retained.key.get_secret_value()}),
                    now + SCOPED_KEY_VALIDITY,
                    key=key,
                )
            if retained is None:
                raise HTTPException(409, "Owned tenant credential could not be recovered")
            return OwnedTenant(
                owner_user_id=request.owner_user_id,
                organization_id=identity.organization_id,
                collection=identity.collection,
                api_key_id=identity.api_key_id,
                api_key=retained.key,
                expires_at=key.expiration_date.replace(tzinfo=timezone.utc),
            )
