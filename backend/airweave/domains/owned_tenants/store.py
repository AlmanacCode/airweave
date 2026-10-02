"""Existing tenant, collection and encrypted key rows; no enrollment ledger."""

from datetime import datetime

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.domains.owned_tenants.models import OwnedTenantIdentity
from airweave.models.api_key import APIKey
from airweave.models.collection import Collection
from airweave.models.organization import Organization
from airweave.models.vector_db_deployment_metadata import VectorDbDeploymentMetadata


class OwnedTenantStore:
    """Caller owns the transaction. The organization lock serializes every ensure."""

    async def lock(
        self, db: AsyncSession, owner: str, identity: OwnedTenantIdentity, *, existing_only: bool
    ) -> tuple[Organization, bool]:
        """Serialize on exact organization and refuse legacy adoption or owner conflicts."""
        inserted = None
        if not existing_only:
            inserted = await db.scalar(
                insert(Organization)
                .values(
                    id=identity.organization_id,
                    name="Almanac personal tenant",
                    owned_owner_user_id=owner,
                )
                .on_conflict_do_nothing()
                .returning(Organization.id)
            )
        organization = await db.scalar(
            select(Organization)
            .where(Organization.id == identity.organization_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if organization is None and existing_only:
            raise HTTPException(404, {"code": "not_enrolled"})
        if organization is None or organization.owned_owner_user_id != owner:
            # No legacy/unbound adoption, collision reuse or owner-driven rerouting.
            raise HTTPException(409, "Owned tenant identity conflicts with stored authority")
        return organization, inserted is not None

    async def collection(
        self, db: AsyncSession, identity: OwnedTenantIdentity, *, new: bool
    ) -> None:
        """Create the first collection only with an existing shared deployment singleton."""
        existing = await db.scalar(
            select(Collection).where(
                (Collection.id == identity.collection_id)
                | (Collection.readable_id == identity.collection)
            )
        )
        if existing is not None:
            if (
                existing.id != identity.collection_id
                or existing.organization_id != identity.organization_id
                or existing.readable_id != identity.collection
            ):
                raise HTTPException(409, "Owned tenant collection conflicts with stored authority")
            if new:
                raise HTTPException(409, "New owned tenant has unexplained existing collection")
            return
        if not new:
            raise HTTPException(
                409, "Owned tenant collection is missing; operator recovery required"
            )
        metadata = (await db.scalars(select(VectorDbDeploymentMetadata))).all()
        if len(metadata) != 1:
            raise HTTPException(503, "Shared index deployment metadata is unavailable")
        db.add(
            Collection(
                id=identity.collection_id,
                organization_id=identity.organization_id,
                name="Almanac originals",
                readable_id=identity.collection,
                vector_db_deployment_metadata_id=metadata[0].id,
            )
        )
        await db.flush()

    async def key(
        self, db: AsyncSession, identity: OwnedTenantIdentity, *, new: bool
    ) -> APIKey | None:
        """A missing key on existing enrollment is withdrawal, never initial issuance."""
        key = await db.scalar(
            select(APIKey)
            .where(APIKey.id == identity.api_key_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if key is not None and key.organization_id != identity.organization_id:
            raise HTTPException(409, "Owned tenant credential conflicts with stored authority")
        if new and key is not None:
            raise HTTPException(409, "New owned tenant has unexplained existing credential")
        if not new and key is None:
            raise HTTPException(
                409, "Owned tenant credential is withdrawn; operator recovery required"
            )
        return key

    async def issue(
        self,
        db: AsyncSession,
        identity: OwnedTenantIdentity,
        ciphertext: str,
        expires_at: datetime,
        *,
        key: APIKey | None,
    ) -> APIKey:
        """Issue or renew the designated encrypted credential in the existing APIKey row."""
        if key is None:
            key = APIKey(
                id=identity.api_key_id,
                organization_id=identity.organization_id,
                encrypted_key=ciphertext,
                expiration_date=expires_at,
            )
            db.add(key)
        else:
            key.encrypted_key = ciphertext
            key.expiration_date = expires_at
        await db.flush()
        return key
