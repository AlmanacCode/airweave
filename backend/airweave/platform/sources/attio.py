"""Attio originals with durable page progress and explicit discovery-only coverage."""

from __future__ import annotations

import hashlib
import json
from collections.abc import AsyncGenerator
from datetime import datetime, timezone
from functools import partial
from uuid import UUID

from pydantic import JsonValue
from tenacity import retry, retry_if_exception_type, stop_after_attempt

from airweave.core.logging import ContextualLogger
from airweave.core.shared_models import RateLimitLevel
from airweave.domains.auth_provider.exceptions import AuthProviderRateLimitError
from airweave.domains.browse_tree.types import NodeSelectionData
from airweave.domains.entities.canonical.cycle_models import CycleConfiguration
from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.page_source import CapturePage
from airweave.domains.entities.canonical.requests import (
    CaptureRecord,
    CompletedScope,
    RecordIdentity,
)
from airweave.domains.entities.canonical.scan_models import ScanContinuation
from airweave.domains.sources.exceptions import SourceAuthError
from airweave.domains.sources.token_providers.credential import DirectCredentialProvider
from airweave.domains.sources.token_providers.protocol import (
    SourceAuthProvider,
    authorization_headers,
)
from airweave.domains.storage.file_service import FileService
from airweave.domains.syncs.cursors.cursor import SyncCursor
from airweave.platform.configs.auth import AttioAuthConfig
from airweave.platform.configs.config import AttioConfig
from airweave.platform.decorators import source
from airweave.platform.entities._base import BaseEntity
from airweave.platform.http_client.airweave_client import AirweaveHttpClient
from airweave.platform.http_client.retry_helpers import (
    retry_if_rate_limit_or_timeout,
    wait_rate_limit_with_backoff,
)
from airweave.platform.sources._base import BaseSource
from airweave.platform.sources.attio_models import (
    AttioNative,
    AttioPage,
    AttioPrincipal,
    AttioProgress,
)
from airweave.platform.sources.http_helpers import raise_for_status
from airweave.schemas.source_connection import AuthenticationMethod

_API = "https://api.attio.com/v2"


@source(
    name="Attio",
    short_name="attio",
    auth_methods=[AuthenticationMethod.DIRECT, AuthenticationMethod.AUTH_PROVIDER],
    oauth_type=None,
    auth_config_class=AttioAuthConfig,
    config_class=AttioConfig,
    labels=["CRM"],
    supports_continuous=False,
    rate_limit_level=RateLimitLevel.ORG,
)
class AttioSource(BaseSource):
    """Keep CRM records, list memberships and notes as separate native originals.

    Offset pagination is not a snapshot. Every scope is discovery-only, so missing
    rows never authorize deletion. Exact-read and webhook qualification remains
    required before this connector can claim complete synchronization.
    """

    canonical_record_types = ("object", "record", "list", "entry", "note")
    canonical_container_parents = {"record": "object", "entry": "list", "note": "record"}

    @property
    def capture_cycle_configuration(self) -> CycleConfiguration:
        """Bind the engine's existing durable cycle to the attested workspace."""
        return self._cycle_configuration

    @classmethod
    async def create(
        cls,
        *,
        auth: SourceAuthProvider,
        logger: ContextualLogger,
        http_client: AirweaveHttpClient,
        config: AttioConfig,
    ) -> AttioSource:
        """Attest account identity without extracting managed credentials."""
        instance = cls(auth=auth, logger=logger, http_client=http_client)
        instance.config = config
        await instance.validate()
        fingerprint = {
            "version": 1,
            "workspace": str(config.workspace_id),
            "principal": instance.principal.model_dump(mode="json"),
            "kinds": cls.canonical_record_types,
        }
        instance._cycle_configuration = CycleConfiguration.from_source(
            fingerprint=hashlib.sha256(
                json.dumps(fingerprint, sort_keys=True).encode()
            ).hexdigest(),
            record_types=cls.canonical_record_types,
            container_parents=cls.canonical_container_parents,
            completion_policies={kind: "discovery_only" for kind in cls.canonical_record_types},
        )
        return instance

    async def _headers(self, *, refresh: bool = False) -> dict[str, str]:
        if isinstance(self.auth, DirectCredentialProvider):
            credentials = AttioAuthConfig.model_validate(self.auth.credentials.model_dump())
            return {"Authorization": f"Bearer {credentials.api_key}"}
        return await authorization_headers(self.auth, refresh=refresh)

    @retry(
        stop=stop_after_attempt(5),
        retry=retry_if_rate_limit_or_timeout | retry_if_exception_type(AuthProviderRateLimitError),
        wait=partial(wait_rate_limit_with_backoff, max_rate_limit_wait=None),
        reraise=True,
    )
    async def _request(
        self,
        path: str,
        *,
        body: dict[str, JsonValue] | None = None,
        params: dict[str, str | int] | None = None,
    ) -> dict[str, JsonValue]:
        headers = await self._headers()

        async def send():
            if body is not None:
                return await self.http_client.post(_API + path, headers=headers, json=body)
            return await self.http_client.get(_API + path, headers=headers, params=params)

        response = await send()
        if response.status_code == 401 and self.auth.supports_refresh:
            headers = await self._headers(refresh=True)
            response = await send()
        # In particular, 404 merge_in_progress must never become an empty page.
        raise_for_status(
            response, source_short_name="attio", token_provider_kind=self.auth.provider_kind
        )
        return response.json()

    async def validate(self) -> None:
        """HTTP200 with active=false is revoked access, never identity evidence."""
        principal = AttioPrincipal.model_validate(await self._request("/self"))
        if not principal.active:
            raise SourceAuthError(
                "Attio token is inactive; reconnect the account",
                source_short_name="attio",
                status_code=200,
                token_provider_kind=self.auth.provider_kind,
            )
        if principal.workspace_id != self.config.workspace_id:
            raise ValueError("Attio workspace identity does not match")
        self.principal = principal

    def child_scope(self, parent: SourceRecord, record_type: str) -> CompletedScope:
        """Use provider-global UUIDs while retaining the full canonical parent."""
        if self.canonical_container_parents.get(record_type) != parent.identity.record_type:
            raise ValueError("Unsupported Attio parent relationship")
        return CompletedScope(
            record_type=record_type, container_id=parent.identity.native_id, parent=parent.identity
        )

    async def capture_page(
        self,
        scope: CompletedScope,
        continuation: ScanContinuation,
        *,
        files: FileService,
        parent: SourceRecord | None = None,
    ) -> CapturePage:
        """Fetch one atomic page, retaining raw native fields and a resumable offset."""
        progress = AttioProgress.model_validate(continuation.value)
        kind = scope.record_type
        limit = 50 if kind == "note" else 500
        root = kind in {"object", "list"}
        object_slug: str | None = None
        if root:
            if scope.container_id is not None or scope.parent is not None or progress.offset:
                raise ValueError("Invalid Attio root scope")
            data = await self._request("/objects" if kind == "object" else "/lists")
        else:
            if parent is None or scope != self.child_scope(parent, kind):
                raise ValueError("Attio child scope requires its exact captured parent")
            UUID(scope.container_id)
            if kind == "record":
                data = await self._request(
                    f"/objects/{scope.container_id}/records/query",
                    body={"limit": limit, "offset": progress.offset},
                )
            elif kind == "entry":
                data = await self._request(
                    f"/lists/{scope.container_id}/entries/query",
                    body={"limit": limit, "offset": progress.offset},
                )
            elif kind == "note":
                object_id = parent.identity.container_id
                UUID(object_id)
                object_response = await self._request(f"/objects/{object_id}")
                native_object = AttioNative.model_validate(object_response.get("data"))
                if (
                    native_object.id.workspace_id != self.config.workspace_id
                    or str(native_object.id.object_id) != object_id
                    or not native_object.api_slug
                ):
                    raise ValueError("Attio note parent object identity is invalid")
                object_slug = native_object.api_slug
                data = await self._request(
                    "/notes",
                    params={
                        "parent_object": object_id,
                        "parent_record_id": scope.container_id,
                        "limit": limit,
                        "offset": progress.offset,
                    },
                )
            else:
                raise ValueError("Unsupported Attio scope")
        page = AttioPage.model_validate(data)
        if len(page.data) > limit:
            raise ValueError("Attio returned more rows than requested")
        records = tuple(self._capture(scope, raw, parent, object_slug) for raw in page.data)
        ids = tuple(record.identity.native_id for record in records)
        if len(set(ids)) != len(ids) or set(ids).intersection(progress.previous_ids):
            raise ValueError("Attio offset page repeated identities; restart discovery")
        return CapturePage(
            records=records,
            final=root or len(records) < limit,
            continuation=ScanContinuation(
                value=AttioProgress(
                    offset=progress.offset + len(records), previous_ids=ids
                ).model_dump(mode="json")
            ),
        )

    def _capture(
        self,
        scope: CompletedScope,
        raw: dict[str, JsonValue],
        parent: SourceRecord | None,
        object_slug: str | None = None,
    ) -> CaptureRecord:
        item = AttioNative.model_validate(raw)
        if item.id.workspace_id != self.config.workspace_id:
            raise ValueError("Attio returned a different workspace")
        kind = scope.record_type
        identity = {
            "object": item.id.object_id,
            "record": item.id.record_id,
            "list": item.id.list_id,
            "entry": item.id.entry_id,
            "note": item.id.note_id,
        }.get(kind)
        if identity is None:
            raise ValueError("Attio omitted the native identity")
        if kind == "record" and str(item.id.object_id) != scope.container_id:
            raise ValueError("Attio record belongs to a different object")
        if kind == "entry" and (
            str(item.id.list_id) != scope.container_id
            or item.parent_record_id is None
            or not item.parent_object
        ):
            raise ValueError("Attio entry has an invalid list or record reference")
        if kind == "note" and (
            parent is None
            or str(item.parent_record_id) != scope.container_id
            or item.parent_object not in {parent.identity.container_id, object_slug}
        ):
            raise ValueError("Attio note belongs to a different object or record")
        return CaptureRecord(
            identity=RecordIdentity(
                record_type=kind, native_id=str(identity), container_id=scope.container_id
            ),
            parent=scope.parent,
            payload=raw,
            source_created_at=item.created_at,
            observed_at=datetime.now(timezone.utc),
        )

    async def confirm_absent(self, record: SourceRecord) -> None:
        """Offset omissions and ambiguous404 responses cannot certify deletion."""
        raise ValueError(
            "Attio discovery cannot confirm absence without qualified provider evidence"
        )

    async def generate_entities(
        self,
        *,
        cursor: SyncCursor | None = None,
        files: FileService | None = None,
        node_selections: list[NodeSelectionData] | None = None,
    ) -> AsyncGenerator[BaseEntity, None]:
        """Canonical page capture replaces the legacy lossy entity generator."""
        raise NotImplementedError("Attio requires canonical page capture")
        yield  # pragma: no cover
