"""Selected-team Linear originals, with engine-owned durable full-scope pagination."""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import AsyncGenerator
from datetime import datetime, timezone
from functools import partial
from pathlib import Path
from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, JsonValue, StrictBool
from tenacity import retry, retry_if_exception_type, stop_after_attempt

from airweave.core.logging import ContextualLogger
from airweave.core.shared_models import RateLimitLevel
from airweave.domains.auth_provider.exceptions import AuthProviderRateLimitError
from airweave.domains.browse_tree.types import NodeSelectionData
from airweave.domains.entities.canonical.cycle_models import CycleConfiguration
from airweave.domains.entities.canonical.page_source import CapturePage, ScopeAccessLost
from airweave.domains.entities.canonical.requests import (
    CaptureRecord,
    CompletedScope,
    RecordIdentity,
)
from airweave.domains.entities.canonical.scan_models import ScanContinuation
from airweave.domains.sources.exceptions import SourceError, SourceRateLimitError
from airweave.domains.sources.token_providers.protocol import (
    SourceAuthProvider,
    authorization_headers,
)
from airweave.domains.storage.file_service import FileService
from airweave.domains.syncs.cursors.cursor import SyncCursor
from airweave.platform.configs.config import LinearConfig
from airweave.platform.decorators import source
from airweave.platform.entities._base import BaseEntity
from airweave.platform.http_client.airweave_client import AirweaveHttpClient
from airweave.platform.http_client.retry_helpers import (
    retry_if_rate_limit_or_timeout,
    wait_rate_limit_with_backoff,
)
from airweave.platform.sources._base import BaseSource
from airweave.platform.sources.http_helpers import raise_for_status
from airweave.schemas.source_connection import AuthenticationMethod, OAuthType

_URL = "https://api.linear.app/graphql"
_QUERIES = Path(__file__).with_name("linear_queries")
_QUERY_NAMES = ("identity", "teams", "issues", "comments", "attachments", "issue-membership")
QUERIES = {name: (_QUERIES / f"{name}.graphql").read_text() for name in _QUERY_NAMES}
FIELD_SET_VERSION = 1


class _Model(BaseModel):
    model_config = ConfigDict(extra="ignore")


class _Identity(_Model):
    id: UUID


class _PageInfo(_Model):
    hasNextPage: StrictBool
    endCursor: str | None


class _Connection(_Model):
    nodes: list[dict[str, JsonValue]] = Field(max_length=50)
    pageInfo: _PageInfo


class _Issue(_Identity):
    team: _Identity


class _Child(_Identity):
    issue: _Identity


class _Dates(_Model):
    createdAt: AwareDatetime
    updatedAt: AwareDatetime


class _ErrorExtensions(_Model):
    code: str | None = None


class _GraphError(_Model):
    extensions: _ErrorExtensions = Field(default_factory=_ErrorExtensions)


class _Envelope(_Model):
    data: dict[str, JsonValue] | None = None
    errors: list[_GraphError] = Field(default_factory=list)


class _Progress(BaseModel):
    model_config = ConfigDict(extra="forbid")
    version: Literal[1] = 1
    after: str | None = Field(default=None, min_length=1)


@source(
    name="Linear",
    short_name="linear",
    auth_methods=[
        AuthenticationMethod.OAUTH_BROWSER,
        AuthenticationMethod.OAUTH_TOKEN,
        AuthenticationMethod.AUTH_PROVIDER,
    ],
    oauth_type=OAuthType.ACCESS_ONLY,
    auth_config_class=None,
    config_class=LinearConfig,
    labels=["Project Management"],
    supports_continuous=False,
    rate_limit_level=RateLimitLevel.ORG,
)
class LinearSource(BaseSource):
    """Capture selected issue originals and independently paginated child originals."""

    canonical_record_types = ("issue", "comment", "attachment")
    canonical_container_parents = {"comment": "issue", "attachment": "issue"}

    @property
    def capture_cycle_configuration(self) -> CycleConfiguration:
        """Expose canonical capability on the class, with instance-specific scope binding."""
        return self._cycle_configuration

    @classmethod
    async def create(
        cls,
        *,
        auth: SourceAuthProvider,
        logger: ContextualLogger,
        http_client: AirweaveHttpClient,
        config: LinearConfig,
    ) -> LinearSource:
        """Attest workspace and selected-team access before constructing a capture cycle."""
        instance = cls(auth=auth, logger=logger, http_client=http_client)
        instance.config = config
        instance.team_ids = frozenset(str(value) for value in config.team_ids)
        fingerprint = {
            "workspace": str(config.workspace_id),
            "teams": sorted(instance.team_ids),
            "field_set": FIELD_SET_VERSION,
            "include_archived": True,
            "queries": QUERIES,
            "kinds": cls.canonical_record_types,
        }
        instance._cycle_configuration = CycleConfiguration(
            fingerprint=hashlib.sha256(
                json.dumps(fingerprint, sort_keys=True).encode()
            ).hexdigest(),
            root_record_type="issue",
            child_record_types=("comment", "attachment"),
        )
        await instance.validate()
        return instance

    @retry(
        stop=stop_after_attempt(5),
        retry=retry_if_rate_limit_or_timeout | retry_if_exception_type(AuthProviderRateLimitError),
        wait=partial(wait_rate_limit_with_backoff, max_rate_limit_wait=None),
        reraise=True,
    )
    async def _query(self, name: str, variables: dict[str, JsonValue]) -> dict[str, JsonValue]:
        headers = await authorization_headers(self.auth)
        response = await self.http_client.post(
            _URL, headers=headers, json={"query": QUERIES[name], "variables": variables}
        )
        if response.status_code == 401 and self.auth.supports_refresh:
            headers = await authorization_headers(self.auth, refresh=True)
            response = await self.http_client.post(
                _URL, headers=headers, json={"query": QUERIES[name], "variables": variables}
            )
        # Inspect only the documented GraphQL rate-limit response before HTTP translation.
        if response.status_code in {200, 400}:
            envelope = _Envelope.model_validate(response.json())
            if any(error.extensions.code == "RATELIMITED" for error in envelope.errors):
                raw_reset = response.headers.get("X-RateLimit-Requests-Reset")
                try:
                    delay = max(1.0, float(raw_reset) / 1000 - time.time()) if raw_reset else 60.0
                except ValueError:
                    delay = 60.0
                raise SourceRateLimitError(source_short_name="linear", retry_after=delay)
        raise_for_status(
            response, source_short_name="linear", token_provider_kind=self.auth.provider_kind
        )
        envelope = _Envelope.model_validate(response.json())
        if envelope.errors or envelope.data is None:
            # Never log potentially sensitive provider error messages or accept partial data.
            raise SourceError(
                "Linear GraphQL did not return complete data", source_short_name="linear"
            )
        return envelope.data

    async def validate(self) -> None:
        """Verify native workspace identity and current discovery of configured teams."""
        data = await self._query("identity", {})
        workspace = _Identity.model_validate(data.get("organization"))
        self.viewer_id = _Identity.model_validate(data.get("viewer")).id
        if workspace.id != self.config.workspace_id:
            raise ValueError("Linear workspace does not match configured workspace")
        found: set[str] = set()
        after = None
        seen: set[str] = set()
        while True:
            data = await self._query("teams", {"first": 50, "after": after})
            page = _Connection.model_validate(data.get("teams"))
            found.update(str(_Identity.model_validate(node).id) for node in page.nodes)
            after = self._following(page, after)
            if after is None:
                break
            if after in seen:
                raise ValueError("Linear team discovery repeated a cursor")
            seen.add(after)
        if not self.team_ids <= found:
            raise ValueError("One or more selected Linear teams are unavailable")

    @staticmethod
    def _following(page: _Connection, current: str | None) -> str | None:
        if not page.pageInfo.hasNextPage:
            return None
        following = page.pageInfo.endCursor
        if not following or following == current:
            raise ValueError("Linear returned a missing or non-advancing continuation")
        return following

    def _check_issue(self, payload: JsonValue, expected: str | None = None) -> _Issue:
        issue = _Issue.model_validate(payload)
        if expected is not None and str(issue.id) != expected:
            raise ValueError("Linear returned the wrong issue identity")
        if str(issue.team.id) not in self.team_ids:
            raise ScopeAccessLost(
                "Linear issue moved outside selected teams", removal_reason="scope_removed"
            )
        return issue

    async def capture_page(
        self, scope: CompletedScope, continuation: ScanContinuation
    ) -> CapturePage:
        """Fetch exactly one whole-scope page; engine commits it and its continuation."""
        progress = _Progress.model_validate(continuation.value)
        variables: dict[str, JsonValue] = {"first": 50, "after": progress.after}
        if scope.record_type == "issue" and scope.container_id is None:
            variables["teamIds"] = sorted(self.team_ids)
            data = await self._query("issues", variables)
            page = _Connection.model_validate(data.get("issues"))
            for node in page.nodes:
                issue = _Issue.model_validate(node)
                if str(issue.team.id) not in self.team_ids:
                    raise ValueError("Linear root page contains an unselected team")
        elif scope.record_type in {"comment", "attachment"} and scope.container_id:
            UUID(scope.container_id)
            variables["issueId"] = scope.container_id
            name = "comments" if scope.record_type == "comment" else "attachments"
            data = await self._query(name, variables)
            issue_payload = data.get("issue")
            # Null without documented error evidence is not a deletion or access-loss proof.
            self._check_issue(issue_payload, scope.container_id)
            issue_data = _ChildConnection.model_validate(issue_payload)
            page = issue_data.comments if name == "comments" else issue_data.attachments
            if page is None:
                raise ValueError("Linear omitted the requested child connection")
            for node in page.nodes:
                if str(_Child.model_validate(node).issue.id) != scope.container_id:
                    raise ValueError("Linear child belongs to a different issue")
        else:
            raise ValueError("Unsupported Linear scope")
        following = self._following(page, progress.after)
        return CapturePage(
            records=tuple(self._capture(scope, node) for node in page.nodes),
            continuation=ScanContinuation(value=_Progress(after=following).model_dump()),
            final=following is None,
        )

    @staticmethod
    def _capture(scope: CompletedScope, payload: dict[str, JsonValue]) -> CaptureRecord:
        identity = _Identity.model_validate(payload)
        dates = _Dates.model_validate(payload)
        text = payload.get("description") if scope.record_type == "issue" else payload.get("body")
        # Retain links and native text; no claim that embedded or attachment bytes are stored.
        partial_content = scope.record_type == "attachment" or (
            isinstance(text, str) and "uploads.linear.app" in text
        )
        return CaptureRecord(
            identity=RecordIdentity(
                record_type=scope.record_type,
                native_id=str(identity.id),
                container_id=scope.container_id,
            ),
            payload=payload,
            payload_schema_version=FIELD_SET_VERSION,
            completeness="partial" if partial_content else "complete",
            source_created_at=dates.createdAt,
            source_updated_at=dates.updatedAt,
            observed_at=datetime.now(timezone.utc),
        )

    async def confirm_root_absent(self, native_id: str) -> None:
        """Fail closed on ambiguous absence; readable out-of-scope roots may be removed."""
        UUID(native_id)
        data = await self._query("issue-membership", {"issueId": native_id})
        issue = _Issue.model_validate(data.get("issue"))
        if str(issue.id) != native_id:
            raise ValueError("Linear returned the wrong issue identity")
        if str(issue.team.id) in self.team_ids:
            raise ValueError("Linear omitted an issue still readable in the selected scope")

    async def generate_entities(
        self,
        *,
        cursor: SyncCursor | None = None,
        files: FileService | None = None,
        node_selections: list[NodeSelectionData] | None = None,
    ) -> AsyncGenerator[BaseEntity, None]:
        """Legacy crawling is replaced by canonical page capture and offline projection."""
        raise NotImplementedError("Linear requires canonical page capture")
        yield  # pragma: no cover


class _ChildConnection(_Model):
    comments: _Connection | None = None
    attachments: _Connection | None = None
