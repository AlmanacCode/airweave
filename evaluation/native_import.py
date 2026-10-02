"""Bounded staged-native publisher; retry by retaining the same input and request key."""

import hashlib
import json
from typing import Literal
from urllib.parse import quote
from uuid import UUID, uuid5

import httpx
from pydantic import Field, ValidationError

from airweave.domains.entities.canonical.requests import CompletedScope
from airweave.domains.native_ingestion.import_models import (
    NativeImportState,
    StartNativeImport,
)
from airweave.domains.native_ingestion.models import NativeModel, NativeSnapshot
from airweave.domains.native_ingestion.page_models import (
    CommitNativePage,
    NativePageAck,
)
from airweave.domains.native_ingestion.scope_models import (
    BeginNativeScope,
    NativeScopeState,
    ReconcileNativeScope,
)
from airweave.domains.native_ingestion.source_models import (
    EnsureNativeSource,
    NativeSource,
)

_PAGE_NAMESPACE = UUID("3c2ba962-a612-4ca3-a29b-342521fd35bc")
_PAGE_SIZE = 500


class NativePublishError(ValueError):
    """Publication cannot safely resume; no original content appears in diagnostics."""


class _Cursor(NativeModel):
    snapshot_id: str
    next_offset: int = Field(ge=0)


class _Scope(NativeModel):
    scope: CompletedScope
    snapshots: tuple[NativeSnapshot, ...]


def _prepare(
    owner: str,
    dataset: Literal["knowledge", "sessions"],
    snapshots: tuple[NativeSnapshot, ...],
) -> tuple[_Scope, ...]:
    # Validate copied/constructed models again before any server mutation.
    checked = tuple(
        NativeSnapshot.model_validate(item.model_dump(mode="json"))
        for item in snapshots
    )
    unique = set()
    for item in checked:
        key = (item.identity.record_type, item.identity.entity_key)
        if key in unique or item.owner_id != owner or item.operation != "upsert":
            raise NativePublishError(
                "Staged native identity, owner or operation is invalid"
            )
        unique.add(key)
        allowed = ("knowledge",) if dataset == "knowledge" else ("session", "message")
        if item.identity.record_type not in allowed:
            raise NativePublishError("Staged native dataset is mixed")
    kind = "knowledge" if dataset == "knowledge" else "session"
    roots = tuple(
        sorted(
            (item for item in checked if item.identity.record_type == kind),
            key=lambda item: item.identity.native_id,
        )
    )
    scopes = [_Scope(scope=CompletedScope(record_type=kind), snapshots=roots)]
    if dataset == "sessions":
        parents = {item.identity.native_id: item for item in roots}
        children: dict[str, list[NativeSnapshot]] = {key: [] for key in parents}
        for item in checked:
            if item.identity.record_type != "message":
                continue
            parent = parents.get(item.identity.container_id)
            if (
                parent is None
                or item.parent != parent.identity
                or item.version != parent.version
            ):
                raise NativePublishError(
                    "Staged message has no matching session/version"
                )
            children[parent.identity.native_id].append(item)
        for parent in roots:
            scopes.append(
                _Scope(
                    scope=CompletedScope(
                        record_type="message",
                        container_id=parent.identity.native_id,
                        parent=parent.identity,
                    ),
                    snapshots=tuple(
                        sorted(
                            children[parent.identity.native_id],
                            key=lambda item: item.identity.native_id,
                        )
                    ),
                )
            )
    return tuple(scopes)


async def _call[T: NativeModel](
    client: httpx.AsyncClient,
    method: str,
    path: str,
    model: type[T],
    body: NativeModel | None = None,
) -> T:
    response = await client.request(
        method, path, json=body.model_dump(mode="json") if body else None
    )
    if response.status_code != 200:
        raise NativePublishError(
            f"Native destination rejected {method} with HTTP {response.status_code}"
        )
    try:
        return model.model_validate_json(response.content)
    except ValidationError:
        raise NativePublishError(
            "Native destination returned malformed progress"
        ) from None


def _attest_import(
    state: NativeImportState, source: NativeSource, key: str, digest: str
) -> None:
    if (
        state.source_id != source.source_connection_id
        or state.request_key != key
        or state.request.snapshot_id != digest
        or state.request.coverage != "bounded"
    ):
        raise NativePublishError("Native import response differs from staged intent")
    if state.status == "completed" and (
        state.summary is None
        or not state.summary.capture_complete
        or state.summary.outcome != "completed"
        or state.summary.coverage != "bounded"
    ):
        raise NativePublishError(
            "Completed import has no valid bounded capture summary"
        )


def _attest_scope(
    state: NativeScopeState, scope: _Scope, imported: NativeImportState
) -> None:
    if (
        state.cycle_id != imported.cycle_id
        or state.scope != scope.scope
        or state.coverage != "bounded"
    ):
        raise NativePublishError("Native scope does not belong to this bounded import")


async def publish_native(
    client: httpx.AsyncClient,
    *,
    owner_id: str,
    dataset: Literal["knowledge", "sessions"],
    collection: str,
    request_key: str,
    snapshots: tuple[NativeSnapshot, ...],
) -> NativeImportState:
    """Publish already staged originals; transport failures require identical caller retry.

    The authenticated client base URL targets the destination API root. No source
    reads, credentials, discovery, automatic retries or indexing claims are involved.
    """
    ensure = EnsureNativeSource(
        owner_id=owner_id, dataset=dataset, collection=collection
    )
    if not 1 <= len(request_key) <= 128:
        raise NativePublishError("Native request key must contain 1 to 128 characters")
    scopes = _prepare(owner_id, dataset, snapshots)
    digest = hashlib.sha256(
        json.dumps(
            {
                "publisher_version": 1,
                "page_size": _PAGE_SIZE,
                "source": ensure.model_dump(mode="json"),
                "scopes": [scope.model_dump(mode="json") for scope in scopes],
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode()
    ).hexdigest()
    source = await _call(client, "PUT", "native/sources", NativeSource, ensure)
    if source.binding != ensure.binding() or source.collection != collection:
        raise NativePublishError(
            "Native destination source binding differs from staged input"
        )
    base = f"native/sources/{source.source_connection_id}/imports/{quote(request_key, safe='')}"
    started = await _call(
        client,
        "PUT",
        base,
        NativeImportState,
        StartNativeImport(snapshot_id=digest, coverage="bounded"),
    )
    _attest_import(started, source, request_key, digest)
    if started.status == "completed":
        return started
    if started.status != "running":
        raise NativePublishError("Native import is terminal or unavailable")
    for scope in scopes:
        state = await _call(
            client,
            "PUT",
            base + "/scopes",
            NativeScopeState,
            BeginNativeScope(scope=scope.scope),
        )
        _attest_scope(state, scope, started)
        if not state.cursor:
            if state.phase != "collecting" or state.last_page is not None:
                raise NativePublishError(
                    "Native scope is missing its staged input cursor"
                )
            offset = 0
        else:
            try:
                cursor = _Cursor.model_validate(state.cursor)
            except ValidationError:
                raise NativePublishError("Native scope cursor is malformed") from None
            offset = cursor.next_offset
            if cursor.snapshot_id != digest or offset > len(scope.snapshots):
                raise NativePublishError(
                    "Native scope cursor differs from staged input"
                )
            if state.phase != "collecting" and offset != len(scope.snapshots):
                raise NativePublishError("Native scope ended before the staged input")
            if state.phase == "collecting" and (
                offset == len(scope.snapshots) or offset % _PAGE_SIZE
            ):
                raise NativePublishError(
                    "Native scope cursor is not a deterministic page boundary"
                )
        while state.phase == "collecting":
            end = min(offset + _PAGE_SIZE, len(scope.snapshots))
            page_id = uuid5(
                _PAGE_NAMESPACE,
                json.dumps(
                    [digest, scope.scope.model_dump(mode="json"), offset],
                    sort_keys=True,
                ),
            )
            ack = await _call(
                client,
                "PUT",
                base + "/pages",
                NativePageAck,
                CommitNativePage(
                    page_id=page_id,
                    scope=scope.scope,
                    expected=state.version,
                    snapshots=scope.snapshots[offset:end],
                    cursor=_Cursor(snapshot_id=digest, next_offset=end).model_dump(
                        mode="json"
                    ),
                    final=end == len(scope.snapshots),
                ),
            )
            if (
                ack.page_id != page_id
                or ack.version.sweep_id != state.version.sweep_id
                or ack.version.revision != state.version.revision + 1
                or ack.phase
                != ("reconciling" if end == len(scope.snapshots) else "collecting")
            ):
                raise NativePublishError(
                    "Native destination acknowledged inconsistent page progress"
                )
            state = state.model_copy(
                update={"phase": ack.phase, "version": ack.version}
            )
            offset = end
        if state.phase == "reconciling":
            state = await _call(
                client,
                "POST",
                base + "/scopes/reconcile",
                NativeScopeState,
                ReconcileNativeScope(scope=scope.scope, expected=state.version),
            )
        _attest_scope(state, scope, started)
        if state.phase != "complete":
            raise NativePublishError("Bounded native scope did not complete")
    completed = await _call(client, "POST", base + "/complete", NativeImportState)
    _attest_import(completed, source, request_key, digest)
    if (
        completed.import_id != started.import_id
        or completed.cycle_id != started.cycle_id
        or completed.status != "completed"
    ):
        raise NativePublishError("Native destination did not complete this import")
    return completed
