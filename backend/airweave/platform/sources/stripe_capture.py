"""Account-bound Stripe v1 originals; full discovery, never absence reconciliation.

The caller owns credentials/HTTP errors and must apply every supplied header. No
activation is implied by constructing this adapter. Nested collections and linked
files are retained as returned, not claimed to be fully acquired.
"""

import hashlib
from datetime import datetime, timezone
from typing import Annotated, Literal, Protocol

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    StrictBool,
    TypeAdapter,
    ValidationError,
)

from airweave.domains.entities.canonical.cycle_models import CaptureCycle, CycleConfiguration
from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.page_source import CapturePage, CapturePlan
from airweave.domains.entities.canonical.requests import (
    CaptureRecord,
    CompletedScope,
    RecordIdentity,
)
from airweave.domains.entities.canonical.scan_models import ScanContinuation
from airweave.domains.storage.file_service import FileService
from airweave.platform.configs.config import StripeCaptureConfig

NativeID = Annotated[str, Field(pattern=r"^[A-Za-z0-9_]{1,255}$")]
_JSON = TypeAdapter(dict[str, JsonValue])
_ENDPOINTS = {
    "balance": "balance",
    "balance_transaction": "balance_transactions",
    "charge": "charges",
    "customer": "customers",
    "event": "events",
    "invoice": "invoices",
    "payment_intent": "payment_intents",
    "payment_method": "payment_methods",
    "payout": "payouts",
    "refund": "refunds",
    "subscription": "subscriptions",
}


class StripeCaptureError(ValueError):
    """Sanitized boundary failure; the current page must not commit."""


class StripeRead(Protocol):
    """Authenticated transport; fixed relative v1 paths and no provider URL following."""

    async def __call__(
        self, path: str, *, params: dict[str, str | int], headers: dict[str, str]
    ) -> dict[str, JsonValue]:
        """Raise existing auth/rate/transport errors; return only successful native JSON."""
        ...


class _Account(BaseModel):
    id: NativeID
    object: Literal["account"]


class _Balance(BaseModel):
    object: Literal["balance"]
    livemode: StrictBool


class _Object(BaseModel):
    id: NativeID
    object: str
    livemode: StrictBool | None = None
    created: Annotated[int, Field(strict=True, ge=0, le=253402300799)] | None = None


class _Page(BaseModel):
    object: Literal["list"]
    data: list[dict[str, JsonValue]] = Field(max_length=100)
    has_more: StrictBool


class _Continuation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    fingerprint: str
    record_type: str
    starting_after: NativeID | None = None
    done: StrictBool = False


class StripeCapture:
    """Eleven independent account-owned roots, preserving legacy resource breadth.

    Payment methods use the unfiltered list (custom types are excluded by Stripe).
    Subscriptions request all statuses; test-clock inventories and full event history
    remain unqualified. Older API versions rejecting unfiltered lists fail explicitly;
    no card-only fallback silently narrows capture.
    """

    canonical_record_types = tuple(_ENDPOINTS)
    canonical_container_parents: dict[str, str] = {}

    def __init__(self, read: StripeRead, config: StripeCaptureConfig) -> None:
        """Use create to attest access before exposing capture configuration."""
        self._read = read
        self.config = config
        self._verified = False
        self.fingerprint = hashlib.sha256(
            ("stripe-full-v2:" + config.model_dump_json()).encode()
        ).hexdigest()

    @classmethod
    async def create(cls, read: StripeRead, config: StripeCaptureConfig) -> "StripeCapture":
        """Attest the effective account and mode through the capture request context."""
        instance = cls(read, config)
        await instance.verify_principal()
        return instance

    async def _get(self, path: str, params: dict[str, str | int]) -> dict[str, JsonValue]:
        headers = {"Stripe-Version": self.config.api_version}
        if self.config.connected_account_id is not None:
            headers["Stripe-Account"] = self.config.connected_account_id
        raw = await self._read(path, params=params, headers=headers)
        try:
            return _JSON.validate_python(raw)
        except ValidationError:
            raise StripeCaptureError("Stripe returned invalid native JSON") from None

    async def verify_principal(self) -> None:
        """Reset prior attestation before any validation/reconnect attempt."""
        self._verified = False
        try:
            account = _Account.model_validate(await self._get("/v1/account", {}))
            if account.id != self.config.expected_account_id or (
                self.config.connected_account_id is not None
                and account.id != self.config.connected_account_id
            ):
                raise StripeCaptureError("Stripe account does not match trusted binding")
            balance = _Balance.model_validate(await self._get("/v1/balance", {}))
            if balance.livemode != self.config.livemode:
                raise StripeCaptureError("Stripe mode does not match trusted binding")
        except ValidationError:
            raise StripeCaptureError("Stripe identity response is invalid") from None
        self._verified = True

    def _require_principal(self) -> None:
        if not self._verified:
            raise StripeCaptureError("Stripe principal is not attested")

    @property
    def capture_cycle_configuration(self) -> CycleConfiguration:
        """Completed list traversal never proves deletion or exhaustive provider coverage."""
        self._require_principal()
        return CycleConfiguration.from_source(
            fingerprint=self.fingerprint,
            record_types=self.canonical_record_types,
            container_parents={},
            completion_policies={kind: "discovery_only" for kind in self.canonical_record_types},
        )

    async def prepare_cycle(self, previous: CaptureCycle | None) -> CapturePlan:
        """Re-attest each full cycle; event capture is not an incremental checkpoint."""
        await self.verify_principal()
        return CapturePlan()

    def initial_continuation(self, cycle: CaptureCycle) -> ScanContinuation:
        """The engine binds cycle configuration; per-kind cursor binding occurs on first page."""
        if cycle.configuration != self.capture_cycle_configuration:
            raise StripeCaptureError("Stripe capture configuration changed")
        return ScanContinuation()

    def child_scope(self, parent: SourceRecord, record_type: str) -> CompletedScope:
        """Relationships to other Stripe objects are references, not visibility parents."""
        raise StripeCaptureError("Stripe capture has no child scopes")

    async def confirm_absent(self, record: SourceRecord) -> None:
        """Aging events, list omission and generic HTTP errors never establish deletion."""
        raise StripeCaptureError("Stripe discovery cannot confirm absence")

    def _record(self, raw: dict[str, JsonValue], kind: str) -> CaptureRecord:
        if kind == "balance":
            balance = _Balance.model_validate(raw)
            native_id, mode, created = "balance", balance.livemode, None
        else:
            obj = _Object.model_validate(raw)
            if "livemode" in raw and obj.livemode is None:
                raise StripeCaptureError("Stripe object mode is invalid")
            # Stripe documents object as the kind discriminator; ID examples are
            # not an exhaustive prefix contract (including historical IDs).
            if obj.object != kind:
                raise StripeCaptureError("Stripe object kind does not match scope")
            native_id, mode = obj.id, obj.livemode
            created = (
                datetime.fromtimestamp(obj.created, timezone.utc)
                if obj.created is not None
                else None
            )
        if mode is not None and mode != self.config.livemode:
            raise StripeCaptureError("Stripe object mode does not match source")
        return CaptureRecord(
            identity=RecordIdentity(record_type=kind, native_id=native_id),
            payload=raw,
            completeness="partial",
            source_created_at=created,
            observed_at=datetime.now(timezone.utc),
        )

    async def capture_page(
        self,
        scope: CompletedScope,
        continuation: ScanContinuation,
        *,
        files: FileService,
        parent: SourceRecord | None = None,
    ) -> CapturePage:
        """One list response and its exact cursor are committed by the shared engine."""
        self._require_principal()
        kind = scope.record_type
        if (
            kind not in _ENDPOINTS
            or scope.container_id is not None
            or scope.parent is not None
            or parent is not None
        ):
            raise StripeCaptureError("Unsupported Stripe capture scope")
        try:
            progress = (
                _Continuation.model_validate(continuation.value)
                if continuation.value
                else _Continuation(fingerprint=self.fingerprint, record_type=kind)
            )
            if (
                progress.fingerprint != self.fingerprint
                or progress.record_type != kind
                or progress.done
            ):
                raise StripeCaptureError("Stripe continuation binding is invalid")
            params: dict[str, str | int] = {} if kind == "balance" else {"limit": 100}
            if progress.starting_after is not None:
                if kind == "balance":
                    raise StripeCaptureError("Stripe balance cannot be paginated")
                params["starting_after"] = progress.starting_after
            if kind == "subscription":
                params["status"] = "all"
            raw = await self._get("/v1/" + _ENDPOINTS[kind], params)
            if kind == "balance":
                objects, has_more = [raw], False
            else:
                page = _Page.model_validate(raw)
                objects, has_more = page.data, page.has_more
            records = tuple(self._record(item, kind) for item in objects)
            ids = [record.identity.native_id for record in records]
            if (
                len(set(ids)) != len(ids)
                or progress.starting_after in ids
                or (has_more and not ids)
            ):
                raise StripeCaptureError("Stripe pagination did not make progress")
            following = _Continuation(
                fingerprint=self.fingerprint,
                record_type=kind,
                starting_after=ids[-1] if has_more else None,
                done=not has_more,
            )
            return CapturePage(
                records=records,
                continuation=ScanContinuation(value=following.model_dump(mode="json")),
                final=not has_more,
            )
        except (ValidationError, OverflowError, OSError):
            raise StripeCaptureError("Stripe capture response or continuation is invalid") from None
