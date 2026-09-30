"""Typed engine-owned cycle state inside the existing SyncCursor."""

import hashlib
import json
from typing import Annotated, Literal
from uuid import UUID

from pydantic import (
    AfterValidator,
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    field_validator,
    model_validator,
)

from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.requests import RecordKind, ScanVersion, WriterFence

CompletionPolicy = Literal["exhaustive", "discovery_only", "discovery_with_validation"]

CaptureMode = Literal["full", "changes", "mixed"]
CompletedDiscovery = Literal["incomplete", "scope_enumeration_complete"]

CYCLE_KEY = "canonical_cycle"
TERMINAL_CHECKPOINT_KEY = "_canonical_terminal_checkpoint"


def bounded_source_plan(value: dict[str, JsonValue]) -> dict[str, JsonValue]:
    """Immutable cycle parameters must not become a response archive."""
    if len(json.dumps(value, allow_nan=False).encode()) > 65536:
        raise ValueError("Cycle source plan exceeds64KiB")
    return value


SourcePlan = Annotated[dict[str, JsonValue], AfterValidator(bounded_source_plan)]


class CycleConfiguration(BaseModel):
    """Immutable copy of the source's allowed record-parent relationships."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    parents: dict[RecordKind, tuple[RecordKind | None, ...]]
    completion_policies: dict[RecordKind, CompletionPolicy] = Field(default_factory=dict)
    known_object_validation: tuple[RecordKind, ...] = ()
    scope_changes: tuple[RecordKind, ...] = ()

    def digest(self) -> str:
        """Bind history to scope, topology and guarantees, including default policies."""
        value = self.model_dump(mode="json")
        if not self.known_object_validation:
            value.pop("known_object_validation")  # Preserve existing default configuration digests.
        if not self.scope_changes:
            value.pop("scope_changes")
        value["completion_policies"] = {kind: self.policy(kind) for kind in self.parents}
        return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()

    def policy(self, kind: str) -> CompletionPolicy:
        """Omitted declarations preserve historical exhaustive semantics."""
        return self.completion_policies.get(kind, "exhaustive")

    @classmethod
    def from_source(
        cls,
        *,
        fingerprint: str,
        record_types: tuple[str, ...],
        container_parents: dict[str, str | tuple[str | None, ...]],
        completion_policies: dict[RecordKind, CompletionPolicy] | None = None,
        known_object_validation: tuple[RecordKind, ...] = (),
        scope_changes: tuple[RecordKind, ...] = (),
    ) -> "CycleConfiguration":
        """Snapshot the existing source declaration; no second topology registry."""
        if len(set(record_types)) != len(record_types):
            raise ValueError("Duplicate source record kind")
        if set(container_parents) - set(record_types):
            raise ValueError("Parent relationship has an undeclared source record kind")
        parents = {}
        for kind in record_types:
            declared = container_parents.get(kind, (None,))
            parents[kind] = (declared,) if isinstance(declared, str) else declared
        return cls(
            fingerprint=fingerprint,
            parents=parents,
            completion_policies=completion_policies or {},
            known_object_validation=known_object_validation,
            scope_changes=scope_changes,
        )

    @model_validator(mode="before")
    @classmethod
    def upgrade_flat(cls, value):
        """Read schema1 flat declarations without retaining a competing topology."""
        if isinstance(value, dict) and "root_record_type" in value:
            upgraded = dict(value)
            root = upgraded.pop("root_record_type")
            children = upgraded.pop("child_record_types", ())
            if not isinstance(children, (list, tuple)):
                raise ValueError("Legacy child kinds must be an array")
            if "parents" in upgraded or root in children or len(set(children)) != len(children):
                raise ValueError("Ambiguous capture topology")
            upgraded["parents"] = {root: (None,), **{child: (root,) for child in children}}
            return upgraded
        return value

    @model_validator(mode="after")
    def reachable_types(self) -> "CycleConfiguration":
        """Type cycles are valid; every declared kind must be reachable from a root."""
        if len(set(self.known_object_validation)) != len(self.known_object_validation) or (
            set(self.known_object_validation) - set(self.parents)
        ):
            raise ValueError("Known-object validation requires unique declared kinds")
        if len(set(self.scope_changes)) != len(self.scope_changes) or any(
            kind not in self.parents or self.children_of(kind) for kind in self.scope_changes
        ):
            raise ValueError("Scope changes require unique declared leaf kinds")
        if set(self.completion_policies) - set(self.parents):
            raise ValueError("Completion policy has an undeclared record kind")
        if not self.parents or any(not kind or not values for kind, values in self.parents.items()):
            raise ValueError("Capture topology must declare kinds and their parents")
        if any(len(set(values)) != len(values) for values in self.parents.values()):
            raise ValueError("Duplicate allowed parent kind")
        if any(
            parent is not None and parent not in self.parents
            for values in self.parents.values()
            for parent in values
        ):
            raise ValueError("Unknown parent kind")
        reachable = {kind for kind, values in self.parents.items() if None in values}
        while True:
            expanded = reachable | {
                kind
                for kind, values in self.parents.items()
                if any(parent in reachable for parent in values)
            }
            if expanded == reachable:
                break
            reachable = expanded
        if reachable != set(self.parents):
            raise ValueError("Capture kinds must be reachable from a root")
        return self

    @property
    def root_record_types(self) -> tuple[str, ...]:
        """Kinds permitting records without a parent."""
        return tuple(kind for kind, parents in self.parents.items() if None in parents)

    def children_of(self, kind: str) -> tuple[str, ...]:
        """Scopes required for a visible record of this kind."""
        return tuple(child for child, parents in self.parents.items() if kind in parents)


class CycleVersion(BaseModel):
    """Exact checkpoint version observed by the caller."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    cycle_id: UUID
    revision: int = Field(ge=1)


class ProviderCheckpoint(BaseModel):
    """Bounded connector state; distinct from the engine's change-sequence checkpoint."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    value: dict[str, JsonValue]

    @field_validator("value")
    @classmethod
    def bounded(cls, value: dict[str, JsonValue]) -> dict[str, JsonValue]:
        """Never store an unbounded provider response or work queue."""
        if not value or len(json.dumps(value, allow_nan=False).encode()) > 65536:
            raise ValueError("Provider checkpoint must contain at most 64 KiB of state")
        return value


class FullCaptureEvidence(BaseModel):
    """A completed full pass certifies only its explicit discovery guarantee."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    cycle_id: UUID
    completed_at: AwareDatetime
    discovery: CompletedDiscovery
    configuration_digest: str = Field(pattern=r"^[a-f0-9]{64}$")


class PromotedCheckpoint(BaseModel):
    """Only successful cycle finalization advances this private provider boundary."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    checkpoint: ProviderCheckpoint
    configuration_digest: str = Field(pattern=r"^[a-f0-9]{64}$")
    promoted_at: AwareDatetime


class TerminalCheckpoint(BaseModel):
    """Source-parsed boundary from an exact completed root scan, attested during publication."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    record_type: RecordKind
    expected: ScanVersion
    checkpoint: ProviderCheckpoint


class CaptureCycle(BaseModel):
    """Completion stays durable until an explicit next-cycle CAS."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal[2] = 2
    version: CycleVersion
    configuration: CycleConfiguration
    phase: Literal["active", "complete"] = "active"
    completed_job_id: UUID | None = None
    mode: CaptureMode = "full"
    starting_checkpoint: ProviderCheckpoint | None = None
    source_plan: SourcePlan = Field(default_factory=dict)
    force_full_scopes: bool = False
    promoted_checkpoint: PromotedCheckpoint | None = None
    last_full_capture: FullCaptureEvidence | None = None

    @model_validator(mode="before")
    @classmethod
    def upgrade_version(cls, value):
        """Flat persisted cycles normalize through the same configuration boundary."""
        if isinstance(value, dict) and value.get("schema_version") == 1:
            upgraded = {**value, "schema_version": 2}
            upgraded.pop("root_writer_attempt_id", None)
            return upgraded
        return value


class BeginCycle(BaseModel):
    """Resume an active cycle or CAS-advance its completed predecessor."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    fence: WriterFence
    configuration: CycleConfiguration
    expected: CycleVersion | None = None
    mode: CaptureMode = "full"
    starting_checkpoint: ProviderCheckpoint | None = None
    source_plan: SourcePlan = Field(default_factory=dict)
    force_full_scopes: bool = False


class CompleteCycle(BaseModel):
    """Publish only an unchanged active cycle with all required scans complete."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    fence: WriterFence
    expected: CycleVersion
    terminal_checkpoint: TerminalCheckpoint | None = None


class RestartCycle(BaseModel):
    """Explicitly abandon the exact active cycle without certifying its partial capture."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    fence: WriterFence
    expected: CycleVersion
    configuration: CycleConfiguration
    mode: CaptureMode = "full"
    starting_checkpoint: ProviderCheckpoint | None = None
    source_plan: SourcePlan = Field(default_factory=dict)
    force_full_scopes: bool = False


class ScopeWork(BaseModel):
    """An eligible incomplete inventory and its exact captured owner, never a durable queue."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    record_type: str
    parent: SourceRecord | None = None
    parent_visibility_epoch: int | None = None
