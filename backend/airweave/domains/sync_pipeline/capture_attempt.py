"""Source activity attempt identity, including explicit non-Temporal invocation."""

from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import BaseModel, ConfigDict, Field
from temporalio import activity


class CaptureAttempt(BaseModel):
    """Attempt number is supplied by the execution owner, never invented on restart."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    id: UUID
    number: int = Field(ge=1)


def resolve_capture_attempt(explicit: CaptureAttempt | None = None) -> CaptureAttempt:
    """Use real Temporal metadata or require a deliberate local invocation identity."""
    if explicit is not None:
        return explicit
    if not activity.in_activity():
        raise ValueError("Canonical capture outside Temporal requires an explicit CaptureAttempt")
    info = activity.info()
    identity = (
        f"temporal:{info.workflow_namespace}:{info.workflow_id}:"
        f"{info.workflow_run_id}:{info.activity_id}:{info.attempt}"
    )
    return CaptureAttempt(id=uuid5(NAMESPACE_URL, identity), number=info.attempt)
