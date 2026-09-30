"""Gmail's structured 403 quota reasons, separate from permission denial."""

import math
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from airweave.domains.sources.exceptions import SourceServerError


class _Reason(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True)
    domain: str
    reason: str


class _Error(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True)
    code: int
    errors: list[_Reason] = Field(min_length=1)


class _Envelope(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True)
    error: _Error


class GmailThrottleError(SourceServerError):
    """Native 403 throttle; unknown provider delay remains unknown."""

    status_code = 403

    def __init__(self, retry_after: float | None):
        """Retain only safe native timing, never provider bodies or request URLs."""
        self.retry_after = retry_after
        super().__init__(
            "Gmail rate limit exceeded (403); capture remains resumable",
            source_short_name="gmail",
            status_code=403,
        )


def _retry_after(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        seconds = float(value)
    except ValueError:
        try:
            date = parsedate_to_datetime(value)
            if date.tzinfo is None:
                return None
            seconds = (date - datetime.now(timezone.utc)).total_seconds()
        except (ValueError, TypeError, OverflowError):
            return None
    return max(0.0, seconds) if math.isfinite(seconds) else None


def raise_gmail_throttle(response: httpx.Response) -> None:
    """Retry only documented transient quota reasons, not mixed or unknown errors."""
    if response.status_code != 403:
        return
    try:
        envelope = _Envelope.model_validate_json(response.content)
    except ValidationError:
        return
    if envelope.error.code != 403 or not all(
        item.domain == "usageLimits"
        and item.reason in {"rateLimitExceeded", "userRateLimitExceeded"}
        for item in envelope.error.errors
    ):
        return
    raise GmailThrottleError(_retry_after(response.headers.get("Retry-After")))
