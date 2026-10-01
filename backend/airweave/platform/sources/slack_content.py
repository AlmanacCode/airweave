"""Acquire Slack originals before the existing page transaction advances its cursor."""

from typing import TYPE_CHECKING
from urllib.parse import urlsplit

import httpx
from pydantic import BaseModel, ConfigDict, Field, JsonValue

from airweave.domains.entities.canonical.requests import BlobReference, CaptureRecord
from airweave.domains.entities.canonical.slack_files import (
    SlackFileManifest,
    SlackFileOutcome,
    SlackFileReason,
)
from airweave.domains.sources.exceptions import (
    SourceEntityForbiddenError,
    SourceEntityNotFoundError,
    SourceServerError,
)
from airweave.domains.storage.exceptions import FileSkippedException
from airweave.domains.storage.file_service import FileService
from airweave.platform.sources.http_helpers import raise_for_status
from airweave.platform.sources.slack_errors import SlackApiError

if TYPE_CHECKING:
    from airweave.platform.sources.slack import SlackSource


class SlackFileMetadata(BaseModel):
    """Parse only acquisition fields; original native JSON remains authoritative."""

    model_config = ConfigDict(extra="ignore", strict=True)
    id: str = Field(pattern=r"^F[A-Z0-9]+$", max_length=128)
    size: int | None = Field(default=None, ge=0)
    mimetype: str | None = Field(default=None, max_length=256)
    is_external: bool = False
    mode: str | None = None
    url_private_download: str | None = Field(default=None, max_length=16384)
    url_private: str | None = Field(default=None, max_length=16384)


class SlackMessageFiles(BaseModel):
    """Malformed file lists fail capture instead of quietly dropping originals."""

    model_config = ConfigDict(extra="ignore", strict=True)
    files: list[SlackFileMetadata] = Field(default_factory=list, max_length=100)


def _original_url(file: SlackFileMetadata) -> str | None:
    url = file.url_private_download or file.url_private
    if not url:
        return None
    try:
        parsed = urlsplit(url)
        valid = (
            parsed.scheme == "https"
            and parsed.hostname in {"slack.com", "files.slack.com"}
            and parsed.port in {None, 443}
            and parsed.username is None
            and parsed.password is None
            and not parsed.fragment
        )
    except ValueError:
        valid = False
    if not valid:
        raise ValueError("Slack original URL is outside the permitted origin")
    return url


async def capture_slack_file(
    source: "SlackSource", record: CaptureRecord, files: FileService
) -> CaptureRecord:
    """Acquire one message-owned file; the caller commits its child page atomically."""
    file = SlackFileMetadata.model_validate(record.payload)
    if record.identity.record_type != "file" or record.identity.native_id != file.id:
        raise ValueError("Slack file payload does not match its record identity")
    file, native, reason = await _enrich(source, file)
    blobs = []
    if reason is None:
        if file.is_external or file.mode == "external":
            reason = "unsupported_external"
        elif file.size is not None and file.size > files.MAX_FILE_SIZE_BYTES:
            reason = "oversized"
        elif file.size is None or not file.mimetype or (url := _original_url(file)) is None:
            reason = "missing_metadata"
        else:
            blob, reason = await _download(source, file, url, files)
            if blob is not None:
                blobs.append(blob.model_copy(update={"source_path": ""}))
    manifest = SlackFileManifest(
        files=(
            SlackFileOutcome(
                index=0,
                native_id=file.id,
                outcome="captured" if reason is None else "unavailable",
                reason=reason,
                file=native,
            ),
        )
    )
    reference = await files.store_canonical_blob(
        manifest.model_dump_json().encode(), media_type="application/json"
    )
    blobs.append(reference.model_copy(update={"role": "representation_manifest"}))
    return record.model_copy(
        update={
            "blobs": tuple(blobs),
            "completeness": "partial" if reason else "complete",
        }
    )


async def _download(
    source: "SlackSource", file: SlackFileMetadata, url: str, files: FileService
) -> tuple[BlobReference | None, SlackFileReason | None]:
    try:
        blob = await files.capture_canonical_url(
            url,
            source.http_client,
            source.auth,
            source.logger,
            media_type=file.mimetype,
            expected_media_type=file.mimetype,
            follow_redirects=False,
        )
    except FileSkippedException:
        return None, "oversized"
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code in {403, 404}:
            return None, "access_denied" if exc.response.status_code == 403 else "not_found"
        # Never put private download URLs or response bodies in domain diagnostics.
        safe = httpx.Response(
            exc.response.status_code,
            headers={"Retry-After": exc.response.headers["Retry-After"]}
            if "Retry-After" in exc.response.headers
            else {},
            request=httpx.Request("GET", "https://slack.com/"),
        )
        raise_for_status(
            safe, source_short_name="slack", token_provider_kind=source.auth.provider_kind
        )
        raise
    except httpx.RequestError:
        raise SourceServerError(
            "Slack original download failed", source_short_name="slack"
        ) from None
    if file.size is not None and blob.size_bytes != file.size:
        raise ValueError("Slack original size changed during capture; retry the page")
    return blob, None


async def _enrich(
    source: "SlackSource", file: SlackFileMetadata
) -> tuple[SlackFileMetadata, dict[str, JsonValue] | None, SlackFileReason | None]:
    if file.is_external or file.mode == "external":
        return file, None, None
    if _original_url(file) and file.size is not None and file.mimetype:
        return file, None, None
    try:
        response = await source._get("https://slack.com/api/files.info", {"file": file.id})
    except SourceEntityForbiddenError:
        return file, None, "access_denied"
    except SourceEntityNotFoundError:
        return file, None, "not_found"
    except SlackApiError as exc:
        if exc.code in {"access_denied", "not_visible"}:
            return file, None, "access_denied"
        if exc.code in {"file_not_found", "file_deleted"}:
            return file, None, "not_found"
        # Missing scope is a connection/configuration failure, not evidence of file denial.
        raise
    native = response.get("file")
    enriched = SlackFileMetadata.model_validate(native)
    if enriched.id != file.id:
        raise ValueError("Slack file metadata identity changed")
    return enriched, native, None
