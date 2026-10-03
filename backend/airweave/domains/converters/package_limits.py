"""Shared content bounds for ZIP-based local document preparation."""

from collections.abc import Iterable
from pathlib import Path
from zipfile import ZipFile

from pydantic import BaseModel, ConfigDict, Field

from airweave.domains.storage.limits import MAX_FILE_SIZE_BYTES
from airweave.domains.sync_pipeline.exceptions import EntityProcessingError


class PackageTextLimits(BaseModel):
    """Bound content, not hard process RSS or elapsed time inside mature parsers."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    maximum_package_members: int = Field(default=1024, gt=0)
    maximum_expanded_bytes: int = Field(default=32 * 1024 * 1024, gt=0)
    maximum_member_bytes: int = Field(default=16 * 1024 * 1024, gt=0)
    maximum_output_bytes: int = Field(default=8 * 1024 * 1024, gt=0)


class PreparationLimit(EntityProcessingError):
    """The original exceeds supported preparation content bounds."""


def check_package(path: str, limits: PackageTextLimits) -> None:
    """Inspect declared expanded package sizes before a document parser opens it."""
    if Path(path).stat().st_size > MAX_FILE_SIZE_BYTES:
        raise PreparationLimit("original input bytes")
    with ZipFile(path) as package:
        members = package.infolist()
        if len(members) > limits.maximum_package_members:
            raise PreparationLimit("package member count")
        if any(member.file_size > limits.maximum_member_bytes for member in members):
            raise PreparationLimit("expanded package member bytes")
        if sum(member.file_size for member in members) > limits.maximum_expanded_bytes:
            raise PreparationLimit("expanded package bytes")


def text_size(text: str, current_bytes: int, separator: str, limits: PackageTextLimits) -> int:
    """Check the next materialized library paragraph/row before retaining output."""
    size = current_bytes + len(text.encode("utf-8")) + len(separator.encode("utf-8"))
    if size > limits.maximum_output_bytes:
        raise PreparationLimit("extracted UTF-8 bytes")
    return size


def bounded_join(parts: Iterable[str], separator: str, limits: PackageTextLimits) -> str:
    """Include every delimiter in the budget; reject instead of truncating content."""
    retained: list[str] = []
    size = 0
    for part in parts:
        size = text_size(part, size, separator if retained else "", limits)
        retained.append(part)
    return separator.join(retained)
