"""Verify canonical blob identity before materializing disposable projection inputs."""

import hashlib
import re
from pathlib import Path

import aiofiles

from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.requests import BlobReference
from airweave.domains.storage.limits import MAX_FILE_SIZE_BYTES
from airweave.domains.storage.protocols import StorageBackend


async def read_blob(record: SourceRecord, ref: BlobReference, storage: StorageBackend) -> bytes:
    """Read only this sync's content-addressed bytes; reject corrupt references/content."""
    expected = f"canonical/{record.sync_id}/blobs/sha256/{ref.sha256}"
    if ref.key != expected:
        raise ValueError("Canonical blob key does not match source scope and digest")
    if ref not in record.blobs:
        raise ValueError("Canonical blob reference does not belong to the source record")
    if ref.size_bytes > MAX_FILE_SIZE_BYTES:
        raise ValueError("Canonical blob exceeds projection size limit")
    content = await storage.read_file(ref.key)
    if len(content) != ref.size_bytes or hashlib.sha256(content).hexdigest() != ref.sha256:
        raise ValueError("Canonical blob bytes do not match recorded size and digest")
    return content


async def write_blob(content: bytes, directory: Path, *, suffix: str) -> Path:
    """Use trusted extension and content hash, never provider filenames, for local paths."""
    if not re.fullmatch(r"\.[a-z0-9]{1,12}", suffix):
        raise ValueError("Projection filename suffix must be one simple extension")
    if len(content) > MAX_FILE_SIZE_BYTES:
        raise ValueError("Projection content exceeds size limit")
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / (hashlib.sha256(content).hexdigest() + suffix)
    # A disposable per-projection directory is owned by the caller, not provider data.
    if path.is_symlink():
        raise ValueError("Projection path must not be a symbolic link")
    async with aiofiles.open(path, "wb") as output:
        await output.write(content)
    return path
