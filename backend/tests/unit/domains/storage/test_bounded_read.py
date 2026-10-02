"""Actual oversized storage objects cannot bypass a canonical reference size bound."""

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest

from airweave.adapters.storage.aws_s3 import S3Backend
from airweave.adapters.storage.azure_blob import AzureBlobBackend
from airweave.adapters.storage.filesystem import FilesystemBackend
from airweave.adapters.storage.gcp_gcs import GCSBackend
from airweave.domains.storage.exceptions import StorageReadLimitExceeded


async def test_filesystem_reads_actual_bound_and_preserves_unbounded_callers(tmp_path):
    storage = FilesystemBackend(tmp_path)
    await storage.write_file("blob", b"12345")
    with pytest.raises(StorageReadLimitExceeded):
        await storage.read_file("blob", max_bytes=3)
    assert await storage.read_file("blob", max_bytes=5) == b"12345"
    assert await storage.read_file("blob") == b"12345"
    await storage.write_file("empty", b"")
    assert await storage.read_file("empty", max_bytes=0) == b""


@pytest.mark.parametrize("cancel", [False, True])
async def test_s3_stream_closes_on_oversize_and_cancellation(cancel):
    storage = S3Backend(bucket="synthetic", region="us-east-1")
    stream = AsyncMock()
    stream.__aenter__.return_value = stream
    stream.read.side_effect = asyncio.CancelledError() if cancel else None
    stream.read.return_value = b"1234"
    client = AsyncMock()
    client.get_object.return_value = {"Body": stream}
    storage._get_client = AsyncMock(return_value=client)
    with pytest.raises(asyncio.CancelledError if cancel else StorageReadLimitExceeded):
        await storage.read_file("blob", max_bytes=3)
    stream.read.assert_awaited_once_with(4)
    stream.__aexit__.assert_awaited_once()


async def test_azure_range_is_bounded_before_sdk_buffers():
    storage = AzureBlobBackend.__new__(AzureBlobBackend)
    storage._resolve = lambda path: path
    downloader = AsyncMock()
    downloader.readall.return_value = b"1234"
    blob = AsyncMock()
    blob.download_blob.return_value = downloader
    container = Mock()
    container.get_blob_client.return_value = blob
    storage._get_container_client = AsyncMock(return_value=container)
    with pytest.raises(StorageReadLimitExceeded):
        await storage.read_file("blob", max_bytes=3)
    blob.download_blob.assert_awaited_once_with(offset=0, length=4)


async def test_gcs_range_is_bounded_before_sdk_buffers():
    storage = GCSBackend(bucket="synthetic")
    blob = Mock()
    blob.download_as_bytes.return_value = b"1234"
    bucket = Mock()
    bucket.blob.return_value = blob
    storage._get_bucket = Mock(return_value=bucket)
    with pytest.raises(StorageReadLimitExceeded):
        await storage.read_file("blob", max_bytes=3)
    blob.download_as_bytes.assert_called_once_with(start=0, end=3, raw_download=True)


async def test_storage_does_not_disguise_programming_errors():
    storage = S3Backend(bucket="synthetic", region="us-east-1")
    storage._get_client = AsyncMock(side_effect=TypeError("programming error"))
    with pytest.raises(TypeError):
        await storage.read_file("blob", max_bytes=3)
