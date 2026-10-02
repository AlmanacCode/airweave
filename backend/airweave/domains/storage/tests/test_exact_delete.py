"""Strict exact-object deletion using installed SDK errors, without cloud calls."""

from unittest.mock import AsyncMock, Mock

import pytest
from azure.core.exceptions import HttpResponseError, ResourceNotFoundError
from botocore.exceptions import ClientError
from google.cloud.exceptions import Forbidden, NotFound

from airweave.adapters.storage.aws_s3 import S3Backend
from airweave.adapters.storage.azure_blob import AzureBlobBackend
from airweave.adapters.storage.filesystem import FilesystemBackend
from airweave.adapters.storage.gcp_gcs import GCSBackend
from airweave.domains.storage.exceptions import StorageException

pytestmark = pytest.mark.asyncio


async def test_filesystem_exact_delete_is_idempotent_and_never_recursive(tmp_path, monkeypatch):
    storage = FilesystemBackend(tmp_path)
    await storage.write_file("one", b"body")
    await storage.delete_file("one")
    await storage.delete_file("one")
    await storage.write_file("folder/child", b"keep")
    with pytest.raises(StorageException):
        await storage.delete_file("folder")
    assert (tmp_path / "folder/child").read_bytes() == b"keep"
    monkeypatch.setattr("aiofiles.os.remove", AsyncMock(side_effect=PermissionError("private")))
    with pytest.raises(StorageException, match="^Exact file deletion failed$"):
        await storage.delete_file("folder/child")


async def test_s3_delete_uses_only_exact_key_and_preserves_errors():
    storage = S3Backend("bucket", "us-east-1", prefix="owned")
    client = Mock(delete_object=AsyncMock())
    storage._get_client = AsyncMock(return_value=client)
    await storage.delete_file("key")
    client.delete_object.assert_awaited_once_with(Bucket="bucket", Key="owned/key")
    client.delete_object.side_effect = ClientError({"Error": {"Code": "NoSuchKey"}}, "DeleteObject")
    await storage.delete_file("key")
    client.delete_object.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied"}}, "DeleteObject"
    )
    with pytest.raises(StorageException):
        await storage.delete_file("key")
    client.head_object.assert_not_called()
    client.get_paginator.assert_not_called()


async def test_azure_delete_uses_only_exact_blob_and_preserves_errors():
    storage = AzureBlobBackend("account", "container", prefix="owned")
    blob = Mock(delete_blob=AsyncMock())
    container = Mock(get_blob_client=Mock(return_value=blob))
    storage._get_container_client = AsyncMock(return_value=container)
    await storage.delete_file("key")
    container.get_blob_client.assert_called_once_with("owned/key")
    blob.delete_blob.assert_awaited_once_with()
    blob.delete_blob.side_effect = ResourceNotFoundError("missing")
    await storage.delete_file("key")
    blob.delete_blob.side_effect = HttpResponseError("private")
    with pytest.raises(StorageException):
        await storage.delete_file("key")
    container.list_blobs.assert_not_called()


async def test_gcs_delete_uses_only_exact_blob_and_preserves_errors():
    storage = GCSBackend("bucket", prefix="owned")
    blob = Mock(delete=Mock())
    bucket = Mock(blob=Mock(return_value=blob))
    storage._get_bucket = Mock(return_value=bucket)
    await storage.delete_file("key")
    bucket.blob.assert_called_once_with("owned/key")
    blob.delete.assert_called_once_with()
    blob.delete.side_effect = NotFound("missing")
    await storage.delete_file("key")
    blob.delete.side_effect = Forbidden("private")
    with pytest.raises(StorageException):
        await storage.delete_file("key")
    bucket.list_blobs.assert_not_called()
