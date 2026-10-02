"""Tests for HybridDocumentConverter._try_read_as_text binary detection."""

import os
import tempfile
from unittest.mock import AsyncMock

import pytest

from airweave.domains.converters._base import HybridDocumentConverter
from airweave.domains.converters.pdf import PdfConverter
from airweave.domains.sync_pipeline.exceptions import SyncFailureError


async def test_infrastructure_failure_does_not_become_unavailable_conversion(tmp_path):
    """A missing parser dependency cannot be relabeled a failed content part."""
    path = tmp_path / "input.pdf"
    path.write_bytes(b"synthetic PDF bytes")
    fallback = AsyncMock()
    converter = PdfConverter(ocr_provider=fallback)
    converter._extract_local = AsyncMock(side_effect=SyncFailureError("Parser unavailable"))
    with pytest.raises(SyncFailureError, match="Parser unavailable"):
        await converter.convert_batch([str(path)])
    fallback.convert_batch.assert_not_awaited()


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


class TestTryReadAsText:
    """Tests for _try_read_as_text static method (binary detection)."""

    def test_plain_text_file_returns_content(self, temp_dir):
        path = os.path.join(temp_dir, "readme.docx")
        with open(path, "w", encoding="utf-8") as f:
            f.write("This is actually a plain text file with enough characters to pass threshold.")

        result = HybridDocumentConverter._try_read_as_text(path)
        assert result is not None
        assert "plain text file" in result

    def test_binary_file_returns_none(self, temp_dir):
        """File with >5% control chars → None."""
        path = os.path.join(temp_dir, "binary.docx")
        with open(path, "wb") as f:
            # Lots of control characters
            f.write(bytes(range(0, 32)) * 50)

        result = HybridDocumentConverter._try_read_as_text(path)
        assert result is None

    def test_empty_file_returns_none(self, temp_dir):
        path = os.path.join(temp_dir, "empty.docx")
        with open(path, "wb") as f:
            f.write(b"")

        result = HybridDocumentConverter._try_read_as_text(path)
        assert result is None

    def test_short_file_returns_none(self, temp_dir):
        """File with <10 chars stripped → None."""
        path = os.path.join(temp_dir, "short.docx")
        with open(path, "w", encoding="utf-8") as f:
            f.write("  hi  ")

        result = HybridDocumentConverter._try_read_as_text(path)
        assert result is None

    def test_non_utf8_returns_none(self, temp_dir):
        """Non-UTF-8 file → None."""
        path = os.path.join(temp_dir, "latin.docx")
        with open(path, "wb") as f:
            f.write(b"\xff\xfe" + "Héllo wörld".encode("utf-16-le"))

        result = HybridDocumentConverter._try_read_as_text(path)
        assert result is None

    def test_nonexistent_file_returns_none(self):
        result = HybridDocumentConverter._try_read_as_text("/nonexistent/file.docx")
        assert result is None
