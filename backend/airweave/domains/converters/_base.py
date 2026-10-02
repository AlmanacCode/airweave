"""Base converter interfaces for text converters."""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from typing import Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict

from airweave.core.logging import logger
from airweave.domains.ocr.protocols import OcrProvider
from airweave.domains.sync_pipeline.exceptions import EntityProcessingError, SyncFailureError


class ConversionResult(BaseModel):
    """Known extracted content and a bounded gap; None without a gap is failure."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    text: str | None
    gap: Literal["ocr_unavailable"] | None = None


class BaseTextConverter(ABC):
    """Base class for all text converters."""

    @abstractmethod
    async def convert_batch(self, file_paths: List[str]) -> Dict[str, ConversionResult]:
        """Batch convert files to markdown text.

        Args:
            file_paths: List of file paths to convert

        Returns:
            Mapping from file path to extracted content and any known gap.
            None text without a gap means conversion failed.
        """
        pass


class HybridDocumentConverter(BaseTextConverter):
    """Converter that tries cheap local text extraction before falling back to OCR.

    Subclasses implement :meth:`_try_extract` for format-specific extraction.
    The shared :meth:`convert_batch` handles the extract-first / OCR-fallback
    orchestration so each format only needs to provide the extraction logic.

    Usage::

        class DocxConverter(HybridDocumentConverter):
            async def _try_extract(self, path: str) -> Optional[str]:
                return await extract_docx_text(path)


        converter = DocxConverter(ocr_provider=MistralOCR())
    """

    def __init__(self, ocr_provider: Optional[OcrProvider] = None) -> None:
        self._ocr_provider = ocr_provider

    @abstractmethod
    async def _try_extract(self, path: str) -> Optional[str]:
        """Attempt local text extraction for a single file.

        Returns:
            Extracted markdown if successful, or ``None`` if OCR is needed.
        """

    async def _extract_local(self, path: str) -> ConversionResult:
        return ConversionResult(text=await self._try_extract(path))

    @staticmethod
    def _try_read_as_text(path: str, max_probe_bytes: int = 8192) -> Optional[str]:
        """Check if a file is actually plain text despite its extension."""
        try:
            with open(path, "rb") as f:
                probe = f.read(max_probe_bytes)

            if not probe:
                return None

            try:
                probe.decode("utf-8")
            except UnicodeDecodeError:
                return None

            control_count = sum(1 for b in probe if b < 32 and b not in (9, 10, 13))
            if control_count / len(probe) > 0.05:
                return None

            with open(path, "r", encoding="utf-8") as f:
                content = f.read()

            if len(content.strip()) < 10:
                return None

            return content

        except Exception:
            return None

    async def _extract_with_fallback(self, path: str) -> ConversionResult:
        """A text-disguised-as-binary fallback does not hide infrastructure errors."""
        try:
            local = await self._extract_local(path)
            if local.text is not None or local.gap is not None:
                return local
        except SyncFailureError:
            raise
        except Exception as exc:
            logger.warning(f"{os.path.basename(path)}: extraction error ({exc}), needs OCR")
        return ConversionResult(text=self._try_read_as_text(path))

    async def convert_batch(self, file_paths: List[str]) -> Dict[str, ConversionResult]:
        """Convert files to markdown, trying extraction first.

        For each file, calls :meth:`_try_extract`. If that returns content,
        uses it directly (0 API calls). Otherwise, batches the file for OCR.
        """
        results: Dict[str, ConversionResult] = {}
        needs_ocr: Dict[str, ConversionResult] = {}

        for path in file_paths:
            local = await self._extract_with_fallback(path)
            if (local.text is not None and local.gap is None) or (
                local.gap is not None and self._ocr_provider is None
            ):
                results[path] = local
            else:
                needs_ocr[path] = local

        if needs_ocr:
            if self._ocr_provider is None:
                logger.warning(f"No OCR converter configured, {len(needs_ocr)} files will fail")
                for path in needs_ocr:
                    results[path] = ConversionResult(text=None)
            else:
                try:
                    ocr_results = await self._ocr_provider.convert_batch(list(needs_ocr))
                except EntityProcessingError:
                    # A recoverable OCR failure cannot discard locally recovered text.
                    # Infrastructure errors and cancellation still propagate.
                    ocr_results = {}
                for path, local in needs_ocr.items():
                    text = ocr_results.get(path)
                    results[path] = (
                        ConversionResult(text=text)
                        if text
                        else local
                        if local.gap is not None
                        else ConversionResult(text=text)
                    )

        return results


class OcrConverterAdapter(BaseTextConverter):
    """Adapts an OcrProvider to the BaseTextConverter interface."""

    def __init__(self, ocr: OcrProvider) -> None:
        self._ocr = ocr

    async def convert_batch(self, file_paths: List[str]) -> Dict[str, ConversionResult]:
        results = await self._ocr.convert_batch(file_paths)
        return {path: ConversionResult(text=value) for path, value in results.items()}
