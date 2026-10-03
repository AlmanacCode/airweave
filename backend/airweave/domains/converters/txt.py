"""Text file to markdown converter."""

import asyncio
import codecs
import json
import os
import xml.dom.minidom
from typing import Dict, List

import aiofiles

from airweave.core.logging import logger
from airweave.domains.converters._base import BaseTextConverter, ConversionResult
from airweave.domains.sync_pipeline.async_helpers import run_in_thread_pool
from airweave.domains.sync_pipeline.exceptions import EntityProcessingError


class TxtConverter(BaseTextConverter):
    """Converts text files (TXT, JSON, XML, MD, YAML, TOML) to markdown."""

    async def convert_batch(self, file_paths: List[str]) -> Dict[str, ConversionResult]:
        """Convert text files to markdown."""
        logger.debug(f"Converting {len(file_paths)} text files to markdown...")

        results = {}
        semaphore = asyncio.Semaphore(20)

        async def _convert_one(path: str):
            async with semaphore:
                try:
                    _, ext = os.path.splitext(path)
                    ext = ext.lower()

                    if ext == ".json":
                        text = await self._convert_json(path)
                    elif ext == ".xml":
                        text = await self._convert_xml(path)
                    else:
                        text = await self._convert_plain_text(path)

                    if text and text.strip():
                        results[path] = text.strip()
                        logger.debug(f"Converted text file: {path} ({len(text)} chars)")
                    else:
                        logger.warning(f"Text file conversion produced no content: {path}")
                        results[path] = None

                except Exception as e:
                    logger.error(f"Text file conversion failed for {path}: {e}")
                    results[path] = None

        await asyncio.gather(*[_convert_one(p) for p in file_paths], return_exceptions=True)

        successful = sum(1 for r in results.values() if r)
        logger.debug(f"Text conversion complete: {successful}/{len(file_paths)} successful")

        return {key: ConversionResult(text=value) for key, value in results.items()}

    @staticmethod
    def _try_chardet_decode(raw_bytes: bytes, path: str) -> str | None:
        try:
            import chardet

            detection = chardet.detect(raw_bytes[:100000])
            if not detection or detection.get("confidence", 0) <= 0.7:
                return None
            detected_encoding = detection["encoding"]
            if not detected_encoding:
                return None
            text = raw_bytes.decode(detected_encoding)
            if text.count("\ufffd") == 0:
                logger.debug(f"Detected encoding {detected_encoding} for {os.path.basename(path)}")
                return text
        except (UnicodeDecodeError, LookupError):
            pass
        except ImportError:
            logger.debug("chardet not available; unknown text encoding cannot be converted")
        return None

    async def _convert_plain_text(self, path: str) -> str:
        async with aiofiles.open(path, "rb") as f:
            raw_bytes = await f.read()

        if not raw_bytes:
            return ""

        # A BOM is authoritative; malformed bytes must not fall through to guessing.
        for bom, encoding in (
            (codecs.BOM_UTF32_LE, "utf-32"),
            (codecs.BOM_UTF32_BE, "utf-32"),
            (codecs.BOM_UTF16_LE, "utf-16"),
            (codecs.BOM_UTF16_BE, "utf-16"),
            (codecs.BOM_UTF8, "utf-8-sig"),
        ):
            if raw_bytes.startswith(bom):
                return raw_bytes.decode(encoding, errors="strict")
        try:
            return raw_bytes.decode("utf-8", errors="strict")
        except UnicodeDecodeError:
            detected = self._try_chardet_decode(raw_bytes, path)
            if detected is not None:
                return detected
            raise EntityProcessingError(
                "Text encoding could not be decoded without data loss"
            ) from None

    async def _convert_json(self, path: str) -> str:
        def _read_and_format():
            with open(path, "rb") as f:
                raw_bytes = f.read()

            # The stdlib honors JSON UTF-8/16/32 and BOMs without replacement decoding.
            data = json.loads(raw_bytes)
            formatted = json.dumps(data, indent=2, ensure_ascii=False)
            return f"```json\n{formatted}\n```"

        try:
            return await run_in_thread_pool(_read_and_format)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in {path}: {e}")
            raise EntityProcessingError(f"Invalid JSON syntax in {path}")

    async def _convert_xml(self, path: str) -> str:
        def _read_and_format():
            with open(path, "rb") as f:
                raw_bytes = f.read()

            # Parse bytes so XML's declared encoding remains authoritative.
            dom = xml.dom.minidom.parseString(raw_bytes)
            formatted = dom.toprettyxml()
            return f"```xml\n{formatted}\n```"

        try:
            return await run_in_thread_pool(_read_and_format)
        except Exception as error:
            raise EntityProcessingError("XML could not be parsed without data loss") from error
