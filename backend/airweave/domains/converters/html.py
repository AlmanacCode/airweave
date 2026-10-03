"""HTML to markdown converter."""

import asyncio
import re
from typing import Dict, List

from pydantic import BaseModel, ConfigDict, Field, JsonValue

from airweave.core.logging import logger
from airweave.domains.converters._base import BaseTextConverter, ConversionResult
from airweave.domains.sync_pipeline.async_helpers import run_in_thread_pool
from airweave.domains.sync_pipeline.exceptions import EntityProcessingError

_DISPLAY_DECLARATION = re.compile(r"(?:^|;)\s*display\s*:\s*([^;]+)", re.IGNORECASE)
_DISPLAY_NONE = re.compile(r"none\s*(?:!\s*important)?", re.IGNORECASE)
_PREHEADER_PADDING = re.compile(r"(?<!\S)\u034f(?:\s*\u034f)+(?!\S)")


class _HtmlNodeContext(BaseModel):
    """Only element attributes are needed from the library's visitor context."""

    model_config = ConfigDict(extra="ignore")
    attributes: dict[str, str] = Field(default_factory=dict)


class _PreheaderPaddingVisitor:
    """Remove empty mail padding while preserving hidden previews and language."""

    def visit_element_end(self, context: dict[str, JsonValue], output: str) -> dict[str, str]:
        """Clean standalone repeated joiners only with explicit hidden evidence."""
        if "\u034f" not in output or "`" in output:
            # Code may deliberately quote invisible characters. Preserve it unchanged.
            return {"type": "continue"}
        attributes = _HtmlNodeContext.model_validate(context).attributes
        style = attributes.get("style", "")
        declarations = _DISPLAY_DECLARATION.findall(style)
        hidden = "hidden" in attributes or (
            "/*" not in style
            and len(declarations) == 1
            and _DISPLAY_NONE.fullmatch(declarations[0].strip()) is not None
        )
        # This is narrow inline evidence, not a CSS cascade or visibility engine.
        # Ambiguous declarations, class styles and aria-hidden alone remain intact.
        # Library 2.24 omits valueless attributes, so bare `hidden` also stays intact.
        if hidden:
            cleaned = _PREHEADER_PADDING.sub("", output)
            if cleaned != output:
                return {"type": "custom", "output": cleaned}
        return {"type": "continue"}


class HtmlConverter(BaseTextConverter):
    """Converts HTML files to markdown text using html-to-markdown."""

    async def convert_batch(self, file_paths: List[str]) -> Dict[str, ConversionResult]:
        """Convert HTML files to markdown text."""
        try:
            from html_to_markdown import ConversionOptions, convert_with_visitor
        except ImportError:
            logger.error("html-to-markdown package not installed for HTML conversion")
            raise EntityProcessingError(
                "HTML conversion requires html-to-markdown package. "
                "Install with: pip install html-to-markdown"
            )

        logger.info(f"Converting {len(file_paths)} HTML files to markdown...")

        results = {}
        semaphore = asyncio.Semaphore(20)

        async def _convert_one(path: str):
            async with semaphore:
                try:

                    def _convert():
                        with open(path, "rb") as f:
                            raw_bytes = f.read()

                        if not raw_bytes:
                            return ""

                        try:
                            html_content = raw_bytes.decode("utf-8")
                        except UnicodeDecodeError:
                            html_content = raw_bytes.decode("utf-8", errors="replace")
                            replacement_count = html_content.count("\ufffd")
                            if replacement_count > 100:
                                raise EntityProcessingError(
                                    f"HTML contains excessive binary data "
                                    f"({replacement_count} replacement chars)"
                                )

                        if not html_content.strip():
                            return ""

                        # Keep generated head metadata out of body chunks/snippets.
                        # The retained HTML remains the authoritative original.
                        markdown = convert_with_visitor(
                            html_content,
                            ConversionOptions(extract_metadata=False),
                            visitor=_PreheaderPaddingVisitor(),
                        )

                        return markdown.strip() if markdown else ""

                    text = await run_in_thread_pool(_convert)

                    if text is not None:
                        results[path] = text
                        logger.debug(f"Converted HTML file: {path} ({len(text)} characters)")
                    else:
                        logger.warning(f"HTML conversion produced no content for {path}")
                        results[path] = None

                except Exception as e:
                    logger.error(f"HTML conversion failed for {path}: {e}")
                    results[path] = None

        await asyncio.gather(*[_convert_one(p) for p in file_paths], return_exceptions=True)

        successful = sum(1 for r in results.values() if r is not None)
        logger.info(f"HTML conversion complete: {successful}/{len(file_paths)} files successful")

        return {key: ConversionResult(text=value) for key, value in results.items()}
