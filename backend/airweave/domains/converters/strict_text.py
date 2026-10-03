"""Inert structured text: no parsing, charset guessing, or replacement characters."""

import aiofiles

from airweave.domains.converters._base import BaseTextConverter, ConversionResult


class StrictTextConverter(BaseTextConverter):
    """Read exact UTF-8 text; MIME-aware producers normalize declared charsets first."""

    async def convert_batch(self, file_paths: list[str]) -> dict[str, ConversionResult]:
        """Preserve lines and whitespace; malformed bytes are an explicit failure."""
        results = {}
        for path in file_paths:
            try:
                async with aiofiles.open(path, "rb") as source:
                    text = (await source.read()).decode("utf-8", errors="strict")
            except (OSError, UnicodeDecodeError):
                text = None
            results[path] = ConversionResult(text=text)
        return results
