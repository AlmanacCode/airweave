"""Direct text extraction from DOCX files using python-docx."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Iterator
from typing import TYPE_CHECKING, Optional

from airweave.core.logging import logger
from airweave.domains.converters._base import ConversionResult
from airweave.domains.converters.package_limits import (
    PackageTextLimits,
    PreparationLimit,
    bounded_join,
    check_package,
)
from airweave.domains.sync_pipeline.exceptions import SyncFailureError

if TYPE_CHECKING:
    from docx.document import Document as DocumentObject
    from docx.table import Table
    from docx.text.paragraph import Paragraph

_HEADING_MAP = (
    ("heading 1", "# "),
    ("heading 2", "## "),
    ("heading 3", "### "),
    ("heading", "#### "),
)


def _format_paragraph(para: Paragraph) -> Optional[str]:
    text = para.text.strip()
    if not text:
        return None

    style_name = (para.style.name or "").lower() if para.style else ""

    for keyword, prefix in _HEADING_MAP:
        if keyword in style_name:
            return f"{prefix}{text}"

    if "list" in style_name:
        return f"- {text}"

    return text


def _format_table(table: Table, limits: PackageTextLimits) -> str | None:
    has_content = False

    def lines() -> Iterator[str]:
        nonlocal has_content
        for index, row in enumerate(table.rows):
            cells = [cell.text.strip() for cell in row.cells]
            has_content |= any(cells)
            yield "| " + " | ".join(cells) + " |"
            if index == 0 and len(table.rows) > 1:
                yield "| " + " | ".join(["---"] * len(cells)) + " |"

    rendered = bounded_join(lines(), "\n", limits)
    return rendered if has_content else None


def _document_parts(document: DocumentObject, limits: PackageTextLimits) -> Iterator[str]:
    from docx.text.paragraph import Paragraph

    for block in document.iter_inner_content():
        text = (
            _format_paragraph(block)
            if isinstance(block, Paragraph)
            else _format_table(block, limits)
        )
        if text:
            yield text


async def extract_docx_text(path: str, limits: PackageTextLimits | None = None) -> Optional[str]:
    """Return local text for consumers that do not own extraction coverage."""
    return (await extract_docx(path, limits)).text


async def extract_docx(path: str, limits: PackageTextLimits | None = None) -> ConversionResult:
    """Preserve text and disclose related content that this extractor cannot interpret."""
    try:
        from docx import Document
    except ImportError:
        raise SyncFailureError("python-docx required for DOCX text extraction but not installed")

    limits = limits or PackageTextLimits()

    def _extract() -> ConversionResult:
        name = os.path.basename(path)

        try:
            check_package(path, limits)
            doc = Document(path)
        except PreparationLimit:
            raise
        except Exception as exc:
            logger.warning(f"Failed to open DOCX {name}: {exc}")
            return ConversionResult(text=None)

        markdown = bounded_join(_document_parts(doc, limits), "\n\n", limits)
        gap = "embedded_content_unprocessed" if _has_embedded_content(doc) else None
        logger.debug(f"DOCX {name}: extracted {len(markdown)} chars")
        return ConversionResult(text=markdown or (None if gap else ""), gap=gap)

    return await asyncio.to_thread(_extract)


def _has_embedded_content(document: DocumentObject) -> bool:
    """Relationship evidence is conservative; it does not establish visibility or meaning."""
    from docx.opc.constants import CONTENT_TYPE as CT
    from docx.opc.constants import RELATIONSHIP_TYPE as RT

    stories = {
        CT.WML_DOCUMENT_MAIN,
        CT.WML_HEADER,
        CT.WML_FOOTER,
        CT.WML_FOOTNOTES,
        CT.WML_ENDNOTES,
        CT.WML_COMMENTS,
    }
    unprocessed = {RT.IMAGE, RT.CHART, RT.DIAGRAM_DATA, RT.OLE_OBJECT, RT.AUDIO, RT.VIDEO}
    return any(
        relationship.reltype in unprocessed
        for part in document.part.package.iter_parts()
        if part.content_type in stories
        for relationship in part.rels.values()
    )
