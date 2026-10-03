"""Direct text extraction from DOCX files using python-docx."""

from __future__ import annotations

import asyncio
import os
from typing import TYPE_CHECKING, Optional

from airweave.core.logging import logger
from airweave.domains.converters._base import ConversionResult
from airweave.domains.sync_pipeline.exceptions import SyncFailureError

if TYPE_CHECKING:
    from docx.document import Document as DocumentObject
    from docx.table import Table
    from docx.text.paragraph import Paragraph

MIN_TOTAL_CHARS = 50

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


def _format_table(table: Table) -> str:
    rows: list[str] = []
    for row in table.rows:
        cells = [cell.text.strip() for cell in row.cells]
        rows.append("| " + " | ".join(cells) + " |")

    if len(rows) > 1:
        col_count = len(table.rows[0].cells)
        separator = "| " + " | ".join(["---"] * col_count) + " |"
        rows.insert(1, separator)

    return "\n".join(rows)


async def extract_docx_text(path: str) -> Optional[str]:
    """Return local text for consumers that do not own extraction coverage."""
    return (await extract_docx(path)).text


async def extract_docx(path: str) -> ConversionResult:
    """Preserve text and disclose related content that this extractor cannot interpret."""
    try:
        from docx import Document
    except ImportError:
        raise SyncFailureError("python-docx required for DOCX text extraction but not installed")

    def _extract() -> ConversionResult:
        name = os.path.basename(path)

        try:
            doc = Document(path)
        except Exception as exc:
            logger.warning(f"Failed to open DOCX {name}: {exc}")
            return ConversionResult(text=None)

        parts: list[str] = []

        for para in doc.paragraphs:
            line = _format_paragraph(para)
            if line:
                parts.append(line)

        for table in doc.tables:
            md_table = _format_table(table)
            if md_table:
                parts.append(md_table)

        markdown = "\n\n".join(parts)

        total_chars = len(markdown.strip())
        if total_chars < MIN_TOTAL_CHARS:
            logger.debug(f"DOCX {name}: only {total_chars} chars extracted, insufficient")
            markdown = None

        logger.debug(f"DOCX {name}: extracted {total_chars} chars")
        return ConversionResult(
            text=markdown,
            gap="embedded_content_unprocessed" if _has_embedded_content(doc) else None,
        )

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
