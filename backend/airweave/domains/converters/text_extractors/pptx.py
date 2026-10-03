"""Direct text extraction from PPTX files using python-pptx."""

from __future__ import annotations

import asyncio
import os
from typing import TYPE_CHECKING, Optional, cast

from airweave.core.logging import logger
from airweave.domains.converters._base import ConversionResult
from airweave.domains.sync_pipeline.exceptions import SyncFailureError

if TYPE_CHECKING:
    from pptx.shapes.base import BaseShape
    from pptx.slide import Slide

MIN_TOTAL_CHARS = 50


def _extract_shape_text(shape: BaseShape) -> list[str]:
    from pptx.shapes.graphfrm import GraphicFrame
    from pptx.shapes.group import GroupShape

    if isinstance(shape, GroupShape):
        return [line for child in shape.shapes for line in _extract_shape_text(child)]
    lines: list[str] = []

    if shape.has_text_frame:
        for paragraph in shape.text_frame.paragraphs:
            text = paragraph.text.strip()
            if text:
                lines.append(text)

    if shape.has_table:
        for row in cast(GraphicFrame, shape).table.rows:
            cells = [cell.text.strip() for cell in row.cells]
            lines.append("| " + " | ".join(cells) + " |")

    return lines


def _extract_slide(slide: Slide, slide_idx: int) -> str:
    parts: list[str] = [f"## Slide {slide_idx}"]

    for shape in slide.shapes:
        parts.extend(_extract_shape_text(shape))

    if slide.has_notes_slide and slide.notes_slide.notes_text_frame:
        notes_text = slide.notes_slide.notes_text_frame.text.strip()
        if notes_text:
            parts.append(f"\n> **Notes:** {notes_text}")

    return "\n\n".join(parts)


async def extract_pptx_text(path: str) -> Optional[str]:
    """Keep the text-only consumer contract used by oversized-PPTX OCR fallback."""
    return (await extract_pptx(path)).text


async def extract_pptx(path: str) -> ConversionResult:
    """Extract local text with conservative disclosure of uninterpreted related content."""
    try:
        from pptx import Presentation
    except ImportError:
        raise SyncFailureError("python-pptx required for PPTX text extraction but not installed")

    def _extract() -> ConversionResult:
        name = os.path.basename(path)

        try:
            prs = Presentation(path)
        except Exception as exc:
            logger.warning(f"Failed to open PPTX {name}: {exc}")
            return ConversionResult(text=None)

        slide_markdowns = [
            _extract_slide(slide, idx) for idx, slide in enumerate(prs.slides, start=1)
        ]
        markdown = "\n\n---\n\n".join(slide_markdowns)

        total_chars = len(markdown.strip())
        if total_chars < MIN_TOTAL_CHARS:
            logger.debug(f"PPTX {name}: only {total_chars} chars extracted, insufficient")
            markdown = None

        logger.debug(f"PPTX {name}: extracted {total_chars} chars")
        return ConversionResult(
            text=markdown,
            gap=(
                "embedded_content_unprocessed"
                if any(_has_embedded_content(slide) for slide in prs.slides)
                else None
            ),
        )

    return await asyncio.to_thread(_extract)


def _has_embedded_content(slide: Slide) -> bool:
    """Inspect used slide/story relationships, never follow external media links."""
    from pptx.opc.constants import RELATIONSHIP_TYPE as RT

    unprocessed = {
        RT.IMAGE,
        RT.CHART,
        RT.DIAGRAM_DATA,
        RT.OLE_OBJECT,
        RT.AUDIO,
        RT.VIDEO,
        RT.MEDIA,
    }
    parts = {slide.part, slide.slide_layout.part, slide.slide_layout.slide_master.part}
    if slide.has_notes_slide:
        parts.add(slide.notes_slide.part)
        parts.add(slide.notes_slide.part.notes_master.part)
    return any(
        relationship.reltype in unprocessed for part in parts for relationship in part.rels.values()
    )
