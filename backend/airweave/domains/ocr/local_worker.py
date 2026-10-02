"""Isolated bounded PyMuPDF/Tesseract process; no application imports or downloads."""

import argparse
import math
from pathlib import Path

DPI = 150
# Match storage/limits.py without importing the application in this isolated worker.
MAX_INPUT_BYTES = 200 * 1024 * 1024
MAX_PAGES = 200
MAX_PAGE_PIXELS = 20_000_000
MAX_OUTPUT_BYTES = 8 * 1024 * 1024
SUPPORTED_EXTENSIONS = frozenset(
    {".pdf", ".png", ".jpg", ".jpeg", ".bmp", ".webp", ".tif", ".tiff"}
)


def _check_page(width: float, height: float) -> None:
    """Reject excessive render surfaces before constructing any page pixmap."""
    if not all(math.isfinite(value) and value > 0 for value in (width, height)):
        raise ValueError("Invalid page dimensions")
    pixels = math.ceil(width * DPI / 72) * math.ceil(height * DPI / 72)
    if pixels > MAX_PAGE_PIXELS:
        raise ValueError("Page render limit exceeded")


def _require_single_frame(path: Path) -> None:
    """PyMuPDF image opening must not silently omit later container frames."""
    from PIL import Image

    with Image.open(path) as image:
        try:
            image.seek(1)
        except EOFError:
            return
    raise ValueError("Multi-frame images are not supported")


def extract(path: Path, tessdata: Path, languages: str) -> bytes:
    """Preserve native text and OCR images; fail rather than publish a truncated result.

    Locked PyMuPDF 1.26.7 appends image OCR after native text, without restoring
    reading order or recognizing illegible/vector text. Originals remain authoritative.
    """
    import pymupdf

    if path.suffix.lower() not in SUPPORTED_EXTENSIONS:
        raise ValueError("Unsupported input format")
    if path.stat().st_size > MAX_INPUT_BYTES:
        raise ValueError("Input size limit exceeded")
    with pymupdf.open(path) as document:
        if document.needs_pass or not 1 <= document.page_count <= MAX_PAGES:
            raise ValueError("Unsupported document bounds or encryption")
        if not document.is_pdf:
            _require_single_frame(path)
        for page in document:
            _check_page(page.rect.width, page.rect.height)
        output = bytearray()
        for page in document:
            textpage = page.get_textpage_ocr(
                language=languages, dpi=DPI, full=False, tessdata=str(tessdata)
            )
            text = page.get_text("text", textpage=textpage).strip().encode("utf-8")
            separator = b"\n\n" if output and text else b""
            if len(output) + len(separator) + len(text) > MAX_OUTPUT_BYTES:
                raise ValueError("OCR output limit exceeded")
            output.extend(separator)
            output.extend(text)
        if not output:
            raise ValueError("OCR produced no text")
        return bytes(output)


def main() -> int:
    """Only a successfully completed extraction may create the private result file."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--tessdata", required=True, type=Path)
    parser.add_argument("--languages", required=True)
    args = parser.parse_args()
    try:
        content = extract(args.input, args.tessdata, args.languages)
        with args.output.open("xb") as destination:
            destination.write(content)
        return 0
    except Exception:
        # Provider/native exception messages may contain document text or private paths.
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
