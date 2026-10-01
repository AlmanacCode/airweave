"""Qualify packaged OCR weights and the real isolated worker without external inference."""

import argparse
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory

import pymupdf

from airweave.domains.ocr.local import LocalOcrProvider


async def main(tessdata: str) -> None:
    """A raster image ensures this checks OCR rather than a PDF text-layer shortcut."""
    with TemporaryDirectory(prefix="ocr-smoke-") as directory:
        path = Path(directory) / "scan.png"
        with pymupdf.open() as document:
            page = document.new_page(width=420, height=160)
            page.insert_text((25, 80), "ALMANAC 7429", fontsize=30)
            page.get_pixmap(dpi=150).save(path)
        provider = LocalOcrProvider(tessdata_path=tessdata, languages=("eng", "hin"))
        results = await provider.convert_batch([str(path)])
        result = results[str(path)]
        if result is None or "ALMANAC" not in result or "7429" not in result:
            raise RuntimeError("Packaged local OCR did not recover the synthetic identifier")
    print("Packaged local OCR passed; no external inference calls")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tessdata", required=True)
    asyncio.run(main(parser.parse_args().tessdata))
