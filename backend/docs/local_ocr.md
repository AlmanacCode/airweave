# Local OCR preserves native PDF text

The local worker uses PyMuPDF `get_textpage_ocr(full=False)`: native PDF text is
retained and images can contribute OCR text. The locked runtime is PyMuPDF 1.26.7.
Its partial OCR covers images; vector or illegible native text is not independently
recovered. Recognized image text follows native text, so this derived representation
does not restore original reading order. Retained originals remain authoritative.

Explicitly provision language files through the existing `LOCAL_OCR_TESSDATA_PATH`
and `LOCAL_OCR_LANGUAGES` settings. There are no runtime downloads. The qualification
used official [tessdata_fast](https://github.com/tesseract-ocr/tessdata_fast) English
and Hindi models at commit `87416418657359cb625c412a48b6e1d6d41c29bd`, under
Apache 2.0; exact sources, hashes and sizes are recorded in
[the qualification evidence](../tests/live/evidence/local-ocr-native-preservation-20261002.json).
The task cache is an operator fixture, not a production deployment.

One actual retained fourteen-page PDF preserved all 1,491 native token occurrences
and all fourteen native page strings with partial OCR. Full OCR had preserved
1,203 tokens and one page string. Partial OCR took 3.174 seconds versus 9.664 seconds;
image-only content was not independently judged. A synthetic native-text-plus-image
regression checks actual native text and recognized image text with explicitly
configured real models. Run it with `LOCAL_OCR_TEST_TESSDATA` set to their directory.

The known bilingual image preserved English and Hindi letters, but omitted Hindi
word spaces. Exact spaced Hindi queries can therefore miss recognized image text.
These observations do not establish accuracy for arbitrary layouts or languages.
See the official [PyMuPDF partial OCR contract](https://pymupdf.readthedocs.io/en/latest/page.html#Page.get_textpage_ocr).

Existing input, page, render, output and 120-second per-file limits remain in force;
cancellation kills and reaps the isolated worker. Failed OCR returns unavailable
conversion rather than a truncated successful document. Configuring OCR also makes
JPEG/PNG inputs selectable by the converter registry; broader image policy and
reprojection require separate qualification. The current sixteen pending Gmail
records contain only HTML, PDF and an unsupported GIF, so that bounded retry adds
no newly selectable JPEG/PNG parts. This change alone does not fix parent-body
coupling when another required part cannot be converted.
