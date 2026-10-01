# Optional local OCR

The shared OCR provider can use PyMuPDF's bundled Tesseract runtime with explicitly
provisioned language models. It uses the existing conversion interface and does
not introduce a service, database, or Python dependency. Original bytes are
unchanged; recognized text is a derived representation, not an exact transcription.

## Configuration

The backend image includes checksum-pinned `tessdata_fast` English and Hindi
models and their Apache 2.0 license under `/usr/share/tessdata`. Enable the adapter:

```sh
LOCAL_OCR_TESSDATA_PATH=/usr/share/tessdata
LOCAL_OCR_LANGUAGES='["eng","hin"]'
```

Without the path, local OCR stays disabled. Missing configured models fail startup.
Inference does not download models. Other languages require provisioning their
model files and explicitly adding their language codes.

The configured chain is local OCR, then Mistral, then Docling. Only unresolved
files reach the next configured provider; successful files are not processed
again. A configured cloud fallback can incur its normal charges. Local OCR uses
CPU and memory, so zero external inference calls does not mean zero hosting cost.

## Bounds and failure behavior

Each file runs sequentially in a disposable process, killed and reaped on timeout
or cancellation. Limits: 120 seconds, 200 MiB input, 200 pages, 20 million rendered
pixels per page at 150 DPI, and 8 MiB UTF-8 output. PDF, PNG, JPEG, BMP, WebP and
single-frame TIFF are supported. Encrypted PDFs and multiframe images are refused.
An over-limit or failed file returns no text to the fallback chain; it does not
publish a truncated prefix. These are processing bounds, not a full OS memory
sandbox. Native parser memory use still depends on the file.

Full-page OCR is intentional: the installed PyMuPDF partial-OCR path can omit
vector-drawn content. This may recognize native text less faithfully than direct
extraction. Original downloads remain the reference for exact reading.

## Qualification

`make test-ocr` checks fallback behavior, configuration and subprocess boundaries.
Set `LOCAL_OCR_TEST_TESSDATA` to a model directory to include real extraction tests.
The image CI runs `python -m scripts.smoke_local_ocr --tessdata /usr/share/tessdata`.

Local evaluation recovered a retained three-page PDF that previously failed
conversion, without changing its original hash. A synthetic English/Hindi image
recovered the English identifier and Hindi characters but dropped a space between
Hindi words. This demonstrates execution and a real quality limitation; it is not
a multilingual accuracy benchmark or proof of complete provider indexing.
