"""Charset recovery never guesses, replaces invalid bytes or overrides valid declarations."""

import pytest

from airweave.domains.entities.canonical.mime_text import decode_mime_text


def test_valid_declared_charset_wins_even_when_bytes_also_form_valid_utf8():
    # UTF-8 for café is also valid Latin-1: the explicit declaration remains authoritative.
    raw = "café".encode("utf-8")
    result = decode_mime_text(raw, "iso-8859-1", "/body")
    assert result.text.encode("iso-8859-1") == raw
    assert result.recovery is None


def test_invalid_declared_charset_recovers_exact_utf8_and_records_evidence():
    raw = "नमस्ते — café".encode("utf-8")
    result = decode_mime_text(raw, "us-ascii", "/parts/1")
    assert result.text.encode("utf-8") == raw
    assert result.recovery.model_dump() == {
        "source_path": "/parts/1",
        "from_charset": "us-ascii",
        "to_charset": "utf-8",
    }


@pytest.mark.parametrize(
    "raw,charset,error",
    [(b"\xff", "utf-8", UnicodeDecodeError), (b"valid", "unknown-charset", LookupError)],
)
def test_invalid_or_unknown_encoding_still_fails(raw, charset, error):
    with pytest.raises(error):
        decode_mime_text(raw, charset, "/body")
