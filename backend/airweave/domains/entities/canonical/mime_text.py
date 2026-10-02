"""Strict MIME text decoding with explicit, bounded recovery evidence."""

from pydantic import BaseModel, ConfigDict

from airweave.domains.entities.canonical.extraction_models import CharsetRecovery


class DecodedText(BaseModel):
    """Derived text and any charset correction; original bytes remain authoritative."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    text: str
    recovery: CharsetRecovery | None = None


def decode_mime_text(content: bytes, charset: str, source_path: str) -> DecodedText:
    """Honor the charset first; recover only bytes that are strictly valid UTF-8.

    Unknown codecs and content invalid in both encodings still fail. No detector,
    replacement characters or discarded bytes can turn a failure into success.
    """
    try:
        return DecodedText(text=content.decode(charset, errors="strict"))
    except UnicodeDecodeError:
        text = content.decode("utf-8", errors="strict")
        return DecodedText(
            text=text,
            recovery=CharsetRecovery(source_path=source_path, from_charset=charset),
        )
