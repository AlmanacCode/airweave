"""Library-based decoding only; never an acquisition API or lossy regex fallback."""

import base64
import binascii
import hashlib
import zlib

from google.protobuf.message import DecodeError

# Also loaded by the isolated worker directly from this trusted package directory.
if __package__:
    from .models import ApplePreparationError, BodyDecodeRequest, PreparedNotesBody
    from .notestore_pb2 import NoteStoreProto
else:
    from models import ApplePreparationError, BodyDecodeRequest, PreparedNotesBody
    from notestore_pb2 import NoteStoreProto


def decode_notes(
    request: BodyDecodeRequest, *, cpu_enforced: bool, memory_enforced: bool
) -> PreparedNotesBody:
    """Decode only a complete gzip member and initialized NoteStoreProto."""
    try:
        original = base64.b64decode(request.original_base64, validate=True)
    except (ValueError, binascii.Error) as error:
        raise ApplePreparationError("invalid_request") from error
    limits = request.limits
    if not original or len(original) > limits.maximum_input_bytes:
        raise ApplePreparationError("input_limit")
    unpack = zlib.decompressobj(zlib.MAX_WBITS + 16)
    try:
        decoded = unpack.decompress(original, limits.maximum_decompressed_bytes + 1)
    except zlib.error as error:
        raise ApplePreparationError("invalid_gzip") from error
    if len(decoded) > limits.maximum_decompressed_bytes or unpack.unconsumed_tail:
        raise ApplePreparationError("decompressed_limit")
    # No concatenated members, trailing garbage, or successful partial decompression.
    if not unpack.eof or unpack.unused_data:
        raise ApplePreparationError("invalid_gzip")
    note = NoteStoreProto()
    try:
        note.ParseFromString(decoded)
    except DecodeError as error:
        raise ApplePreparationError("invalid_protobuf") from error
    if (
        not note.IsInitialized()
        or not note.HasField("document")
        or not note.document.HasField("note")
        or not note.document.note.HasField("note_text")
    ):
        raise ApplePreparationError("invalid_protobuf")
    text = note.document.note.note_text
    if len(text.encode("utf-8")) > limits.maximum_text_bytes:
        raise ApplePreparationError("text_limit")
    return PreparedNotesBody(
        text=text,
        omissions=("rich_formatting", "embedded_content"),
        original_sha256=hashlib.sha256(original).hexdigest(),
        cpu_limit_enforced=cpu_enforced,
        memory_limit_enforced=memory_enforced,
    )
