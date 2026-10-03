"""Typedstream text through an unmodified installed LGPL library, never NSUnarchiver."""

import base64
import binascii
import hashlib
from importlib.metadata import PackageNotFoundError, version

import typedstream
from typedstream.archiving import GenericArchivedObject, TypedValue
from typedstream.stream import InvalidTypedStreamError
from typedstream.types.foundation import NSString

if __package__:
    from .models import ApplePreparationError, BodyDecodeRequest, PreparedMessageBody
else:
    from models import ApplePreparationError, BodyDecodeRequest, PreparedMessageBody


def decode_message(
    request: BodyDecodeRequest, *, cpu_enforced: bool, memory_enforced: bool
) -> PreparedMessageBody:
    """Accept only the exact attributed root/backing string, never string-search a graph.

    Unknown archive classes are inert Python archive representations produced by
    the library. They are not resolved to Objective-C classes or executed.
    """
    try:
        installed_version = version("pytypedstream")
    except PackageNotFoundError as error:
        raise ApplePreparationError("parser_unavailable") from error
    if installed_version != "0.1.0":
        raise ApplePreparationError("unsupported_parser_version")
    try:
        original = base64.b64decode(request.original_base64, validate=True)
    except (ValueError, binascii.Error) as error:
        raise ApplePreparationError("invalid_request") from error
    if not original or len(original) > request.limits.maximum_input_bytes:
        raise ApplePreparationError("input_limit")
    try:
        root = typedstream.unarchive_from_data(original)
    except (
        InvalidTypedStreamError,
        ValueError,
        EOFError,
        TypeError,
        IndexError,
        OverflowError,
        RecursionError,
    ) as error:
        raise ApplePreparationError("invalid_typedstream") from error
    if (
        not isinstance(root, GenericArchivedObject)
        or root.clazz.name not in (b"NSAttributedString", b"NSMutableAttributedString")
        or root.clazz.version != 0
        or not root.contents
    ):
        raise ApplePreparationError("unsupported_typedstream_root")
    backing = root.contents[0]
    if (
        not isinstance(backing, TypedValue)
        or backing.encoding != b"@"
        or not isinstance(backing.value, NSString)
    ):
        raise ApplePreparationError("unsupported_typedstream_root")
    text = backing.value.value
    if len(text.encode("utf-8")) > request.limits.maximum_text_bytes:
        raise ApplePreparationError("text_limit")
    return PreparedMessageBody(
        text=text,
        omissions=("rich_formatting", "embedded_content"),
        original_sha256=hashlib.sha256(original).hexdigest(),
        cpu_limit_enforced=cpu_enforced,
        memory_limit_enforced=memory_enforced,
    )
