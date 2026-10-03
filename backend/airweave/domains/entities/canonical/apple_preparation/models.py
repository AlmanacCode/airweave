"""Bounded offline Apple body decoding; this contract never authorizes source access."""

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

DECODER_VERSION = "apple-notes-protobuf-text-v1"
SCHEMA_COMMIT = "4754a2b62686570cca46690d101079e80cf6ae66"
SCHEMA_SHA256 = "0f5e608fa5f547ea770d29125146614f7ba3e09e4dc8207fa1c9c80102d75778"


class PreparationModel(BaseModel):
    """Strict immutable process and projection values."""

    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)


class PreparationLimits(PreparationModel):
    """Caller chooses budgets; none is a production scheduling default."""

    maximum_input_bytes: int = Field(strict=True, gt=0)
    maximum_decompressed_bytes: int = Field(strict=True, gt=0)
    maximum_text_bytes: int = Field(strict=True, gt=0)
    cpu_seconds: int = Field(strict=True, gt=0)
    wall_seconds: float = Field(gt=0)
    memory_bytes: int = Field(strict=True, gt=0)

    @property
    def response_bytes(self) -> int:
        """JSON may escape each text byte; reserve bounded metadata/error space."""
        return self.maximum_text_bytes * 6 + 8192


class BodyDecodeRequest(PreparationModel):
    """Internal process message. Original bytes are base64 encoded without modification."""

    format: Literal["notes_gzip_protobuf", "messages_typedstream"]
    original_base64: str
    limits: PreparationLimits


class PreparedBody(PreparationModel):
    """Readable plain text plus omissions and explicit resource/identity facts."""

    text: str
    fidelity: Literal["plain_text_only"] = "plain_text_only"
    omissions: tuple[Literal["rich_formatting", "embedded_content"], ...]
    original_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    cpu_limit_enforced: bool
    memory_limit_enforced: bool
    # Resource limits and a sanitized environment do not provide an OS sandbox.
    os_sandboxed: Literal[False] = False


class PreparedNotesBody(PreparedBody):
    """Notes text decoded through the licensed pinned protobuf schema."""

    format: Literal["notes_gzip_protobuf"] = "notes_gzip_protobuf"
    decoder_version: Literal["apple-notes-protobuf-text-v1"] = DECODER_VERSION
    schema_commit: Literal["4754a2b62686570cca46690d101079e80cf6ae66"] = SCHEMA_COMMIT


class PreparedMessageBody(PreparedBody):
    """Exact attributed backing string; styles/embedded data are not flattened."""

    format: Literal["messages_typedstream"] = "messages_typedstream"
    decoder_version: Literal["pytypedstream-0.1.0-backing-string-v1"] = (
        "pytypedstream-0.1.0-backing-string-v1"
    )


AppleFailureCode = Literal[
    "locked_content",
    "input_limit",
    "decompressed_limit",
    "text_limit",
    "invalid_gzip",
    "invalid_protobuf",
    "invalid_typedstream",
    "unsupported_typedstream_root",
    "parser_unavailable",
    "unsupported_parser_version",
    "invalid_request",
    "resource_limit_unavailable",
    "worker_failed",
    "worker_timeout",
    "worker_output_limit",
    "invalid_worker_response",
]


class WorkerReply(PreparationModel):
    """Either one bounded result or one safe error code; no original content in errors."""

    result: (
        Annotated[PreparedNotesBody | PreparedMessageBody, Field(discriminator="format")] | None
    ) = None
    error: AppleFailureCode | None = None

    @model_validator(mode="after")
    def exactly_one_outcome(self) -> "WorkerReply":
        """Reject ambiguous success/error protocol messages."""
        if (self.result is None) == (self.error is None):
            raise ValueError("Worker reply requires exactly one outcome")
        return self


class ApplePreparationError(ValueError):
    """Stable failure code for pending/incomplete projection handling."""

    def __init__(self, code: AppleFailureCode):
        """Retain only a safe stable code, never parser/source details."""
        self.code = code
        super().__init__(code)
