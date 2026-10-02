"""Generation-owned extraction evidence, separate from captured original completeness."""

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, computed_field, model_validator


class CharsetRecovery(BaseModel):
    """A disclosed strict UTF-8 recovery after the attempted charset rejected bytes."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    source_path: str = Field(min_length=1, max_length=2048)
    from_charset: str = Field(min_length=1, max_length=256)
    to_charset: Literal["utf-8"] = "utf-8"


class ExtractionPart(BaseModel):
    """Stable source-local identity and format; no private content or download URLs."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    part_index: int = Field(ge=0)
    key: str = Field(min_length=1, max_length=2048)
    kind: Literal["body", "file", "record"]
    media_type: str | None = Field(default=None, max_length=256)
    extension: str | None = Field(default=None, max_length=32)
    charset_recoveries: tuple[CharsetRecovery, ...] = ()


class ExtractionOutcome(ExtractionPart):
    """Indexed means every required chunk for this part was successfully published."""

    outcome: Literal["indexed", "unsupported", "unavailable_original", "failed"]
    reason: Literal["unsupported_format", "original_not_captured", "conversion_failed"] | None = (
        None
    )

    @model_validator(mode="after")
    def reason_matches(self):
        """Reject contradictory or free-text explanations."""
        expected = {
            "indexed": None,
            "unsupported": "unsupported_format",
            "unavailable_original": "original_not_captured",
            "failed": "conversion_failed",
        }
        if self.reason != expected[self.outcome]:
            raise ValueError("Extraction outcome requires its exact bounded reason")
        return self


class ExtractionCoverage(BaseModel):
    """Complete accounting of expected parts in one immutable publication generation."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    parts: tuple[ExtractionOutcome, ...]

    @model_validator(mode="before")
    @classmethod
    def validate_serialized_status(cls, value: Any) -> Any:
        """Accept wire status only as a checked attestation of the underlying parts."""
        if not isinstance(value, dict) or "status" not in value:
            return value
        facts = {key: item for key, item in value.items() if key != "status"}
        coverage = cls.model_validate(facts)
        if value["status"] != coverage.status:
            raise ValueError("Serialized extraction status conflicts with its parts")
        return facts

    @model_validator(mode="after")
    def unique_parts(self):
        """Each stable expected part appears once, with no ordinal gaps."""
        if len({p.key for p in self.parts}) != len(self.parts):
            raise ValueError("Extraction part keys must be unique")
        if tuple(p.part_index for p in self.parts) != tuple(range(len(self.parts))):
            raise ValueError("Extraction must account for every expected part exactly once")
        return self

    @computed_field
    @property
    def status(self) -> Literal["complete", "partial", "unavailable", "excluded"]:
        """Empty coverage is reserved for intentional exclusions/tombstones."""
        if not self.parts:
            return "excluded"
        indexed = sum(p.outcome == "indexed" for p in self.parts)
        return "complete" if indexed == len(self.parts) else "partial" if indexed else "unavailable"

    def persisted(self) -> dict:
        """Computed status remains queryable but validates as derived, not trusted input."""
        return self.model_dump(mode="json", exclude={"status"})
