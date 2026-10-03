"""Runtime provenance, independent of publication version and original identity."""

import hashlib
import json
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class RecipeFact(BaseModel):
    """Explicit composition facts; unknown values are never inferred from current code."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class ModelIdentity(RecipeFact):
    """Configured model identifier or local language artifact, without private paths."""

    identifier: str
    sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    resolution: Literal["digest", "mutable_alias", "unknown"] = "unknown"


class LocalOcrLimits(RecipeFact):
    """Bounded render policy actually used by the isolated local worker."""

    dpi: int
    maximum_pages: int
    maximum_page_pixels: int
    maximum_input_bytes: int
    maximum_output_bytes: int
    full_page: Literal[False] = False


class OcrPolicy(RecipeFact):
    """Configured fallback step, never evidence that this provider handled a part."""

    provider: str
    models: tuple[ModelIdentity, ...] = ()
    languages: tuple[str, ...] = ()
    policy: str
    local_limits: LocalOcrLimits | None = None


class ExtractionRecipe(RecipeFact):
    """Build identity covers converter implementation; capability reflects actual wiring."""

    policy: str = "canonical-extraction-v1"
    converter_extensions: tuple[str, ...]
    configured_ocr: tuple[OcrPolicy, ...]
    actual_ocr_outcome: Literal["unknown"] = "unknown"


class SemanticRecipe(RecipeFact):
    """Parameters actually supplied to the semantic chunker and its safety net."""

    tokenizer: str
    boundary_model: ModelIdentity
    target_tokens: int
    maximum_tokens: int
    overlap_tokens: int
    similarity_threshold: float
    similarity_window: int
    minimum_sentences: int
    minimum_characters: int
    skip_window: int
    filter_window: int
    filter_polyorder: int
    filter_tolerance: float
    sentence_delimiters: tuple[str, ...]
    include_delimiter: str


class CodeRecipe(RecipeFact):
    """AST chunker configuration; model/library artifacts remain covered by the build."""

    tokenizer: str
    target_tokens: int
    maximum_tokens: int
    language: Literal["auto"] = "auto"
    include_nodes: Literal[False] = False


class ChunkingRecipe(RecipeFact):
    """Chunk boundary identity is distinct from extraction and final search embedding."""

    policy: str = "canonical-chunks-v1"
    semantic: SemanticRecipe
    code: CodeRecipe


class EmbeddingRecipe(RecipeFact):
    """Record the existing deployment registry selection without replacing its authority."""

    dense_provider: str
    dense_name: str
    dense_model: str
    dimensions: int = Field(gt=0)
    sparse_provider: str
    sparse_name: str
    sparse_model: str
    model_revision: None = None
    purpose: Literal["document"] = "document"


class PreparationRecipe(RecipeFact):
    """An attempt's immutable facts; defaults explicitly represent unknown provenance."""

    schema_version: Literal[1] = 1
    artifact_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    extraction: ExtractionRecipe | None = None
    chunking: ChunkingRecipe | None = None
    embedding: EmbeddingRecipe | None = None

    def component_digest(self, component: Literal["extraction", "chunking", "embedding"]) -> str:
        """Keep phase identities separate; a reranker is not a preparation input."""
        values = self.model_dump(mode="json")
        encoded = json.dumps(
            {"artifact_sha256": self.artifact_sha256, component: values[component]},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()
