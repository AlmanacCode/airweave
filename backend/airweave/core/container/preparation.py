"""Compose provenance from the same concrete inputs used to construct preparation."""

from airweave.domains.converters.xlsx_limits import XlsxLimits
from airweave.domains.embedders.types import DenseEmbedderEntry, SparseEmbedderEntry
from airweave.domains.entities.canonical.preparation_recipe import (
    ChunkingRecipe,
    CodeRecipe,
    EmbeddingRecipe,
    ExtractionRecipe,
    ModelIdentity,
    OcrPolicy,
    PreparationRecipe,
    SemanticRecipe,
)
from airweave.platform.chunkers.code import CodeChunker
from airweave.platform.chunkers.semantic import SemanticChunker


def preparation_recipe(
    *,
    artifact_sha256: str | None,
    converter_extensions: tuple[str, ...],
    configured_ocr: tuple[OcrPolicy, ...],
    xlsx_limits: XlsxLimits | None = None,
    dense: DenseEmbedderEntry,
    sparse: SparseEmbedderEntry,
    dimensions: int,
) -> PreparationRecipe:
    """No model loading, provider requests, private URLs, or credential inspection."""
    semantic = SemanticChunker
    code = CodeChunker
    return PreparationRecipe(
        artifact_sha256=artifact_sha256,
        extraction=ExtractionRecipe(
            converter_extensions=converter_extensions,
            configured_ocr=configured_ocr,
            xlsx_limits=xlsx_limits,
        ),
        chunking=ChunkingRecipe(
            semantic=SemanticRecipe(
                tokenizer=semantic.TOKENIZER,
                boundary_model=ModelIdentity(identifier=semantic.EMBEDDING_MODEL),
                target_tokens=semantic.SEMANTIC_CHUNK_SIZE,
                maximum_tokens=semantic.MAX_TOKENS_PER_CHUNK,
                overlap_tokens=semantic.OVERLAP_TOKENS,
                similarity_threshold=semantic.SIMILARITY_THRESHOLD,
                similarity_window=semantic.SIMILARITY_WINDOW,
                minimum_sentences=semantic.MIN_SENTENCES_PER_CHUNK,
                minimum_characters=semantic.MIN_CHARACTERS_PER_SENTENCE,
                skip_window=semantic.SKIP_WINDOW,
                filter_window=semantic.FILTER_WINDOW,
                filter_polyorder=semantic.FILTER_POLYORDER,
                filter_tolerance=semantic.FILTER_TOLERANCE,
                sentence_delimiters=tuple(semantic.SENTENCE_DELIMITERS),
                include_delimiter=semantic.INCLUDE_DELIMITER,
            ),
            code=CodeRecipe(
                tokenizer=code.TOKENIZER,
                target_tokens=code.CHUNK_SIZE,
                maximum_tokens=code.MAX_TOKENS_PER_CHUNK,
            ),
        ),
        embedding=EmbeddingRecipe(
            dense_provider=dense.provider,
            dense_name=dense.short_name,
            dense_model=dense.api_model_name,
            dimensions=dimensions,
            sparse_provider=sparse.provider,
            sparse_name=sparse.short_name,
            sparse_model=sparse.api_model_name,
        ),
    )
