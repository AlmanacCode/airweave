"""Prepared source parts retain exact offsets while making short boundary phrases searchable."""

from types import SimpleNamespace

import pytest
import tiktoken
from chonkie.types import Chunk

from airweave.domains.entities.canonical.content_models import ContentProvenance, MatchedPart
from airweave.platform.chunkers.semantic import SemanticChunker
from airweave.platform.chunkers.unicode_tokens import UnicodeTokenChunker


def chunker_for(parts: list[list[str]]) -> SemanticChunker:
    encoding = tiktoken.get_encoding("cl100k_base")

    def semantic_batch(texts):
        result = []
        for text, pieces in zip(texts, parts, strict=True):
            assert "".join(pieces) == text
            chunks, start = [], 0
            for piece in pieces:
                end = start + len(piece)
                chunks.append(Chunk(text=piece, start_index=start, end_index=end, token_count=1))
                start = end
            result.append(chunks)
        return result

    chunker = object.__new__(SemanticChunker)
    chunker._semantic_chunker = SimpleNamespace(chunk_batch=semantic_batch)
    chunker._tiktoken_tokenizer = encoding
    chunker._token_chunker = UnicodeTokenChunker(
        encoding, chunker.MAX_TOKENS_PER_CHUNK - chunker.OVERLAP_TOKENS
    )
    return chunker


def assert_faithful(text, chunks):
    encoding = tiktoken.get_encoding("cl100k_base")
    end, recovered = 0, ""
    provenance = ContentProvenance(
        part=MatchedPart(part_index=0, key="transcript", kind="body", title="Transcript"),
        content_start=0,
        content_end=len(text),
    )
    for chunk in chunks:
        start, right, value = chunk["start_index"], chunk["end_index"], chunk["text"]
        assert 0 <= start <= end <= right
        assert text[start:right] == value
        assert chunk["token_count"] == len(encoding.encode(value, allowed_special="all"))
        assert chunk["token_count"] <= SemanticChunker.MAX_TOKENS_PER_CHUNK
        provenance.chunk_preview(text, value, start, right)
        recovered += value[end - start :]
        end = right
    assert recovered == text


@pytest.mark.asyncio
async def test_short_boundary_phrase_covered_without_crossing_parts():
    pieces = ["Intro. " * 160 + "meet at the ", "violet bridge tomorrow. " * 150]
    other = "A separate provider summary."
    assert not any("meet at the violet bridge" in value for value in pieces)
    first, second = await chunker_for([pieces, [other]]).chunk_batch(["".join(pieces), other])
    assert any("meet at the violet bridge" in chunk["text"] for chunk in first)
    assert second[0]["text"] == other
    assert_faithful("".join(pieces), first)
    assert_faithful(other, second)


@pytest.mark.asyncio
async def test_oversized_multilingual_parts_keep_overlap_unicode_and_hard_budget():
    text = "हिन्दी बैठक 👩🏽‍💻 کل کی گفتگو café e\u0301 <|endoftext|>\n" * 550
    chunks = (await chunker_for([[text]]).chunk_batch([text]))[0]
    assert len(chunks) > 2
    assert all(b["start_index"] < a["end_index"] for a, b in zip(chunks, chunks[1:], strict=False))
    assert_faithful(text, chunks)


def test_backward_upstream_spans_fail_instead_of_inventing_context():
    chunker = chunker_for([["abcdef"]])
    with pytest.raises(ValueError, match="exact prepared-text offsets"):
        chunker._add_overlap(
            ["abcdef"],
            [[
                {"text": "abcd", "start_index": 0, "end_index": 4},
                {"text": "cdef", "start_index": 2, "end_index": 6},
            ]],
        )
