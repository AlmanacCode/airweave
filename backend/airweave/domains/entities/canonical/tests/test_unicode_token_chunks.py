"""Real tokenizer boundaries must preserve multilingual originals and offsets."""

import pytest
import tiktoken

from airweave.platform.chunkers.unicode_tokens import UnicodeTokenChunker


@pytest.mark.parametrize(
    "text",
    [
        "हिन्दी बैठक 👩🏽‍💻 की बातचीत। " * 30,
        "中文 العربية café e\u0301\n" * 40,
        "<|endoftext|> repeated text � " * 30,
    ],
)
def test_token_fallback_preserves_exact_unicode_text_and_budget(text):
    encoding = tiktoken.get_encoding("cl100k_base")
    chunks = UnicodeTokenChunker(encoding, 16).chunk(text)
    assert len(chunks) > 1
    assert "".join(chunk.text for chunk in chunks) == text
    end = 0
    for chunk in chunks:
        assert chunk.start_index == end
        end = chunk.end_index
        assert text[chunk.start_index : end] == chunk.text
        assert chunk.token_count == len(encoding.encode(chunk.text, allowed_special="all"))
        assert 0 < chunk.token_count <= 16
    assert end == len(text)


def test_impossible_character_budget_fails_instead_of_replacing_text():
    chunker = UnicodeTokenChunker(tiktoken.get_encoding("cl100k_base"), 1)
    with pytest.raises(ValueError, match="complete Unicode character"):
        chunker.chunk("👩")
