"""Token-budget fallback that never decodes a partial UTF-8 character."""

from chonkie.types import Chunk
from tiktoken import Encoding


class UnicodeTokenChunker:
    """Preserve exact character offsets while splitting at complete token boundaries."""

    def __init__(self, encoding: Encoding, chunk_size: int):
        """Use the existing embedding tokenizer and its hard per-chunk limit."""
        if chunk_size < 1:
            raise ValueError("Token chunk size must be positive")
        self.encoding = encoding
        self.chunk_size = chunk_size

    def chunk_batch(self, texts: list[str]) -> list[list[Chunk]]:
        """Keep document order and independent character-offset origins."""
        return [self.chunk(text) for text in texts]

    def chunk(self, text: str) -> list[Chunk]:
        """Back up token cuts to a UTF-8 boundary, then verify the isolated token count."""
        tokens = self.encoding.encode(text, allowed_special="all")
        pieces = [self.encoding.decode_single_token_bytes(token) for token in tokens]
        raw = b"".join(pieces)
        if raw.decode("utf-8") != text:
            raise ValueError("Tokenizer changed the original text")
        boundaries = [0]
        for piece in pieces:
            boundaries.append(boundaries[-1] + len(piece))
        chunks: list[Chunk] = []
        start_token, start_char = 0, 0
        while start_token < len(tokens):
            end_token = min(start_token + self.chunk_size, len(tokens))
            while end_token > start_token:
                end_byte = boundaries[end_token]
                if end_byte == len(raw) or raw[end_byte] & 0xC0 != 0x80:
                    value = raw[boundaries[start_token] : end_byte].decode("utf-8")
                    count = len(self.encoding.encode(value, allowed_special="all"))
                    if count <= self.chunk_size:
                        break
                end_token -= 1
            else:
                raise ValueError("Token budget cannot hold one complete Unicode character")
            end_char = start_char + len(value)
            chunks.append(
                Chunk(text=value, start_index=start_char, end_index=end_char, token_count=count)
            )
            start_token, start_char = end_token, end_char
        return chunks
