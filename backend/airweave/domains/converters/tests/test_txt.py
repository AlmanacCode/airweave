"""Unit tests for TxtConverter encoding validation."""

import os
import tempfile

import pytest

from airweave.domains.converters.txt import TxtConverter


@pytest.fixture
def converter():
    return TxtConverter()


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


class TestTxtConverterEncodingValidation:
    @pytest.mark.asyncio
    async def test_convert_clean_utf8_text(self, converter, temp_dir):
        file_path = os.path.join(temp_dir, "clean.txt")
        with open(file_path, "w", encoding="utf-8") as f:
            f.write("Hello world! This is clean UTF-8 text.")

        result = await converter.convert_batch([file_path])

        assert file_path in result
        assert result[file_path].text == "Hello world! This is clean UTF-8 text."

    @pytest.mark.asyncio
    async def test_convert_unicode_text(self, converter, temp_dir):
        file_path = os.path.join(temp_dir, "unicode.txt")
        with open(file_path, "w", encoding="utf-8") as f:
            f.write("Hello 世界 🌍 こんにちは")

        result = await converter.convert_batch([file_path])

        assert file_path in result
        assert result[file_path].text == "Hello 世界 🌍 こんにちは"

    @pytest.mark.asyncio
    async def test_convert_corrupted_text_file(self, converter, temp_dir):
        file_path = os.path.join(temp_dir, "corrupted.txt")
        with open(file_path, "wb") as f:
            for _ in range(10000):
                f.write(b"\xc0\x80")

        result = await converter.convert_batch([file_path])
        assert file_path in result

    @pytest.mark.asyncio
    async def test_convert_empty_file(self, converter, temp_dir):
        file_path = os.path.join(temp_dir, "empty.txt")
        with open(file_path, "w", encoding="utf-8") as f:
            f.write("")

        result = await converter.convert_batch([file_path])

        assert file_path in result
        assert result[file_path].text is None

    @pytest.mark.asyncio
    async def test_convert_json_clean(self, converter, temp_dir):
        file_path = os.path.join(temp_dir, "clean.json")
        with open(file_path, "w", encoding="utf-8") as f:
            f.write('{"name": "test", "value": 123}')

        result = await converter.convert_batch([file_path])

        assert file_path in result
        assert result[file_path].text is not None
        assert "name" in result[file_path].text

    @pytest.mark.asyncio
    async def test_convert_json_with_corruption(self, converter, temp_dir):
        file_path = os.path.join(temp_dir, "corrupted.json")
        with open(file_path, "w", encoding="utf-8") as f:
            f.write('{"name": invalid}')

        result = await converter.convert_batch([file_path])

        assert file_path in result
        assert result[file_path].text is None

    @pytest.mark.asyncio
    async def test_convert_xml_clean(self, converter, temp_dir):
        file_path = os.path.join(temp_dir, "clean.xml")
        with open(file_path, "w", encoding="utf-8") as f:
            f.write('<?xml version="1.0"?><root><item>test</item></root>')

        result = await converter.convert_batch([file_path])

        assert file_path in result
        assert result[file_path].text is not None
        assert "item" in result[file_path].text

    @pytest.mark.asyncio
    async def test_convert_batch_mixed_files(self, converter, temp_dir):
        clean_path = os.path.join(temp_dir, "clean.txt")
        with open(clean_path, "w", encoding="utf-8") as f:
            f.write("Clean text")

        empty_path = os.path.join(temp_dir, "empty.txt")
        with open(empty_path, "w", encoding="utf-8") as f:
            f.write("")

        result = await converter.convert_batch([clean_path, empty_path])

        assert result[clean_path].text == "Clean text"
        assert result[empty_path].text is None


class TestTxtConverterChardetFallback:
    """Tests for _try_chardet_decode and fallback encoding paths."""

    @pytest.mark.asyncio
    async def test_non_utf8_with_low_chardet_confidence(self, converter, temp_dir):
        """Low-confidence non-UTF-8 must not become replacement text."""
        file_path = os.path.join(temp_dir, "low_confidence.txt")
        # Random bytes that aren't valid in any encoding
        with open(file_path, "wb") as f:
            f.write(bytes(range(128, 256)) * 100)

        result = await converter.convert_batch([file_path])
        # The result reports failure when no strict decoding is available.
        assert file_path in result

    @pytest.mark.asyncio
    async def test_latin1_file_detected_by_chardet(self, converter, temp_dir):
        """A latin-1 encoded file should be detected and decoded correctly."""
        file_path = os.path.join(temp_dir, "latin1.txt")
        text = "Ça fait plaisir d'être ici, mère Noël"
        with open(file_path, "wb") as f:
            f.write(text.encode("latin-1"))

        result = await converter.convert_batch([file_path])
        assert file_path in result
        assert result[file_path].text == text

    @pytest.mark.asyncio
    async def test_chardet_decode_raises_unicode_error(self, converter, temp_dir):
        """A BOM is decoded strictly, never sent through a replacement fallback."""
        file_path = os.path.join(temp_dir, "bad_decode.txt")
        with open(file_path, "wb") as f:
            # Write bytes that look like a specific encoding to chardet
            # but are actually malformed
            f.write(b"\xfe\xff" + b"\x80\x81" * 500)

        result = await converter.convert_batch([file_path])
        assert file_path in result

    @pytest.mark.asyncio
    async def test_excessive_replacement_chars_raises_error(self, converter, temp_dir):
        """Undecodable text returns an explicit failed conversion."""
        file_path = os.path.join(temp_dir, "binary_garbage.txt")
        with open(file_path, "wb") as f:
            # Bytes that fail UTF-8 and chardet, producing many replacements
            f.write(b"\x80\x81\x82\x83" * 2000)

        result = await converter.convert_batch([file_path])
        assert file_path in result
        assert result[file_path].text is None


class TestTxtConverterJsonXmlReplacementLimits:
    """Structured text never repairs malformed bytes with replacement characters."""

    @pytest.mark.asyncio
    async def test_json_with_many_replacement_chars(self, converter, temp_dir):
        """Malformed JSON bytes fail regardless of their count."""
        file_path = os.path.join(temp_dir, "bad.json")
        # Valid JSON prefix but lots of invalid bytes
        content = b'{"key": "' + b"\x80" * 60 + b'"}'
        with open(file_path, "wb") as f:
            f.write(content)

        result = await converter.convert_batch([file_path])
        assert file_path in result
        assert result[file_path].text is None

    @pytest.mark.asyncio
    async def test_json_with_few_invalid_bytes_fails(self, converter, temp_dir):
        """Even one malformed sequence must not become a successful JSON conversion."""
        file_path = os.path.join(temp_dir, "ok.json")
        content = b'{"key": "value\x80\x81"}'
        with open(file_path, "wb") as f:
            f.write(content)

        result = await converter.convert_batch([file_path])
        assert file_path in result
        assert result[file_path].text is None

    @pytest.mark.asyncio
    async def test_xml_with_many_replacement_chars(self, converter, temp_dir):
        """Malformed XML bytes fail rather than being replaced."""
        file_path = os.path.join(temp_dir, "bad.xml")
        content = b'<?xml version="1.0"?><root>' + b"\x80" * 60 + b"</root>"
        with open(file_path, "wb") as f:
            f.write(content)

        result = await converter.convert_batch([file_path])
        assert file_path in result
        assert result[file_path].text is None

    @pytest.mark.asyncio
    async def test_xml_fallback_raw_with_excessive_binary(self, converter, temp_dir):
        """Invalid XML does not fall back to repaired raw text."""
        file_path = os.path.join(temp_dir, "malformed.xml")
        # Not valid XML at all, plus binary garbage
        content = b"<broken" + b"\x80" * 150
        with open(file_path, "wb") as f:
            f.write(content)

        result = await converter.convert_batch([file_path])
        assert file_path in result
        assert result[file_path].text is None


class TestTryChardetDecode:
    """Direct unit tests for _try_chardet_decode static method branches."""

    def test_returns_none_when_encoding_is_none(self):
        """Detection without an encoding cannot authorize decoding."""
        from unittest.mock import patch

        with patch("chardet.detect", return_value={"confidence": 0.9, "encoding": None}):
            result = TxtConverter._try_chardet_decode(b"some bytes", "/path/to/file.txt")
        assert result is None

    def test_returns_none_when_decode_raises_unicode_error(self):
        """A detected charset still must decode the complete input strictly."""
        from unittest.mock import patch

        with patch("chardet.detect", return_value={"confidence": 0.9, "encoding": "ascii"}):
            result = TxtConverter._try_chardet_decode(b"\x80\x81\x82", "/path/to/file.txt")
        assert result is None

    def test_returns_none_when_chardet_not_installed(self):
        """ImportError from chardet import → returns None (lines 75-77)."""
        import builtins
        from unittest.mock import patch

        real_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "chardet":
                raise ImportError("chardet not installed")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=mock_import):
            result = TxtConverter._try_chardet_decode(b"some bytes", "/path/to/file.txt")
        assert result is None


class TestTxtConverterEdgeCases:
    @pytest.mark.asyncio
    async def test_convert_nonexistent_file(self, converter):
        result = await converter.convert_batch(["/nonexistent/file.txt"])
        assert "/nonexistent/file.txt" in result
        assert result["/nonexistent/file.txt"].text is None

    @pytest.mark.asyncio
    async def test_convert_whitespace_only_file(self, converter, temp_dir):
        file_path = os.path.join(temp_dir, "whitespace.txt")
        with open(file_path, "w", encoding="utf-8") as f:
            f.write("   \n\n   \t\t   ")

        result = await converter.convert_batch([file_path])

        assert file_path in result
        assert result[file_path].text is None


@pytest.mark.parametrize("encoding", ["utf-8", "utf-8-sig", "utf-16", "utf-32"])
async def test_multilingual_text_and_bom_are_preserved(converter, tmp_path, encoding):
    text = "हिन्दी बैठक — اگلی ملاقات — English e\u0301 👩🏽‍💻 �"
    path = tmp_path / "source.txt"
    original = text.encode(encoding)
    path.write_bytes(original)
    converted = (await converter.convert_batch([str(path)]))[str(path)]
    assert converted.text == text
    assert path.read_bytes() == original


@pytest.mark.parametrize("encoding", ["utf-8", "utf-16", "utf-32"])
async def test_json_retains_readable_hindi_urdu_and_escaped_unicode(converter, tmp_path, encoding):
    import json

    data = {"हिन्दी": "अगली बैठक", "اردو": "اگلی ملاقات", "mixed": "English e\u0301 👩🏽‍💻"}
    path = tmp_path / "attachment.json"
    original = json.dumps(data, ensure_ascii=encoding == "utf-32").encode(encoding)
    path.write_bytes(original)
    converted = (await converter.convert_batch([str(path)]))[str(path)]
    assert "अगली बैठक" in converted.text and "اگلی ملاقات" in converted.text
    assert "\\u" not in converted.text
    assert json.loads(converted.text.removeprefix("```json\n").removesuffix("\n```")) == data
    assert path.read_bytes() == original


async def test_xml_honors_declared_legacy_charset(converter, tmp_path):
    path = tmp_path / "attachment.xml"
    original = '<?xml version="1.0" encoding="iso-8859-1"?><note>Ça plaît à Noël</note>'.encode(
        "latin-1"
    )
    path.write_bytes(original)
    converted = (await converter.convert_batch([str(path)]))[str(path)]
    assert "Ça plaît à Noël" in converted.text
    assert path.read_bytes() == original


@pytest.mark.parametrize(
    "extension,original",
    [
        ("txt", b"\xff\xfe\x00"),  # Authoritative UTF-16 BOM with a truncated character.
        ("json", b'{"note": "valid prefix\x80"}'),
        ("xml", b'<?xml version="1.0" encoding="utf-8"?><note>prefix\x80</note>'),
    ],
)
async def test_malformed_attachment_bytes_fail_without_replacement(
    converter, tmp_path, extension, original
):
    path = tmp_path / f"attachment.{extension}"
    path.write_bytes(original)
    converted = (await converter.convert_batch([str(path)]))[str(path)]
    assert converted.text is None and converted.gap is None
    assert path.read_bytes() == original
