"""Unit tests for HtmlConverter encoding validation."""

import os
import tempfile

import pytest

from airweave.domains.converters.html import HtmlConverter, html_to_text


@pytest.mark.parametrize("prefix", ["<br><span></span>", "x <br><span>हाँ</span>"])
def test_visitor_offsets_preserve_hidden_languages_padding_and_original(prefix, tmp_path):
    """Empty output and an offset inside a UTF-8 character must not panic."""
    language = "Meaningful preview हाँ اردو क\u034fि"
    padding = "&#847;&nbsp;" * 5
    original = (
        prefix
        + f'<div style="display:none">{padding}{language}</div>'
        + '<div hidden="">Accessible context اَلْعَرَبِيَّة</div>'
    ).encode("utf-8")
    path = tmp_path / "offset.html"
    path.write_bytes(original)

    text = html_to_text(path.read_text(encoding="utf-8"))

    assert language in text
    assert "Accessible context اَلْعَرَبِيَّة" in text
    assert text.count("\u034f") == 1  # Preserve the Hindi combining character, not empty padding.
    if "हाँ" in prefix:
        assert "x" in text and text.count("हाँ") == 2
    assert path.read_bytes() == original


@pytest.fixture
def converter():
    return HtmlConverter()


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


class TestHtmlConverterEncodingValidation:
    @pytest.mark.asyncio
    async def test_hidden_preheader_padding_preserves_meaning_languages_and_original(
        self, converter, tmp_path
    ):
        path = tmp_path / "mail.html"
        language = "हाँ उर्दू اَلْعَرَبِيَّة اردو क\u034fि"
        padding = "&#847;&nbsp;" * 150
        original = (
            '<html><head><meta name="viewport" content="width=device-width"></head><body>'
            f'<div style="DISPLAY : none !important; font-size:1px">{padding}'
            f"Meaningful preview {language}</div>"
            f'<div hidden="">{padding}<span>Accessible context {language}</span></div>'
            f"<p>Visible body {language}</p></body></html>"
        ).encode("utf-8")
        path.write_bytes(original)

        text = (await converter.convert_batch([str(path)]))[str(path)].text

        assert text is not None
        assert "Meaningful preview " + language in text
        assert "Accessible context " + language in text
        assert "Visible body " + language in text
        assert text.count("\u034f") == 3  # Only the joiner attached to each Hindi word remains.
        assert "viewport" not in text
        assert path.read_bytes() == original

    @pytest.mark.asyncio
    async def test_visible_ambiguous_css_accessibility_and_code_joiners_remain(
        self, converter, tmp_path
    ):
        path = tmp_path / "joiners.html"
        padding = "\u034f \u034f \u034f"
        original = (
            f"<p>Visible {padding} literal</p>"
            f'<div aria-hidden="true">ARIA {padding} literal</div>'
            f'<div class="hidden">Class {padding} literal</div>'
            f"<div hidden>Bare hidden {padding} literal</div>"
            f'<div style="display:none;display:block">Override {padding} literal</div>'
            f'<div style="/*display:none*/">Comment {padding} literal</div>'
            f'<div hidden=""><pre>{padding}</pre></div>'
            f'<div style="display:none"><code>{padding}</code></div>'
            '<div hidden="">क\u034f\u034fि अَ\u034f\u034fر <span>single \u034f joiner</span></div>'
        ).encode("utf-8")
        path.write_bytes(original)

        text = (await converter.convert_batch([str(path)]))[str(path)].text

        assert text is not None
        assert text.count("\u034f") == 29
        assert "क\u034f\u034fि अَ\u034f\u034fر" in text
        assert "single \u034f joiner" in text
        assert f"```\n{padding}\n```" in text
        assert f"`{padding}`" in text
        assert path.read_bytes() == original

    @pytest.mark.asyncio
    async def test_metadata_is_not_body_but_body_literals_and_languages_survive(
        self, converter, tmp_path
    ):
        path = tmp_path / "meeting.html"
        original = (
            "<html><head><title>Mail export</title>"
            '<meta name="description" content="Tracking boilerplate"></head>'
            "<body><h1>Meeting</h1><p>हाँ مرحبا</p>"
            "<pre>meta-description: useful body</pre></body></html>"
        ).encode("utf-8")
        path.write_bytes(original)

        results = await converter.convert_batch([str(path)])

        assert results[str(path)].text == (
            "# Meeting\n\nहाँ مرحبا\n\n```\nmeta-description: useful body\n```"
        )
        assert path.read_bytes() == original

    @pytest.mark.asyncio
    async def test_convert_clean_html(self, converter, temp_dir):
        file_path = os.path.join(temp_dir, "clean.html")
        html = """<!DOCTYPE html>
<html>
<head><title>Test Page</title></head>
<body>
    <h1>Hello World</h1>
    <p>This is a test paragraph.</p>
</body>
</html>
"""
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(html)

        result = await converter.convert_batch([file_path])

        assert file_path in result
        assert result[file_path].text is not None
        assert "Hello World" in result[file_path].text
        assert "test paragraph" in result[file_path].text

    @pytest.mark.asyncio
    @pytest.mark.parametrize("content", ["", "  ", "<html><body></body></html>"])
    async def test_convert_empty_html(self, converter, temp_dir, content):
        file_path = os.path.join(temp_dir, "empty.html")
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)

        result = await converter.convert_batch([file_path])

        assert file_path in result
        assert result[file_path].text == ""

    @pytest.mark.asyncio
    async def test_convert_batch_multiple_html_files(self, converter, temp_dir):
        html1_path = os.path.join(temp_dir, "page1.html")
        with open(html1_path, "w", encoding="utf-8") as f:
            f.write("<html><body><p>Page 1</p></body></html>")

        html2_path = os.path.join(temp_dir, "page2.html")
        with open(html2_path, "w", encoding="utf-8") as f:
            f.write("<html><body><p>Page 2</p></body></html>")

        result = await converter.convert_batch([html1_path, html2_path])

        assert html1_path in result
        assert html2_path in result

    @pytest.mark.asyncio
    async def test_convert_nonexistent_file(self, converter):
        result = await converter.convert_batch(["/nonexistent/page.html"])
        assert "/nonexistent/page.html" in result
        assert result["/nonexistent/page.html"].text is None

    @pytest.mark.asyncio
    async def test_html_with_excessive_binary_returns_none(self, converter, temp_dir):
        """HTML with >100 replacement chars from bad UTF-8 → None."""
        file_path = os.path.join(temp_dir, "binary.html")
        # Valid-ish HTML prefix + lots of invalid bytes
        content = b"<html><body>" + b"\x80" * 200 + b"</body></html>"
        with open(file_path, "wb") as f:
            f.write(content)

        result = await converter.convert_batch([file_path])
        assert file_path in result
        assert result[file_path].text is None
