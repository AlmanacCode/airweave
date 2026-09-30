"""Offline entity timestamp parsing retained after canonical source replacement."""

from airweave.platform.entities.linear import _parse_dt


def test_parse_dt_valid():
    """_parse_dt parses ISO8601 timestamps."""
    dt = _parse_dt("2024-06-15T12:30:00Z")
    assert dt is not None
    assert dt.year == 2024
    assert dt.month == 6
    assert dt.day == 15


def test_parse_dt_none():
    """_parse_dt returns None for None/empty input."""
    assert _parse_dt(None) is None
    assert _parse_dt("") is None


def test_parse_dt_invalid():
    """_parse_dt returns None for invalid strings."""
    assert _parse_dt("not-a-date") is None
