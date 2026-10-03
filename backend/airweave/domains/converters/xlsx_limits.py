"""Supported XLSX content bounds, shared with preparation provenance."""

from pydantic import Field

from airweave.domains.converters.package_limits import PackageTextLimits


class XlsxLimits(PackageTextLimits):
    """Retain XLSX-specific rectangle bounds alongside shared package/text limits."""

    maximum_rows: int = Field(default=50_000, gt=0)
    maximum_columns: int = Field(default=1024, gt=0)
    maximum_cells: int = Field(default=250_000, gt=0)
