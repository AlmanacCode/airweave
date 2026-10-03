"""Supported XLSX content bounds, shared with preparation provenance."""

from pydantic import BaseModel, ConfigDict, Field


class XlsxLimits(BaseModel):
    """Content/work bounds; these do not promise a hard process memory limit."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    maximum_package_members: int = Field(default=1024, gt=0)
    maximum_expanded_bytes: int = Field(default=32 * 1024 * 1024, gt=0)
    maximum_member_bytes: int = Field(default=16 * 1024 * 1024, gt=0)
    maximum_rows: int = Field(default=50_000, gt=0)
    maximum_columns: int = Field(default=1024, gt=0)
    maximum_cells: int = Field(default=250_000, gt=0)
    maximum_output_bytes: int = Field(default=8 * 1024 * 1024, gt=0)
