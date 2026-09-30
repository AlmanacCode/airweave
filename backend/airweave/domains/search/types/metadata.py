"""Collection metadata types for the search module.

EntityTypeMetadata, SourceMetadata, CollectionMetadata.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class EntityTypeMetadata(BaseModel):
    """Entity type metadata with fields."""

    name: str = Field(..., description="Entity type name (e.g., 'NotionPageEntity').")
    count: int | None = Field(
        ..., description="Known projection entity count, or null when not measured."
    )
    fields: dict[str, str] = Field(..., description="Field names mapped to their descriptions.")

    def to_md(self) -> str:
        """Convert entity type metadata to compact markdown format.

        Shows entity type name, count, and field names as a comma-separated list.
        Field descriptions are omitted to save tokens — the field names are
        self-explanatory enough for the LLM to craft queries and filters.
        """
        field_names = ", ".join(sorted(self.fields.keys()))
        count = self.count if self.count is not None else "indexed count unknown"
        return f"- **{self.name}** ({count}) — fields: {field_names}"


class SourceMetadata(BaseModel):
    """Source schema."""

    short_name: str = Field(..., description="Short name of the source.")
    description: str = Field(..., description="Description of the source.")
    entity_types: list[EntityTypeMetadata] = Field(
        ..., description="Entity types with their fields and counts."
    )
    captured_record_counts: dict[str, int] | None = Field(
        default=None,
        description="Visible captured records by native record type; not index counts.",
    )
    federated: bool = Field(
        default=False,
        description="Whether this source uses federated (real-time) search instead of syncing.",
    )

    def to_md(self) -> str:
        """Convert source metadata to compact markdown format.

        Skips entity types with 0 entities to reduce prompt size.
        Federated sources get a distinct rendering since they have no entity counts.
        """
        if self.federated:
            return (
                f"### {self.short_name} (federated — searched in real-time)\n"
                f"{self.description}\n\n"
                f"*This source is searched automatically via keyword query. "
                f"Filters work normally. No entity counts available.*"
            )

        if self.captured_record_counts is not None:
            counts = ", ".join(
                f"{kind}: {count}" for kind, count in sorted(self.captured_record_counts.items())
            )
            lines = [
                f"### {self.short_name}",
                self.description,
                f"Captured records ({sum(self.captured_record_counts.values())} total): {counts}.",
                "These are captured records; search indexing may lag or be incomplete.",
                "Searchable projection schemas (counts are not measured):",
                *(entity.to_md() for entity in self.entity_types),
            ]
            return "\n".join(lines)

        # Only show known nonempty or unmeasured projection entity types.
        non_empty = [et for et in self.entity_types if et.count is None or et.count > 0]
        if not non_empty:
            return f"### {self.short_name}\n{self.description}\n\n*(no entities)*"

        total = (
            sum(et.count for et in non_empty)
            if all(et.count is not None for et in non_empty)
            else "unknown"
        )
        lines = [
            f"### {self.short_name} ({total} entities total)",
            f"{self.description}",
            "",
        ]

        for entity_type in non_empty:
            lines.append(entity_type.to_md())

        return "\n".join(lines)


class CollectionMetadata(BaseModel):
    """Collection metadata schema."""

    collection_id: str = Field(..., description="The collection ID.")
    collection_readable_id: str = Field(..., description="The collection readable ID.")
    sources: list[SourceMetadata] = Field(..., description="Sources of the collection.")

    def to_md(self) -> str:
        """Convert collection metadata to markdown format for LLM context."""
        lines = [
            f"**Collection:** `{self.collection_readable_id}` (ID: `{self.collection_id}`)",
            "",
        ]

        for source in sorted(self.sources, key=lambda s: s.short_name):
            lines.append(source.to_md())
            lines.append("")

        return "\n".join(lines)
