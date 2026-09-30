"""Build immutable text bytes once, then publish alongside the index generation."""

import hashlib
from uuid import UUID

from airweave.domains.entities.canonical.projection_models import ProjectionLocator
from airweave.domains.entities.canonical.text_models import TextArtifact, representation_id
from airweave.domains.storage.limits import MAX_FILE_SIZE_BYTES
from airweave.domains.sync_pipeline.pipeline.text_models import BuiltText


def prepare_text(
    built: tuple[BuiltText, ...],
    generation: UUID,
) -> tuple[tuple[TextArtifact, bytes], ...]:
    """Keep complete converter output; oversized text fails rather than truncating."""
    artifacts = []
    for item in built:
        locator = ProjectionLocator.parse(item.entity_id)
        if locator is None or locator.generation != generation:
            raise ValueError("Built text lost its publication identity")
        content = item.text.encode("utf-8")
        if len(content) > MAX_FILE_SIZE_BYTES:
            raise ValueError("Derived text exceeds retained representation size limit")
        artifact = TextArtifact(
            id=representation_id(generation, locator.part_index),
            part_index=locator.part_index,
            sha256=hashlib.sha256(content).hexdigest(),
            size_bytes=len(content),
            characters=len(item.text),
            content_start=item.content_start,
        )
        artifacts.append((artifact, content))
    return tuple(artifacts)


def text_manifest(
    artifacts: tuple[TextArtifact, ...] | None,
    indexed_parts: set[int],
    sync_id: UUID,
    generation: UUID,
) -> list[dict] | None:
    """Validate exact part membership and generation-owned keys before committing descriptors."""
    if artifacts is None:
        return None
    if len({item.id for item in artifacts}) != len(artifacts):
        raise ValueError("Text representations must be unique")
    if {item.part_index for item in artifacts} != indexed_parts:
        raise ValueError("Text representations must cover exactly indexed parts")
    for item in artifacts:
        item.storage_key(sync_id, generation)
    return [item.model_dump(mode="json") for item in artifacts]
