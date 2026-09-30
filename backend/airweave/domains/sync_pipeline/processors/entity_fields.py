"""Normalize declared entity display fields before either indexing pipeline."""

from airweave.core.shared_models import AirweaveFieldFlag
from airweave.platform.entities._base import BaseEntity


def populate_base_fields(entity: BaseEntity) -> None:
    """Copy explicitly flagged source fields without serializing native capture JSON."""
    flags = {
        "entity_id": AirweaveFieldFlag.IS_ENTITY_ID,
        "name": AirweaveFieldFlag.IS_NAME,
        "created_at": AirweaveFieldFlag.IS_CREATED_AT,
        "updated_at": AirweaveFieldFlag.IS_UPDATED_AT,
    }
    for target, flag in flags.items():
        if getattr(entity, target):
            continue
        for name, field in type(entity).model_fields.items():
            metadata = field.json_schema_extra
            if isinstance(metadata, dict) and metadata.get(flag.value):
                value = getattr(entity, name)
                if value is not None:
                    setattr(
                        entity, target, str(value) if target in ("entity_id", "name") else value
                    )
                break
