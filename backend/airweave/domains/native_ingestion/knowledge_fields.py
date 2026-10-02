"""Searchable authored native fields, separate from body and reference resolution."""

from datetime import date
from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, JsonValue, StrictStr


class _Fields(BaseModel):
    model_config = ConfigDict(extra="ignore")


class _Link(_Fields):
    label: StrictStr
    url: StrictStr
    handle: StrictStr | None = None


class _Role(_Fields):
    title: StrictStr
    start: StrictStr | None = None
    end: StrictStr | None = None


class _Education(_Fields):
    degree: StrictStr | None = None
    subject: StrictStr | None = None
    start: StrictStr | None = None
    end: StrictStr | None = None


class _RelatedPerson(_Fields):
    relationship: StrictStr


class _Person(_Fields):
    related_people: tuple[_RelatedPerson, ...] = ()
    emails: tuple[StrictStr, ...] = ()
    phones: tuple[StrictStr, ...] = ()
    timezone: StrictStr | None = None
    roles: tuple[_Role, ...] = ()
    experience: tuple[_Role, ...] = ()
    education: tuple[_Education, ...] = ()
    links: tuple[_Link, ...] = ()


class _Organisation(_Fields):
    related_people: tuple[_RelatedPerson, ...] = ()
    domains: tuple[StrictStr, ...] = ()
    website: StrictStr | None = None
    industry: tuple[StrictStr, ...] = ()
    founded: StrictStr | None = None
    links: tuple[_Link, ...] = ()


class _Address(_Fields):
    street: StrictStr | None = None
    locality: StrictStr | None = None
    region: StrictStr | None = None
    postal_code: StrictStr | None = None
    country: StrictStr | None = None


class _Coordinates(_Fields):
    latitude: float = Field(ge=-90, le=90, allow_inf_nan=False)
    longitude: float = Field(ge=-180, le=180, allow_inf_nan=False)


class _Place(_Fields):
    coordinates: _Coordinates | None = None
    place_kind: StrictStr | None = None
    address: _Address | None = None


class _Timed(_Fields):
    kind: Literal["timed"]
    start: StrictStr
    end: StrictStr
    timezone: StrictStr


class _AllDay(_Fields):
    kind: Literal["all_day"]
    start_on: StrictStr
    end_on_exclusive: StrictStr


class _Event(_Fields):
    schedule: Annotated[_Timed | _AllDay, Field(discriminator="kind")] | None = None


class _CreativeWork(_Fields):
    work_kind: StrictStr | None = None
    published_on: StrictStr | None = None


class _Work(_Fields):
    state: Literal["open", "in_progress", "waiting", "done", "cancelled"]


class _Task(_Work):
    priority: Literal["low", "normal", "high", "urgent"] | None = None
    due_on: date | None = None
    due_at: AwareDatetime | None = None
    available_on: date | None = None


class _Project(_Work):
    target_on: date | None = None


def _text(value: _Fields) -> str:
    """Flatten only the validated allowlist, never arbitrary native payload metadata."""
    lines = []
    for key, item in value.model_dump(mode="json", exclude_none=True).items():
        label = key.replace("_", " ")
        if isinstance(item, str):
            lines.append(f"{label}: {item}")
        elif isinstance(item, list | tuple):
            for entry in item:
                if isinstance(entry, str):
                    lines.append(f"{label}: {entry}")
                elif entry:
                    lines.append(f"{label}: " + "; ".join(f"{k}: {v}" for k, v in entry.items()))
        elif isinstance(item, dict) and item:
            lines.append(f"{label}: " + "; ".join(f"{k}: {v}" for k, v in item.items()))
    return "\n".join(lines)


def knowledge_details(kind: str, original: dict[str, JsonValue]) -> str | None:
    """Do not fetch referenced records or invent names from their opaque IDs."""
    model = {
        "person": _Person,
        "organisation": _Organisation,
        "place": _Place,
        "creative_work": _CreativeWork,
        "event": _Event,
        "task": _Task,
        "project": _Project,
    }.get(kind)
    return _text(model.model_validate(original)) or None if model else None
