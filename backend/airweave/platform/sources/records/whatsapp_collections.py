"""Provider-owned collections retaining exact native response envelopes."""

import hashlib
import json
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator

from airweave.platform.sources.records.whatsapp_models import (
    WhatsAppPage,
    WhatsAppParticipant,
    WhatsAppReaction,
)


class WhatsAppCollectionPage(BaseModel):
    """An actual list request and its untouched native JSON response."""

    model_config = ConfigDict(extra="forbid", strict=True)
    query: dict[str, str | int]
    response: dict[str, JsonValue]


class WhatsAppParticipantCollection(BaseModel):
    """Exhausted observation interval, never an instantaneous native roster snapshot."""

    model_config = ConfigDict(extra="forbid", strict=True)
    chat_id: str = Field(min_length=1)
    pagination: Literal["cursor", "offset"]
    page_size: int = Field(ge=1, le=100)
    pages: list[WhatsAppCollectionPage] = Field(min_length=1, max_length=256)

    @model_validator(mode="after")
    def validate_enumeration(self):  # noqa: C901 -- one provider collection's pagination proof
        """Malformed or incomplete collections cannot become searchable exclusions."""
        if (
            len(json.dumps(self.model_dump(mode="json"), ensure_ascii=False).encode())
            > 20 * 1024 * 1024
        ):
            raise ValueError("WhatsApp participant collection exceeds maximum retained size")
        cursor = None
        offset = 0
        cursors = set()
        seen_member_ids = set()
        item_count = 0
        for index, retained in enumerate(self.pages):
            expected = {"cursor": cursor} if cursor is not None else {"limit": self.page_size}
            if self.pagination == "offset":
                expected["offset"] = offset
            if retained.query != expected:
                raise ValueError("WhatsApp participant query disagrees with enumeration")
            page = WhatsAppPage[WhatsAppParticipant].model_validate(retained.response)
            item_count += len(page.data)
            if item_count > 10000:
                raise ValueError("WhatsApp participant collection exceeds maximum item count")
            if len(page.data) > self.page_size:
                raise ValueError("WhatsApp participant page exceeded page_size")
            for participant in page.data:
                if participant.user.id in seen_member_ids:
                    raise ValueError(
                        "WhatsApp participant identity repeated; ambiguous enumeration"
                    )
                seen_member_ids.add(participant.user.id)
            if self.pagination == "cursor":
                cursor = page.cursor_after(cursor)
                final = cursor is None
                if cursor is not None:
                    if cursor in cursors:
                        raise ValueError("WhatsApp participant cursor repeated without progress")
                    cursors.add(cursor)
            else:
                next_offset = page.offset_after(offset, self.page_size)
                final = next_offset is None
                if next_offset is not None:
                    offset = next_offset
            if final != (index == len(self.pages) - 1):
                raise ValueError("WhatsApp participant collection is not exactly exhausted")
        return self


def reaction_page_digest(data: list[dict[str, JsonValue]]) -> str:
    """Detect indistinguishable whole pages; never claim individual reaction identity."""
    items = sorted(json.dumps(item, sort_keys=True, ensure_ascii=False) for item in data)
    return hashlib.sha256(json.dumps(items, ensure_ascii=False).encode()).hexdigest()


class WhatsAppReactionCollection(BaseModel):
    """Exact message-owned response collection; reactions have no native IDs or times."""

    model_config = ConfigDict(extra="forbid", strict=True)
    chat_id: str = Field(min_length=1)
    message_id: str = Field(min_length=1)
    pagination: Literal["cursor", "offset"]
    page_size: int = Field(ge=1, le=100)
    pages: list[WhatsAppCollectionPage] = Field(min_length=1, max_length=256)

    @model_validator(mode="after")
    def validate_enumeration(self):  # noqa: C901 -- one provider collection's pagination proof
        """Only a bounded exhausted traversal can become an intentional exclusion."""
        if (
            len(json.dumps(self.model_dump(mode="json"), ensure_ascii=False).encode())
            > 20 * 1024 * 1024
        ):
            raise ValueError("WhatsApp reaction collection exceeds maximum retained size")
        cursor = None
        offset = item_count = 0
        cursors = set()
        page_digests = set()
        for index, retained in enumerate(self.pages):
            expected = {"cursor": cursor} if cursor is not None else {"limit": self.page_size}
            if self.pagination == "offset":
                expected["offset"] = offset
            if retained.query != expected:
                raise ValueError("WhatsApp reaction query disagrees with enumeration")
            page = WhatsAppPage[WhatsAppReaction].model_validate(retained.response)
            item_count += len(page.data)
            if item_count > 10000 or len(page.data) > self.page_size:
                raise ValueError("WhatsApp reaction collection exceeds item or page bound")
            if page.data:
                digest = reaction_page_digest(retained.response["data"])
                if digest in page_digests:
                    raise ValueError("WhatsApp reaction whole page repeated; ambiguous enumeration")
                page_digests.add(digest)
            if self.pagination == "cursor":
                cursor = page.cursor_after(cursor)
                final = cursor is None
                if cursor is not None:
                    if cursor in cursors:
                        raise ValueError("WhatsApp reaction cursor repeated without progress")
                    cursors.add(cursor)
            else:
                next_offset = page.offset_after(offset, self.page_size)
                final = next_offset is None
                if next_offset is not None:
                    offset = next_offset
            if final != (index == len(self.pages) - 1):
                raise ValueError("WhatsApp reaction collection is not exactly exhausted")
        return self
