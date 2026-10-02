"""Export existing owned-search responses for the offline relevance evaluator.

Requires the backend environment. Source identities come from the frozen corpus
census; destination UUIDs are locators, never cross-rebuild evaluation identities.
"""

import hashlib
import json
from typing import Literal
from uuid import UUID

from pydantic import Field

from airweave.domains.entities.canonical.requests import RecordIdentity
from airweave.domains.search.owned_models import OwnedSearchMatch, OwnedSearchResponse
from evaluation.retrieval import Identifier, Result, Value


def _identifier(kind: str, values: object) -> str:
    encoded = json.dumps(
        values, sort_keys=True, ensure_ascii=False, separators=(",", ":")
    )
    return kind + ":" + hashlib.sha256(encoded.encode()).hexdigest()


class ConversationIdentity(Value):
    """Frozen grouping attestation from retained canonical identity, before retrieval."""

    kind: Literal["session", "email_thread"]
    native_id: str = Field(min_length=1)


class CorpusRecord(Value):
    """One exact destination locator and its preserved stable source identity."""

    record_id: UUID
    sync_id: UUID
    revision: int = Field(ge=1)
    source_id: Identifier
    provider: Identifier
    identity: RecordIdentity
    conversation: ConversationIdentity | None = None

    @property
    def evaluation_id(self) -> str:
        """Keep native IDs account-scoped, including their original container."""
        return _identifier(
            "original",
            [self.source_id, self.provider, self.identity.model_dump(mode="json")],
        )

    @property
    def card_id(self) -> str:
        """Use frozen membership even when a different original wins the ranking."""
        if self.conversation is None:
            return self.evaluation_id
        return _identifier(
            "card",
            [
                self.source_id,
                self.provider,
                self.conversation.kind,
                self.conversation.native_id,
            ],
        )


def delivered_result(
    query_id: str,
    response: OwnedSearchResponse,
    records: tuple[CorpusRecord, ...],
    *,
    unit: Literal["card", "displayed_original"],
) -> Result:
    """Preserve card order; flatten only explicitly delivered originals when requested."""
    census = {record.record_id: record for record in records}
    if len(census) != len(records):
        raise ValueError("Duplicate destination record in corpus census")

    def reference(match: OwnedSearchMatch) -> CorpusRecord:
        record = census[match.record_id]
        if (
            record.sync_id != match.sync_id
            or record.revision != match.revision
            or record.provider != match.provider
            or record.identity != match.identity
        ):
            raise ValueError("Delivered match conflicts with frozen source identity")
        return record

    ranking = []
    for hit in response.items:
        original = reference(hit)
        conversation = (
            None
            if hit.group is None
            else ConversationIdentity(
                kind=hit.group.kind, native_id=hit.group.native_id
            )
        )
        if conversation != original.conversation:
            raise ValueError(
                "Delivered group conflicts with frozen conversation membership"
            )
        additional = () if hit.group is None else hit.group.additional_matches
        for match in additional:
            member = reference(match)
            if (member.source_id, member.provider, member.sync_id) != (
                original.source_id,
                original.provider,
                original.sync_id,
            ):
                raise ValueError("Conversation contains a different source identity")
            if member.conversation != conversation:
                raise ValueError(
                    "Additional match conflicts with frozen conversation membership"
                )
        if unit == "card" and hit.group is not None:
            ranking.append(original.card_id)
        else:
            ranking.append(original.evaluation_id)
            if unit == "displayed_original":
                ranking.extend(reference(match).evaluation_id for match in additional)
    return Result(
        query_id=query_id,
        status="partial"
        if response.retrieval_incomplete or response.engine_partial
        else "success",
        record_ids=tuple(ranking),
    )
