"""Actual PostgreSQL bounded candidates, ambiguity and authority fencing."""

import json
import os
import time
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import select, update

from airweave.domains.entities.canonical.contact_query import (
    ContactCandidatePage,
    ContactLookup,
    lookup_contacts,
)
from airweave.domains.entities.canonical.cycle_models import BeginCycle, CycleConfiguration
from airweave.domains.entities.canonical.query import InvalidRecordCursor
from airweave.domains.entities.canonical.requests import CaptureBatch, RecordIdentity
from airweave.domains.entities.canonical.store import SourceNotFound
from airweave.domains.entities.canonical.tests.helpers import bind_projection, observation
from airweave.domains.entities.canonical.tests.test_query import query_service
from airweave.models.entity import Entity
from airweave.models.source_connection import SourceConnection


async def test_contact_candidates_and_1000_card_cost(database, source):
    capture, fence = source
    await bind_projection(database, fence, source_name="apple_contacts")
    base = json.loads(
        (Path(__file__).parents[1] / "apple_payload_tests/fixtures/contact-swift.json").read_text()
    )
    async with database() as db:
        await capture.begin_cycle(
            db,
            BeginCycle(
                fence=fence,
                configuration=CycleConfiguration(
                    fingerprint="b" * 64,
                    parents={"apple_contact": (None,)},
                    completion_policies={"apple_contact": "discovery_only"},
                ),
            ),
        )
    records = []
    for i in range(1000):
        original = json.loads(json.dumps(base))
        original["contact"]["nativeID"] = str(i)
        original["contact"]["phones"][0]["rawValue"] = (
            "+1 415 555 0100 ext. 2" if i in (0, 999) else "+1 415 555 0101"
        )
        records.append(
            observation(
                str(i),
                identity=RecordIdentity(record_type="apple_contact", native_id=str(i)),
                payload={
                    "authority": "device",
                    "source_kind": "apple_contacts",
                    "account_id": "11111111-1111-4111-8111-111111111111",
                    "original": original,
                },
            )
        )
    for start in range(0, 1000, 500):
        async with database() as db:
            await capture.capture(
                db, CaptureBatch(fence=fence, records=tuple(records[start : start + 500]))
            )
    # Put duplicate endpoints at either end of the real stable UUID order.
    async with database() as db:
        ordered = tuple(
            await db.scalars(
                select(Entity).where(Entity.sync_id == fence.sync_id).order_by(Entity.id)
            )
        )
        expected = [ordered[0].native_id, ordered[-1].native_id]
        for index, row in enumerate(ordered):
            payload = json.loads(json.dumps(row.source_payload))
            payload["original"]["contact"]["phones"][0]["rawValue"] = (
                "+1 415 555 0100 ext. 2" if index in (0, 999) else "+1 415 555 0101"
            )
            row.source_payload = payload
        await db.commit()
    service = query_service()
    started = time.perf_counter()
    pages = []
    cursor = None
    while True:
        async with database() as db:
            page = await lookup_contacts(
                service,
                db,
                fence.organization_id,
                fence.sync_id,
                ContactLookup(
                    mode="international_phone", value="+14155550100 x2", limit=100, cursor=cursor
                ),
            )
        pages.append(page)
        if not page.has_more:
            break
        cursor = page.next_cursor
    elapsed = time.perf_counter() - started
    assert len(pages) == 10 and sum(p.scanned for p in pages) == 1000
    assert [c.contact.origin.native_id for p in pages for c in p.candidates] == expected
    assert not pages[1].candidates and pages[1].has_more
    assert pages[0].candidates[0].matches[0].handle.label == base["contact"]["phones"][0]["label"]
    print(f"1000-card international traversal: {elapsed:.4f}s, 10 windows, 100 cards/window")
    async with database() as db:
        no_extension = await lookup_contacts(
            service,
            db,
            fence.organization_id,
            fence.sync_id,
            ContactLookup(mode="international_phone", value="+14155550100"),
        )
        national = await lookup_contacts(
            service,
            db,
            fence.organization_id,
            fence.sync_id,
            ContactLookup(mode="international_phone", value="4155550100"),
        )
        assert (
            not no_extension.candidates
            and not national.candidates
            and national.interpretation == "region_required"
        )
        raw = await lookup_contacts(
            service,
            db,
            fence.organization_id,
            fence.sync_id,
            ContactLookup(mode="raw_handle", value="+1 415 555 0100 ext. 2", limit=1),
        )
        assert len(raw.candidates) == 1 and raw.has_more
        with pytest.raises(InvalidRecordCursor):
            await lookup_contacts(
                service,
                db,
                uuid4(),
                fence.sync_id,
                ContactLookup(
                    mode="international_phone", value="+14155550100 x2", cursor=pages[0].next_cursor
                ),
            )
        with pytest.raises(InvalidRecordCursor):
            await lookup_contacts(
                service,
                db,
                fence.organization_id,
                fence.sync_id,
                ContactLookup(
                    mode="international_phone", value="+14155550101", cursor=pages[0].next_cursor
                ),
            )
        if fixture_output := os.environ.get("CONTACT_CANDIDATE_FIXTURE_OUTPUT"):
            # A separate synthetic window holds duplicate cards for wire-contract review.
            row = await db.scalar(select(Entity).where(Entity.id == ordered[1].id))
            payload = json.loads(json.dumps(row.source_payload))
            payload["original"]["contact"]["phones"][0]["rawValue"] = "+1 (415) 555-0100 x2"
            row.source_payload = payload
            await db.commit()
            fixture_page = await lookup_contacts(
                service,
                db,
                fence.organization_id,
                fence.sync_id,
                ContactLookup(mode="international_phone", value="+14155550100 x2"),
            )
            assert len(fixture_page.candidates) == 2 and fixture_page.capture is not None
            from fastapi import FastAPI
            from httpx import ASGITransport, AsyncClient

            app = FastAPI()

            @app.get("/fixture", response_model=ContactCandidatePage, response_model_by_alias=True)
            async def serialized_page():
                return fixture_page

            async with AsyncClient(
                transport=ASGITransport(app), base_url="http://fixture"
            ) as client:
                wire = await client.get("/fixture")
            assert wire.status_code == 200
            Path(fixture_output).parent.mkdir(parents=True, exist_ok=True)
            Path(fixture_output).write_text(
                json.dumps(wire.json(), ensure_ascii=False, indent=2) + "\n"
            )
        await db.execute(
            update(SourceConnection)
            .where(SourceConnection.sync_id == fence.sync_id)
            .values(is_authenticated=False)
        )
        await db.commit()
    async with database() as db:
        with pytest.raises(SourceNotFound):
            await lookup_contacts(
                service,
                db,
                fence.organization_id,
                fence.sync_id,
                ContactLookup(
                    mode="international_phone", value="+14155550100 x2", cursor=pages[0].next_cursor
                ),
            )


async def test_unicode_decimal_candidates_and_extension_identity(database, source):
    from jose import jwt

    capture, fence = source
    await bind_projection(database, fence, source_name="apple_contacts")
    base = json.loads(
        (Path(__file__).parents[1] / "apple_payload_tests/fixtures/contact-swift.json").read_text()
    )
    async with database() as db:
        await capture.begin_cycle(
            db,
            BeginCycle(
                fence=fence,
                configuration=CycleConfiguration(
                    fingerprint="c" * 64,
                    parents={"apple_contact": (None,)},
                    completion_policies={"apple_contact": "discovery_only"},
                ),
            ),
        )
        records = []
        for native_id, raw in [
            ("unicode", "+۱۴۱۵۵۵۵۲۶۷۱ ext. १२"),
            ("ascii", "+14155552671 x12"),
            ("other-extension", "+١٤١٥٥٥٥٢٦٧١ x١٣"),
        ]:
            original = json.loads(json.dumps(base))
            original["contact"]["nativeID"] = native_id
            original["contact"]["phones"][0]["rawValue"] = raw
            records.append(
                observation(
                    native_id,
                    identity=RecordIdentity(record_type="apple_contact", native_id=native_id),
                    payload={
                        "authority": "device",
                        "source_kind": "apple_contacts",
                        "account_id": "synthetic",
                        "original": original,
                    },
                )
            )
        await capture.capture(db, CaptureBatch(fence=fence, records=tuple(records)))
        service = query_service()
        for value in ["+14155552671 ext. 12", "+१४१५५५५२६७१ x۱۲", "+١٤١٥٥٥٥٢٦٧١ #१२"]:
            page = await lookup_contacts(
                service,
                db,
                fence.organization_id,
                fence.sync_id,
                ContactLookup(mode="international_phone", value=value),
            )
            assert {c.contact.origin.native_id for c in page.candidates} == {"unicode", "ascii"}
            assert page.preparation_version == "contacts-fields-v2"
            assert {c.matches[0].handle.raw_value for c in page.candidates} == {
                "+۱۴۱۵۵۵۵۲۶۷۱ ext. १२",
                "+14155552671 x12",
            }
        first = await lookup_contacts(
            service,
            db,
            fence.organization_id,
            fence.sync_id,
            ContactLookup(mode="international_phone", value="+14155552671 x12", limit=1),
        )
        claims = jwt.decode(first.next_cursor, service.signing_key, algorithms=["HS256"])
        claims["preparation_version"] = "contacts-fields-v1"
        with pytest.raises(InvalidRecordCursor):
            await lookup_contacts(
                service,
                db,
                fence.organization_id,
                fence.sync_id,
                ContactLookup(
                    mode="international_phone",
                    value="+14155552671 x12",
                    limit=1,
                    cursor=jwt.encode(claims, service.signing_key, algorithm="HS256"),
                ),
            )
