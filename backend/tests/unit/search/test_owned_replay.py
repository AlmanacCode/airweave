"""HTTP replay boundaries; synthetic fixtures do not qualify a real corpus."""

import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path
from uuid import UUID

import httpx
import pytest
from evaluation.native_import_cli import Settings
from evaluation.owned_retrieval import ConversationIdentity
from evaluation.replay import (
    CorpusMismatch,
    FrozenCorpus,
    FrozenRecord,
    ReplayConfiguration,
    replay,
)
from evaluation.replay_cli import ReplayArguments, execute, main
from evaluation.retrieval import Dataset, Judgment, Query

from airweave.domains.entities.canonical.extraction_models import (
    ExtractionCoverage,
    ExtractionOutcome,
)
from airweave.domains.entities.canonical.models import RecordPage, SourceRecord
from airweave.domains.entities.canonical.requests import RecordIdentity
from airweave.domains.search.owned_models import (
    OwnedSearchGroup,
    OwnedSearchHit,
    OwnedSearchMatch,
    OwnedSearchResponse,
)


def fixture():
    """Two delivered members with frozen capture, group and extraction facts."""
    sync_id = UUID(int=100)
    parent = RecordIdentity(record_type="session", native_id="session")
    extraction = ExtractionCoverage(
        parts=(ExtractionOutcome(part_index=0, key="body", kind="body", outcome="indexed"),)
    )
    records = tuple(
        FrozenRecord(
            record_id=UUID(int=i),
            sync_id=sync_id,
            revision=3,
            source_id="synthetic-source",
            provider="almanac",
            identity=RecordIdentity(
                record_type="message", native_id=str(i), container_id="session"
            ),
            conversation=ConversationIdentity(kind="session", native_id="session"),
            capture_hash="a" * 64,
            indexed_pipeline_version=2,
            extraction=extraction,
            parent=parent,
        )
        for i in (1, 2)
    )
    corpus = FrozenCorpus(
        corpus_id="synthetic-corpus",
        organization_id=UUID(int=200),
        sync_ids=(sync_id,),
        records=records,
    )
    dataset = Dataset(
        corpus_id=corpus.corpus_id,
        version="v1",
        queries=tuple(
            Query(
                id=f"q{i}",
                text=f"Synthetic query {i}",
                tags=("unit:card",),
                expectation="relevant_records",
                expectation_assessor="synthetic",
                judgments=(
                    Judgment(record_id=records[0].card_id, relevance=3, assessor="synthetic"),
                ),
            )
            for i in range(4)
        ),
    )
    live = tuple(
        SourceRecord(
            id=r.record_id,
            sync_id=r.sync_id,
            identity=r.identity,
            parent=r.parent,
            revision=r.revision,
            payload={"synthetic": True},
            payload_schema_version=1,
            capture_hash=r.capture_hash,
            content_hash=None,
            completeness="complete",
            observed_at="2026-10-01T00:00:00Z",
            source_created_at=None,
            source_updated_at=None,
            deleted_at=None,
            removal_reason=None,
            blobs=(),
            indexed_revision=r.revision,
            indexed_pipeline_version=r.indexed_pipeline_version,
        )
        for r in records
    )
    hits = tuple(
        OwnedSearchHit(
            record_id=r.record_id,
            sync_id=r.sync_id,
            revision=r.revision,
            source_connection_id=UUID(int=300),
            provider=r.provider,
            identity=r.identity,
            extraction=r.extraction,
            title="Synthetic",
            excerpts=("Synthetic passage",),
            observed_at="2026-10-01T00:00:00Z",
            source_created_at=None,
            source_updated_at=None,
            completeness="complete",
        )
        for r in records
    )
    hits[0].group = OwnedSearchGroup(
        kind="session",
        native_id="session",
        matched_records=2,
        additional_matches=(
            OwnedSearchMatch.model_validate(hits[1].model_dump(exclude={"group"})),
        ),
    )
    wire = OwnedSearchResponse(
        items=(hits[0],),
        sources=(),
        candidate_window_full=True,
        engine_partial=False,
        excluded_candidates=0,
        postfilter_excluded=0,
        retrieval_incomplete=True,
    ).model_dump(mode="json")
    return corpus, dataset, live, wire


def run_with_handler(tmp_path, handler):
    """Use the same asynchronous HTTP boundary as the real runner."""
    corpus, dataset, _, _ = fixture()

    async def execute():
        async with httpx.AsyncClient(
            base_url="http://localhost/api/v1/",
            transport=httpx.MockTransport(handler),
            follow_redirects=False,
        ) as client:
            return await replay(
                client,
                dataset,
                corpus,
                ReplayConfiguration(system="Synthetic MiniLM", mode="hybrid"),
                tmp_path / "run",
            )

    return asyncio.run(execute())


def page(records, *, next_cursor=None):
    """Actual public wire serialization, including the live traversal contract."""
    return RecordPage(
        records=records, next_cursor=next_cursor, has_more=next_cursor is not None
    ).model_dump(mode="json")


def test_wire_replay_retains_failed_outcomes_raw_bodies_and_private_permissions(tmp_path):
    _, dataset, live, wire = fixture()
    calls = []

    def handler(request):
        calls.append(request)
        if request.method == "GET":
            assert request.url.params["state"] == "active"
            assert request.url.params["limit"] == "500"
            return httpx.Response(
                200,
                json=page(live[:1], next_cursor="second")
                if "cursor" not in request.url.params
                else page(live[1:]),
            )
        query = json.loads(request.content)["query"]
        assert json.loads(request.content)["mode"] == "hybrid"
        if query.endswith("0"):
            return httpx.Response(200, json=wire)
        if query.endswith("1"):
            return httpx.Response(429, content=b"private provider failure")
        if query.endswith("2"):
            raise httpx.ReadTimeout("private timeout", request=request)
        return httpx.Response(200, content=b"private malformed body")

    run = run_with_handler(tmp_path, handler)
    assert [r.status for r in run.results] == ["partial", "error", "timeout", "error"]
    assert run.dataset_sha256 == dataset.fingerprint()
    assert len(run.results[0].record_ids) == 1
    assert len(run.timings) == 4
    assert [request.method for request in calls] == [
        "GET",
        "GET",
        "POST",
        "POST",
        "POST",
        "POST",
        "GET",
        "GET",
    ]
    output = tmp_path / "run"
    assert (output / "request-00001-response.json").read_bytes() == b"private provider failure"
    assert not (output / "request-00002-response.json").exists()
    evidence = json.loads((output / "evidence.json").read_bytes())
    assert evidence["outcome"] == "verified"
    assert evidence["observations"][0]["ranking"]["fallback_reason"] == "unconfigured"
    assert evidence["census_before"] and evidence["census_after"]
    assert output.stat().st_mode & 0o777 == 0o700
    assert all(p.stat().st_mode & 0o777 == 0o600 for p in output.rglob("*") if p.is_file())


@pytest.mark.parametrize(
    "violation",
    ("revision", "capture", "pipeline", "missing", "extra", "unavailable", "cursor_loop"),
)
def test_before_census_rejects_changed_or_incomplete_qualified_set(tmp_path, violation):
    _, _, live, _ = fixture()

    def handler(request):
        assert request.method == "GET"  # A rejected census must not issue search.
        records = live
        if violation == "revision":
            records = (live[0].model_copy(update={"revision": 4}), live[1])
        elif violation == "capture":
            records = (live[0].model_copy(update={"capture_hash": "b" * 64}), live[1])
        elif violation == "pipeline":
            records = (live[0].model_copy(update={"indexed_pipeline_version": 3}), live[1])
        elif violation == "missing":
            records = live[:1]
        elif violation == "extra":
            records = (*live, live[1].model_copy(update={"id": UUID(int=3)}))
        elif violation == "unavailable":
            records = (live[0].model_copy(update={"content_access": "unavailable"}), live[1])
        return httpx.Response(
            200, json=page(records, next_cursor="loop" if violation == "cursor_loop" else None)
        )

    with pytest.raises(CorpusMismatch):
        run_with_handler(tmp_path, handler)
    assert not (tmp_path / "run/run.json").exists()
    assert json.loads((tmp_path / "run/evidence.json").read_bytes())["outcome"] == "invalid"


def test_after_census_rejects_revision_change_and_retains_valid_raw_responses(tmp_path):
    _, _, live, wire = fixture()
    before = True

    def handler(request):
        nonlocal before
        if request.method == "POST":
            before = False
            return httpx.Response(200, json=wire)
        records = live if before else (live[0].model_copy(update={"indexed_revision": 4}), live[1])
        return httpx.Response(200, json=page(records))

    with pytest.raises(CorpusMismatch):
        run_with_handler(tmp_path, handler)
    output = tmp_path / "run"
    assert len(list(output.glob("*-response.json"))) == 4
    assert not (output / "run.json").exists()
    assert json.loads((output / "evidence.json").read_bytes())["census_before"]


@pytest.mark.parametrize("violation", ("extraction", "revision", "foreign_group"))
def test_delivered_proof_mismatch_aborts_comparison(tmp_path, violation):
    _, _, live, wire = fixture()

    def handler(request):
        if request.method == "GET":
            return httpx.Response(200, json=page(live))
        if violation == "extraction":
            wire["items"][0]["group"]["additional_matches"][0]["extraction"] = None
        elif violation == "revision":
            wire["items"][0]["revision"] = 4
        else:
            wire["items"][0]["group"]["native_id"] = "foreign"
        return httpx.Response(200, json=wire)

    with pytest.raises(CorpusMismatch):
        run_with_handler(tmp_path, handler)
    assert (tmp_path / "run/request-00000-response.json").exists()
    outcome = json.loads((tmp_path / "run/request-00000-outcome.json").read_bytes())
    assert outcome["status"] == "error"
    assert not (tmp_path / "run/run.json").exists()


def test_cli_auth_scope_and_redirect_rules_are_applied_to_actual_http(tmp_path, monkeypatch):
    corpus, dataset, live, _ = fixture()
    dataset_file, census_file = tmp_path / "dataset.json", tmp_path / "corpus.json"
    dataset_file.write_text(dataset.model_dump_json())
    census_file.write_text(corpus.model_dump_json())
    constructor = httpx.AsyncClient
    calls = []

    def handler(request):
        calls.append(request)
        assert request.headers["X-API-Key"] == "synthetic-key"
        assert request.headers["X-Organization-ID"] == str(corpus.organization_id)
        assert request.url.path.startswith("/api/v1/sync/")
        if request.method == "GET":
            return httpx.Response(200, json=page(live))
        assert json.loads(request.content)["sync_ids"] == [str(corpus.sync_ids[0])]
        return httpx.Response(302, headers={"Location": "https://foreign.invalid/private"})

    def client_factory(**kwargs):
        assert kwargs["follow_redirects"] is False
        assert kwargs["trust_env"] is False
        return constructor(**kwargs, transport=httpx.MockTransport(handler))

    monkeypatch.setattr("evaluation.replay_cli.httpx.AsyncClient", client_factory)
    result = asyncio.run(
        execute(
            ReplayArguments(
                url="http://localhost/api/v1/",
                dataset=dataset_file,
                census=census_file,
                output=tmp_path / "run",
                system="synthetic",
                mode="keyword",
            ),
            Settings(api_key="synthetic-key"),
        )
    )
    assert "4 failed" in result
    assert len(calls) == 6  # Two census pages and four requests, no redirect destinations.
    run = json.loads((tmp_path / "run/run.json").read_bytes())
    assert all(r["status"] == "error" and not r["record_ids"] for r in run["results"])


@pytest.mark.parametrize("violation", ("corpus", "unit", "unknown_label"))
def test_wrong_dataset_binding_rejects_before_network_or_output(tmp_path, violation):
    corpus, dataset, _, _ = fixture()
    wire = dataset.model_dump(mode="json")
    if violation == "corpus":
        wire["corpus_id"] = "other"
    elif violation == "unit":
        wire["queries"][0]["tags"] = ["unit:displayed_original"]
    else:
        wire["queries"][0]["judgments"][0]["record_id"] = "unknown"
    dataset = Dataset.model_validate(wire)

    def handler(request):
        pytest.fail("Invalid frozen binding must not make HTTP requests")

    async def execute_invalid():
        async with httpx.AsyncClient(
            base_url="http://localhost/", transport=httpx.MockTransport(handler)
        ) as client:
            return await replay(
                client,
                dataset,
                corpus,
                ReplayConfiguration(system="synthetic", mode="keyword"),
                tmp_path / "run",
            )

    with pytest.raises(CorpusMismatch):
        asyncio.run(execute_invalid())
    assert not (tmp_path / "run").exists()


@pytest.mark.parametrize("unit", ("card", "displayed_original"))
def test_oversized_card_response_receives_no_scoring_credit(tmp_path, unit):
    corpus, dataset, live, wire = fixture()
    additional = wire["items"][0]["group"]["additional_matches"][0]
    wire["items"][0]["group"] = None
    wire["items"].append({**additional, "group": None})
    corpus = corpus.model_copy(
        update={
            "records": tuple(
                r.model_copy(update={"conversation": None, "parent": None}) for r in corpus.records
            )
        }
    )
    live = tuple(r.model_copy(update={"parent": None}) for r in live)
    dataset = dataset.model_copy(
        update={
            "queries": tuple(
                q.model_copy(
                    update={
                        "tags": (f"unit:{unit}",),
                        "judgments": (
                            Judgment(
                                record_id=corpus.records[0].evaluation_id,
                                relevance=3,
                                assessor="synthetic",
                            ),
                        ),
                    }
                )
                for q in dataset.queries
            )
        }
    )

    def handler(request):
        return httpx.Response(200, json=page(live) if request.method == "GET" else wire)

    async def execute_oversized():
        async with httpx.AsyncClient(
            base_url="http://localhost/", transport=httpx.MockTransport(handler)
        ) as client:
            return await replay(
                client,
                dataset,
                corpus,
                ReplayConfiguration(system="synthetic", mode="keyword", limit=1, unit=unit),
                tmp_path / "run",
            )

    run = asyncio.run(execute_oversized())
    assert all(r.status == "error" and not r.record_ids for r in run.results)
    assert (
        len(json.loads((tmp_path / "run/request-00000-response.json").read_bytes())["items"]) == 2
    )


def test_displayed_original_expansion_can_exceed_card_limit(tmp_path):
    corpus, dataset, live, wire = fixture()
    dataset = dataset.model_copy(
        update={
            "queries": tuple(
                q.model_copy(
                    update={
                        "tags": ("unit:displayed_original",),
                        "judgments": (
                            Judgment(
                                record_id=corpus.records[0].evaluation_id,
                                relevance=3,
                                assessor="synthetic",
                            ),
                        ),
                    }
                )
                for q in dataset.queries
            )
        }
    )

    def handler(request):
        return httpx.Response(200, json=page(live) if request.method == "GET" else wire)

    async def execute_grouped():
        async with httpx.AsyncClient(
            base_url="http://localhost/", transport=httpx.MockTransport(handler)
        ) as client:
            return await replay(
                client,
                dataset,
                corpus,
                ReplayConfiguration(
                    system="synthetic", mode="keyword", limit=1, unit="displayed_original"
                ),
                tmp_path / "run",
            )

    run = asyncio.run(execute_grouped())
    assert all(r.status == "partial" and len(r.record_ids) == 2 for r in run.results)


def test_http_dtos_and_help_import_without_server_settings_or_database(tmp_path):
    root = Path(__file__).resolve().parents[4]
    env = {"PATH": os.environ["PATH"], "PYTHONPATH": f"{root / 'backend'}:{root}"}
    code = (
        "from evaluation.replay_cli import main; import sys; "
        "from airweave.domains.entities.canonical.models import IndexedRecordRead; "
        "assert 'extraction' in IndexedRecordRead.model_json_schema()['properties']; "
        "assert 'airweave.core.config' not in sys.modules; "
        "assert 'airweave.platform.sources' not in sys.modules; "
        "assert 'sqlalchemy' not in sys.modules"
    )
    imported = subprocess.run(
        [sys.executable, "-c", code], env=env, cwd=tmp_path, capture_output=True, text=True
    )
    assert imported.returncode == 0, imported.stderr
    helped = subprocess.run(
        [sys.executable, "-m", "evaluation.replay_cli", "--help"],
        env=env,
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert helped.returncode == 0, helped.stderr
    assert "--census" in helped.stdout


@pytest.mark.parametrize(
    "url",
    (
        "http://remote.example/api/v1/",
        "https://user:private@host/api/v1/",
        "https://host/api/v1/?private",
    ),
)
def test_cli_reuses_destination_transport_rules(url):
    with pytest.raises(ValueError):
        ReplayArguments(
            url=url, dataset="dataset", census="census", output="run", system="test", mode="keyword"
        )


def test_argument_errors_do_not_echo_private_input(capsys):
    with pytest.raises(SystemExit) as failure:
        main(["--url", "https://private.invalid/secret"])
    assert failure.value.code == 2
    captured = capsys.readouterr()
    assert "secret" not in captured.err
