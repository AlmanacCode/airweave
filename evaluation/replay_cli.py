"""Replay queries against an already-qualified retained owned-search corpus."""

import asyncio
import sys
from pathlib import Path

import httpx
from pydantic import ValidationError, field_validator

from evaluation.native_import_cli import (
    MAX_INPUT_BYTES,
    Parser,
    PublishArguments,
    Settings,
)
from evaluation.replay import (
    CorpusMismatch,
    FrozenCorpus,
    ReplayConfiguration,
    replay,
    save_model,
)
from evaluation.retrieval import Dataset, summarize


class ReplayArguments(ReplayConfiguration):
    """Explicit HTTP destination and frozen input files; credentials stay in env."""

    url: str
    dataset: Path
    census: Path
    output: Path
    report: bool = False

    @field_validator("url")
    @classmethod
    def destination_url(cls, value: str) -> str:
        """Reuse the native publisher's destination and credential transport boundary."""
        return PublishArguments.destination_url(value)


def parse_args(argv: list[str] | None) -> ReplayArguments:
    """Require explicit corpus, request mode and system descriptor."""
    parser = Parser(description=__doc__)
    parser.add_argument(
        "--url", required=True, help="API root, e.g. https://host/api/v1/"
    )
    parser.add_argument(
        "--dataset", required=True, help="Frozen Dataset JSON (max 32 MiB)"
    )
    parser.add_argument(
        "--census", required=True, help="Qualified FrozenCorpus JSON (max 32 MiB)"
    )
    parser.add_argument("--output", required=True, help="New private run directory")
    parser.add_argument(
        "--system", required=True, help="Operator-supplied system/model descriptor"
    )
    parser.add_argument(
        "--mode", required=True, choices=("keyword", "semantic", "hybrid")
    )
    parser.add_argument(
        "--unit", default="card", choices=("card", "displayed_original")
    )
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument(
        "--report",
        action="store_true",
        help="Also score using the existing offline evaluator",
    )
    return ReplayArguments.model_validate(vars(parser.parse_args(argv)))


def read_input(path: Path) -> bytes:
    """Read a bounded caller-owned file without printing its private content."""
    with path.open("rb") as staged:
        data = staged.read(MAX_INPUT_BYTES + 1)
    if len(data) > MAX_INPUT_BYTES:
        raise ValueError("Input exceeds the size limit")
    return data


async def execute(arguments: ReplayArguments, settings: Settings) -> str:
    """Use the existing API models and offline scorer, without server configuration."""
    dataset = Dataset.model_validate_json(read_input(arguments.dataset))
    corpus = FrozenCorpus.model_validate_json(read_input(arguments.census))
    configuration = ReplayConfiguration(
        system=arguments.system,
        mode=arguments.mode,
        unit=arguments.unit,
        limit=arguments.limit,
    )
    async with httpx.AsyncClient(
        base_url=arguments.url,
        headers={
            "X-API-Key": settings.api_key.get_secret_value(),
            "X-Organization-ID": str(corpus.organization_id),
        },
        follow_redirects=False,
        trust_env=False,
        timeout=httpx.Timeout(60, connect=10),
    ) as client:
        run = await replay(client, dataset, corpus, configuration, arguments.output)
    if arguments.report:
        save_model(arguments.output / "report.json", summarize(dataset, run))
    failed = sum(result.status in {"error", "timeout"} for result in run.results)
    return f"Corpus checks passed; retained {len(run.results)} query outcomes ({failed} failed)."


def main(argv: list[str] | None = None) -> int:
    """Emit static errors; private query text, response bodies and secrets stay in files."""
    try:
        arguments = parse_args(argv)
        summary = asyncio.run(execute(arguments, Settings()))
    except CorpusMismatch:
        print(
            "Corpus or delivered proof differs from frozen input; comparison rejected. Retain evidence.",
            file=sys.stderr,
        )
        return 4
    except httpx.HTTPError:
        print(
            "Census HTTP verification failed; comparison rejected. Retain evidence.",
            file=sys.stderr,
        )
        return 3
    except (ValidationError, ValueError, OSError):
        print(
            "Invalid configuration, frozen input or output. Check --help; inputs are limited to 32 MiB.",
            file=sys.stderr,
        )
        return 2
    except ModuleNotFoundError:
        print(
            "Offline scoring dependency unavailable; a verified run may already be retained. "
            "Install the pinned evaluation dependencies before scoring.",
            file=sys.stderr,
        )
        return 5
    except KeyboardInterrupt:
        print(
            "Interrupted; retain private evidence. No corpus mutations were requested.",
            file=sys.stderr,
        )
        return 130
    except Exception:
        print(
            "Unexpected replay failure; retain private evidence. No comparison is established.",
            file=sys.stderr,
        )
        return 1
    print(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
