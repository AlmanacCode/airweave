"""Single-use restricted-budget parser process, not a filesystem/network sandbox."""

import resource
import sys
from pathlib import Path

from pydantic import ValidationError

# -I ignores ambient PYTHONPATH; load only this trusted package's local modules.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from models import ApplePreparationError, BodyDecodeRequest, WorkerReply  # noqa: E402


def run() -> WorkerReply:
    """Never read a provider path or return source bytes/error details."""
    try:
        maximum_request = int(sys.argv[1])
        if maximum_request <= 0:
            return WorkerReply(error="invalid_request")
        payload = sys.stdin.buffer.read(maximum_request + 1)
        if len(payload) > maximum_request:
            return WorkerReply(error="input_limit")
        request = BodyDecodeRequest.model_validate_json(payload)
    except (ValueError, ValidationError):
        return WorkerReply(error="invalid_request")
    try:
        # Apply before importing/processing either parser. Fail rather than claim limits applied.
        resource.setrlimit(resource.RLIMIT_CPU, (request.limits.cpu_seconds,) * 2)
        memory_enforced = sys.platform.startswith("linux")
        if memory_enforced:
            resource.setrlimit(resource.RLIMIT_AS, (request.limits.memory_bytes,) * 2)
    except (OSError, ValueError):
        return WorkerReply(error="resource_limit_unavailable")
    try:
        if request.format == "notes_gzip_protobuf":
            from decoder import decode_notes  # noqa: PLC0415 -- limits precede parser imports

            result = decode_notes(request, cpu_enforced=True, memory_enforced=memory_enforced)
        else:
            from messages import decode_message  # noqa: PLC0415 -- limits precede parser imports

            result = decode_message(request, cpu_enforced=True, memory_enforced=memory_enforced)
        return WorkerReply(result=result)
    except ImportError:
        return WorkerReply(error="parser_unavailable")
    except ApplePreparationError as error:
        return WorkerReply(error=error.code)
    except Exception:
        # No traceback, native payload, filename or credential in the process protocol.
        return WorkerReply(error="worker_failed")


if __name__ == "__main__":
    reply = run()
    sys.stdout.buffer.write(reply.model_dump_json().encode("utf-8"))
