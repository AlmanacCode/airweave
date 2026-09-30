"""Fresh-interpreter storage imports must not depend on eager source catalogue order."""

import subprocess
import sys
from pathlib import Path


def test_file_service_imports_before_source_catalogue():
    root = Path(__file__).resolve().parents[4]
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import conftest; "
                "from airweave.domains.storage.file_service import FileService; "
                "from airweave.platform.sources import GmailSource, GoogleDriveSource; "
                "assert FileService and GmailSource and GoogleDriveSource"
            ),
        ],
        cwd=root,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stderr
