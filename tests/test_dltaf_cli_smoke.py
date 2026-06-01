from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def test_dltaf_cli_root_help() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    proc = subprocess.run(
        [sys.executable, "-m", "dltaf.cli", "--repo-root", str(repo_root), "--help"],
        cwd=str(repo_root),
        text=True,
        capture_output=True,
    )
    assert proc.returncode == 0
    assert "manifest" in proc.stdout
    assert "dags" in proc.stdout
    assert "lineage" in proc.stdout
