from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

ALLOWED_TEMPLATES = {"api_kafka_json", "http_pull", "noop"}


@dataclass(frozen=True)
class ScaffoldOptions:
    root_dir: Path
    pipeline_name: str
    source_kind: str
    package_name: str
    template: str
    destination: str
    dataset: str
    schedule: str
    update_pyproject: bool
    force: bool


@dataclass(frozen=True)
class ScaffoldArtifacts:
    integration_dir: Path
    manifest_path: Path
    pyproject_path: Optional[Path] = None


__all__ = ["ALLOWED_TEMPLATES", "ScaffoldArtifacts", "ScaffoldOptions"]
