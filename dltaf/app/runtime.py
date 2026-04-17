from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Optional, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from dltaf.app.services import ApplicationServices


@dataclass(frozen=True)
class RunOptions:
    """Run-time options for a pipeline execution."""

    validate_only: bool = False
    write_disposition: Optional[str] = None
    explain_config: bool = False
    dry_run: bool = False
    dry_run_online: bool = False
    dry_run_strict: bool = False
    plan: bool = False
    overrides: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RunContext:
    """Run-scoped context passed to runners and hooks."""

    run_id: str
    started_at: datetime
    manifest_path: Path
    pipeline_name: str
    source_kind: str
    destination: str
    dataset: str
    logger: logging.Logger
    options: RunOptions
    env: Mapping[str, str] = field(default_factory=dict)
    env_sources: Mapping[str, str] = field(default_factory=dict)
    services: Optional["ApplicationServices"] = None

    def elapsed_seconds(self, now: Optional[datetime] = None) -> float:
        n = now or datetime.utcnow()
        return max(0.0, (n - self.started_at).total_seconds())
