from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
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
    observability_verbosity: str = "compact"
    dlt_progress: str = "default"
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
        if now is not None:
            n = now
        elif self.started_at.tzinfo is not None:
            n = datetime.now(self.started_at.tzinfo)
        else:
            n = datetime.now(timezone.utc)
        if self.started_at.tzinfo is not None and n.tzinfo is None:
            n = n.replace(tzinfo=self.started_at.tzinfo)
        elif self.started_at.tzinfo is None and n.tzinfo is not None:
            n = n.replace(tzinfo=None)
        return max(0.0, (n - self.started_at).total_seconds())
