from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Optional


@dataclass(frozen=True)
class ManifestExecutionRequest:
    manifest_path: str | Path
    write_disposition: Optional[str] = None
    overrides: Mapping[str, Any] = field(default_factory=dict)
    extra_env: Mapping[str, str] = field(default_factory=dict)
    validate_only: bool = False
    explain_config: bool = False
    plan: bool = False
    dry_run: bool = False
    dry_run_online: bool = False
    dry_run_strict: bool = False
    online_timeout_seconds: float = 5.0
    plan_output: Optional[str] = None
    plan_format: Optional[str] = None
    configure_logging: bool = True
    log_level: str = "INFO"

    def validate(self) -> None:
        enabled_modes = sum([1 if self.plan else 0, 1 if self.dry_run else 0, 1 if self.dry_run_online else 0])
        if enabled_modes > 1:
            raise ValueError("Only one of plan/dry_run/dry_run_online can be enabled")
        if self.plan_output and not (self.plan or self.dry_run or self.dry_run_online):
            raise ValueError("plan_output can only be used with plan/dry_run/dry_run_online")
        if self.plan_format and not (self.plan or self.dry_run or self.dry_run_online):
            raise ValueError("plan_format can only be used with plan/dry_run/dry_run_online")
        if self.dry_run_strict and not (self.dry_run or self.dry_run_online):
            raise ValueError("dry_run_strict can only be used with dry_run/dry_run_online")
