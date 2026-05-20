from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping
from uuid import uuid4

from dltaf.app.runtime import RunContext, RunOptions
from dltaf.app.services import ApplicationServices
from .environment import ExecutionEnvironment
from .request import ManifestExecutionRequest


class RunContextBuilder:
    def build(
        self,
        *,
        manifest: Mapping[str, Any],
        request: ManifestExecutionRequest,
        env: ExecutionEnvironment,
        services: ApplicationServices,
        run_id: str | None = None,
    ) -> RunContext:
        pipeline_cfg = manifest.get("pipeline") or {}
        run_cfg = manifest.get("run") or {}
        observability_cfg = (run_cfg.get("observability") or {}) if isinstance(run_cfg, Mapping) else {}
        manifest_path = Path(str(manifest.get("__manifest_path__") or request.manifest_path)).resolve()
        actual_run_id = str(run_id or uuid4())
        effective_write_disposition = str(run_cfg.get("write_disposition") or "replace")
        if request.write_disposition:
            effective_write_disposition = str(request.write_disposition)
        return RunContext(
            run_id=actual_run_id,
            started_at=datetime.utcnow(),
            manifest_path=manifest_path,
            pipeline_name=str(pipeline_cfg.get("name") or ""),
            source_kind=str((manifest.get("source") or {}).get("kind") or "").strip(),
            destination=str(pipeline_cfg.get("destination") or ""),
            dataset=str(pipeline_cfg.get("dataset") or ""),
            logger=logging.getLogger(f"dlt.run.{pipeline_cfg.get('name') or 'unknown'}"),
            options=RunOptions(
                validate_only=False,
                write_disposition=effective_write_disposition,
                explain_config=bool(request.explain_config),
                dry_run=bool(request.dry_run or request.dry_run_online),
                dry_run_online=bool(request.dry_run_online),
                dry_run_strict=bool(request.dry_run_strict),
                plan=bool(request.plan),
                observability_verbosity=str(observability_cfg.get("verbosity") or "compact"),
                dlt_progress=str(observability_cfg.get("dlt_progress") or "default"),
                overrides=dict(request.overrides or {}),
            ),
            env=env.values,
            env_sources=env.sources,
            services=services,
        )
