from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

from dltaf.app.runtime import RunContext
from dltaf.services.execution.redaction import safe_exception_message
from dlt_utils.core.run_result import RunResult


@dataclass
class BasicLoggingHook:
    """Standard run logging.

    Logs:
    - start of the run
    - finish of the run + duration
    - errors + duration

    IMPORTANT: must never log secret values.
    """

    name: str = "basic_logging"

    def pre_run(self, manifest: Mapping[str, Any], ctx: RunContext) -> None:
        ctx.logger.info(
            "Run started: pipeline=%s kind=%s destination=%s dataset=%s run_id=%s manifest=%s",
            ctx.pipeline_name,
            ctx.source_kind,
            ctx.destination,
            ctx.dataset,
            ctx.run_id,
            str(ctx.manifest_path),
        )

    def post_run(self, manifest: Mapping[str, Any], ctx: RunContext, result: Any) -> None:
        if isinstance(result, RunResult):
            status = result.status
            elapsed = float(result.duration_seconds)
        else:
            status = "success"
            elapsed = ctx.elapsed_seconds(datetime.now(timezone.utc))

        ctx.logger.info(
            "Run finished: pipeline=%s run_id=%s status=%s duration_s=%.3f",
            ctx.pipeline_name,
            ctx.run_id,
            status,
            elapsed,
        )
        ctx.logger.debug("Run result type: %s", type(result))

    def on_error(self, manifest: Mapping[str, Any], ctx: RunContext, exc: Exception) -> None:
        elapsed = ctx.elapsed_seconds(datetime.now(timezone.utc))
        safe_err = safe_exception_message(exc)
        ctx.logger.error(
            "Run failed: pipeline=%s run_id=%s duration_s=%.3f error=%s",
            ctx.pipeline_name,
            ctx.run_id,
            elapsed,
            safe_err,
            exc_info=True,
        )
