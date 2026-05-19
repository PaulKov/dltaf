from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Any, Mapping

from dltaf.app.runtime import RunContext


@dataclass
class EnvSummaryHook:
    """Logs injected env keys count (without values)."""

    name: str = "env_summary"

    def pre_run(self, manifest: Mapping[str, Any], ctx: RunContext) -> None:
        keys = sorted(ctx.env.keys())
        if not keys:
            ctx.logger.debug("No env vars injected for this run")
            return

        if ctx.env_sources:
            c = Counter(ctx.env_sources.get(k, "unknown") for k in keys)
            summary = ", ".join([f"{src}={cnt}" for src, cnt in sorted(c.items())])
            ctx.logger.info("Injected env vars: %d keys (sources: %s)", len(keys), summary)
        else:
            ctx.logger.info("Injected env vars: %d keys", len(keys))
        # Show keys only in debug logs to reduce noise in Airflow.
        ctx.logger.debug("Injected env keys: %s", ", ".join(keys))

    def post_run(self, manifest: Mapping[str, Any], ctx: RunContext, result: Any) -> None:
        return

    def on_error(self, manifest: Mapping[str, Any], ctx: RunContext, exc: Exception) -> None:
        return
