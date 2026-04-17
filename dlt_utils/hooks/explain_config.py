from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from dltaf.app.runtime import RunContext


@dataclass
class ExplainConfigHook:
    """Logs config/env *sources* (never values) when `--explain-config` is enabled."""

    name: str = "explain_config"

    def pre_run(self, manifest: Mapping[str, Any], ctx: RunContext) -> None:
        if not ctx.options.explain_config:
            return

        if not ctx.env_sources:
            ctx.logger.info("Explain config: no injected env vars for this run")
            return

        ctx.logger.info("Explain config: resolved env keys (values are hidden)")
        for k in sorted(ctx.env_sources.keys()):
            ctx.logger.info("  %s <- %s", k, ctx.env_sources.get(k))

    def post_run(self, manifest: Mapping[str, Any], ctx: RunContext, result: Any) -> None:
        return

    def on_error(self, manifest: Mapping[str, Any], ctx: RunContext, exc: Exception) -> None:
        return
