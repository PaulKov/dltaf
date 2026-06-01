"""Example hook plugin.

This module demonstrates the minimal contract expected by the framework.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from dlt_utils.core.context import RunContext
from dlt_utils.core.hooks import HookRegistry


@dataclass
class ExampleHook:
    """Example hook that logs a small safe banner."""

    name: str = "example_hook"
    description: str = "Example hook plugin (safe banner logging)."

    def pre_run(self, manifest: Mapping[str, Any], ctx: RunContext) -> None:
        pipeline = manifest.get("pipeline") or {}
        ctx.logger.info("[example_hook] starting pipeline=%s", pipeline.get("name"))

    def post_run(self, manifest: Mapping[str, Any], ctx: RunContext, result: Any) -> None:
        ctx.logger.info("[example_hook] finished status=%s", getattr(result, "status", "unknown"))

    def on_error(self, manifest: Mapping[str, Any], ctx: RunContext, exc: Exception) -> None:
        ctx.logger.info("[example_hook] error=%s", exc.__class__.__name__)


def register_hooks(registry: HookRegistry) -> None:
    """Register hooks into the framework registry."""

    registry.register(ExampleHook)
