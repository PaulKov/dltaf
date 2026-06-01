"""Example runner plugin.

This module demonstrates how to register a new SourceRunner kind without
changing core code.

Important: this runner does NOT execute dlt. It is intended only as a minimal
example and for unit tests.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from dlt_utils.core.context import RunContext
from dlt_utils.core.registry import RunnerRegistry


@dataclass
class ExampleNoopRunner:
    """No-op runner used only for tests/examples."""

    kind: str = "example_noop"

    def validate(self, manifest: Mapping[str, Any]) -> None:
        # Minimal validation: ensure pipeline section exists.
        pipeline = manifest.get("pipeline") or {}
        if not pipeline or not str(pipeline.get("name") or "").strip():
            raise ValueError("pipeline.name is required")

    def run(self, manifest: Mapping[str, Any], ctx: RunContext) -> Any:
        ctx.logger.info("[example_noop] run called")
        return {"status": "noop"}


def register_runners(registry: RunnerRegistry) -> None:
    registry.register(ExampleNoopRunner())
