from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Optional, Sequence

from dltaf.extensions.infra_checks import build_infra_check_registry, evaluate_infra_checks, run_infra_checks


@dataclass(frozen=True)
class OnlineChecksService:
    """Runtime service for infrastructure checks."""

    def run(
        self,
        *,
        manifest: Mapping[str, Any],
        ctx: Any,
        timeout_seconds: float,
        registry: Optional[Any] = None,
        names_override: Optional[Sequence[str]] = None,
    ) -> Mapping[str, Any]:
        return run_infra_checks(
            manifest=manifest,
            ctx=ctx,
            timeout_seconds=timeout_seconds,
            registry=registry,
            names_override=names_override,
        )

    def evaluate(self, checks: Mapping[str, Any], *, registry: Optional[Any] = None) -> Mapping[str, Any]:
        return evaluate_infra_checks(checks, registry=registry)

    def build_registry(self, *, manifest: Mapping[str, Any], ctx: Any, include_env_plugins: bool = True) -> Any:
        return build_infra_check_registry(manifest=manifest, ctx=ctx, include_env_plugins=include_env_plugins)


__all__ = [
    "OnlineChecksService",
    "build_infra_check_registry",
    "evaluate_infra_checks",
    "run_infra_checks",
]
