"""Compatibility shim for non-executing run modes.

Stage 35
--------
`execute_plan_or_dry_run` now lives in `dltaf.services.execution.planner`.
"""

from dltaf.app.deprecation import warn_legacy_module

warn_legacy_module(__name__, replacement="dltaf.services.execution.executor")

from dltaf.services.execution.planner import execute_plan_or_dry_run

__all__ = ["execute_plan_or_dry_run"]
