"""Compatibility shim for run planning / dry-run helpers.

Stage 35
--------
The implementation now lives in `dltaf.services.execution.planner`.
"""

from dltaf.app.deprecation import warn_legacy_module

warn_legacy_module(__name__, replacement="dltaf.services.execution.planner")

from dltaf.services.execution.planner import (
    DryRunStrictError,
    ExecutionPlanner,
    build_run_plan,
    dry_run_checks,
    dry_run_online_checks,
    evaluate_dry_run_checks,
    evaluate_dry_run_online_checks,
    execute_plan_or_dry_run,
    merge_reports,
)

__all__ = [
    "DryRunStrictError",
    "ExecutionPlanner",
    "build_run_plan",
    "dry_run_checks",
    "dry_run_online_checks",
    "evaluate_dry_run_checks",
    "evaluate_dry_run_online_checks",
    "execute_plan_or_dry_run",
    "merge_reports",
]
