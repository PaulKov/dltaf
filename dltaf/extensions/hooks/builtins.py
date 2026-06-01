from __future__ import annotations

from typing import Callable, Dict

from .protocol import Hook

DEFAULT_HOOK_NAMES = [
    "basic_logging",
    "env_summary",
    "dlt_progress_control",
    "runtime_summary",
    "explain_config",
    "audit_run",
]


def build_builtin_hook_factories() -> Dict[str, Callable[[], Hook]]:
    from dlt_utils.hooks.audit_run import AuditRunHook
    from dlt_utils.hooks.basic_logging import BasicLoggingHook
    from dlt_utils.hooks.dlt_progress_control import DltProgressControlHook
    from dlt_utils.hooks.env_summary import EnvSummaryHook
    from dlt_utils.hooks.explain_config import ExplainConfigHook
    from dlt_utils.hooks.replace_policy import ReplacePolicyHook
    from dlt_utils.hooks.runtime_summary import RuntimeSummaryHook

    return {
        "basic_logging": BasicLoggingHook,
        "env_summary": EnvSummaryHook,
        "dlt_progress_control": DltProgressControlHook,
        "runtime_summary": RuntimeSummaryHook,
        "explain_config": ExplainConfigHook,
        "audit_run": AuditRunHook,
        "replace_policy": ReplacePolicyHook,
    }


__all__ = ["DEFAULT_HOOK_NAMES", "build_builtin_hook_factories"]
