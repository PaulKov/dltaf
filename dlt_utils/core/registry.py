"""Backward-compatible runner registry facade.

Stage 36 / Wave 3 migrated the implementation into `dltaf.extensions.runners.*`.
This module remains as a compatibility import path for legacy code and plugins.
"""

from dltaf.app.deprecation import warn_legacy_module

warn_legacy_module(__name__, replacement="dltaf.extensions.runners")

from dltaf.extensions.runners import (
    RunnerInfo,
    RunnerRegistry,
    available_runner_kinds,
    build_runner_registry,
    get_default_registry,
    load_runner_plugins,
    resolve_runner_plugins,
)

__all__ = [
    "RunnerInfo",
    "RunnerRegistry",
    "available_runner_kinds",
    "build_runner_registry",
    "get_default_registry",
    "load_runner_plugins",
    "resolve_runner_plugins",
]
