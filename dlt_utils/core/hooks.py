"""Backward-compatible hook facade.

Stage 36 / Wave 3 migrated hook registries and pipelines into `dltaf.extensions.hooks.*`.
This module remains as a compatibility import path for legacy code and plugins.
"""

from dltaf.app.deprecation import warn_legacy_module

warn_legacy_module(__name__, replacement="dltaf.extensions.hooks")

from dltaf.extensions.hooks import (
    DEFAULT_HOOK_NAMES,
    Hook,
    HookInfo,
    HookPipeline,
    HookRegistry,
    available_hook_names,
    build_default_hooks,
    build_hook_registry,
    build_hooks,
    get_default_hook_registry,
    load_hook_plugins,
    resolve_hook_plugins,
    select_hook_names,
)

__all__ = [
    "DEFAULT_HOOK_NAMES",
    "Hook",
    "HookInfo",
    "HookPipeline",
    "HookRegistry",
    "available_hook_names",
    "build_default_hooks",
    "build_hook_registry",
    "build_hooks",
    "get_default_hook_registry",
    "load_hook_plugins",
    "resolve_hook_plugins",
    "select_hook_names",
]
