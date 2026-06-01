from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Tuple

from ..base import ExtensionInfo, NamedObjectRegistry, resolve_plugin_specs
from .builtins import build_builtin_runners
from .plugin_loader import load_runner_plugins


@dataclass
class RunnerRegistry(NamedObjectRegistry[Any]):
    def kinds(self) -> List[str]:
        return self.names()

    def __init__(self) -> None:
        super().__init__(
            key_getter=lambda runner: str(getattr(runner, "kind", "")),
            validator=lambda obj: (
                bool(getattr(obj, "kind", None))
                and callable(getattr(obj, "validate", None))
                and callable(getattr(obj, "run", None))
            ),
            kind_label="runner",
        )


RunnerInfo = ExtensionInfo
_DEFAULT_REGISTRY: Optional[RunnerRegistry] = None


def get_default_registry() -> RunnerRegistry:
    global _DEFAULT_REGISTRY
    if _DEFAULT_REGISTRY is None:
        reg = RunnerRegistry()
        for runner in build_builtin_runners():
            reg.register(runner, origin="builtin")
        _DEFAULT_REGISTRY = reg
    return _DEFAULT_REGISTRY


def resolve_runner_plugins(
    *, manifest: Optional[Mapping[str, Any]], include_env_plugins: bool
) -> List[str]:
    return resolve_plugin_specs(
        manifest=manifest,
        include_env_plugins=include_env_plugins,
        env_var="DLT_RUNNER_PLUGINS",
        manifest_path=("run", "runners", "plugins"),
    )


def build_runner_registry(
    *, manifest: Optional[Mapping[str, Any]], include_env_plugins: bool = True
) -> Tuple[RunnerRegistry, Dict[str, Any]]:
    fresh = RunnerRegistry()
    base = get_default_registry()
    for name in base.names():
        meta = base.info(name)
        fresh.register(base.get(name), origin=meta.origin, description=meta.description)

    plugins = resolve_runner_plugins(manifest=manifest, include_env_plugins=include_env_plugins)
    report: Dict[str, Any] = {"requested": [], "results": [], "errors": []}
    if plugins:
        report = load_runner_plugins(registry=fresh, plugin_specs=plugins)
    return fresh, report


def available_runner_kinds(
    *, manifest: Optional[Mapping[str, Any]] = None, include_env_plugins: bool = True
) -> List[str]:
    reg, _ = build_runner_registry(manifest=manifest, include_env_plugins=include_env_plugins)
    return reg.names()


__all__ = [
    "RunnerInfo",
    "RunnerRegistry",
    "available_runner_kinds",
    "build_runner_registry",
    "get_default_registry",
    "resolve_runner_plugins",
]
