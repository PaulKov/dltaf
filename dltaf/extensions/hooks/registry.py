from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Tuple, cast

from ..base import ExtensionInfo, NamedFactoryRegistry, resolve_plugin_specs, select_named_extensions
from .builtins import DEFAULT_HOOK_NAMES, build_builtin_hook_factories
from .plugin_loader import load_hook_plugins
from .protocol import Hook


@dataclass
class HookRegistry(NamedFactoryRegistry[Hook]):
    def __init__(self) -> None:
        super().__init__(
            validator=lambda obj: (
                bool(getattr(obj, "name", None))
                and callable(getattr(obj, "pre_run", None))
                and callable(getattr(obj, "post_run", None))
                and callable(getattr(obj, "on_error", None))
            ),
            kind_label="hook",
        )

    def register(self, hook_or_factory: Any, *, origin: Optional[str] = None) -> None:
        if hook_or_factory is None:
            raise ValueError("Hook cannot be None")

        if isinstance(hook_or_factory, type):
            try:
                instance = hook_or_factory()
            except Exception as exc:
                raise ValueError(f"Failed to instantiate hook class {hook_or_factory}: {exc}") from exc
            if not self.validator(instance):
                raise ValueError(f"Object from {hook_or_factory} is not a Hook")
            name = str(getattr(instance, "name", "")).strip()
            if not name:
                raise ValueError("Hook.name must be a non-empty string")
            self.register_factory(name, hook_or_factory, origin=origin)
            return

        if self.validator(hook_or_factory):
            instance = hook_or_factory
            name = str(getattr(instance, "name", "")).strip()
            if not name:
                raise ValueError("Hook.name must be a non-empty string")

            def _factory() -> Hook:
                return cast(Hook, instance)

            self.register_factory(name, _factory, origin=origin)
            return

        if callable(hook_or_factory):
            try:
                instance = hook_or_factory()
            except Exception as exc:
                raise ValueError(f"Hook factory failed to create hook: {exc}") from exc
            if not self.validator(instance):
                raise ValueError("Hook factory produced an invalid Hook")
            name = str(getattr(instance, "name", "")).strip()
            if not name:
                raise ValueError("Hook.name must be a non-empty string")
            self.register_factory(name, hook_or_factory, origin=origin)
            return

        raise ValueError(f"Unsupported hook object: {type(hook_or_factory)}")


HookInfo = ExtensionInfo
_DEFAULT_HOOK_REGISTRY: Optional[HookRegistry] = None


def get_default_hook_registry() -> HookRegistry:
    global _DEFAULT_HOOK_REGISTRY
    if _DEFAULT_HOOK_REGISTRY is None:
        reg = HookRegistry()
        for name, factory in build_builtin_hook_factories().items():
            reg.register_factory(name, factory, origin="builtin")
        _DEFAULT_HOOK_REGISTRY = reg
    return _DEFAULT_HOOK_REGISTRY


def resolve_hook_plugins(*, manifest: Optional[Mapping[str, Any]], include_env_plugins: bool) -> List[str]:
    return resolve_plugin_specs(
        manifest=manifest,
        include_env_plugins=include_env_plugins,
        env_var="DLT_HOOK_PLUGINS",
        manifest_path=("run", "hooks", "plugins"),
    )


def build_hook_registry(
    *, manifest: Optional[Mapping[str, Any]], include_env_plugins: bool = True
) -> Tuple[HookRegistry, Dict[str, Any]]:
    fresh = HookRegistry()
    base = get_default_hook_registry()
    for name in base.names():
        meta = base.info(name)
        fresh.register_factory(name, base._factories[name], origin=meta.origin, description=meta.description)

    plugins = resolve_hook_plugins(manifest=manifest, include_env_plugins=include_env_plugins)
    report: Dict[str, Any] = {"requested": [], "results": [], "errors": []}
    if plugins:
        report = load_hook_plugins(registry=fresh, plugin_specs=plugins)
    return fresh, report


def available_hook_names(
    *, manifest: Optional[Mapping[str, Any]] = None, include_env_plugins: bool = True
) -> List[str]:
    reg, _ = build_hook_registry(manifest=manifest, include_env_plugins=include_env_plugins)
    return reg.names()


def select_hook_names(manifest: Mapping[str, Any], *, registry: Optional[HookRegistry] = None) -> List[str]:
    reg = registry or get_default_hook_registry()
    run_cfg = manifest.get("run") or {}
    hooks_cfg = run_cfg.get("hooks") if isinstance(run_cfg, Mapping) else {}
    defaults = [name for name in DEFAULT_HOOK_NAMES if name in reg.names()]
    return select_named_extensions(default_names=defaults, config=hooks_cfg if isinstance(hooks_cfg, Mapping) else None)


def build_default_hooks() -> List[Hook]:
    return [get_default_hook_registry().create(name) for name in DEFAULT_HOOK_NAMES if name in get_default_hook_registry().names()]


def build_hooks(manifest: Mapping[str, Any], ctx: Any) -> List[Hook]:
    reg, plugin_report = build_hook_registry(manifest=manifest, include_env_plugins=True)
    plugin_errors = plugin_report.get("errors") if isinstance(plugin_report, Mapping) else None
    if isinstance(plugin_errors, list) and plugin_errors:
        raise ValueError("Hook plugins failed to load: " + "; ".join(plugin_errors))
    return [reg.create(name) for name in select_hook_names(manifest, registry=reg)]


__all__ = [
    "DEFAULT_HOOK_NAMES",
    "HookInfo",
    "HookRegistry",
    "available_hook_names",
    "build_default_hooks",
    "build_hook_registry",
    "build_hooks",
    "get_default_hook_registry",
    "resolve_hook_plugins",
    "select_hook_names",
]
