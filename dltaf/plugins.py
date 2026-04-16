from __future__ import annotations

import hashlib
import importlib
import importlib.metadata
import importlib.util
import inspect
import json
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType
from typing import Any, Callable, Iterable, Mapping, Optional, Sequence


PluginValidator = Callable[[Mapping[str, Any]], None]
PluginRunner = Callable[[Mapping[str, Any]], Any]
PluginEnvBuilder = Callable[[Mapping[str, Any]], Mapping[str, str]]


@dataclass(frozen=True)
class SourcePlugin:
    kind: str
    run: PluginRunner
    validate: Optional[PluginValidator] = None
    build_runtime_env: Optional[PluginEnvBuilder] = None
    aliases: tuple[str, ...] = ()
    display_name: Optional[str] = None
    docs_url: Optional[str] = None
    required_packages: tuple[str, ...] = ()
    healthcheck_hint: Optional[str] = None


@dataclass(frozen=True)
class RegisteredSourcePlugin:
    plugin: SourcePlugin
    origin: str
    source: str
    requested_kind: str
    matched_kind: str

    @property
    def kind(self) -> str:
        return self.plugin.kind

    @property
    def aliases(self) -> tuple[str, ...]:
        return self.plugin.aliases


@dataclass
class PluginRegistry:
    _plugins: dict[str, tuple[SourcePlugin, str, str]] = field(default_factory=dict)
    _alias_map: dict[str, str] = field(default_factory=dict)
    discovery_errors: list[str] = field(default_factory=list)

    def register(self, plugin: SourcePlugin, *, origin: str, source: str) -> None:
        key = _normalize_kind(plugin.kind)
        if key in self._plugins or key in self._alias_map:
            raise ValueError(f"Duplicate source.kind registration: {plugin.kind}")
        self._plugins[key] = (plugin, origin, source)
        for alias in plugin.aliases:
            alias_key = _normalize_kind(alias)
            if alias_key == key:
                continue
            if alias_key in self._plugins or alias_key in self._alias_map:
                raise ValueError(f"Duplicate source.kind alias registration: {alias}")
            self._alias_map[alias_key] = key

    def resolve(self, requested_kind: str) -> RegisteredSourcePlugin:
        normalized = _normalize_kind(requested_kind)
        canonical = self._alias_map.get(normalized, normalized)
        plugin_info = self._plugins.get(canonical)
        if plugin_info is None:
            raise SourcePluginNotFoundError(
                requested_kind=requested_kind,
                available_kinds=self.available_kinds(),
                discovery_errors=list(self.discovery_errors),
            )
        plugin, origin, source = plugin_info
        return RegisteredSourcePlugin(
            plugin=plugin,
            origin=origin,
            source=source,
            requested_kind=requested_kind,
            matched_kind=canonical,
        )

    def available_kinds(self) -> list[str]:
        kinds = [plugin.kind for plugin, _, _ in self._plugins.values()]
        return sorted(kinds)

    def records(self) -> list[RegisteredSourcePlugin]:
        records: list[RegisteredSourcePlugin] = []
        for canonical, (plugin, origin, source) in sorted(self._plugins.items()):
            records.append(
                RegisteredSourcePlugin(
                    plugin=plugin,
                    origin=origin,
                    source=source,
                    requested_kind=canonical,
                    matched_kind=canonical,
                )
            )
        return records

    def remember_error(self, message: str) -> None:
        self.discovery_errors.append(message)


class SourcePluginError(RuntimeError):
    pass


class SourcePluginNotFoundError(SourcePluginError):
    def __init__(
        self,
        *,
        requested_kind: str,
        available_kinds: Sequence[str],
        discovery_errors: Sequence[str],
    ) -> None:
        self.requested_kind = requested_kind
        self.available_kinds = list(available_kinds)
        self.discovery_errors = list(discovery_errors)

        hint = ", ".join(self.available_kinds) if self.available_kinds else "<none>"
        message = f"Unsupported source.kind: {requested_kind!r}. Available source kinds: {hint}."
        if self.discovery_errors:
            message += " Plugin discovery errors: " + " | ".join(self.discovery_errors)
        super().__init__(message)


class SourcePluginValidationError(SourcePluginError):
    def __init__(self, *, plugin: RegisteredSourcePlugin, cause: Exception) -> None:
        details = str(cause).strip() or cause.__class__.__name__
        super().__init__(
            f"Validation failed for source.kind {plugin.requested_kind!r} "
            f"(resolved to {plugin.kind!r} from {plugin.origin}): {details}"
        )
        self.plugin = plugin
        self.cause = cause


def _normalize_kind(kind: str) -> str:
    return str(kind or "").strip().lower()


def _parse_env_list(raw: str) -> list[str]:
    value = (raw or "").strip()
    if not value:
        return []
    if value.startswith("["):
        try:
            payload = json.loads(value)
            if isinstance(payload, list):
                return [str(item).strip() for item in payload if str(item).strip()]
        except Exception:
            pass
    return [part.strip() for part in value.split(",") if part.strip()]


def _unique_paths(paths: Iterable[Path]) -> list[Path]:
    unique: list[Path] = []
    seen: set[Path] = set()
    for path in paths:
        resolved = path.expanduser().resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        unique.append(resolved)
    return unique


def _module_name_for_path(path: Path) -> str:
    digest = hashlib.sha1(str(path).encode("utf-8")).hexdigest()[:12]
    stem = path.stem if path.is_file() else path.name
    return f"_dltaf_plugin_{stem}_{digest}"


def _load_module_from_path(path: Path) -> ModuleType:
    if path.is_dir():
        init_py = path / "__init__.py"
        if not init_py.exists():
            raise ValueError(f"Plugin package path must contain __init__.py: {path}")
        spec = importlib.util.spec_from_file_location(
            _module_name_for_path(path),
            init_py,
            submodule_search_locations=[str(path)],
        )
    else:
        spec = importlib.util.spec_from_file_location(_module_name_for_path(path), path)

    if spec is None or spec.loader is None:
        raise ValueError(f"Failed to build import spec for plugin path: {path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _register_plugins_from_object(
    exported: Any,
    *,
    registry: PluginRegistry,
    origin: str,
    source: str,
) -> None:
    if isinstance(exported, SourcePlugin):
        registry.register(exported, origin=origin, source=source)
        return

    if isinstance(exported, Iterable) and not isinstance(exported, (str, bytes, Mapping, ModuleType)):
        for item in exported:
            _register_plugins_from_object(item, registry=registry, origin=origin, source=source)
        return

    if inspect.isfunction(exported):
        _register_plugins_from_object(exported(), registry=registry, origin=origin, source=source)
        return

    if isinstance(exported, ModuleType):
        if hasattr(exported, "register_plugins"):
            exported.register_plugins(registry)
            return
        if hasattr(exported, "get_plugins"):
            _register_plugins_from_object(
                exported.get_plugins(),
                registry=registry,
                origin=origin,
                source=source,
            )
            return
        if hasattr(exported, "PLUGINS"):
            _register_plugins_from_object(
                exported.PLUGINS,
                registry=registry,
                origin=origin,
                source=source,
            )
            return

    raise ValueError(f"Unsupported plugin export from {source}: {exported!r}")


def _discover_entrypoint_plugins(registry: PluginRegistry) -> None:
    try:
        entry_points = importlib.metadata.entry_points()
        candidates = (
            entry_points.select(group="dltaf.plugins")
            if hasattr(entry_points, "select")
            else entry_points.get("dltaf.plugins", [])
        )
    except Exception as exc:
        registry.remember_error(f"entry point discovery failed: {exc}")
        return

    for entry_point in candidates:
        try:
            exported = entry_point.load()
            _register_plugins_from_object(
                exported,
                registry=registry,
                origin="entry_point",
                source=f"{entry_point.module}:{entry_point.attr or ''}".rstrip(":"),
            )
        except Exception as exc:
            registry.remember_error(f"entry point {entry_point.name} failed: {exc}")


def _discover_module_plugins(registry: PluginRegistry, module_names: Sequence[str]) -> None:
    for module_name in module_names:
        try:
            exported = importlib.import_module(module_name)
            _register_plugins_from_object(
                exported,
                registry=registry,
                origin="module",
                source=module_name,
            )
        except Exception as exc:
            registry.remember_error(f"plugin module {module_name} failed: {exc}")


def _iter_plugin_path_targets(path: Path) -> list[Path]:
    if path.is_file():
        return [path]

    if path.is_dir() and (path / "__init__.py").exists():
        return [path]

    targets: list[Path] = []
    for child in sorted(path.iterdir()):
        if child.name.startswith(("_", ".")):
            continue
        if child.name == "plugin_template.py":
            continue
        if child.is_file() and child.suffix == ".py":
            targets.append(child)
        elif child.is_dir() and (child / "__init__.py").exists():
            targets.append(child)
    return targets


def _discover_path_plugins(registry: PluginRegistry, plugin_paths: Sequence[Path]) -> None:
    for plugin_path in _unique_paths(plugin_paths):
        if not plugin_path.exists():
            registry.remember_error(f"plugin path does not exist: {plugin_path}")
            continue
        try:
            targets = _iter_plugin_path_targets(plugin_path)
        except Exception as exc:
            registry.remember_error(f"failed to scan plugin path {plugin_path}: {exc}")
            continue

        for target in targets:
            try:
                exported = _load_module_from_path(target)
                _register_plugins_from_object(
                    exported,
                    registry=registry,
                    origin="local_path",
                    source=str(target),
                )
            except Exception as exc:
                registry.remember_error(f"plugin path target {target} failed: {exc}")


def build_source_registry(
    *,
    builtins: Sequence[SourcePlugin] = (),
    plugin_paths: Sequence[Path] = (),
    plugin_modules: Sequence[str] = (),
) -> PluginRegistry:
    registry = PluginRegistry()
    for plugin in builtins:
        registry.register(plugin, origin="builtin", source=plugin.kind)

    _discover_entrypoint_plugins(registry)
    _discover_module_plugins(registry, plugin_modules)
    _discover_path_plugins(registry, plugin_paths)
    return registry


def env_plugin_paths(*, default_paths: Sequence[Path] = ()) -> list[Path]:
    parsed = [Path(item) for item in _parse_env_list(os.getenv("DLTAF_PLUGIN_PATHS", ""))]
    return _unique_paths([*default_paths, *parsed])


def env_plugin_modules() -> list[str]:
    return _parse_env_list(os.getenv("DLTAF_PLUGIN_MODULES", ""))


def scaffold_plugin_source(*, kind: str, module_name: str, legacy_alias: Optional[str] = None) -> str:
    alias_block = f'        aliases=("{legacy_alias}",),\n' if legacy_alias else ""
    return f'''from __future__ import annotations

from typing import Any, Mapping

from dltaf.plugins import SourcePlugin


def validate(manifest: Mapping[str, Any]) -> None:
    source = manifest.get("source") or {{}}
    if not isinstance(source, Mapping):
        raise ValueError("source section must be a mapping")
    # TODO: add plugin-specific validation.


def build_runtime_env(manifest: Mapping[str, Any]) -> dict[str, str]:
    # TODO: optionally return plugin-specific runtime env vars.
    return {{}}


def run(manifest: Mapping[str, Any]) -> Any:
    raise NotImplementedError("Implement plugin runner for {kind}")


PLUGINS = [
    SourcePlugin(
        kind="{kind}",
{alias_block}        validate=validate,
        build_runtime_env=build_runtime_env,
        run=run,
        display_name="{kind}",
        docs_url="README.md",
        healthcheck_hint="Document network dependencies, credentials, and smoke checks.",
    )
]
'''
