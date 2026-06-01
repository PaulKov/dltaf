from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from ..base import ExtensionInfo, NamedObjectRegistry, resolve_plugin_specs, select_named_extensions
from .builtins import build_builtin_infra_checks
from .plugin_loader import load_infra_check_plugins


@dataclass
class InfraCheckRegistry(NamedObjectRegistry[Any]):
    def __init__(self) -> None:
        super().__init__(
            key_getter=lambda check: str(getattr(check, "name", "")),
            validator=lambda obj: (
                bool(getattr(obj, "name", None))
                and callable(getattr(obj, "run", None))
                and callable(getattr(obj, "evaluate", None))
            ),
            kind_label="infra check",
        )


InfraCheckInfo = ExtensionInfo
_DEFAULT_REGISTRY: Optional[InfraCheckRegistry] = None


def get_default_infra_check_registry() -> InfraCheckRegistry:
    global _DEFAULT_REGISTRY
    if _DEFAULT_REGISTRY is None:
        reg = InfraCheckRegistry()
        for check in build_builtin_infra_checks():
            reg.register(check, origin="builtin")
        _DEFAULT_REGISTRY = reg
    return _DEFAULT_REGISTRY


def resolve_infra_check_plugins(
    *, manifest: Optional[Mapping[str, Any]], include_env_plugins: bool
) -> List[str]:
    return resolve_plugin_specs(
        manifest=manifest,
        include_env_plugins=include_env_plugins,
        env_var="DLT_INFRA_CHECK_PLUGINS",
        manifest_path=("run", "online_checks", "plugins"),
    )


def build_infra_check_registry(
    *, manifest: Optional[Mapping[str, Any]], ctx: Any, include_env_plugins: bool = True
) -> Tuple[InfraCheckRegistry, Dict[str, Any]]:
    base = get_default_infra_check_registry().clone()
    fresh = InfraCheckRegistry()
    for name in base.names():
        meta = base.info(name)
        fresh.register(base.get(name), origin=meta.origin, description=meta.description)

    plugins = resolve_infra_check_plugins(manifest=manifest, include_env_plugins=include_env_plugins)
    report: Dict[str, Any] = {"requested": [], "results": [], "errors": []}
    if plugins:
        report = load_infra_check_plugins(registry=fresh, plugin_specs=plugins)
    return fresh, report


def available_infra_check_names(
    *, manifest: Optional[Mapping[str, Any]] = None, ctx: Any = None, include_env_plugins: bool = True
) -> List[str]:
    reg, _ = build_infra_check_registry(
        manifest=manifest,
        ctx=ctx,
        include_env_plugins=include_env_plugins,
    )
    return reg.names()


def select_infra_check_names(
    *, manifest: Mapping[str, Any], ctx: Any, registry: Optional[InfraCheckRegistry] = None
) -> List[str]:
    reg = registry or get_default_infra_check_registry()
    run_cfg = manifest.get("run") or {}
    online_cfg = run_cfg.get("online_checks") if isinstance(run_cfg, Mapping) else {}
    defaults = [check.name for check in reg.all() if check.applies(manifest, ctx)]
    return select_named_extensions(
        default_names=defaults,
        config=online_cfg if isinstance(online_cfg, Mapping) else None,
    )


def run_infra_checks(
    *,
    manifest: Mapping[str, Any],
    ctx: Any,
    timeout_seconds: float = 5.0,
    registry: Optional[InfraCheckRegistry] = None,
    names_override: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    plugin_report: Dict[str, Any] = {"requested": [], "results": [], "errors": []}
    reg = registry
    if reg is None:
        reg, plugin_report = build_infra_check_registry(
            manifest=manifest,
            ctx=ctx,
            include_env_plugins=True,
        )

    names = list(names_override) if names_override is not None else select_infra_check_names(
        manifest=manifest,
        ctx=ctx,
        registry=reg,
    )

    out: Dict[str, Any] = {
        "timeout_seconds": float(timeout_seconds),
        "selected_checks": list(names),
        "plugins": plugin_report,
    }

    for name in names:
        try:
            check = reg.get(name)
        except Exception:
            out[name] = {"attempted": False, "ok": False, "reason": "unknown_check"}
            continue

        try:
            out[name] = dict(check.run(manifest, ctx, timeout_seconds=float(timeout_seconds)))
        except Exception as exc:
            out[name] = {
                "attempted": True,
                "ok": False,
                "reason": "exception",
                "error": f"{exc.__class__.__name__}: {exc}",
            }

    return out


def evaluate_infra_checks(
    checks: Mapping[str, Any], *, registry: Optional[InfraCheckRegistry] = None
) -> Dict[str, Any]:
    reg = registry or get_default_infra_check_registry()
    errors: List[str] = []
    warnings: List[str] = []

    plugins = (checks or {}).get("plugins") if isinstance(checks, Mapping) else None
    if isinstance(plugins, Mapping):
        plugin_errors = plugins.get("errors") or []
        if isinstance(plugin_errors, list):
            for error in plugin_errors:
                if str(error).strip():
                    errors.append(str(error))

    for check in reg.all():
        if check.name not in checks:
            continue
        try:
            report = check.evaluate(checks.get(check.name))
            for error in report.get("errors") or []:
                if str(error).strip():
                    errors.append(str(error))
            for warning in report.get("warnings") or []:
                if str(warning).strip():
                    warnings.append(str(warning))
        except Exception:
            errors.append(f"Infra check evaluation failed: {check.name}")

    known = set(reg.names())
    for key, value in (checks or {}).items():
        if key in {"timeout_seconds", "selected_checks", "plugins"}:
            continue
        if str(key) not in known and isinstance(value, Mapping) and str(value.get("reason") or "") == "unknown_check":
            errors.append(f"Unknown infra check name: {key}")

    return {"ok": len(errors) == 0, "errors": errors, "warnings": warnings}


__all__ = [
    "InfraCheckInfo",
    "InfraCheckRegistry",
    "available_infra_check_names",
    "build_infra_check_registry",
    "evaluate_infra_checks",
    "get_default_infra_check_registry",
    "resolve_infra_check_plugins",
    "run_infra_checks",
    "select_infra_check_names",
]
