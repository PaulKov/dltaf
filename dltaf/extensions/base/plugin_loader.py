from __future__ import annotations

import importlib
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Sequence


@dataclass(frozen=True)
class PluginConventions:
    kind_label: str
    register_function_names: Sequence[str]
    collection_attribute_names: Sequence[str]

    def format_error(self, module_path: str) -> str:
        options = []
        options.extend([f"{name}(registry)" for name in self.register_function_names])
        options.extend(self.collection_attribute_names)
        return (
            f"Plugin {module_path!r} must define "
            + " or ".join(options)
        )


def load_module_plugins(
    *,
    registry: Any,
    plugin_specs: Sequence[str],
    conventions: PluginConventions,
    register_payload: Callable[[Any, Any], List[str]],
) -> Dict[str, Any]:
    """Generic module-plugin loader used by runners/hooks/infra checks."""

    results: List[Dict[str, Any]] = []
    errors: List[str] = []

    for raw_spec in plugin_specs:
        spec = str(raw_spec or "").strip()
        if not spec:
            continue

        before = set(registry.names())
        module_path = spec
        attr_name: Optional[str] = None
        if ":" in spec:
            module_path, attr_name = spec.split(":", 1)
            module_path = module_path.strip()
            attr_name = attr_name.strip() or None

        try:
            mod = importlib.import_module(module_path)
            with registry.default_origin(f"plugin:{spec}"):
                if attr_name:
                    obj = getattr(mod, attr_name)
                    if callable(obj):
                        obj(registry)
                    else:
                        register_payload(registry, obj)
                else:
                    handled = False
                    for fn_name in conventions.register_function_names:
                        reg_fn = getattr(mod, fn_name, None)
                        if callable(reg_fn):
                            reg_fn(registry)
                            handled = True
                            break
                    if not handled:
                        for attr in conventions.collection_attribute_names:
                            if hasattr(mod, attr):
                                register_payload(registry, getattr(mod, attr))
                                handled = True
                                break
                    if not handled:
                        raise ValueError(conventions.format_error(module_path))

            after = set(registry.names())
            added = sorted(after - before)
            results.append({"plugin": spec, "ok": True, "registered": added, "error": None})
        except Exception as exc:
            msg = (
                f"{conventions.kind_label.capitalize()} plugin failed to load: {spec} "
                f"({exc.__class__.__name__}: {exc})"
            )
            errors.append(msg)
            results.append({"plugin": spec, "ok": False, "registered": [], "error": msg})

    return {"requested": list(plugin_specs), "results": results, "errors": errors}


__all__ = ["PluginConventions", "load_module_plugins"]
