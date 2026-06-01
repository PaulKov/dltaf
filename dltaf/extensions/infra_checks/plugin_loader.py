from __future__ import annotations

from typing import Any, Dict, Iterable, List, Mapping, Sequence

from ..base import PluginConventions, load_module_plugins


CONVENTIONS = PluginConventions(
    kind_label="infra check",
    register_function_names=("register_infra_checks",),
    collection_attribute_names=("INFRA_CHECKS",),
)


def _is_check_obj(obj: Any) -> bool:
    return bool(getattr(obj, "name", None)) and callable(getattr(obj, "run", None)) and callable(
        getattr(obj, "evaluate", None)
    )


def register_many_infra_checks(registry: Any, items: Any) -> List[str]:
    registered: List[str] = []
    if items is None:
        return registered

    if isinstance(items, Mapping):
        values: Iterable[Any] = items.values()
    elif isinstance(items, (list, tuple, set)):
        values = items
    else:
        values = [items]

    for item in values:
        if item is None:
            continue
        if _is_check_obj(item):
            registry.register(item)
            registered.append(str(getattr(item, "name")))
            continue
        if isinstance(item, type):
            try:
                inst = item()
            except Exception as exc:
                raise ValueError(f"Failed to instantiate infra check class {item}: {exc}") from exc
            if not _is_check_obj(inst):
                raise ValueError(f"Object from {item} is not an InfraCheck")
            registry.register(inst)
            registered.append(str(getattr(inst, "name")))
            continue
        raise ValueError(f"Unsupported infra check object: {type(item)}")

    return registered


def load_infra_check_plugins(*, registry: Any, plugin_specs: Sequence[str]) -> Dict[str, Any]:
    return load_module_plugins(
        registry=registry,
        plugin_specs=plugin_specs,
        conventions=CONVENTIONS,
        register_payload=register_many_infra_checks,
    )


__all__ = ["load_infra_check_plugins", "register_many_infra_checks"]
