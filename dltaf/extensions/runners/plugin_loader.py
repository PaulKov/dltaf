from __future__ import annotations

from typing import Any, Dict, Iterable, List, Mapping, Sequence

from ..base import PluginConventions, load_module_plugins


CONVENTIONS = PluginConventions(
    kind_label="runner",
    register_function_names=("register_runners",),
    collection_attribute_names=("RUNNERS",),
)


def _is_runner_obj(obj: Any) -> bool:
    return (
        bool(getattr(obj, "kind", None))
        and callable(getattr(obj, "validate", None))
        and callable(getattr(obj, "run", None))
    )


def register_many_runners(registry: Any, items: Any) -> List[str]:
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
        if _is_runner_obj(item):
            registry.register(item)
            registered.append(str(getattr(item, "kind")))
            continue
        if isinstance(item, type) or callable(item):
            try:
                instance = item()
            except Exception as exc:
                raise ValueError(f"Failed to instantiate runner {item}: {exc}") from exc
            if not _is_runner_obj(instance):
                raise ValueError(f"Object from {item} is not a SourceRunner")
            registry.register(instance)
            registered.append(str(getattr(instance, "kind")))
            continue
        raise ValueError(f"Unsupported runner object: {type(item)}")

    return registered


def load_runner_plugins(*, registry: Any, plugin_specs: Sequence[str]) -> Dict[str, Any]:
    return load_module_plugins(
        registry=registry,
        plugin_specs=plugin_specs,
        conventions=CONVENTIONS,
        register_payload=register_many_runners,
    )


__all__ = ["load_runner_plugins", "register_many_runners"]
