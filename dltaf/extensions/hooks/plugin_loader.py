from __future__ import annotations

from typing import Any, Dict, Iterable, List, Mapping, Sequence

from ..base import PluginConventions, load_module_plugins


CONVENTIONS = PluginConventions(
    kind_label="hook",
    register_function_names=("register_hooks",),
    collection_attribute_names=("HOOKS", "HOOK_FACTORIES"),
)


def _is_hook_obj(obj: Any) -> bool:
    return (
        bool(getattr(obj, "name", None))
        and callable(getattr(obj, "pre_run", None))
        and callable(getattr(obj, "post_run", None))
        and callable(getattr(obj, "on_error", None))
    )


def register_many_hooks(registry: Any, items: Any) -> List[str]:
    registered: List[str] = []
    if items is None:
        return registered

    if isinstance(items, Mapping):
        if all(callable(v) for v in items.values()):
            for name, factory in items.items():
                registry.register_factory(str(name), factory)
                registered.append(str(name))
            return registered
        values: Iterable[Any] = items.values()
    elif isinstance(items, (list, tuple, set)):
        values = items
    else:
        values = [items]

    for item in values:
        if item is None:
            continue
        registry.register(item)
        if _is_hook_obj(item):
            registered.append(str(getattr(item, "name")))
        elif isinstance(item, type):
            try:
                inst = item()
            except Exception as exc:
                raise ValueError(f"Failed to instantiate hook class {item}: {exc}") from exc
            registered.append(str(getattr(inst, "name")))
        elif callable(item):
            try:
                inst = item()
            except Exception as exc:
                raise ValueError(f"Hook factory failed to create hook: {exc}") from exc
            registered.append(str(getattr(inst, "name")))
        else:
            raise ValueError(f"Unsupported hook object: {type(item)}")

    return registered


def load_hook_plugins(*, registry: Any, plugin_specs: Sequence[str]) -> Dict[str, Any]:
    return load_module_plugins(
        registry=registry,
        plugin_specs=plugin_specs,
        conventions=CONVENTIONS,
        register_payload=register_many_hooks,
    )


__all__ = ["load_hook_plugins", "register_many_hooks"]
