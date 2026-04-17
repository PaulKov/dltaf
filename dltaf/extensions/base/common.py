from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Iterable, List, Mapping, Sequence


@dataclass(frozen=True)
class ExtensionInfo:
    """Human-friendly metadata about an extension (safe, no secrets)."""

    name: str
    origin: str
    description: str


def infer_description(obj: Any) -> str:
    desc = getattr(obj, "description", None)
    if desc is not None and str(desc).strip():
        return str(desc).strip()

    doc = getattr(obj, "__doc__", None)
    if doc and str(doc).strip():
        first = str(doc).strip().splitlines()[0].strip()
        if first:
            return first

    cls = getattr(obj, "__class__", None)
    if cls is not None and getattr(cls, "__name__", None):
        return str(cls.__name__)
    return type(obj).__name__


def parse_name_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        s = value.strip()
        return [s] if s else []
    if isinstance(value, (list, tuple, set)):
        out: List[str] = []
        for item in value:
            s = str(item).strip()
            if s:
                out.append(s)
        return out
    return []


def merge_unique(items: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for item in items:
        s = str(item).strip()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def split_plugin_specs(raw: str) -> List[str]:
    if not str(raw or "").strip():
        return []

    parts: List[str] = [str(raw)]
    for sep in (",", ";", "\n"):
        next_parts: List[str] = []
        for part in parts:
            next_parts.extend(part.split(sep))
        parts = next_parts

    return merge_unique(parts)


def parse_plugin_specs_from_env(env_var: str) -> List[str]:
    return split_plugin_specs(str(os.getenv(env_var, "") or ""))


def parse_plugin_specs_from_manifest(manifest: Mapping[str, Any] | None, path: Sequence[str]) -> List[str]:
    if not manifest:
        return []

    node: Any = manifest
    for segment in path:
        if not isinstance(node, Mapping):
            return []
        node = node.get(segment)

    return parse_name_list(node)


def resolve_plugin_specs(
    *,
    manifest: Mapping[str, Any] | None,
    include_env_plugins: bool,
    env_var: str,
    manifest_path: Sequence[str],
) -> List[str]:
    merged: List[str] = []
    if include_env_plugins:
        merged.extend(parse_plugin_specs_from_env(env_var))
    merged.extend(parse_plugin_specs_from_manifest(manifest, manifest_path))
    return merge_unique(merged)


def select_named_extensions(
    *,
    default_names: Sequence[str],
    config: Mapping[str, Any] | None,
) -> List[str]:
    cfg = config or {}
    only = parse_name_list(cfg.get("only")) if isinstance(cfg, Mapping) else []
    enable = parse_name_list(cfg.get("enable")) if isinstance(cfg, Mapping) else []
    disable = set(parse_name_list(cfg.get("disable")) if isinstance(cfg, Mapping) else [])

    if only:
        return merge_unique(only)

    selected = [name for name in default_names if str(name).strip() not in disable]
    for name in enable:
        if name not in selected:
            selected.append(name)
    return selected


__all__ = [
    "ExtensionInfo",
    "infer_description",
    "merge_unique",
    "parse_name_list",
    "parse_plugin_specs_from_env",
    "parse_plugin_specs_from_manifest",
    "resolve_plugin_specs",
    "select_named_extensions",
    "split_plugin_specs",
]
