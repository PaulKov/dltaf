from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, Mapping

import yaml

from .normalizer import (
    CATALOG_TOP_LEVEL_FIELDS,
    DIALECT_TOP_LEVEL_FIELDS,
    QUERY_TOP_LEVEL_FIELDS,
    normalize_sqldb_manifest,
)

_CANONICAL_SOURCE_FIELDS = {
    "kind",
    "dialect",
    "mode",
    "name",
    "payload_contract",
    "time_window",
    "catalog",
    "query",
    "dialect_options",
}

_LEGACY_SQL_LAYOUT_FIELDS = set(CATALOG_TOP_LEVEL_FIELDS) | set(QUERY_TOP_LEVEL_FIELDS) | set(DIALECT_TOP_LEVEL_FIELDS)
_SQLISH_KINDS = {"sqldb", "sql_database", "oracle_custom_sql", "oracle"}


def is_sqlish_source_kind(kind: str) -> bool:
    return str(kind or "").strip() in _SQLISH_KINDS


def canonicalize_sqldb_source_layout(source: Mapping[str, Any]) -> Dict[str, Any]:
    normalized = normalize_sqldb_manifest({"source": deepcopy(dict(source))}).get("source") or {}
    result: Dict[str, Any] = {}

    for key, value in source.items():
        if key in _LEGACY_SQL_LAYOUT_FIELDS:
            continue
        if key == "__sqldb_warnings__":
            continue
        if key in {"catalog", "query", "dialect_options"}:
            continue
        result[key] = deepcopy(value)

    for key in _CANONICAL_SOURCE_FIELDS:
        if key in normalized and normalized[key] is not None:
            result[key] = deepcopy(normalized[key])

    if result.get("kind") == "oracle":
        result["kind"] = "sqldb"
    return result


def canonicalize_sqldb_manifest_layout(manifest: Mapping[str, Any]) -> Dict[str, Any]:
    data = deepcopy(dict(manifest))
    source = data.get("source") or {}
    if isinstance(source, Mapping) and is_sqlish_source_kind(str(source.get("kind") or "")):
        data["source"] = canonicalize_sqldb_source_layout(source)
    return data


def render_sqldb_canonical_manifest_yaml(manifest: Mapping[str, Any]) -> str:
    data = canonicalize_sqldb_manifest_layout(manifest)
    return yaml.safe_dump(data, sort_keys=False, allow_unicode=True)


__all__ = [
    "canonicalize_sqldb_manifest_layout",
    "canonicalize_sqldb_source_layout",
    "is_sqlish_source_kind",
    "render_sqldb_canonical_manifest_yaml",
]
