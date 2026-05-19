from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, Mapping


CATALOG_TOP_LEVEL_FIELDS = {
    "schema",
    "db_schema",
    "tables",
    "schemas",
    "reflection_level",
    "detect_precision_hints",
    "table_name_transform",
}
QUERY_TOP_LEVEL_FIELDS = {
    "queries",
    "fetch_batch_size",
}
DIALECT_TOP_LEVEL_FIELDS = {
    "init_sql",
    "thick_mode",
}
COMMON_PASS_THROUGH_FIELDS = {"name", "payload_contract", "time_window"}


def _copy_mapping(value: Mapping[str, Any] | None) -> Dict[str, Any]:
    return deepcopy(dict(value or {}))


def _normalize_catalog_section(source: Mapping[str, Any]) -> Dict[str, Any] | None:
    catalog = _copy_mapping(source.get("catalog") if isinstance(source.get("catalog"), Mapping) else None)
    for field in CATALOG_TOP_LEVEL_FIELDS:
        if field in source and field not in catalog:
            catalog[field] = deepcopy(source[field])
    if not catalog:
        return None
    if "db_schema" in catalog and "schema" not in catalog:
        catalog["schema"] = catalog.pop("db_schema")
    return catalog


def _normalize_query_section(source: Mapping[str, Any]) -> Dict[str, Any] | None:
    query = _copy_mapping(source.get("query") if isinstance(source.get("query"), Mapping) else None)
    for field in QUERY_TOP_LEVEL_FIELDS:
        if field in source and field not in query:
            query[field] = deepcopy(source[field])
    if not query:
        return None
    return query


def _normalize_dialect_options(source: Mapping[str, Any]) -> Dict[str, Any] | None:
    options = _copy_mapping(source.get("dialect_options") if isinstance(source.get("dialect_options"), Mapping) else None)
    for field in DIALECT_TOP_LEVEL_FIELDS:
        if field in source and field not in options:
            options[field] = deepcopy(source[field])
    if not options:
        return None
    return options


def normalize_sqldb_source(source: Mapping[str, Any]) -> Dict[str, Any]:
    src = _copy_mapping(source)
    kind = str(src.get("kind") or "").strip()
    normalized: Dict[str, Any] = {}
    for field in COMMON_PASS_THROUGH_FIELDS:
        if field in src:
            normalized[field] = deepcopy(src[field])

    warnings: list[str] = []

    if kind == "sql_database":
        normalized["kind"] = "sqldb"
        normalized["dialect"] = "generic"
        normalized["mode"] = "catalog"
    elif kind == "oracle_custom_sql":
        normalized["kind"] = "sqldb"
        normalized["dialect"] = "oracle"
        normalized["mode"] = "query"
    elif kind == "oracle":
        normalized["kind"] = "oracle"
        normalized["dialect"] = str(src.get("dialect") or "oracle").strip() or "oracle"
        normalized["mode"] = str(src.get("mode") or "query").strip() or "query"
    else:
        normalized["kind"] = str(src.get("kind") or "sqldb").strip() or "sqldb"
        mode = str(src.get("mode") or "").strip()
        if not mode:
            if "query" in src or any(field in src for field in QUERY_TOP_LEVEL_FIELDS | DIALECT_TOP_LEVEL_FIELDS):
                mode = "query"
            elif "catalog" in src or any(field in src for field in CATALOG_TOP_LEVEL_FIELDS):
                mode = "catalog"
        if mode:
            normalized["mode"] = mode
        dialect = str(src.get("dialect") or "").strip()
        if not dialect:
            if kind == "oracle":
                dialect = "oracle"
            else:
                dialect = "generic"
        normalized["dialect"] = dialect

    catalog = _normalize_catalog_section(src)
    query = _normalize_query_section(src)
    dialect_options = _normalize_dialect_options(src)
    if catalog is not None:
        normalized["catalog"] = catalog
    if query is not None:
        normalized["query"] = query
    if dialect_options is not None:
        normalized["dialect_options"] = dialect_options

    if normalized.get("mode") == "query" and normalized.get("dialect") != "oracle":
        warnings.append("sqldb mode=query is currently validated only for dialect=oracle")

    if warnings:
        normalized["__sqldb_warnings__"] = warnings
    return normalized


def normalize_sqldb_manifest(manifest: Mapping[str, Any]) -> Dict[str, Any]:
    normalized = deepcopy(dict(manifest))
    source = manifest.get("source") or {}
    normalized["source"] = normalize_sqldb_source(source if isinstance(source, Mapping) else {})
    return normalized


def build_sqldb_execution_manifest(manifest: Mapping[str, Any]) -> Dict[str, Any]:
    normalized = normalize_sqldb_manifest(manifest)
    source = normalized.get("source") or {}
    mode = str(source.get("mode") or "").strip()
    dialect = str(source.get("dialect") or "").strip()

    if mode == "catalog":
        catalog = source.get("catalog") or {}
        legacy_source: Dict[str, Any] = {
            "kind": "sql_database",
            "name": source.get("name"),
            "schema": catalog.get("schema"),
            "tables": deepcopy(catalog.get("tables")),
            "schemas": deepcopy(catalog.get("schemas")),
            "reflection_level": catalog.get("reflection_level"),
            "detect_precision_hints": catalog.get("detect_precision_hints"),
            "table_name_transform": deepcopy(catalog.get("table_name_transform")),
            "payload_contract": deepcopy(source.get("payload_contract")),
            "time_window": deepcopy(source.get("time_window")),
        }
        normalized["source"] = {k: v for k, v in legacy_source.items() if v is not None}
        return normalized

    if mode == "query" and dialect == "oracle":
        query = source.get("query") or {}
        dialect_options = source.get("dialect_options") or {}
        legacy_source = {
            "kind": "oracle_custom_sql",
            "name": source.get("name"),
            "queries": deepcopy(query.get("queries") or []),
            "fetch_batch_size": query.get("fetch_batch_size"),
            "init_sql": dialect_options.get("init_sql"),
            "payload_contract": deepcopy(source.get("payload_contract")),
            "time_window": deepcopy(source.get("time_window")),
        }
        normalized["source"] = {k: v for k, v in legacy_source.items() if v is not None}
        return normalized

    raise ValueError(
        f"Unsupported sqldb combination for execution: dialect={dialect!r}, mode={mode!r}. "
        "Stage 46 foundation supports catalog mode for generic SQL and query mode for oracle."
    )


__all__ = [
    "build_sqldb_execution_manifest",
    "normalize_sqldb_manifest",
    "normalize_sqldb_source",
]
