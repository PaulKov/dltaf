from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Callable, List, Mapping, Sequence

ClickHouseFetcher = Callable[[str], Sequence[Any]]


def _as_mapping(obj: Any) -> Mapping[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, Mapping):
        return obj
    dump = getattr(obj, "model_dump", None)
    if callable(dump):
        return dump(mode="python", by_alias=True)
    raise TypeError(f"Expected mapping-like values config, got: {type(obj)}")


def _stable_dedupe(values: Sequence[Any]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for value in values:
        if value is None:
            continue
        normalized = str(value).strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


def parse_csv_or_json_list(value: str) -> List[str]:
    raw = (value or "").strip()
    if not raw:
        return []
    if raw.startswith("["):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, list):
            return _stable_dedupe(parsed)
    return _stable_dedupe(raw.split(","))


def _resolve_relative_path(path: str, manifest: Mapping[str, Any]) -> Path:
    candidate = Path(path).expanduser()
    if candidate.is_absolute():
        return candidate
    manifest_path = str(manifest.get("__manifest_path__") or "").strip()
    base_dir = Path(manifest_path).expanduser().parent if manifest_path else Path.cwd()
    return (base_dir / candidate).resolve()


def _read_values_file(path: Path) -> List[str]:
    content = path.read_text(encoding="utf-8")
    flattened = ",".join(line.strip() for line in content.splitlines() if line.strip())
    return parse_csv_or_json_list(flattened)


def _build_clickhouse_query(config: Mapping[str, Any]) -> str:
    query = str(config.get("query") or "").strip()
    if query:
        return query

    database = str(config.get("database") or "").strip()
    table = str(config.get("table") or "").strip()
    column = str(config.get("column") or "bin").strip() or "bin"
    where = str(config.get("where") or "").strip()
    limit = config.get("limit")

    if not table:
        raise ValueError("from_clickhouse.table is required when query is not provided")

    qualified_table = f"{database}.{table}" if database else table
    built = f"SELECT DISTINCT {column} FROM {qualified_table}"
    if where:
        built += f" WHERE {where}"
    if limit not in (None, ""):
        built += f" LIMIT {int(limit)}"
    return built


def resolve_values_list(
    values_cfg: Any,
    manifest: Mapping[str, Any],
    *,
    allow_clickhouse: bool = False,
    clickhouse_fetcher: ClickHouseFetcher | None = None,
    env: Mapping[str, str] | None = None,
) -> List[str]:
    """Resolve stable runtime partition values from manifest/file/env/ClickHouse."""

    config = _as_mapping(values_cfg)
    environ = env or os.environ

    explicit = config.get("values") or []
    if explicit:
        return _stable_dedupe(explicit if isinstance(explicit, Sequence) else [explicit])

    values_file = str(config.get("from_file") or "").strip()
    if values_file:
        return _read_values_file(_resolve_relative_path(values_file, manifest))

    env_name = str(config.get("from_env") or "").strip()
    if env_name:
        return parse_csv_or_json_list(str(environ.get(env_name, "")))

    clickhouse_config = config.get("from_clickhouse") or {}
    if allow_clickhouse and isinstance(clickhouse_config, Mapping) and clickhouse_config:
        if clickhouse_fetcher is None:
            from dltaf.services.clickhouse import fetch_first_column_values

            clickhouse_fetcher = fetch_first_column_values
        return _stable_dedupe(clickhouse_fetcher(_build_clickhouse_query(clickhouse_config)))

    return []
