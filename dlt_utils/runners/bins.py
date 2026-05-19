"""BINs list resolution helper.

Stage 1: extracted from `manifest_runner` to avoid duplicating the logic in
multiple custom runners.

Supported inputs:
  - source.bins.values: inline YAML list
  - source.bins.from_file: path (relative to manifest dir) with BINs (one-per-line or CSV)
  - source.bins.from_env: env var name with CSV/JSON array
  - source.bins.from_clickhouse: ClickHouse query or {database, table, column, where, limit}

Return:
  Stable de-duplicated list of BIN strings.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, List, Mapping

from dlt_utils.clickhouse_helpers import fetch_first_column_values
from dlt_utils.vault_env import parse_csv_or_json_list


def _as_mapping(obj: Any) -> Mapping[str, Any]:
    """Best-effort conversion of config objects to Mapping.

    Supports:
      - dict-like mappings
      - Pydantic BaseModel via `.model_dump()`

    This keeps runner code clean: runners may pass either a raw YAML mapping
    or a typed Pydantic model (parsed from manifest_schema).
    """

    if obj is None:
        return {}
    if isinstance(obj, Mapping):
        return obj
    dump = getattr(obj, "model_dump", None)
    if callable(dump):
        return dump(mode="python", by_alias=True)
    raise TypeError(f"Expected mapping-like bins config, got: {type(obj)}")


def resolve_bins_list(
    bins_cfg: Any,
    manifest: Mapping[str, Any],
    *,
    allow_clickhouse: bool = False,
) -> List[str]:
    bins_cfg = _as_mapping(bins_cfg)
    bins: List[str] = []

    explicit = bins_cfg.get("values") or []
    if explicit:
        bins = [str(x).strip() for x in explicit if str(x).strip()]

    if not bins:
        bins_file = str(bins_cfg.get("from_file") or "").strip()
        if bins_file:
            p = Path(bins_file).expanduser()
            if not p.is_absolute():
                base_dir = Path(str(manifest.get("__manifest_path__"))).parent
                p = (base_dir / p).resolve()
            content = p.read_text(encoding="utf-8")
            raw = ",".join([line.strip() for line in content.splitlines() if line.strip()])
            bins = parse_csv_or_json_list(raw)

    if not bins:
        env_name = str(bins_cfg.get("from_env") or "").strip()
        if env_name:
            bins = parse_csv_or_json_list(os.getenv(env_name, ""))

    if not bins and allow_clickhouse:
        ch_cfg = bins_cfg.get("from_clickhouse") or {}
        if isinstance(ch_cfg, Mapping) and ch_cfg:
            query = str(ch_cfg.get("query") or "").strip()
            if not query:
                database = str(ch_cfg.get("database") or "").strip()
                table = str(ch_cfg.get("table") or "").strip()
                column = str(ch_cfg.get("column") or "bin").strip() or "bin"
                where = str(ch_cfg.get("where") or "").strip()
                limit = ch_cfg.get("limit")

                if not table:
                    raise ValueError(
                        "source.bins.from_clickhouse.table is required when query is not provided"
                    )

                fq_table = f"{database}.{table}" if database else table
                query = f"SELECT DISTINCT {column} FROM {fq_table}"
                if where:
                    query += f" WHERE {where}"
                if limit not in (None, ""):
                    query += f" LIMIT {int(limit)}"

            bins = fetch_first_column_values(query)

    # sanitize + stable de-duplication
    out: List[str] = []
    seen: set[str] = set()
    for b in [str(x).strip() for x in bins if str(x).strip()]:
        if b in seen:
            continue
        seen.add(b)
        out.append(b)

    return out
