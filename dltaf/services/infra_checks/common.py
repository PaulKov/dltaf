from __future__ import annotations

import json
import uuid
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import yaml


def build_cli_ctx() -> Any:
    return SimpleNamespace(run_id=str(uuid.uuid4()))


def parse_name_list(raw: Optional[str], *, comma_sep: bool = True) -> List[str]:
    if raw is None:
        return []
    value = str(raw).strip()
    if not value:
        return []
    if comma_sep:
        parts = [part.strip() for part in value.replace(";", ",").split(",")]
        return [part for part in parts if part]
    return [value]


def dump_payload(data: Any, fmt: str) -> str:
    normalized = (fmt or "").strip().lower()
    if normalized == "yaml":
        return yaml.safe_dump(data, sort_keys=False, allow_unicode=True)
    if normalized == "json":
        return json.dumps(data, ensure_ascii=False, indent=2)
    raise ValueError(f"Unsupported format: {fmt!r} (expected 'json' or 'yaml')")


def render_table(rows: List[Dict[str, Any]]) -> str:
    if not rows:
        return "No checks.\n"
    columns = [
        ("name", "NAME"),
        ("origin", "ORIGIN"),
        ("applies", "APPLIES"),
        ("selected", "SELECTED"),
        ("description", "DESCRIPTION"),
    ]
    widths: Dict[str, int] = {}
    for key, header in columns:
        widths[key] = max(len(header), max(len(str(row.get(key, ""))) for row in rows))
    header_line = "  ".join(header.ljust(widths[key]) for key, header in columns)
    sep_line = "  ".join("-" * widths[key] for key, _ in columns)
    lines = [header_line, sep_line]
    for row in rows:
        lines.append("  ".join(str(row.get(key, "")).ljust(widths[key]) for key, _ in columns))
    return "\n".join(lines) + "\n"


__all__ = ["build_cli_ctx", "dump_payload", "parse_name_list", "render_table"]
