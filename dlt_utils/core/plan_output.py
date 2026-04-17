"""Plan output helpers.

Stage 15
--------
Adds UX improvements for plan/dry-run mode:
- write plan to a file (JSON/YAML)
- print plan to stdout ("-")

Notes:
- Plan must not contain secret values.
- This module only serializes/writes the already-built plan dict.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Mapping, Optional

import yaml


def infer_plan_format(*, output_path: Optional[str], explicit_format: Optional[str]) -> str:
    """Infer plan format.

    Args:
        output_path: optional file path (can be '-')
        explicit_format: optional user-specified format: json|yaml|yml

    Returns:
        "json" or "yaml".
    """

    if explicit_format:
        fmt = str(explicit_format).strip().lower()
        if fmt in {"yml", "yaml"}:
            return "yaml"
        if fmt == "json":
            return "json"
        raise ValueError(f"Unsupported plan format: {explicit_format!r} (expected json|yaml)")

    if output_path:
        p = str(output_path).strip().lower()
        if p.endswith(".yaml") or p.endswith(".yml"):
            return "yaml"

    return "json"


def serialize_plan(plan: Mapping[str, Any], *, fmt: str) -> str:
    """Serialize plan to JSON or YAML."""

    fmt = str(fmt).strip().lower()

    if fmt == "json":
        return json.dumps(plan, ensure_ascii=False, indent=2) + "\n"

    if fmt == "yaml":
        # Keep keys order stable and human-friendly.
        return yaml.safe_dump(
            dict(plan),
            allow_unicode=True,
            sort_keys=False,
            default_flow_style=False,
        )

    raise ValueError(f"Unsupported plan format: {fmt!r}")


def write_plan_output(
    plan: Mapping[str, Any],
    *,
    output_path: str,
    fmt: str,
    logger: Optional[Any] = None,
) -> str:
    """Write plan to a file (atomic) or to stdout when output_path == '-'.

    Returns:
        The resolved output path (or '-' for stdout).
    """

    out = str(output_path).strip()
    if not out:
        raise ValueError("output_path is empty")

    text = serialize_plan(plan, fmt=fmt)

    if out == "-":
        # stdout
        print(text, end="")
        return "-"

    path = Path(out).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)

    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(text, encoding="utf-8")
    os.replace(str(tmp_path), str(path))

    if logger is not None:
        try:
            logger.info("Plan written to: %s", str(path))
        except Exception:
            pass

    return str(path)
