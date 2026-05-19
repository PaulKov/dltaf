from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import yaml


def discover_yaml_files(base: Path) -> List[Path]:
    return sorted([path for path in base.glob("*.yaml") if path.is_file()])


def load_yaml_mapping(path: Path) -> Dict[str, Any]:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ValueError(f"Manifest must be a YAML mapping: {path}")
    return data


__all__ = ["discover_yaml_files", "load_yaml_mapping"]
