from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Mapping, MutableMapping, Optional

import yaml


def _is_blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    if isinstance(value, (list, tuple, set, dict)):
        return len(value) == 0
    return False


def _parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    normalized = str(value or "").strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"Cannot parse boolean runtime param from {value!r}")


def _parse_int(value: Any) -> int:
    if isinstance(value, bool):
        return int(value)
    return int(value)


def _transform_value(value: Any, transform: str) -> Any:
    normalized = str(transform or "identity").strip().lower()
    if normalized in {"", "identity"}:
        return value
    if normalized == "int":
        return _parse_int(value)
    if normalized == "float":
        return float(value)
    if normalized == "bool":
        return _parse_bool(value)
    if normalized == "single_item_list":
        return [str(value)]
    if normalized == "single_int_list":
        return [_parse_int(value)]
    if normalized == "csv_list":
        return [item.strip() for item in str(value).split(",") if item.strip()]
    raise ValueError(f"Unsupported airflow runtime param transform: {transform!r}")


def extract_param_defaults(params_cfg: Mapping[str, Any]) -> Dict[str, Any]:
    """Extract plain default values from manifest `airflow.params`.

    The config may use either a direct literal value or a mapping with a
    `default` field.
    """

    defaults: Dict[str, Any] = {}
    for name, spec in (params_cfg or {}).items():
        if isinstance(spec, Mapping) and "default" in spec:
            defaults[str(name)] = spec.get("default")
        else:
            defaults[str(name)] = spec
    return defaults


def build_runtime_overrides(
    airflow_cfg: Mapping[str, Any],
    raw_params: Optional[Mapping[str, Any]],
) -> Dict[str, Any]:
    """Map runtime params to manifest overrides using `airflow.runtime_overrides`."""

    runtime_cfg = (airflow_cfg.get("runtime_overrides") or {}) if isinstance(airflow_cfg, Mapping) else {}
    if not isinstance(runtime_cfg, Mapping):
        raise ValueError("airflow.runtime_overrides must be a mapping")

    params = dict(raw_params or {})
    overrides: Dict[str, Any] = {}

    for param_name, spec in runtime_cfg.items():
        raw_value = params.get(str(param_name))
        if _is_blank(raw_value):
            continue

        if isinstance(spec, str):
            overrides[str(spec)] = raw_value
            continue

        if not isinstance(spec, Mapping):
            raise ValueError(
                "airflow.runtime_overrides items must be either strings or mappings, "
                f"got {type(spec)!r} for {param_name!r}"
            )

        path = str(spec.get("path") or "").strip()
        if not path:
            raise ValueError(f"airflow.runtime_overrides.{param_name} must define a non-empty path")

        value = raw_value
        transform = str(spec.get("transform") or "identity").strip()
        if transform:
            value = _transform_value(raw_value, transform)

        overrides[path] = value

    return overrides


def load_runtime_overrides_from_manifest(
    manifest_path: str | Path,
    raw_params: Optional[Mapping[str, Any]],
) -> Dict[str, Any]:
    manifest_abs = Path(manifest_path).expanduser().resolve()
    raw = yaml.safe_load(manifest_abs.read_text(encoding="utf-8"))
    if not isinstance(raw, MutableMapping):
        raise ValueError(f"Manifest must be a YAML mapping, got: {type(raw)}")
    airflow_cfg = raw.get("airflow") or {}
    if airflow_cfg and not isinstance(airflow_cfg, Mapping):
        raise ValueError("manifest airflow section must be a mapping")
    return build_runtime_overrides(airflow_cfg, raw_params)


__all__ = [
    "build_runtime_overrides",
    "extract_param_defaults",
    "load_runtime_overrides_from_manifest",
]
