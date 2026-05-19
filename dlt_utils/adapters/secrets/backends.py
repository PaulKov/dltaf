"""Secret backends.

These helpers intentionally keep a *tiny* surface area.
They wrap existing Vault/Airflow integrations and return dict payloads.

Stage 3 introduces these helpers to standardize the precedence:
Vault -> Airflow Variables.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Mapping, Optional, Tuple

from dlt_utils.airflow_variables import get_airflow_variable
from dlt_utils.vault_env import apply_overrides, get_secret_from_vault, parse_vault_ref

logger = logging.getLogger(__name__)


def try_get_vault_secret(
    vault_ref: Optional[Any], *, overrides: Optional[Mapping[str, Any]] = None
) -> Optional[Tuple[Dict[str, Any], str]]:
    """Try to read secret from Vault.

    Returns:
        (secret_dict, source_label) or None if `vault_ref` is empty or unreadable.
    """

    if not vault_ref:
        return None
    try:
        ref = parse_vault_ref(vault_ref)
        secret = get_secret_from_vault(ref)
        secret = apply_overrides(secret, overrides)
        return secret, f"vault:{ref.mount_point}:{ref.path}"
    except Exception as e:
        logger.warning(
            "Vault secret is not available (%s). Falling back to Airflow Variables. Error: %s",
            vault_ref,
            e,
        )
        return None


def _parse_json_dict(raw: str, *, label: str) -> Dict[str, Any]:
    try:
        data = json.loads(raw)
    except Exception as e:
        raise ValueError(f"{label} must be a JSON object (dict). Parse error: {e}") from e
    if not isinstance(data, dict):
        raise ValueError(f"{label} must be a JSON object (dict), got: {type(data)}")
    return data


def try_get_airflow_json_secret(
    var_name: Optional[str], *, overrides: Optional[Mapping[str, Any]] = None
) -> Optional[Tuple[Dict[str, Any], str]]:
    """Try to read a JSON secret from an Airflow Variable.

    The variable value must be a JSON object.
    """

    if not var_name:
        return None

    raw = get_airflow_variable(str(var_name).strip())
    if raw is None:
        return None
    raw_str = str(raw).strip()
    if not raw_str:
        return None

    data = _parse_json_dict(raw_str, label=f"Airflow Variable '{var_name}'")
    data = apply_overrides(data, overrides)
    return data, f"airflow_variable_json:{var_name}"


def try_get_airflow_fields_secret(
    *,
    prefix: str,
    fields: Mapping[str, str],
    mapping: Optional[Mapping[str, Any]] = None,
    overrides: Optional[Mapping[str, Any]] = None,
) -> Optional[Tuple[Dict[str, Any], str]]:
    """Try to build a secret dict from multiple Airflow Variables.

    Args:
        prefix: variable prefix, e.g. "KAFKA__" or "CLICKHOUSE__"
        fields: field -> suffix mapping, e.g. {"host": "HOST"}
        mapping: optional field -> variable name override mapping
        overrides: overrides to apply to resulting secret
    """

    mp = dict(mapping or {})
    out: Dict[str, Any] = {}
    for field, suffix in fields.items():
        var_name = str(mp.get(field) or f"{prefix}{suffix}").strip()
        if not var_name:
            continue
        val = get_airflow_variable(var_name)
        if val is None:
            continue
        if isinstance(val, str) and val.strip() == "":
            continue
        out[field] = val

    if not out:
        return None

    out = apply_overrides(out, overrides)
    return out, f"airflow_variables_prefix:{prefix}"
