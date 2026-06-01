from __future__ import annotations

import re
from typing import Any

from pydantic import BaseModel, ConfigDict

_ENV_PATTERN = re.compile(r"^\$\{ENV:[A-Z0-9_]+(?:\|[^}]*)?\}$")


def is_env_placeholder(value: Any) -> bool:
    return isinstance(value, str) and _ENV_PATTERN.match(value.strip()) is not None


def coerce_int_or_env(value: Any) -> int | str:
    if isinstance(value, bool):
        raise ValueError("expected int, got bool")
    if isinstance(value, int):
        return value
    if value is None:
        raise ValueError("value is required")
    if isinstance(value, str):
        s = value.strip()
        if is_env_placeholder(s):
            return value
        if s == "":
            raise ValueError("empty string is not a valid int")
        try:
            return int(s)
        except Exception as exc:  # pragma: no cover
            raise ValueError(f"expected int or ${'{'}ENV:...{'}'}, got: {value!r}") from exc
    raise ValueError(f"expected int or ${'{'}ENV:...{'}'}, got: {type(value)}")


def coerce_bool_or_env(value: Any) -> bool | str:
    if isinstance(value, bool):
        return value
    if value is None:
        raise ValueError("value is required")
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        s = value.strip()
        if is_env_placeholder(s):
            return value
        s_low = s.lower()
        if s_low in {"1", "true", "yes", "y", "on"}:
            return True
        if s_low in {"0", "false", "no", "n", "off"}:
            return False
        raise ValueError(f"expected bool-like value or ${'{'}ENV:...{'}'}, got: {value!r}")
    raise ValueError(f"expected bool-like value or ${'{'}ENV:...{'}'}, got: {type(value)}")


class AllowExtraModel(BaseModel):
    model_config = ConfigDict(
        extra="allow",
        populate_by_name=True,
        union_mode="left_to_right",
    )


class ForbidExtraModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
        union_mode="left_to_right",
    )


__all__ = [
    "AllowExtraModel",
    "ForbidExtraModel",
    "coerce_bool_or_env",
    "coerce_int_or_env",
    "is_env_placeholder",
]
