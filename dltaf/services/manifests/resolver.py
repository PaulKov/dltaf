from __future__ import annotations

import os
import re
from datetime import datetime
from typing import Any

_ENV_PATTERN = re.compile(r"^\$\{ENV:([A-Z0-9_]+)(?:\|([^}]*))?\}$")


class ManifestResolver:
    """Resolve supported templating inside manifest values.

    Supported expressions:
      - TODAY_STR
      - ${ENV:VAR|default}
    """

    def today_str(self) -> str:
        return datetime.now().strftime("%Y/%m/%d")

    def resolve_str(self, value: str) -> Any:
        if value == "TODAY_STR":
            return self.today_str()

        match = _ENV_PATTERN.match(value)
        if not match:
            return value

        env_key = match.group(1)
        default = match.group(2) if match.group(2) is not None else ""
        raw = os.getenv(env_key)
        if raw is None or str(raw).strip() == "":
            raw = default
        if raw == "TODAY_STR":
            return self.today_str()
        return raw

    def resolve(self, obj: Any) -> Any:
        if isinstance(obj, dict):
            return {k: self.resolve(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self.resolve(v) for v in obj]
        if isinstance(obj, str):
            return self.resolve_str(obj)
        return obj


_default_resolver = ManifestResolver()


def resolve_manifest(obj: Any) -> Any:
    return _default_resolver.resolve(obj)


__all__ = ["ManifestResolver", "resolve_manifest"]
