"""Secrets adapters.

Stage 3 goal
------------
Provide a standardized way to resolve connection credentials with the
following precedence:

1) HashiCorp Vault (preferred)
2) Airflow Variables (fallback)

The adapter also tracks the *source* of each resolved env key so we can
implement `--explain-config` UX without leaking secret values.
"""

from __future__ import annotations

from dlt_utils.adapters.secrets.connections import build_resolved_env_from_connections
from dlt_utils.adapters.secrets.types import ResolvedEnv, ResolvedValue

__all__ = [
    "ResolvedEnv",
    "ResolvedValue",
    "build_resolved_env_from_connections",
]
