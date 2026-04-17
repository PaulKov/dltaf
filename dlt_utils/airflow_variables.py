"""Airflow Variables access helpers.

This module is intentionally safe to import outside of Airflow.

Rationale
---------
The framework supports retrieving configuration (including secrets) from:

1) HashiCorp Vault (preferred)
2) Airflow Variables (fallback)

Airflow deployments often configure a secrets backend (e.g. Vault) so
``Variable.get`` can be effectively Vault-backed.

When running locally (CLI), Airflow is usually not installed, so this
module must degrade gracefully.
"""

from __future__ import annotations

import logging
from typing import Optional


logger = logging.getLogger(__name__)


def get_airflow_variable(key: str, default: Optional[str] = None) -> Optional[str]:
    """Get an Airflow Variable value.

    If Airflow is not available (e.g. local CLI run), returns ``default``.
    If the variable is missing, returns ``default``.

    Args:
        key: Variable name
        default: value to return if not found / not available

    Returns:
        Variable value as string or ``default``.
    """

    try:
        # Import inside function to avoid hard dependency on Airflow.
        from airflow.models import Variable  # type: ignore
    except Exception:
        return default

    try:
        return Variable.get(key)
    except Exception as e:
        # Airflow uses different exception classes depending on version/backend.
        from dltaf.services.execution.redaction import safe_exception_message

        logger.debug(
            "Airflow Variable '%s' not found or unreadable: %s",
            key,
            safe_exception_message(e),
        )
        return default
