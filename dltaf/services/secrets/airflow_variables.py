"""Optional Airflow Variable access for package-mode secret fallback."""

from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)


def get_airflow_variable(key: str, default: Optional[str] = None) -> Optional[str]:
    """Return an Airflow Variable value when Airflow is importable.

    The import is intentionally lazy so CLI/local environments do not need
    Apache Airflow installed just to use the core framework.
    """

    try:
        from airflow.models import Variable  # type: ignore
    except Exception:
        return default

    try:
        return Variable.get(key)
    except Exception as exc:
        logger.debug("Airflow Variable '%s' is unavailable: %s", key, exc)
        return default
