from __future__ import annotations

from dltaf.app.deprecation import warn_legacy_module

warn_legacy_module(__name__, replacement="dltaf.integrations.oracle_custom_sql.runner")

"""Compatibility wrapper for the native Wave 9 oracle_custom_sql runner."""

from dltaf.integrations.oracle_custom_sql.runner import OracleCustomSQLRunner

__all__ = ["OracleCustomSQLRunner"]
