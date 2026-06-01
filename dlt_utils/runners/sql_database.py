from __future__ import annotations

from dltaf.app.deprecation import warn_legacy_module

warn_legacy_module(__name__, replacement="dltaf.integrations.sql_database.runner")

"""Compatibility wrapper for the native Wave 9 sql_database runner."""

from dltaf.integrations.sql_database.runner import SQLDatabaseRunner

__all__ = ["SQLDatabaseRunner"]
