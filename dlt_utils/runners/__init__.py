"""Built-in runner implementations.

This package intentionally avoids eager imports of all runner modules to keep
imports lightweight and to prevent circular dependencies during the staged v2
migration. Import concrete runners either from their modules or via attribute
access on this package.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

from dlt_utils.runners.base import SourceRunner

__all__ = [
    "SourceRunner",
    "OracleCustomSQLRunner",
    "SQLDatabaseRunner",
    "MongoDBRunner",
]

_MODULES = {
    "OracleCustomSQLRunner": "dlt_utils.runners.oracle_custom_sql",
    "SQLDatabaseRunner": "dlt_utils.runners.sql_database",
    "MongoDBRunner": "dlt_utils.runners.mongodb",
}


def __getattr__(name: str) -> Any:
    module_name = _MODULES.get(name)
    if module_name is None:
        raise AttributeError(name)
    module = import_module(module_name)
    value = getattr(module, name)
    globals()[name] = value
    return value
