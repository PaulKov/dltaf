# ruff: noqa: E402
from __future__ import annotations

"""Compatibility alias for the canonical sqldb catalog runner.

`sql_database` remains supported as a legacy source kind, but the runtime path is
now implemented by `sqldb + mode=catalog`.
"""

from dltaf.integrations.sqldb.runner import SqlDbRunner


class SQLDatabaseRunner(SqlDbRunner):
    kind = 'sql_database'


__all__ = ['SQLDatabaseRunner']
