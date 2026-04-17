# ruff: noqa: E402
from __future__ import annotations

"""Compatibility alias for the canonical sqldb oracle query runner.

`oracle_custom_sql` remains supported as a legacy source kind, but runtime logic
now lives in `sqldb + dialect=oracle + mode=query`.
"""

from dltaf.integrations.sqldb.runner import SqlDbRunner


class OracleCustomSQLRunner(SqlDbRunner):
    kind = 'oracle_custom_sql'


__all__ = ['OracleCustomSQLRunner']
