from __future__ import annotations

from typing import List

from .protocol import SourceRunner


def build_builtin_runners() -> List[SourceRunner]:
    from dltaf.integrations.mongodb.runner import MongoDBRunner
    from dltaf.integrations.oracle_custom_sql.runner import OracleCustomSQLRunner
    from dltaf.integrations.sqldb.runner import OraclePresetRunner, SqlDbRunner
    from dltaf.integrations.sql_database.runner import SQLDatabaseRunner

    return [
        SqlDbRunner(),
        OraclePresetRunner(),
        OracleCustomSQLRunner(),
        SQLDatabaseRunner(),
        MongoDBRunner(),
    ]


__all__ = ["build_builtin_runners"]
