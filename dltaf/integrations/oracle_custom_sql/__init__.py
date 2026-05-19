"""Legacy oracle_custom_sql compatibility package.

Canonical runtime logic now lives in `dltaf.integrations.sqldb`.
"""

from .config import (
    OracleConnectionConfig,
    OracleCustomSQLConfigParser,
    OracleQueryConfig,
    ResolvedOracleCustomSQLConfig,
    extract_bind_params,
    read_sql_file,
)
from .runner import OracleCustomSQLRunner
from .workflow import OracleCustomSQLWorkflow

__all__ = [
    "OracleConnectionConfig",
    "OracleCustomSQLConfigParser",
    "OracleCustomSQLRunner",
    "OracleCustomSQLWorkflow",
    "OracleQueryConfig",
    "ResolvedOracleCustomSQLConfig",
    "extract_bind_params",
    "read_sql_file",
]
