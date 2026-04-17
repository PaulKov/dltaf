# ruff: noqa: E402
from __future__ import annotations

"""Compatibility wrappers for legacy oracle_custom_sql imports.

Canonical query-mode SQL logic now lives under `dltaf.integrations.sqldb`.
These names are retained to keep older imports working during migration.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional, Sequence

from dltaf.app.runtime import RunContext
from dltaf.integrations.sqldb.config import (
    ResolvedSqlDbQueryRuntime,
    SqlDbConfigParser,
    SqlDbOracleConnectionConfig,
    extract_bind_params,
    read_sql_file,
)


OracleConnectionConfig = SqlDbOracleConnectionConfig


@dataclass(frozen=True)
class OracleQueryConfig:
    name: str
    table_name: str
    sql_text: str
    write_disposition: str
    primary_key: Optional[Sequence[str]]
    params: Dict[str, Any] = field(default_factory=dict)


ResolvedOracleCustomSQLConfig = ResolvedSqlDbQueryRuntime


class OracleCustomSQLConfigParser:
    def __init__(self, *, delegate: SqlDbConfigParser | None = None) -> None:
        self._delegate = delegate or SqlDbConfigParser()

    def validate(self, manifest: Mapping[str, Any]) -> None:
        self._delegate.validate(manifest)

    def parse(self, manifest: Mapping[str, Any], ctx: RunContext) -> ResolvedOracleCustomSQLConfig:
        resolved = self._delegate.parse(manifest, ctx)
        return self._delegate.build_query_runtime(resolved)


__all__ = [
    "OracleConnectionConfig",
    "OracleCustomSQLConfigParser",
    "OracleQueryConfig",
    "ResolvedOracleCustomSQLConfig",
    "extract_bind_params",
    "read_sql_file",
]
