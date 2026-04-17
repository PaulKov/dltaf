from __future__ import annotations

from typing import Any, Protocol

from dltaf.app.runtime import RunContext
from dltaf.integrations.sqldb.config import ResolvedSqlDbConfig
from dltaf.integrations.sqldb.dialects.base import SqlDialectAdapter


class SqlExtractionMode(Protocol):
    name: str

    def validate(self, config: ResolvedSqlDbConfig) -> None:
        ...

    def run(self, config: ResolvedSqlDbConfig, ctx: RunContext, dialect: SqlDialectAdapter) -> Any:
        ...
