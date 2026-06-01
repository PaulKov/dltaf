from __future__ import annotations

from typing import Protocol

from dltaf.integrations.sqldb.config import ResolvedSqlDbConfig


class SqlDialectAdapter(Protocol):
    name: str

    def validate(self, config: ResolvedSqlDbConfig) -> None:
        ...
