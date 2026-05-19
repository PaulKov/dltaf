# ruff: noqa: E402
from __future__ import annotations

"""Compatibility wrapper for the canonical sqldb oracle query path."""

import logging
from typing import Any, Callable

from dltaf.integrations.oracle_custom_sql.config import ResolvedOracleCustomSQLConfig
from dltaf.integrations.sqldb.dialects.oracle import OracleDialect
from dltaf.integrations.sqldb.factories import build_oracle_query_source_factory


class OracleCustomSQLWorkflow:
    def __init__(self, *, logger_: logging.Logger | None = None) -> None:
        self._logger = logger_ or logging.getLogger(__name__)
        self._dialect = OracleDialect()

    def build_source_factory(self, config: ResolvedOracleCustomSQLConfig) -> Callable[[], Any]:
        return build_oracle_query_source_factory(config, logger_=self._logger, dialect=self._dialect)


__all__ = ["OracleCustomSQLWorkflow"]
