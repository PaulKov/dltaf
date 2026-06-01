from __future__ import annotations

import logging
from typing import Any

from dltaf.app.runtime import RunContext

from .config import ResolvedSqlDbConfig, SqlDbConfigParser
from .dialects.generic import GenericSqlDialect
from .dialects.oracle import OracleDialect
from .modes.catalog import CatalogMode
from .modes.query import QueryMode


class SqlDbWorkflow:
    def __init__(self, *, logger_: logging.Logger | None = None, config_parser: SqlDbConfigParser | None = None) -> None:
        self._logger = logger_ or logging.getLogger(__name__)
        self._config_parser = config_parser or SqlDbConfigParser()
        self._catalog_mode = CatalogMode()
        self._query_mode = QueryMode(logger_=self._logger, config_parser=self._config_parser)
        self._generic_dialect = GenericSqlDialect()
        self._oracle_dialect = OracleDialect()

    def execute(self, config: ResolvedSqlDbConfig, ctx: RunContext) -> Any:
        dialect_name = str(config.dialect)
        mode_name = str(config.mode)

        if dialect_name == 'oracle':
            dialect = self._oracle_dialect
        else:
            dialect = self._generic_dialect

        if mode_name == 'catalog':
            mode = self._catalog_mode
        elif mode_name == 'query':
            mode = self._query_mode
        else:  # pragma: no cover
            raise ValueError(f'Unsupported sqldb mode: {mode_name}')

        dialect.validate(config)
        mode.validate(config)
        self._logger.debug(
            'Executing sqldb workflow: requested_kind=%s dialect=%s mode=%s',
            config.requested_kind,
            dialect.name,
            mode.name,
        )
        return mode.run(config, ctx, dialect)


__all__ = ['SqlDbWorkflow']
