from __future__ import annotations

import logging
from typing import Any

from dlt_utils.runners.common import run_with_replace_protection

from dltaf.app.runtime import RunContext
from dltaf.integrations.sqldb.config import ResolvedSqlDbConfig, SqlDbConfigParser
from dltaf.integrations.sqldb.dialects.base import SqlDialectAdapter
from dltaf.integrations.sqldb.dialects.oracle import OracleDialect
from dltaf.integrations.sqldb.factories import build_oracle_query_source_factory


class QueryMode:
    name = "query"

    def __init__(self, *, logger_: logging.Logger | None = None, config_parser: SqlDbConfigParser | None = None) -> None:
        self._logger = logger_ or logging.getLogger(__name__)
        self._config_parser = config_parser or SqlDbConfigParser()

    def validate(self, config: ResolvedSqlDbConfig) -> None:
        if config.source.query is None or not config.source.query.queries:
            raise ValueError("sqldb mode=query requires source.query.queries")
        if config.dialect != 'oracle':
            raise ValueError("sqldb mode=query currently supports only dialect=oracle")

    def run(self, config: ResolvedSqlDbConfig, ctx: RunContext, dialect: SqlDialectAdapter) -> Any:
        if not isinstance(dialect, OracleDialect):
            raise ValueError(
                f"Unsupported sqldb query dialect: {dialect.name}. Current implementation supports only oracle."
            )
        runtime = self._config_parser.build_query_runtime(config)

        import dlt

        pipeline_cfg = config.normalized_manifest.get('pipeline') or {}
        run_cfg = config.normalized_manifest.get('run') or {}
        pipeline = dlt.pipeline(
            pipeline_name=pipeline_cfg['name'],
            destination=pipeline_cfg.get('destination', 'clickhouse'),
            dataset_name=pipeline_cfg.get('dataset'),
            progress=pipeline_cfg.get('progress', 'log'),
            dev_mode=bool(pipeline_cfg.get('dev_mode', False)),
        )
        source_factory = build_oracle_query_source_factory(runtime, logger_=ctx.logger, dialect=dialect)
        write_disposition = str(run_cfg.get('write_disposition') or 'merge')
        return run_with_replace_protection(
            pipeline=pipeline,
            source_factory=source_factory,
            manifest=config.execution_manifest,
            write_disposition=write_disposition,
        )
