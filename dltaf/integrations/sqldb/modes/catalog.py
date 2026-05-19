from __future__ import annotations

from typing import Any

from dltaf.app.runtime import RunContext
from dltaf.integrations.sqldb.config import ResolvedSqlDbConfig
from dltaf.integrations.sqldb.dialects.base import SqlDialectAdapter
from dltaf.integrations.sqldb.factories import build_catalog_source_factory
from dltaf.integrations.sqldb.partial_success import resolve_partial_success_policy, run_catalog_with_partial_success
from dlt_utils.runners.common import run_with_replace_protection


class CatalogMode:
    name = 'catalog'

    def validate(self, config: ResolvedSqlDbConfig) -> None:
        if config.source.catalog is None:
            raise ValueError('sqldb mode=catalog requires source.catalog')

    def run(self, config: ResolvedSqlDbConfig, ctx: RunContext, dialect: SqlDialectAdapter) -> Any:
        if dialect.name not in {'generic', 'oracle'}:
            raise ValueError(f'Unsupported sqldb catalog dialect: {dialect.name}')

        execution_manifest = config.execution_manifest
        pipeline_cfg = execution_manifest.get('pipeline') or {}
        run_cfg = execution_manifest.get('run') or {}

        import dlt

        pipeline = dlt.pipeline(
            pipeline_name=pipeline_cfg['name'],
            destination=pipeline_cfg.get('destination', 'clickhouse'),
            dataset_name=pipeline_cfg.get('dataset'),
            progress=pipeline_cfg.get('progress', 'log'),
            dev_mode=bool(pipeline_cfg.get('dev_mode', False)),
        )
        write_disposition = str(run_cfg.get('write_disposition') or 'merge')
        partial_success_policy = resolve_partial_success_policy(config.normalized_manifest)
        if partial_success_policy is not None:
            return run_catalog_with_partial_success(
                config=config,
                ctx=ctx,
                pipeline=pipeline,
                write_disposition=write_disposition,
            )
        source_factory = build_catalog_source_factory(config)
        return run_with_replace_protection(
            pipeline=pipeline,
            source_factory=source_factory,
            manifest=execution_manifest,
            write_disposition=write_disposition,
        )
