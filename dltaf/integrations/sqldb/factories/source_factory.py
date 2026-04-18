from __future__ import annotations

import logging
from typing import Any, Callable, Dict, Iterator

from dlt_utils.oracle_serialization import serialize_oracle_value

from dltaf.integrations.sqldb.config import ResolvedSqlDbConfig, ResolvedSqlDbQueryRuntime
from dltaf.integrations.sqldb.dialects.oracle import OracleDialect

from .resources import (
    resolve_catalog_reflection_settings,
    transform_table_name,
    iter_catalog_resources,
)



def build_catalog_source_factory(config: ResolvedSqlDbConfig) -> Callable[[], Any]:
    import dlt
    from dlt.sources.sql_database import sql_database as dlt_sql_database

    catalog = config.source.catalog
    if catalog is None:  # pragma: no cover - guarded by validation
        raise ValueError('sqldb catalog source requires source.catalog')

    source_name = str(config.source.name or 'sqldb')

    @dlt.source(name=source_name)
    def sqldb_catalog_source() -> dlt.Source:
        yield from iter_catalog_resources(catalog=catalog, dlt_sql_database=dlt_sql_database)

    return sqldb_catalog_source


def build_single_catalog_table_source_factory(
    config: ResolvedSqlDbConfig,
    *,
    schema_name: str,
    source_table: str,
) -> Callable[[], Any]:
    import dlt
    from dlt.sources.sql_database import sql_database as dlt_sql_database

    catalog = config.source.catalog
    if catalog is None:  # pragma: no cover - guarded by validation
        raise ValueError("sqldb catalog source requires source.catalog")

    source_name = str(config.source.name or "sqldb")
    use_reflection_level, reflection_level_value, detect_precision_hints_value = (
        resolve_catalog_reflection_settings(catalog)
    )
    table_name_transform = dict(getattr(catalog, "table_name_transform", None) or {})
    logical_name = transform_table_name(table_name_transform, schema_name, source_table)

    @dlt.source(name=source_name)
    def sqldb_single_table_source() -> dlt.Source:
        if use_reflection_level:
            schema_source = dlt_sql_database(
                schema=schema_name,
                table_names=[source_table],
                reflection_level=reflection_level_value,
            )
        else:
            schema_source = dlt_sql_database(
                schema=schema_name,
                table_names=[source_table],
                detect_precision_hints=detect_precision_hints_value,
            )
        for resource in schema_source.resources.values():
            resource.apply_hints(table_name=logical_name)
            yield resource.with_name(logical_name)

    return sqldb_single_table_source



def build_oracle_query_source_factory(
    runtime: ResolvedSqlDbQueryRuntime,
    *,
    logger_: logging.Logger,
    dialect: OracleDialect,
) -> Callable[[], Any]:
    import dlt
    from sqlalchemy import text

    thick_mode_enabled = dialect.try_enable_thick_mode(enabled=runtime.thick_mode, logger_=logger_)
    engine = dialect.make_engine(runtime.connection, logger_=logger_)
    logger_.info(
        "Oracle connection details: host=%s port=%s database=%s username=%s thick_mode=%s dsn=%s",
        runtime.connection.host,
        runtime.connection.port,
        runtime.connection.database,
        runtime.connection.username,
        thick_mode_enabled,
        bool(runtime.connection.dsn),
    )
    init_sql = runtime.init_sql
    fetch_batch_size = int(runtime.fetch_batch_size)
    queries = tuple(runtime.queries)

    @dlt.source(name=runtime.source_name)
    def sqldb_oracle_query_source() -> dlt.Source:
        for query in queries:
            params = dict(query.params or {})

            @dlt.resource(
                name=str(query.table_name),
                write_disposition=str(query.write_disposition),
                primary_key=list(query.primary_key) if query.primary_key else None,
            )
            def _resource(
                sql_text: str = query.sql_text,
                query_params: Dict[str, Any] = params,
                qname: str = query.name,
            ) -> Iterator[Dict[str, Any]]:
                rows = 0
                with engine.connect() as conn:
                    if init_sql:
                        conn.execute(text(str(init_sql)))
                        conn.commit()
                    result = conn.execute(text(sql_text), query_params)
                    while True:
                        batch = result.fetchmany(fetch_batch_size)
                        if not batch:
                            break
                        for row in batch:
                            rows += 1
                            yield serialize_oracle_value(dict(row._mapping))
                logger_.info("sqldb oracle query '%s' finished: %s rows", qname, rows)

            yield _resource

    return sqldb_oracle_query_source


__all__ = [
    'build_catalog_source_factory',
    'build_oracle_query_source_factory',
    'build_single_catalog_table_source_factory',
]
