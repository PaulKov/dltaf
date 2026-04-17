from __future__ import annotations

import logging
from typing import Any, Callable

from .config import ResolvedSQLDatabaseConfig


class SQLDatabaseWorkflow:
    def __init__(self, *, logger_: logging.Logger | None = None) -> None:
        self._logger = logger_ or logging.getLogger(__name__)

    def build_source_factory(self, config: ResolvedSQLDatabaseConfig) -> Callable[[], Any]:
        import dlt
        from dlt.sources.sql_database import sql_database

        reflection_level = config.reflection_level
        detect_precision_hints = config.detect_precision_hints
        table_name_transform = dict(config.table_name_transform or {})

        if reflection_level is not None:
            use_reflection_level = True
            reflection_level_value = str(reflection_level)
            detect_precision_hints_value = False
        elif detect_precision_hints is not None:
            use_reflection_level = False
            reflection_level_value = "full"
            detect_precision_hints_value = bool(detect_precision_hints)
        else:
            use_reflection_level = True
            reflection_level_value = "full"
            detect_precision_hints_value = False

        def transform_table_name(schema: str, table: str) -> str:
            _ = schema
            name = table
            drop_prefix = table_name_transform.get("drop_prefix")
            if drop_prefix and name.startswith(str(drop_prefix)):
                name = name[len(str(drop_prefix)) :]
            return name

        @dlt.source(name=config.name)
        def sql_database_source() -> dlt.Source:
            if config.schemas:
                for schema_name, tables in config.schemas.items():
                    if use_reflection_level:
                        schema_source = sql_database(
                            schema=schema_name,
                            table_names=list(tables),
                            reflection_level=reflection_level_value,
                        )
                    else:
                        schema_source = sql_database(
                            schema=schema_name,
                            table_names=list(tables),
                            detect_precision_hints=detect_precision_hints_value,
                        )
                    for resource in schema_source.resources.values():
                        logical_name = f"{schema_name}__{resource.name}"
                        resource.apply_hints(table_name=logical_name)
                        yield resource.with_name(logical_name)
            else:
                schema_name = str(config.db_schema or "")
                tables = list(config.tables or [])
                if use_reflection_level:
                    schema_source = sql_database(
                        schema=schema_name,
                        table_names=tables,
                        reflection_level=reflection_level_value,
                    )
                else:
                    schema_source = sql_database(
                        schema=schema_name,
                        table_names=tables,
                        detect_precision_hints=detect_precision_hints_value,
                    )
                for resource in schema_source.resources.values():
                    logical_name = transform_table_name(schema_name, resource.name)
                    resource.apply_hints(table_name=logical_name)
                    yield resource.with_name(logical_name)

        return sql_database_source


__all__ = ["SQLDatabaseWorkflow"]
