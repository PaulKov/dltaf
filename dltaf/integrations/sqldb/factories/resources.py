from __future__ import annotations

from typing import Any, Iterator, Mapping, Sequence


def resolve_catalog_reflection_settings(catalog: Any) -> tuple[bool, str, bool]:
    reflection_level = getattr(catalog, 'reflection_level', None)
    detect_precision_hints = getattr(catalog, 'detect_precision_hints', None)

    if reflection_level is not None:
        return True, str(reflection_level), False
    if detect_precision_hints is not None:
        return False, 'full', bool(detect_precision_hints)
    return True, 'full', False


def transform_table_name(table_name_transform: Mapping[str, Any] | None, schema: str, table: str) -> str:
    _ = schema
    name = str(table)
    transform = dict(table_name_transform or {})
    drop_prefix = transform.get('drop_prefix')
    if drop_prefix and name.startswith(str(drop_prefix)):
        name = name[len(str(drop_prefix)) :]
    return name


def _iter_single_schema_resources(
    *,
    dlt_sql_database: Any,
    schema_name: str,
    table_names: Sequence[str],
    use_reflection_level: bool,
    reflection_level_value: str,
    detect_precision_hints_value: bool,
) -> Iterator[Any]:
    if use_reflection_level:
        schema_source = dlt_sql_database(
            schema=schema_name,
            table_names=list(table_names),
            reflection_level=reflection_level_value,
        )
    else:
        schema_source = dlt_sql_database(
            schema=schema_name,
            table_names=list(table_names),
            detect_precision_hints=detect_precision_hints_value,
        )
    yield from schema_source.resources.values()



def iter_catalog_resources(*, catalog: Any, dlt_sql_database: Any) -> Iterator[Any]:
    use_reflection_level, reflection_level_value, detect_precision_hints_value = resolve_catalog_reflection_settings(catalog)
    table_name_transform = dict(getattr(catalog, 'table_name_transform', None) or {})

    schemas = getattr(catalog, 'schemas', None)
    if schemas:
        for schema_name, schema_cfg in schemas.items():
            tables = tuple(getattr(schema_cfg, 'tables', None) or [])
            for resource in _iter_single_schema_resources(
                dlt_sql_database=dlt_sql_database,
                schema_name=str(schema_name),
                table_names=tables,
                use_reflection_level=use_reflection_level,
                reflection_level_value=reflection_level_value,
                detect_precision_hints_value=detect_precision_hints_value,
            ):
                logical_name = f"{schema_name}__{resource.name}"
                resource.apply_hints(table_name=logical_name)
                yield resource.with_name(logical_name)
        return

    schema_name = str(getattr(catalog, 'db_schema', None) or '')
    tables = tuple(getattr(catalog, 'tables', None) or ())
    for resource in _iter_single_schema_resources(
        dlt_sql_database=dlt_sql_database,
        schema_name=schema_name,
        table_names=tables,
        use_reflection_level=use_reflection_level,
        reflection_level_value=reflection_level_value,
        detect_precision_hints_value=detect_precision_hints_value,
    ):
        logical_name = transform_table_name(table_name_transform, schema_name, resource.name)
        resource.apply_hints(table_name=logical_name)
        yield resource.with_name(logical_name)


__all__ = [
    'iter_catalog_resources',
    'resolve_catalog_reflection_settings',
    'transform_table_name',
]
