from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional, Sequence

from dltaf.app.runtime import RunContext
from dltaf.services.manifests.schema import SourceSQLDatabase
from dlt_utils.runners.source_config import parse_source_config


@dataclass(frozen=True)
class ResolvedSQLDatabaseConfig:
    name: str
    reflection_level: Optional[str]
    detect_precision_hints: Optional[bool]
    db_schema: Optional[str]
    tables: Sequence[str] = field(default_factory=tuple)
    schemas: Optional[Dict[str, Sequence[str]]] = None
    table_name_transform: Dict[str, Any] = field(default_factory=dict)


class SQLDatabaseConfigParser:
    def validate(self, manifest: Mapping[str, Any]) -> None:
        cfg = parse_source_config(manifest, SourceSQLDatabase)
        if cfg.schemas is None:
            if not cfg.db_schema or not cfg.tables:
                raise ValueError(
                    "sql_database source requires either 'schemas' or ('schema' and 'tables')"
                )
        else:
            if not cfg.schemas:
                raise ValueError("source.schemas must be a non-empty mapping")
            for schema_name, schema_cfg in cfg.schemas.items():
                if not getattr(schema_cfg, "tables", None):
                    raise ValueError(f"schema '{schema_name}' must define tables")

    def parse(self, manifest: Mapping[str, Any], ctx: RunContext) -> ResolvedSQLDatabaseConfig:
        _ = ctx
        cfg = parse_source_config(manifest, SourceSQLDatabase)
        schemas = None
        if cfg.schemas:
            schemas = {
                str(schema_name): tuple(getattr(schema_cfg, "tables", None) or [])
                for schema_name, schema_cfg in cfg.schemas.items()
            }
        detect_precision_hints = None
        if cfg.detect_precision_hints is not None:
            detect_precision_hints = bool(cfg.detect_precision_hints)
        return ResolvedSQLDatabaseConfig(
            name=str(cfg.name or "sql_database"),
            reflection_level=(str(cfg.reflection_level).strip() if cfg.reflection_level is not None else None),
            detect_precision_hints=detect_precision_hints,
            db_schema=(str(cfg.db_schema).strip() if cfg.db_schema else None),
            tables=tuple(cfg.tables or ()),
            schemas=schemas,
            table_name_transform=dict(cfg.table_name_transform or {}),
        )


__all__ = ["ResolvedSQLDatabaseConfig", "SQLDatabaseConfigParser"]
