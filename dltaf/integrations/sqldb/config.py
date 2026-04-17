from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Sequence

from dltaf.app.runtime import RunContext
from dltaf.services.manifests.schema import SourceSQLDB

from .normalizer import build_sqldb_execution_manifest, normalize_sqldb_manifest

_BIND_RE = re.compile(r":([A-Za-z_][A-Za-z0-9_]*)")


@dataclass(frozen=True)
class ResolvedSqlDbConfig:
    requested_kind: str
    source: SourceSQLDB
    normalized_manifest: Mapping[str, Any]
    execution_manifest: Mapping[str, Any]
    mode: str
    dialect: str
    warnings: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class SqlDbOracleConnectionConfig:
    host: str
    port: int
    username: str
    password: str
    database: Optional[str]
    dsn: Optional[str]


@dataclass(frozen=True)
class ResolvedSqlDbQueryConfig:
    name: str
    table_name: str
    sql_text: str
    write_disposition: str
    primary_key: Optional[Sequence[str]]
    params: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ResolvedSqlDbQueryRuntime:
    source_name: str
    fetch_batch_size: int
    init_sql: Optional[str]
    thick_mode: Optional[bool]
    connection: SqlDbOracleConnectionConfig
    queries: Sequence[ResolvedSqlDbQueryConfig] = field(default_factory=tuple)


def read_sql_file(sql_path: Path) -> str:
    sql = sql_path.read_text(encoding="utf-8")
    if "{" in sql or "}" in sql:
        raise ValueError(f"SQL file contains '{{' or '}}' which is forbidden for safety: {sql_path}")
    return sql


def extract_bind_params(sql: str) -> set[str]:
    return set(_BIND_RE.findall(sql))


class SqlDbConfigParser:
    def validate(self, manifest: Mapping[str, Any]) -> None:
        resolved = self.parse(manifest, None)
        cfg = resolved.source
        if cfg.mode == "catalog":
            if cfg.catalog is None:
                raise ValueError("sqldb mode=catalog requires source.catalog or compatible legacy catalog fields")
            catalog = cfg.catalog
            schemas = getattr(catalog, "schemas", None)
            schema_name = getattr(catalog, "db_schema", None)
            tables = getattr(catalog, "tables", None)
            if not schemas and (not schema_name or not tables):
                raise ValueError(
                    "sqldb mode=catalog requires either source.catalog.schemas or (source.catalog.schema and source.catalog.tables)"
                )
        elif cfg.mode == "query":
            if cfg.query is None or not cfg.query.queries:
                raise ValueError("sqldb mode=query requires source.query.queries or compatible legacy query fields")
            if cfg.dialect != "oracle":
                raise ValueError("sqldb mode=query currently supports only dialect=oracle")

            manifest_path_value = resolved.normalized_manifest.get("__manifest_path__")
            base_dir = Path(str(manifest_path_value)).resolve().parent if manifest_path_value else None
            run_cfg = resolved.normalized_manifest.get("run") or {}

            for query in cfg.query.queries:
                sql_text = self._resolve_query_sql_text(query, base_dir)
                write_disposition = str(query.write_disposition or run_cfg.get("write_disposition") or "").strip().lower()
                primary_key = query.primary_key
                if write_disposition == "merge" and (not isinstance(primary_key, list) or not primary_key):
                    raise ValueError(
                        f"query '{query.name}': primary_key is required for write_disposition=merge"
                    )
                params = query.params or {}
                if not isinstance(params, Mapping):
                    raise ValueError(f"query '{query.name}': params must be a mapping")
                bind_params = extract_bind_params(sql_text)
                missing = bind_params.difference(set(params.keys()))
                if missing:
                    raise ValueError(
                        f"query '{query.name}': missing params for SQL bind placeholders: {sorted(missing)}"
                    )
        else:  # pragma: no cover
            raise ValueError(f"Unsupported sqldb mode: {cfg.mode}")

    def parse(
        self,
        manifest: Mapping[str, Any],
        ctx: RunContext | None,
    ) -> ResolvedSqlDbConfig:
        _ = ctx
        normalized_manifest = normalize_sqldb_manifest(manifest)
        source = SourceSQLDB.model_validate(normalized_manifest.get("source") or {})
        execution_manifest = build_sqldb_execution_manifest(normalized_manifest)
        source_mapping = normalized_manifest.get("source") or {}
        warnings = tuple(source_mapping.get("__sqldb_warnings__") or ())
        mode_value = source.mode.value if source.mode is not None and hasattr(source.mode, 'value') else str(source.mode)
        dialect_value = source.dialect.value if source.dialect is not None and hasattr(source.dialect, 'value') else str(source.dialect)
        return ResolvedSqlDbConfig(
            requested_kind=str((manifest.get("source") or {}).get("kind") or "").strip(),
            source=source,
            normalized_manifest=normalized_manifest,
            execution_manifest=execution_manifest,
            mode=mode_value,
            dialect=dialect_value,
            warnings=warnings,
        )

    def build_query_runtime(self, config: ResolvedSqlDbConfig) -> ResolvedSqlDbQueryRuntime:
        if config.source.query is None or not config.source.query.queries:
            raise ValueError("sqldb mode=query requires source.query.queries")
        manifest_path_value = config.normalized_manifest.get("__manifest_path__")
        base_dir = Path(str(manifest_path_value)).resolve().parent if manifest_path_value else None
        run_cfg = config.normalized_manifest.get("run") or {}
        dialect_options = config.source.dialect_options
        thick_mode = None
        init_sql = None
        if dialect_options is not None:
            thick_mode = dialect_options.thick_mode
            init_sql = dialect_options.init_sql
        connection = self._resolve_oracle_connection()
        queries: list[ResolvedSqlDbQueryConfig] = []
        for query in config.source.query.queries:
            sql_text = self._resolve_query_sql_text(query, base_dir)
            queries.append(
                ResolvedSqlDbQueryConfig(
                    name=str(query.name),
                    table_name=str(query.table_name or query.name),
                    sql_text=sql_text,
                    write_disposition=str(query.write_disposition or run_cfg.get("write_disposition") or "merge"),
                    primary_key=tuple(query.primary_key) if query.primary_key else None,
                    params=dict(query.params or {}),
                )
            )
        fetch_batch_size = int(config.source.query.fetch_batch_size or 2000)
        return ResolvedSqlDbQueryRuntime(
            source_name=str(config.source.name or "sqldb_query"),
            fetch_batch_size=fetch_batch_size,
            init_sql=(str(init_sql) if init_sql is not None else None),
            thick_mode=bool(thick_mode) if thick_mode is not None else None,
            connection=connection,
            queries=tuple(queries),
        )

    def _resolve_query_sql_text(self, query: Any, base_dir: Path | None) -> str:
        sql_inline = getattr(query, "sql", None)
        if sql_inline:
            sql = str(sql_inline)
            if "{" in sql or "}" in sql:
                raise ValueError(f"query '{query.name}': inline SQL contains '{{' or '}}' which is forbidden for safety")
            return sql
        sql_file = getattr(query, "sql_file", None)
        if not sql_file:
            raise ValueError(f"query '{query.name}': either sql or sql_file is required")
        if base_dir is None:
            raise ValueError(f"query '{query.name}': sql_file requires __manifest_path__ in manifest")
        sql_path = (base_dir / str(sql_file)).resolve()
        if not sql_path.exists():
            raise ValueError(f"query '{query.name}': sql_file does not exist: {sql_path}")
        return read_sql_file(sql_path)

    def _resolve_oracle_connection(self) -> SqlDbOracleConnectionConfig:
        import os

        host = str(os.getenv("SOURCES__SQL_DATABASE__CREDENTIALS__HOST", "")).strip()
        port = int(os.getenv("SOURCES__SQL_DATABASE__CREDENTIALS__PORT", "1521"))
        username = str(os.getenv("SOURCES__SQL_DATABASE__CREDENTIALS__USERNAME", "")).strip()
        password = str(os.getenv("SOURCES__SQL_DATABASE__CREDENTIALS__PASSWORD", "")).strip()
        database = str(os.getenv("SOURCES__SQL_DATABASE__CREDENTIALS__DATABASE", "")).strip() or None
        dsn = str(os.getenv("SOURCES__SQL_DATABASE__CREDENTIALS__DSN", "")).strip() or None
        if not host or not username or not password:
            raise ValueError("Oracle credentials are not configured in ENV")
        return SqlDbOracleConnectionConfig(
            host=host,
            port=port,
            username=username,
            password=password,
            database=database,
            dsn=dsn,
        )


__all__ = [
    "ResolvedSqlDbConfig",
    "ResolvedSqlDbQueryConfig",
    "ResolvedSqlDbQueryRuntime",
    "SqlDbConfigParser",
    "SqlDbOracleConnectionConfig",
    "extract_bind_params",
    "read_sql_file",
]
