from __future__ import annotations

import json
from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Sequence, Union

from pydantic import Field, field_validator, model_validator

from .base import AllowExtraModel, ForbidExtraModel, coerce_bool_or_env, coerce_int_or_env
from .common import SourceBase


class OracleQuery(AllowExtraModel):
    name: str
    table_name: Optional[str] = None
    sql_file: str
    write_disposition: Optional[str] = None
    primary_key: Optional[List[str]] = None
    params: Optional[Dict[str, Any]] = None

    @field_validator("name", "sql_file")
    @classmethod
    def non_empty(cls, value: Any) -> str:
        s = str(value or "").strip()
        if not s:
            raise ValueError("value must be a non-empty string")
        return s

    @field_validator("table_name")
    @classmethod
    def table_name_non_empty_if_set(cls, value: Any) -> Any:
        if value is None:
            return value
        s = str(value or "").strip()
        if not s:
            raise ValueError("table_name must be a non-empty string when provided")
        return s


class SourceOracleCustomSQL(SourceBase):
    kind: Literal["oracle_custom_sql"]
    init_sql: Optional[str] = None
    fetch_batch_size: Optional[Union[int, str]] = None
    queries: List[OracleQuery] = Field(default_factory=list)

    @field_validator("fetch_batch_size")
    @classmethod
    def fetch_batch_size_intish(cls, value: Any) -> Any:
        if value is None:
            return value
        return coerce_int_or_env(value)


class SQLSchemaTables(AllowExtraModel):
    tables: List[str] = Field(default_factory=list)


class SourceSQLDatabase(SourceBase):
    kind: Literal["sql_database"]
    reflection_level: Optional[str] = None
    detect_precision_hints: Optional[Union[bool, str]] = None
    db_schema: Optional[str] = Field(default=None, alias="schema")
    tables: Optional[List[str]] = None
    schemas: Optional[Dict[str, SQLSchemaTables]] = None
    table_name_transform: Optional[Dict[str, Any]] = None

    @field_validator("detect_precision_hints")
    @classmethod
    def detect_precision_boolish(cls, value: Any) -> Any:
        if value is None:
            return value
        return coerce_bool_or_env(value)


class SourceMongoDB(SourceBase):
    kind: Literal["mongodb"]
    database: str
    collection_names: List[str] = Field(default_factory=list)
    max_table_nesting: Optional[Union[int, str]] = None

    @field_validator("database")
    @classmethod
    def database_required(cls, value: Any) -> str:
        s = str(value or "").strip()
        if not s:
            raise ValueError("source.database is required for mongodb")
        return s

    @field_validator("max_table_nesting")
    @classmethod
    def max_table_nesting_intish(cls, value: Any) -> Any:
        if value is None:
            return value
        return coerce_int_or_env(value)


class BinsConfig(AllowExtraModel):
    values: Optional[List[str]] = None
    from_file: Optional[str] = None
    from_env: Optional[str] = None
    from_clickhouse: Optional[Dict[str, Any]] = None


class KafkaWaitConfig(AllowExtraModel):
    bootstrap_servers: Optional[Union[str, List[str]]] = None
    topic: Optional[str] = None
    wait_timeout_seconds: Optional[Union[int, str]] = None
    poll_interval_seconds: Optional[Union[int, str]] = None

    @field_validator("wait_timeout_seconds", "poll_interval_seconds")
    @classmethod
    def intish(cls, value: Any) -> Any:
        if value is None:
            return value
        return coerce_int_or_env(value)


class FetchRetryConfig(AllowExtraModel):
    count: Optional[Union[int, str]] = None
    interval_seconds: Optional[Union[int, str]] = None

    @field_validator("count", "interval_seconds")
    @classmethod
    def intish(cls, value: Any) -> Any:
        if value is None:
            return value
        return coerce_int_or_env(value)


class ConcurrencyConfig(AllowExtraModel):
    bins: Optional[Union[int, str]] = None
    companies: Optional[Union[int, str]] = None
    projects: Optional[Union[int, str]] = None

    @field_validator("bins", "companies", "projects")
    @classmethod
    def intish(cls, value: Any) -> Any:
        if value is None:
            return value
        return coerce_int_or_env(value)


class SqlDialectEnum(str, Enum):
    GENERIC = "generic"
    ORACLE = "oracle"
    POSTGRES = "postgres"
    MYSQL = "mysql"
    MSSQL = "mssql"


class SqlModeEnum(str, Enum):
    CATALOG = "catalog"
    QUERY = "query"


class SqlDbDialectOptions(AllowExtraModel):
    thick_mode: Optional[Union[bool, str]] = None
    init_sql: Optional[str] = None

    @field_validator("thick_mode")
    @classmethod
    def thick_mode_boolish(cls, value: Any) -> Any:
        if value is None:
            return value
        return coerce_bool_or_env(value)


class SqlDbCatalogConfig(AllowExtraModel):
    db_schema: Optional[str] = Field(default=None, alias="schema")
    tables: Optional[List[str]] = None
    schemas: Optional[Dict[str, SQLSchemaTables]] = None
    reflection_level: Optional[str] = None
    detect_precision_hints: Optional[Union[bool, str]] = None
    table_name_transform: Optional[Dict[str, Any]] = None

    @field_validator("detect_precision_hints")
    @classmethod
    def detect_precision_boolish(cls, value: Any) -> Any:
        if value is None:
            return value
        return coerce_bool_or_env(value)


class SqlDbQuery(AllowExtraModel):
    name: str
    table_name: Optional[str] = None
    sql_file: Optional[str] = None
    sql: Optional[str] = None
    write_disposition: Optional[str] = None
    primary_key: Optional[List[str]] = None
    params: Optional[Dict[str, Any]] = None

    @field_validator("name")
    @classmethod
    def name_non_empty(cls, value: Any) -> str:
        s = str(value or "").strip()
        if not s:
            raise ValueError("value must be a non-empty string")
        return s

    @model_validator(mode="after")
    def require_sql_or_file(self) -> "SqlDbQuery":
        if not self.sql_file and not self.sql:
            raise ValueError("query requires either sql_file or sql")
        return self


class SqlDbQueryConfig(AllowExtraModel):
    queries: List[SqlDbQuery] = Field(default_factory=list)
    fetch_batch_size: Optional[Union[int, str]] = None

    @field_validator("fetch_batch_size")
    @classmethod
    def fetch_batch_size_intish(cls, value: Any) -> Any:
        if value is None:
            return value
        return coerce_int_or_env(value)


class SourceSQLDB(SourceBase):
    kind: Literal["sqldb", "oracle"]
    dialect: Optional[SqlDialectEnum] = None
    mode: Optional[SqlModeEnum] = None
    catalog: Optional[SqlDbCatalogConfig] = None
    query: Optional[SqlDbQueryConfig] = None
    dialect_options: Optional[SqlDbDialectOptions] = None

    # Transitional top-level compatibility fields (Stage 46 foundation)
    db_schema: Optional[str] = Field(default=None, alias="schema")
    tables: Optional[List[str]] = None
    schemas: Optional[Dict[str, SQLSchemaTables]] = None
    reflection_level: Optional[str] = None
    detect_precision_hints: Optional[Union[bool, str]] = None
    table_name_transform: Optional[Dict[str, Any]] = None
    queries: Optional[List[SqlDbQuery]] = None
    fetch_batch_size: Optional[Union[int, str]] = None
    init_sql: Optional[str] = None
    thick_mode: Optional[Union[bool, str]] = None

    @field_validator("dialect", mode="before")
    @classmethod
    def normalize_dialect(cls, value: Any) -> Any:
        if value is None or value == "":
            return value
        return str(value).strip().lower()

    @field_validator("mode", mode="before")
    @classmethod
    def normalize_mode(cls, value: Any) -> Any:
        if value is None or value == "":
            return value
        return str(value).strip().lower()

    @field_validator("detect_precision_hints", "thick_mode")
    @classmethod
    def compatibility_boolish(cls, value: Any) -> Any:
        if value is None:
            return value
        return coerce_bool_or_env(value)

    @field_validator("fetch_batch_size")
    @classmethod
    def compatibility_fetch_batch_size_intish(cls, value: Any) -> Any:
        if value is None:
            return value
        return coerce_int_or_env(value)

    @field_validator("query")
    @classmethod
    def ensure_query_non_empty_if_set(cls, value: Any) -> Any:
        if value is None:
            return value
        if not value.queries:
            raise ValueError("source.query.queries must be a non-empty list when query mode is used")
        return value

    @field_validator("catalog")
    @classmethod
    def ensure_catalog_not_empty_if_set(cls, value: Any) -> Any:
        if value is None:
            return value
        if not value.schemas and not (value.db_schema and value.tables):
            return value
        return value

    @classmethod
    def _infer_mode(cls, values: dict[str, Any]) -> str | None:
        if values.get("mode"):
            return str(values["mode"])
        if values.get("query") is not None:
            return "query"
        if values.get("catalog") is not None:
            return "catalog"
        return None

    @classmethod
    def _infer_dialect(cls, values: dict[str, Any]) -> str | None:
        if values.get("dialect"):
            return str(values["dialect"])
        if str(values.get("kind") or "") == "oracle":
            return "oracle"
        return None

    @classmethod
    def _default_kind_mode(cls, values: dict[str, Any]) -> tuple[str | None, str | None]:
        kind = str(values.get("kind") or "").strip()
        if kind == "oracle":
            return "oracle", "query"
        return cls._infer_dialect(values), cls._infer_mode(values)

    @model_validator(mode="before")
    @classmethod
    def apply_kind_defaults(cls, data: Any) -> Any:
        if isinstance(data, dict):
            payload = dict(data)
            dialect, mode = cls._default_kind_mode(payload)
            if dialect and not payload.get("dialect"):
                payload["dialect"] = dialect
            if mode and not payload.get("mode"):
                payload["mode"] = mode
            return payload
        return data


class SourcePKBConclusion(SourceBase):
    kind: Literal["pkb_conclusion"]
    bins: Optional[BinsConfig] = None
    base_url: Optional[str] = None
    conclusion_type: Optional[str] = None
    kafka: Optional[KafkaWaitConfig] = None
    fetch_retry: Optional[FetchRetryConfig] = None
    concurrency: Optional[ConcurrencyConfig] = None


class PeriodEnum(str, Enum):
    QUARTER_1 = "QUARTER_1"
    HALF_YEAR_1 = "HALF_YEAR_1"
    NINE_MONTH = "NINE_MONTH"
    FULL_YEAR = "FULL_YEAR"


class UploaderApiPaths(AllowExtraModel):
    companies_path: Optional[str] = None
    projects_path: Optional[str] = None
    export_path: Optional[str] = None


class ClickhouseBinsRef(AllowExtraModel):
    query: Optional[str] = None
    database: Optional[str] = None
    table: Optional[str] = None
    column: Optional[str] = None
    where: Optional[str] = None
    limit: Optional[Union[int, str]] = None

    @field_validator("limit")
    @classmethod
    def limit_intish(cls, value: Any) -> Any:
        if value is None:
            return value
        if isinstance(value, str) and value.strip() == "":
            return ""
        return coerce_int_or_env(value)


class UploaderBinsConfig(BinsConfig):
    from_clickhouse: Optional[ClickhouseBinsRef] = None


class HttpClientConfig(AllowExtraModel):
    timeout_seconds: Optional[Union[int, str]] = None
    verify_ssl: Optional[Union[bool, str]] = None

    @field_validator("timeout_seconds")
    @classmethod
    def timeout_intish(cls, value: Any) -> Any:
        if value is None:
            return value
        return coerce_int_or_env(value)

    @field_validator("verify_ssl")
    @classmethod
    def verify_boolish(cls, value: Any) -> Any:
        if value is None:
            return value
        return coerce_bool_or_env(value)


class UploaderKafkaConfig(AllowExtraModel):
    bootstrap_servers: Optional[Union[str, List[str]]] = None
    topic: Optional[str] = None
    wait_timeout_seconds: Optional[Union[int, str]] = None
    poll_interval_seconds: Optional[Union[int, str]] = None

    @field_validator("wait_timeout_seconds", "poll_interval_seconds")
    @classmethod
    def intish(cls, value: Any) -> Any:
        if value is None:
            return value
        return coerce_int_or_env(value)


class ProjectsSelectionConfig(AllowExtraModel):
    strategy: Optional[str] = None
    name_regex: Optional[str] = None
    ids: Optional[Union[str, Sequence[Union[int, str]]]] = None

    def ids_as_int_list(self) -> Optional[List[int]]:
        raw = self.ids
        if raw is None:
            return None
        if isinstance(raw, str):
            s = raw.strip()
            if not s:
                return None
            if s.startswith("["):
                try:
                    data = json.loads(s)
                    if isinstance(data, list):
                        return [int(item) for item in data]
                except Exception:
                    pass
            parts = [part.strip() for part in s.split(",") if part.strip()]
            return [int(item) for item in parts]
        if isinstance(raw, (list, tuple)):
            return [int(item) for item in raw]
        return [int(raw)]


class KeycloakConfig(AllowExtraModel):
    token_url: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    grant_type: Optional[str] = None
    scope: Optional[str] = None
    verify_ssl: Optional[Union[bool, str]] = None
    timeout_seconds: Optional[Union[int, str]] = None

    @field_validator("timeout_seconds")
    @classmethod
    def timeout_intish(cls, value: Any) -> Any:
        if value is None:
            return value
        return coerce_int_or_env(value)

    @field_validator("verify_ssl")
    @classmethod
    def verify_boolish(cls, value: Any) -> Any:
        if value is None:
            return value
        return coerce_bool_or_env(value)


class SourceUploaderB057(SourceBase):
    kind: Literal["uploader_b057"]
    year: Union[int, str]
    period: PeriodEnum
    base_url: Optional[str] = None
    api: Optional[UploaderApiPaths] = None
    bins: Optional[UploaderBinsConfig] = None
    keycloak: Optional[KeycloakConfig] = None
    http: Optional[HttpClientConfig] = None
    kafka: Optional[UploaderKafkaConfig] = None
    projects: Optional[ProjectsSelectionConfig] = None
    fail_on_empty: Optional[Union[bool, str]] = None
    concurrency: Optional[ConcurrencyConfig] = None

    @field_validator("year")
    @classmethod
    def year_intish(cls, value: Any) -> Any:
        return coerce_int_or_env(value)

    @field_validator("fail_on_empty")
    @classmethod
    def fail_on_empty_boolish(cls, value: Any) -> Any:
        if value is None:
            return value
        return coerce_bool_or_env(value)


SourceConfig = Union[
    SourceSQLDB,
    SourceOracleCustomSQL,
    SourceSQLDatabase,
    SourceMongoDB,
    SourcePKBConclusion,
    SourceUploaderB057,
    SourceBase,
]


class OracleQueryStrict(OracleQuery, ForbidExtraModel):
    pass


class SourceOracleCustomSQLStrict(SourceOracleCustomSQL, ForbidExtraModel):
    queries: List[OracleQueryStrict] = Field(default_factory=list)


class SQLSchemaTablesStrict(SQLSchemaTables, ForbidExtraModel):
    pass


class SourceSQLDatabaseStrict(SourceSQLDatabase, ForbidExtraModel):
    schemas: Optional[Dict[str, SQLSchemaTablesStrict]] = None


class SqlDbDialectOptionsStrict(SqlDbDialectOptions, ForbidExtraModel):
    pass


class SqlDbCatalogConfigStrict(SqlDbCatalogConfig, ForbidExtraModel):
    schemas: Optional[Dict[str, SQLSchemaTablesStrict]] = None


class SqlDbQueryStrict(SqlDbQuery, ForbidExtraModel):
    pass


class SqlDbQueryConfigStrict(SqlDbQueryConfig, ForbidExtraModel):
    queries: List[SqlDbQueryStrict] = Field(default_factory=list)


class SourceSQLDBStrict(SourceSQLDB, ForbidExtraModel):
    catalog: Optional[SqlDbCatalogConfigStrict] = None
    query: Optional[SqlDbQueryConfigStrict] = None
    dialect_options: Optional[SqlDbDialectOptionsStrict] = None
    schemas: Optional[Dict[str, SQLSchemaTablesStrict]] = None
    queries: Optional[List[SqlDbQueryStrict]] = None


class SourceMongoDBStrict(SourceMongoDB, ForbidExtraModel):
    pass


class BinsConfigStrict(BinsConfig, ForbidExtraModel):
    pass


class KafkaWaitConfigStrict(KafkaWaitConfig, ForbidExtraModel):
    pass


class FetchRetryConfigStrict(FetchRetryConfig, ForbidExtraModel):
    pass


class ConcurrencyConfigStrict(ConcurrencyConfig, ForbidExtraModel):
    pass


class SourcePKBConclusionStrict(SourcePKBConclusion, ForbidExtraModel):
    bins: Optional[BinsConfigStrict] = None
    kafka: Optional[KafkaWaitConfigStrict] = None
    fetch_retry: Optional[FetchRetryConfigStrict] = None
    concurrency: Optional[ConcurrencyConfigStrict] = None


class UploaderApiPathsStrict(UploaderApiPaths, ForbidExtraModel):
    pass


class ClickhouseBinsRefStrict(ClickhouseBinsRef, ForbidExtraModel):
    pass


class UploaderBinsConfigStrict(UploaderBinsConfig, ForbidExtraModel):
    from_clickhouse: Optional[ClickhouseBinsRefStrict] = None


class HttpClientConfigStrict(HttpClientConfig, ForbidExtraModel):
    pass


class UploaderKafkaConfigStrict(UploaderKafkaConfig, ForbidExtraModel):
    pass


class ProjectsSelectionConfigStrict(ProjectsSelectionConfig, ForbidExtraModel):
    pass


class KeycloakConfigStrict(KeycloakConfig, ForbidExtraModel):
    pass


class SourceUploaderB057Strict(SourceUploaderB057, ForbidExtraModel):
    api: Optional[UploaderApiPathsStrict] = None
    bins: Optional[UploaderBinsConfigStrict] = None
    keycloak: Optional[KeycloakConfigStrict] = None
    http: Optional[HttpClientConfigStrict] = None
    kafka: Optional[UploaderKafkaConfigStrict] = None
    projects: Optional[ProjectsSelectionConfigStrict] = None
    concurrency: Optional[ConcurrencyConfigStrict] = None


SourceConfigStrict = Union[
    SourceSQLDBStrict,
    SourceOracleCustomSQLStrict,
    SourceSQLDatabaseStrict,
    SourceMongoDBStrict,
    SourcePKBConclusionStrict,
    SourceUploaderB057Strict,
    SourceBase,
]


__all__ = [
    "BinsConfig",
    "BinsConfigStrict",
    "ClickhouseBinsRef",
    "ClickhouseBinsRefStrict",
    "ConcurrencyConfig",
    "ConcurrencyConfigStrict",
    "FetchRetryConfig",
    "FetchRetryConfigStrict",
    "HttpClientConfig",
    "HttpClientConfigStrict",
    "KafkaWaitConfig",
    "KafkaWaitConfigStrict",
    "KeycloakConfig",
    "KeycloakConfigStrict",
    "OracleQuery",
    "OracleQueryStrict",
    "PeriodEnum",
    "ProjectsSelectionConfig",
    "ProjectsSelectionConfigStrict",
    "SqlDbCatalogConfig",
    "SqlDbCatalogConfigStrict",
    "SqlDbDialectOptions",
    "SqlDbDialectOptionsStrict",
    "SqlDbQuery",
    "SqlDbQueryConfig",
    "SqlDbQueryConfigStrict",
    "SqlDbQueryStrict",
    "SqlDialectEnum",
    "SqlModeEnum",
    "SQLSchemaTables",
    "SQLSchemaTablesStrict",
    "SourceConfig",
    "SourceConfigStrict",
    "SourceMongoDB",
    "SourceMongoDBStrict",
    "SourceSQLDB",
    "SourceSQLDBStrict",
    "SourceOracleCustomSQL",
    "SourceOracleCustomSQLStrict",
    "SourcePKBConclusion",
    "SourcePKBConclusionStrict",
    "SourceSQLDatabase",
    "SourceSQLDatabaseStrict",
    "SourceUploaderB057",
    "SourceUploaderB057Strict",
    "UploaderApiPaths",
    "UploaderApiPathsStrict",
    "UploaderBinsConfig",
    "UploaderBinsConfigStrict",
    "UploaderKafkaConfig",
    "UploaderKafkaConfigStrict",
]
