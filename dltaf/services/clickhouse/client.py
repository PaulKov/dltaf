from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any, Callable, List, Mapping, Sequence

logger = logging.getLogger(__name__)

ClickHouseClientFactory = Callable[..., Any]


@dataclass(frozen=True)
class ClickHouseCredentials:
    """ClickHouse connection settings sourced from dltaf destination env vars."""

    host: str
    port: int = 8123
    username: str = "default"
    password: str = ""
    database: str = "default"
    secure: bool = False

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "ClickHouseCredentials | None":
        environ = env or os.environ
        host = str(environ.get("DESTINATION__CLICKHOUSE__CREDENTIALS__HOST") or "").strip()
        if not host:
            return None
        return cls(
            host=host,
            port=int(environ.get("DESTINATION__CLICKHOUSE__CREDENTIALS__HTTP_PORT") or "8123"),
            username=str(environ.get("DESTINATION__CLICKHOUSE__CREDENTIALS__USERNAME") or "default"),
            password=str(environ.get("DESTINATION__CLICKHOUSE__CREDENTIALS__PASSWORD") or ""),
            database=str(environ.get("DESTINATION__CLICKHOUSE__CREDENTIALS__DATABASE") or "default"),
            secure=str(environ.get("DESTINATION__CLICKHOUSE__CREDENTIALS__SECURE") or "0").lower()
            in {"1", "true", "yes", "on"},
        )

    def as_client_kwargs(self) -> dict[str, Any]:
        return {
            "host": self.host,
            "port": self.port,
            "username": self.username,
            "password": self.password,
            "secure": self.secure,
            "database": self.database,
        }


def _default_client_factory(**kwargs: Any) -> Any:
    try:
        import clickhouse_connect
    except ImportError as exc:  # pragma: no cover - dependency availability
        raise RuntimeError(
            "ClickHouse support requires clickhouse-connect. Install dltaf with ClickHouse runtime dependencies."
        ) from exc
    return clickhouse_connect.get_client(**kwargs)


def get_clickhouse_client(
    *,
    env: Mapping[str, str] | None = None,
    client_factory: ClickHouseClientFactory | None = None,
) -> Any | None:
    """Create a ClickHouse client from standard dltaf destination env vars."""

    credentials = ClickHouseCredentials.from_env(env)
    if credentials is None:
        logger.warning("ClickHouse host is not configured; ClickHouse helper is disabled")
        return None

    factory = client_factory or _default_client_factory
    try:
        return factory(**credentials.as_client_kwargs())
    except Exception as exc:  # pragma: no cover - live ClickHouse dependent
        logger.warning("Could not create ClickHouse client: %s", exc)
        return None


def fetch_first_column_values(
    query: str,
    *,
    client: Any | None = None,
    client_factory: ClickHouseClientFactory | None = None,
    env: Mapping[str, str] | None = None,
) -> List[str]:
    """Run a ClickHouse query and return the first column as strings."""

    active_client = client or get_clickhouse_client(env=env, client_factory=client_factory)
    if active_client is None:
        raise RuntimeError(
            "ClickHouse client is not available. Configure DESTINATION__CLICKHOUSE__CREDENTIALS__* "
            "or pass an explicit client."
        )
    result = active_client.query(query)
    rows = getattr(result, "result_rows", None) or []
    values: List[str] = []
    for row in rows:
        if not row:
            continue
        value = row[0]
        if value is None:
            continue
        values.append(str(value))
    return values


def get_expected_table_names(manifest: Mapping[str, Any]) -> List[str]:
    """Extract expected target table names from a manifest without legacy imports."""

    source_cfg = manifest.get("source") or {}
    kind = str(source_cfg.get("kind") or "").strip()

    if kind == "oracle_custom_sql":
        tables: List[str] = []
        for query_cfg in source_cfg.get("queries") or []:
            if isinstance(query_cfg, Mapping):
                table_name = query_cfg.get("table_name") or query_cfg.get("name")
                if table_name:
                    tables.append(str(table_name))
        return tables

    if kind == "sql_database":
        schemas = source_cfg.get("schemas")
        if isinstance(schemas, Mapping) and schemas:
            tables = []
            for schema_name, schema_cfg in schemas.items():
                if not isinstance(schema_cfg, Mapping):
                    continue
                for table in schema_cfg.get("tables") or []:
                    tables.append(f"{schema_name}__{table}")
            return tables

        transform = source_cfg.get("table_name_transform") or {}
        drop_prefix = str(transform.get("drop_prefix") or "") if isinstance(transform, Mapping) else ""
        tables = []
        for table in source_cfg.get("tables") or []:
            table_name = str(table)
            if drop_prefix and table_name.startswith(drop_prefix):
                table_name = table_name[len(drop_prefix) :]
            tables.append(table_name)
        return tables

    if kind == "mongodb":
        collections: Sequence[Any] = source_cfg.get("collection_names") or []
        return [str(collection) for collection in collections]

    return []
