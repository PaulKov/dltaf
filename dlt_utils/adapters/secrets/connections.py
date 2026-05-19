"""Build ENV for a run from `connections` section.

Stage 3 standardizes secrets resolution:
    Vault (preferred) -> Airflow Variables (fallback)

We return both:
    - env mapping (key -> value)
    - provenance (key -> source label)

The provenance is used by `--explain-config` to show where a value comes from
without leaking the value itself.

Manifest compatibility
---------------------
Existing manifests that only have `vault` continue to work unchanged.
The Airflow Variables fallback is opt-in: either specify `airflow_variable` (JSON)
or configure a prefix for per-field variables.

Supported connection blocks:
    connections.source.kind: oracle | postgres | sql | mongodb | keycloak
    connections.destination.kind: clickhouse
    connections.kafka.kind: kafka

Airflow Variables options (all optional):
    airflow_variable: <VAR_NAME>  # JSON object
    airflow_variable_prefix: <PREFIX__>
    airflow_variables: {field: var_name, ...}

For `connections.kafka` we also support:
    env_prefix: KAFKA__  # which ENV keys will be produced
"""

from __future__ import annotations

from typing import Any, Dict, Mapping, Optional, Tuple

from dlt_utils.adapters.secrets.backends import (
    try_get_airflow_fields_secret,
    try_get_airflow_json_secret,
    try_get_vault_secret,
)
from dlt_utils.adapters.secrets.types import ResolvedEnv, ResolvedValue
from dlt_utils.vault_env import (
    build_clickhouse_env,
    build_kafka_env,
    build_keycloak_env,
    build_mongodb_env,
    build_sql_database_env,
    normalize_env_prefix,
)


SQL_FIELDS: Mapping[str, str] = {
    "drivername": "DRIVERNAME",
    "host": "HOST",
    "port": "PORT",
    "username": "USERNAME",
    "password": "PASSWORD",
    "database": "DATABASE",
    "dsn": "DSN",
}

CLICKHOUSE_FIELDS: Mapping[str, str] = {
    "host": "HOST",
    "port": "PORT",
    "http_port": "HTTP_PORT",
    "secure": "SECURE",
    "database": "DATABASE",
    "username": "USERNAME",
    "password": "PASSWORD",
    # Not a secret, but convenient to store alongside credentials
    "dataset_table_separator": "DATASET_TABLE_SEPARATOR",
}

MONGODB_FIELDS: Mapping[str, str] = {
    "connection_url": "CONNECTION_URL",
    "host": "HOST",
    "port": "PORT",
    "user": "USER",
    "password": "PASSWORD",
    "database": "DATABASE",
}

KEYCLOAK_FIELDS: Mapping[str, str] = {
    "token_url": "TOKEN_URL",
    "client_id": "CLIENT_ID",
    "client_secret": "CLIENT_SECRET",
    "username": "USERNAME",
    "password": "PASSWORD",
    "grant_type": "GRANT_TYPE",
    "scope": "SCOPE",
    "verify_ssl": "VERIFY_SSL",
    "timeout_seconds": "TIMEOUT_SECONDS",
}

KAFKA_FIELDS: Mapping[str, str] = {
    "bootstrap_servers": "BOOTSTRAP_SERVERS",
    "security_protocol": "SECURITY_PROTOCOL",
    "sasl_mechanism": "SASL_MECHANISM",
    "sasl_username": "SASL_USERNAME",
    "sasl_password": "SASL_PASSWORD",
    "ssl_cafile": "SSL_CAFILE",
    "ssl_certfile": "SSL_CERTFILE",
    "ssl_keyfile": "SSL_KEYFILE",
    "ssl_check_hostname": "SSL_CHECK_HOSTNAME",
}


def _optional_non_empty_ref(value: Any) -> Optional[Any]:
    """Return a Vault ref without coercing structured mappings to strings."""

    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return value


def _resolve_secret(
    *,
    block: Mapping[str, Any],
    default_airflow_prefix: str,
    fields: Mapping[str, str],
) -> Optional[Tuple[Dict[str, Any], str]]:
    """Resolve connection secret with precedence Vault -> Airflow Variables."""

    vault_ref = _optional_non_empty_ref(block.get("vault"))
    overrides = block.get("overrides") or {}
    if overrides and not isinstance(overrides, Mapping):
        raise ValueError("connections.*.overrides must be a mapping")

    # 1) Vault
    r = try_get_vault_secret(vault_ref, overrides=overrides)
    if r is not None:
        return r

    # 2) Airflow JSON Variable
    airflow_json = str(block.get("airflow_variable") or "").strip() or None
    r = try_get_airflow_json_secret(airflow_json, overrides=overrides)
    if r is not None:
        return r

    # 3) Airflow per-field variables
    airflow_mapping = block.get("airflow_variables") or {}
    if airflow_mapping and not isinstance(airflow_mapping, Mapping):
        raise ValueError("connections.*.airflow_variables must be a mapping")

    airflow_prefix = normalize_env_prefix(
        block.get("airflow_variable_prefix") or default_airflow_prefix,
        default=default_airflow_prefix,
    )
    r = try_get_airflow_fields_secret(
        prefix=airflow_prefix,
        fields=fields,
        mapping=airflow_mapping,
        overrides=overrides,
    )
    return r


def _add_env(resolved: ResolvedEnv, env_map: Mapping[str, str], *, source: str) -> None:
    for k, v in env_map.items():
        resolved.values[str(k)] = ResolvedValue(value=str(v), source=source)


def build_resolved_env_from_connections(connections: Mapping[str, Any]) -> ResolvedEnv:
    """Build resolved ENV vars from manifest `connections` section."""

    resolved = ResolvedEnv()

    # ------------------------------------------------------------------
    # source
    # ------------------------------------------------------------------
    if connections.get("source"):
        src = connections["source"]
        if not isinstance(src, Mapping):
            raise ValueError("connections.source must be a mapping")

        src_kind = str(src.get("kind") or "").lower().strip()
        if not src_kind:
            raise ValueError("connections.source.kind is required")

        if src_kind in {"oracle", "postgres", "sql"}:
            secret_r = _resolve_secret(block=src, default_airflow_prefix="SQL_DATABASE__", fields=SQL_FIELDS)
            if secret_r is not None:
                secret, src_label = secret_r
                label = src_label + (" + overrides" if src.get("overrides") else "")
                env_map = build_sql_database_env(secret)
                _add_env(resolved, env_map, source=label)

        elif src_kind in {"mongodb", "mongo"}:
            secret_r = _resolve_secret(block=src, default_airflow_prefix="MONGODB__", fields=MONGODB_FIELDS)
            if secret_r is not None:
                secret, src_label = secret_r
                label = src_label + (" + overrides" if src.get("overrides") else "")
                env_map = build_mongodb_env(secret)
                _add_env(resolved, env_map, source=label)

        elif src_kind in {"keycloak"}:
            secret_r = _resolve_secret(block=src, default_airflow_prefix="KEYCLOAK__", fields=KEYCLOAK_FIELDS)
            if secret_r is not None:
                secret, src_label = secret_r
                label = src_label + (" + overrides" if src.get("overrides") else "")
                env_map = build_keycloak_env(secret)
                _add_env(resolved, env_map, source=label)

        else:
            raise ValueError(f"Unsupported connections.source.kind: {src_kind}")

    # ------------------------------------------------------------------
    # destination
    # ------------------------------------------------------------------
    if connections.get("destination"):
        dst = connections["destination"]
        if not isinstance(dst, Mapping):
            raise ValueError("connections.destination must be a mapping")

        dst_kind = str(dst.get("kind") or "").lower().strip()
        if not dst_kind:
            raise ValueError("connections.destination.kind is required")

        if dst_kind in {"clickhouse"}:
            secret_r = _resolve_secret(
                block=dst,
                default_airflow_prefix="CLICKHOUSE__",
                fields=CLICKHOUSE_FIELDS,
            )
            if secret_r is not None:
                secret, src_label = secret_r
                overrides = dst.get("overrides") or {}
                database_override = overrides.get("database")
                sep_override = overrides.get("dataset_table_separator")

                # database
                database = database_override or secret.get("database")
                db_override = str(database) if database not in (None, "") else None

                # dataset_table_separator (not a secret, but important for table naming)
                if sep_override not in (None, ""):
                    sep = str(sep_override)
                    sep_source = "override:connections.destination.overrides.dataset_table_separator"
                elif secret.get("dataset_table_separator") not in (None, ""):
                    sep = str(secret.get("dataset_table_separator"))
                    sep_source = src_label
                else:
                    sep = "__"
                    sep_source = "default:__"

                env_map = build_clickhouse_env(secret, database=db_override, dataset_table_separator=str(sep))

                # Most keys come from the same source
                label = src_label + (" + overrides" if overrides else "")
                _add_env(resolved, env_map, source=label)

                # Override provenance for the separator key
                if "DESTINATION__CLICKHOUSE__DATASET_TABLE_SEPARATOR" in env_map:
                    resolved.values["DESTINATION__CLICKHOUSE__DATASET_TABLE_SEPARATOR"] = ResolvedValue(
                        value=str(env_map["DESTINATION__CLICKHOUSE__DATASET_TABLE_SEPARATOR"]),
                        source=sep_source,
                    )

                # Override provenance for the database key if it was explicitly overridden
                if database_override not in (None, ""):
                    resolved.values["DESTINATION__CLICKHOUSE__CREDENTIALS__DATABASE"] = ResolvedValue(
                        value=str(env_map["DESTINATION__CLICKHOUSE__CREDENTIALS__DATABASE"]),
                        source="override:connections.destination.overrides.database",
                    )

        else:
            raise ValueError(f"Unsupported connections.destination.kind: {dst_kind}")

    # ------------------------------------------------------------------
    # kafka (optional)
    # ------------------------------------------------------------------
    if connections.get("kafka"):
        kcfg = connections["kafka"]
        if not isinstance(kcfg, Mapping):
            raise ValueError("connections.kafka must be a mapping")

        kafka_kind = str(kcfg.get("kind") or "kafka").strip().lower()
        if kafka_kind != "kafka":
            raise ValueError(f"Unsupported connections.kafka.kind: {kafka_kind}")

        env_prefix = normalize_env_prefix(kcfg.get("env_prefix") or "KAFKA__", default="KAFKA__")

        # For Airflow variables, default prefix is the same as env_prefix
        secret_r = _resolve_secret(
            block=kcfg,
            default_airflow_prefix=str(kcfg.get("airflow_variable_prefix") or env_prefix),
            fields=KAFKA_FIELDS,
        )

        if secret_r is not None:
            secret, src_label = secret_r
            label = src_label + (" + overrides" if kcfg.get("overrides") else "")
            env_map = build_kafka_env(secret, env_prefix=env_prefix)
            _add_env(resolved, env_map, source=label)

    return resolved
