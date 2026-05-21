"""Resolve manifest ``connections`` blocks into runtime ENV values.

Precedence is intentionally explicit and stable:

1. Vault secret, if ``vault`` is configured.
2. Airflow JSON Variable, if ``airflow_variable`` is configured.
3. Airflow per-field Variables via prefix/mapping.
4. Inline ``overrides`` merged into whichever backend resolved.

This module is native OSS ``dltaf`` runtime code and must not import the legacy
``dlt_utils`` compatibility package.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, Tuple

from dltaf.services.secrets.airflow_variables import get_airflow_variable
from dltaf.services.secrets.env_builders import (
    build_clickhouse_env,
    build_kafka_env,
    build_keycloak_env,
    build_mongodb_env,
    build_sql_database_env,
    normalize_env_prefix,
)
from dltaf.services.secrets.types import ResolvedEnv, ResolvedValue
from dltaf.services.secrets.vault import VaultGetter, apply_overrides, parse_vault_ref, resolve_vault_secret

logger = logging.getLogger(__name__)

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
    "dataset_table_separator": "DATASET_TABLE_SEPARATOR",
}

MONGODB_FIELDS: Mapping[str, str] = {
    "connection_url": "CONNECTION_URL",
    "host": "HOST",
    "port": "PORT",
    "user": "USER",
    "username": "USERNAME",
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


@dataclass(frozen=True)
class ConnectionSecretResolver:
    """Backend-aware resolver with injectable Vault transport for tests."""

    vault_getter: VaultGetter | None = None

    def resolve_connections(self, connections: Mapping[str, Any]) -> ResolvedEnv:
        resolved = ResolvedEnv()
        self._resolve_source(connections, resolved)
        self._resolve_destination(connections, resolved)
        self._resolve_kafka(connections, resolved)
        return resolved

    def _resolve_source(self, connections: Mapping[str, Any], resolved: ResolvedEnv) -> None:
        source = connections.get("source")
        if not source:
            return
        if not isinstance(source, Mapping):
            raise ValueError("connections.source must be a mapping")

        source_kind = str(source.get("kind") or "").lower().strip()
        if not source_kind:
            raise ValueError("connections.source.kind is required")

        if source_kind in {"oracle", "postgres", "sql"}:
            secret = self._resolve_secret(
                block=source,
                default_airflow_prefix="SQL_DATABASE__",
                fields=SQL_FIELDS,
            )
            if secret is not None:
                payload, label = secret
                _add_env(
                    resolved,
                    build_sql_database_env(payload),
                    source=_with_overrides_label(label, source),
                )
            return

        if source_kind in {"mongodb", "mongo"}:
            secret = self._resolve_secret(
                block=source,
                default_airflow_prefix="MONGODB__",
                fields=MONGODB_FIELDS,
            )
            if secret is not None:
                payload, label = secret
                _add_env(
                    resolved,
                    build_mongodb_env(payload),
                    source=_with_overrides_label(label, source),
                )
            return

        if source_kind == "keycloak":
            secret = self._resolve_secret(
                block=source,
                default_airflow_prefix="KEYCLOAK__",
                fields=KEYCLOAK_FIELDS,
            )
            if secret is not None:
                payload, label = secret
                _add_env(
                    resolved,
                    build_keycloak_env(payload),
                    source=_with_overrides_label(label, source),
                )
            return

        raise ValueError(f"Unsupported connections.source.kind: {source_kind}")

    def _resolve_destination(
        self, connections: Mapping[str, Any], resolved: ResolvedEnv
    ) -> None:
        destination = connections.get("destination")
        if not destination:
            return
        if not isinstance(destination, Mapping):
            raise ValueError("connections.destination must be a mapping")

        destination_kind = str(destination.get("kind") or "").lower().strip()
        if destination_kind != "clickhouse":
            raise ValueError(f"Unsupported connections.destination.kind: {destination_kind}")

        secret = self._resolve_secret(
            block=destination,
            default_airflow_prefix="CLICKHOUSE__",
            fields=CLICKHOUSE_FIELDS,
        )
        if secret is None:
            return

        payload, label = secret
        overrides = _mapping(destination.get("overrides"), label="connections.destination.overrides")
        database_override = overrides.get("database")
        separator_override = overrides.get("dataset_table_separator")
        separator = (
            str(separator_override)
            if separator_override not in (None, "")
            else str(payload.get("dataset_table_separator") or "__")
        )
        database = str(database_override) if database_override not in (None, "") else None
        env = build_clickhouse_env(payload, database=database, dataset_table_separator=separator)
        _add_env(resolved, env, source=_with_overrides_label(label, destination))

        if "DESTINATION__CLICKHOUSE__DATASET_TABLE_SEPARATOR" in env:
            resolved.values["DESTINATION__CLICKHOUSE__DATASET_TABLE_SEPARATOR"] = ResolvedValue(
                value=env["DESTINATION__CLICKHOUSE__DATASET_TABLE_SEPARATOR"],
                source=(
                    "override:connections.destination.overrides.dataset_table_separator"
                    if separator_override not in (None, "")
                    else label
                ),
            )
        if database_override not in (None, ""):
            resolved.values["DESTINATION__CLICKHOUSE__CREDENTIALS__DATABASE"] = ResolvedValue(
                value=env["DESTINATION__CLICKHOUSE__CREDENTIALS__DATABASE"],
                source="override:connections.destination.overrides.database",
            )

    def _resolve_kafka(self, connections: Mapping[str, Any], resolved: ResolvedEnv) -> None:
        kafka = connections.get("kafka")
        if not kafka:
            return
        if not isinstance(kafka, Mapping):
            raise ValueError("connections.kafka must be a mapping")

        kafka_kind = str(kafka.get("kind") or "kafka").lower().strip()
        if kafka_kind != "kafka":
            raise ValueError(f"Unsupported connections.kafka.kind: {kafka_kind}")

        env_prefix = normalize_env_prefix(kafka.get("env_prefix") or "KAFKA__", default="KAFKA__")
        secret = self._resolve_secret(
            block=kafka,
            default_airflow_prefix=str(kafka.get("airflow_variable_prefix") or env_prefix),
            fields=KAFKA_FIELDS,
        )
        if secret is not None:
            payload, label = secret
            _add_env(
                resolved,
                build_kafka_env(payload, env_prefix=env_prefix),
                source=_with_overrides_label(label, kafka),
            )

    def _resolve_secret(
        self,
        *,
        block: Mapping[str, Any],
        default_airflow_prefix: str,
        fields: Mapping[str, str],
    ) -> Optional[Tuple[Dict[str, Any], str]]:
        overrides = _mapping(block.get("overrides"), label="connections.*.overrides")

        vault_ref = _optional_non_empty_ref(block.get("vault"))
        if vault_ref:
            try:
                ref = parse_vault_ref(vault_ref)
                payload = resolve_vault_secret(vault_ref, overrides=overrides, getter=self.vault_getter)
                return payload, f"vault:{ref.mount_point}:{ref.path}"
            except Exception as exc:
                logger.warning(
                    "Vault secret is not available (%s). Falling back to Airflow Variables. Error: %s",
                    vault_ref,
                    exc,
                )

        airflow_json_name = str(block.get("airflow_variable") or "").strip()
        if airflow_json_name:
            payload = _try_airflow_json_secret(airflow_json_name, overrides=overrides)
            if payload is not None:
                return payload, f"airflow_variable_json:{airflow_json_name}"

        field_mapping = _mapping(
            block.get("airflow_variables"),
            label="connections.*.airflow_variables",
        )
        airflow_prefix = normalize_env_prefix(
            block.get("airflow_variable_prefix") or default_airflow_prefix,
            default=default_airflow_prefix,
        )
        payload = _try_airflow_fields_secret(
            prefix=airflow_prefix,
            fields=fields,
            mapping=field_mapping,
            overrides=overrides,
        )
        if payload is not None:
            return payload, f"airflow_variables_prefix:{airflow_prefix}"
        return None


def build_resolved_env_from_connections(
    connections: Mapping[str, Any],
    *,
    vault_getter: VaultGetter | None = None,
) -> ResolvedEnv:
    return ConnectionSecretResolver(vault_getter=vault_getter).resolve_connections(connections)


def _optional_non_empty_ref(value: Any) -> Optional[Any]:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return value


def _mapping(value: Any, *, label: str) -> Mapping[str, Any]:
    if value in (None, ""):
        return {}
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} must be a mapping")
    return value


def _with_overrides_label(label: str, block: Mapping[str, Any]) -> str:
    return label + (" + overrides" if block.get("overrides") else "")


def _add_env(resolved: ResolvedEnv, env: Mapping[str, str], *, source: str) -> None:
    for key, value in env.items():
        resolved.values[str(key)] = ResolvedValue(value=str(value), source=source)


def _try_airflow_json_secret(
    variable_name: str,
    *,
    overrides: Mapping[str, Any],
) -> Optional[Dict[str, Any]]:
    raw = get_airflow_variable(variable_name)
    if raw is None or str(raw).strip() == "":
        return None
    try:
        payload = json.loads(str(raw))
    except Exception as exc:
        raise ValueError(f"Airflow Variable '{variable_name}' must be a JSON object: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"Airflow Variable '{variable_name}' must be a JSON object")
    return apply_overrides(payload, overrides)


def _try_airflow_fields_secret(
    *,
    prefix: str,
    fields: Mapping[str, str],
    mapping: Mapping[str, Any],
    overrides: Mapping[str, Any],
) -> Optional[Dict[str, Any]]:
    payload: Dict[str, Any] = {}
    for field, suffix in fields.items():
        variable_name = str(mapping.get(field) or f"{prefix}{suffix}").strip()
        if not variable_name:
            continue
        value = get_airflow_variable(variable_name)
        if value is None:
            continue
        if isinstance(value, str) and value.strip() == "":
            continue
        payload[field] = value
    if not payload:
        return None
    return apply_overrides(payload, overrides)
