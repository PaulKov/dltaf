"""Connection-secret to ENV mapping helpers.

These functions are deliberately small and deterministic: they accept already
resolved secret dictionaries plus optional non-secret overrides and produce the
ENV contract consumed by ``dlt`` sources/destinations and private ``dltaf``
plugins.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Mapping, Optional
from urllib.parse import quote_plus


def coalesce(*values: Any, default: Any = None) -> Any:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and value.strip() == "":
            continue
        return value
    return default


def as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value != 0
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    return default


def normalize_env_prefix(prefix: Any, *, default: str = "") -> str:
    if prefix is None:
        return default
    normalized = str(prefix).strip()
    if not normalized:
        return default
    return f"{normalized.rstrip('_')}__"


def build_clickhouse_env(
    secret: Mapping[str, Any],
    *,
    database: Optional[str] = None,
    dataset_table_separator: str = "__",
) -> Dict[str, str]:
    host = str(coalesce(secret.get("host"), secret.get("hostname"), default="")).strip()
    port = int(coalesce(secret.get("port"), default=9000))
    http_port = int(coalesce(secret.get("http_port"), secret.get("httpPort"), default=8123))
    secure = as_bool(coalesce(secret.get("secure"), secret.get("tls"), default=False))
    username = str(
        coalesce(secret.get("username"), secret.get("user"), secret.get("login"), default="")
    ).strip()
    password = str(coalesce(secret.get("password"), secret.get("pass"), default="")).strip()
    target_database = str(coalesce(database, secret.get("database"), default="")).strip()

    if not host:
        raise ValueError("ClickHouse secret must contain 'host'")
    if not target_database:
        raise ValueError("ClickHouse database is required (set in secret or override)")

    return {
        "DESTINATION__CLICKHOUSE__DATASET_TABLE_SEPARATOR": str(dataset_table_separator),
        "DESTINATION__CLICKHOUSE__CREDENTIALS__HOST": host,
        "DESTINATION__CLICKHOUSE__CREDENTIALS__PORT": str(port),
        "DESTINATION__CLICKHOUSE__CREDENTIALS__HTTP_PORT": str(http_port),
        "DESTINATION__CLICKHOUSE__CREDENTIALS__SECURE": "1" if secure else "0",
        "DESTINATION__CLICKHOUSE__CREDENTIALS__DATABASE": target_database,
        "DESTINATION__CLICKHOUSE__CREDENTIALS__USERNAME": username,
        "DESTINATION__CLICKHOUSE__CREDENTIALS__PASSWORD": password,
    }


def build_sql_database_env(
    secret: Mapping[str, Any],
    *,
    drivername: Optional[str] = None,
    database: Optional[str] = None,
) -> Dict[str, str]:
    resolved_driver = str(
        coalesce(drivername, secret.get("drivername"), secret.get("driver"), default="")
    ).strip()
    host = str(coalesce(secret.get("host"), secret.get("hostname"), default="")).strip()
    port = int(coalesce(secret.get("port"), default=0) or 0)
    username = str(
        coalesce(secret.get("username"), secret.get("user"), secret.get("login"), default="")
    ).strip()
    password = str(coalesce(secret.get("password"), secret.get("pass"), default="")).strip()
    target_database = str(
        coalesce(database, secret.get("database"), secret.get("schema"), default="")
    ).strip()
    dsn = str(coalesce(secret.get("dsn"), default="")).strip()

    if not resolved_driver:
        raise ValueError("SQL secret must contain 'drivername' (or provide override)")
    if not host:
        raise ValueError("SQL secret must contain 'host'")
    if not username or not password:
        raise ValueError("SQL secret must contain 'username' and 'password'")

    env = {
        "SOURCES__SQL_DATABASE__CREDENTIALS__DRIVERNAME": resolved_driver,
        "SOURCES__SQL_DATABASE__CREDENTIALS__HOST": host,
        "SOURCES__SQL_DATABASE__CREDENTIALS__USERNAME": username,
        "SOURCES__SQL_DATABASE__CREDENTIALS__PASSWORD": password,
    }
    if port:
        env["SOURCES__SQL_DATABASE__CREDENTIALS__PORT"] = str(port)
    if target_database:
        env["SOURCES__SQL_DATABASE__CREDENTIALS__DATABASE"] = target_database
    if dsn:
        env["SOURCES__SQL_DATABASE__CREDENTIALS__DSN"] = dsn
    return env


def build_mongodb_env(secret: Mapping[str, Any]) -> Dict[str, str]:
    url = str(
        coalesce(
            secret.get("connection_url"),
            secret.get("connectionUrl"),
            secret.get("uri"),
            secret.get("url"),
            default="",
        )
    ).strip()

    if not url:
        host = str(secret.get("host") or "").strip()
        if not host:
            raise ValueError(
                "Mongo secret must contain 'connection_url' (or 'uri'/'url') "
                "or at least 'host' (and optionally port, user, password, database)"
            )
        port = secret.get("port") or 27017
        user = str(secret.get("user") or secret.get("username") or "").strip()
        password = str(secret.get("password") or secret.get("pass") or "").strip()
        database = str(secret.get("database") or "").strip()

        if user and password:
            url = f"mongodb://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}"
        else:
            url = f"mongodb://{host}:{port}"
        if database:
            url = f"{url}/{database}"

    if not re.search(r"[?&]directconnection=", url, re.IGNORECASE):
        url = f"{url}{'&' if '?' in url else '?'}directConnection=true"

    return {"SOURCES__MONGODB__CONNECTION_URL": url}


def build_keycloak_env(secret: Mapping[str, Any]) -> Dict[str, str]:
    token_url = str(
        coalesce(secret.get("token_url"), secret.get("tokenUrl"), secret.get("url"), default="")
    ).strip()
    client_id = str(coalesce(secret.get("client_id"), secret.get("clientId"), default="")).strip()
    client_secret = str(
        coalesce(secret.get("client_secret"), secret.get("clientSecret"), default="")
    ).strip()
    username = str(coalesce(secret.get("username"), secret.get("user"), default="")).strip()
    password = str(coalesce(secret.get("password"), secret.get("pass"), default="")).strip()
    grant_type = str(
        coalesce(secret.get("grant_type"), secret.get("grantType"), default="password")
    ).strip()
    scope = str(coalesce(secret.get("scope"), default="openid")).strip()
    verify_ssl = as_bool(
        coalesce(secret.get("verify_ssl"), secret.get("verifySsl"), secret.get("verify"), default=True),
        default=True,
    )
    timeout_seconds = int(
        coalesce(secret.get("timeout_seconds"), secret.get("timeoutSeconds"), default=30) or 30
    )

    if not token_url:
        raise ValueError("Keycloak secret must contain 'token_url'")
    if not client_id:
        raise ValueError("Keycloak secret must contain 'client_id'")

    env = {
        "SOURCES__KEYCLOAK__TOKEN_URL": token_url,
        "SOURCES__KEYCLOAK__CLIENT_ID": client_id,
        "SOURCES__KEYCLOAK__GRANT_TYPE": grant_type,
        "SOURCES__KEYCLOAK__SCOPE": scope,
        "SOURCES__KEYCLOAK__VERIFY_SSL": "1" if verify_ssl else "0",
        "SOURCES__KEYCLOAK__TIMEOUT_SECONDS": str(timeout_seconds),
    }
    if client_secret:
        env["SOURCES__KEYCLOAK__CLIENT_SECRET"] = client_secret
    if username:
        env["SOURCES__KEYCLOAK__USERNAME"] = username
    if password:
        env["SOURCES__KEYCLOAK__PASSWORD"] = password
    return env


def build_kafka_env(secret: Mapping[str, Any], *, env_prefix: str = "KAFKA__") -> Dict[str, str]:
    prefix = normalize_env_prefix(env_prefix, default="KAFKA__")
    bootstrap_servers = coalesce(
        secret.get("bootstrap_servers"),
        secret.get("bootstrapServers"),
        secret.get("brokers"),
        secret.get("servers"),
        secret.get("bootstrap"),
        default=None,
    )
    if isinstance(bootstrap_servers, (list, tuple)):
        bootstrap_value = ",".join(str(item).strip() for item in bootstrap_servers if str(item).strip())
    else:
        bootstrap_value = str(bootstrap_servers).strip() if bootstrap_servers is not None else ""

    key_aliases = {
        "security_protocol": ("security_protocol", "securityProtocol", "protocol"),
        "sasl_mechanism": ("sasl_mechanism", "saslMechanism", "mechanism"),
        "sasl_username": ("sasl_username", "saslUser", "username", "user"),
        "sasl_password": ("sasl_password", "password", "pass"),
        "ssl_cafile": ("ssl_cafile", "cafile", "sslCaFile"),
        "ssl_certfile": ("ssl_certfile", "certfile", "sslCertFile"),
        "ssl_keyfile": ("ssl_keyfile", "keyfile", "sslKeyFile"),
    }

    env: Dict[str, str] = {}
    if bootstrap_value:
        env[f"{prefix}BOOTSTRAP_SERVERS"] = bootstrap_value
    for field, aliases in key_aliases.items():
        value = coalesce(*(secret.get(alias) for alias in aliases), default=None)
        if value is not None and str(value).strip():
            env[f"{prefix}{field.upper()}"] = str(value).strip()
    ssl_check_hostname = secret.get("ssl_check_hostname")
    if ssl_check_hostname is not None and str(ssl_check_hostname).strip():
        env[f"{prefix}SSL_CHECK_HOSTNAME"] = (
            "1" if as_bool(ssl_check_hostname, default=True) else "0"
        )
    return env
