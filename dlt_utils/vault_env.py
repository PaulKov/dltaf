from __future__ import annotations

import contextlib
import json
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterator, Mapping, Optional
from urllib.parse import quote_plus

@dataclass(frozen=True)
class VaultSecretRef:
    """Reference to a Vault KV secret."""

    mount_point: str
    path: str
    kv_version: Optional[str] = None


def parse_vault_ref(ref: Any) -> VaultSecretRef:
    """Parse a vault reference.

    Supported forms:
      - "vault://<mount>/<path>"
      - "<mount>:<path>"  (recommended)
      - {mount_point: ..., path: ..., kv_version: ...}
      - {ref: "mount:path", kv_version: ...}
    """
    if ref is None:
        raise ValueError("vault ref is required")

    if isinstance(ref, VaultSecretRef):
        return ref

    if isinstance(ref, Mapping):
        if "ref" in ref:
            nested_ref = parse_vault_ref(ref.get("ref"))
            kv_version = ref.get("kv_version", nested_ref.kv_version)
            return VaultSecretRef(
                mount_point=nested_ref.mount_point,
                path=nested_ref.path,
                kv_version=str(kv_version) if kv_version not in (None, "") else None,
            )

        mp = str(ref.get("mount_point") or ref.get("mount") or "").strip()
        path = str(ref.get("path") or "").strip().strip("/")
        kv_version = ref.get("kv_version")
        if not mp or not path:
            raise ValueError(f"invalid vault ref dict: {ref}")
        return VaultSecretRef(
            mount_point=mp.rstrip("/"),
            path=path,
            kv_version=str(kv_version) if kv_version not in (None, "") else None,
        )

    if not isinstance(ref, str):
        raise ValueError(f"vault ref must be str or mapping, got: {type(ref)}")

    s = ref.strip()
    if s.startswith("vault://"):
        s = s[len("vault://") :]
        parts = s.split("/", 1)
        if len(parts) != 2:
            raise ValueError(f"invalid vault ref: {ref}")
        mount_point, path = parts[0], parts[1]
        return VaultSecretRef(mount_point=mount_point.rstrip("/"), path=path.strip("/"))

    if ":" in s:
        mount_point, path = s.split(":", 1)
        mount_point = mount_point.strip().rstrip("/")
        path = path.strip().strip("/")
        if not mount_point or not path:
            raise ValueError(f"invalid vault ref: {ref}")
        return VaultSecretRef(mount_point=mount_point, path=path)

    raise ValueError(
        "Vault ref must be 'mount:path' or 'vault://mount/path'. "
        f"Got: {ref!r}"
    )


def _coalesce(*vals: Any, default: Any = None) -> Any:
    for v in vals:
        if v is None:
            continue
        if isinstance(v, str) and v.strip() == "":
            continue
        return v
    return default


def _as_bool(v: Any, default: bool = False) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    if isinstance(v, int):
        return v != 0
    s = str(v).strip().lower()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return default


def normalize_env_prefix(prefix: Any, *, default: str = "") -> str:
    """Normalize an ENV prefix to always end with ``__``.

    Examples:
        - "KAFKA" -> "KAFKA__"
        - "KAFKA__" -> "KAFKA__"
        - "SOURCES__KAFKA" -> "SOURCES__KAFKA__"

    Args:
        prefix: input prefix
        default: returned when prefix is empty/None

    Returns:
        Normalized prefix.
    """

    if prefix is None:
        return default
    p = str(prefix).strip()
    if not p:
        return default
    # Ensure exactly one "__" suffix (allow internal "__" parts).
    p = p.rstrip("_")
    return f"{p}__"


def apply_overrides(secret: Mapping[str, Any], overrides: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    out = dict(secret or {})
    if overrides:
        for k, v in overrides.items():
            out[k] = v
    return out


def get_secret_from_vault(ref: VaultSecretRef) -> Dict[str, Any]:
    """Fetch a KV secret through ``vault-kv-client``.

    ``dltaf`` deliberately delegates Vault authentication and transport details
    to the dedicated OSS client. That keeps this repository free from
    repo-local path hacks and makes the same secret contract work in local
    development, CI, and Airflow.
    """
    try:
        from vault_kv_client import get_default_manager
    except Exception as exc:  # pragma: no cover - import failure is environment-specific
        raise RuntimeError(
            "vault-kv-client is required for manifest Vault integration. "
            "Install dependency 'vault-kv-client>=0.1.0'."
        ) from exc

    _ensure_vault_env_aliases()
    manager = get_default_manager()
    secret = manager.get_secret(
        mount_point=ref.mount_point,
        path=ref.path,
        kv_version=ref.kv_version,
    )
    return dict(secret)


def _ensure_vault_env_aliases() -> None:
    """Normalize Vault env aliases expected by ``vault-kv-client``.

    Consumer runtimes often expose ``VAULT_ADDRESS``, while the public client
    contract uses ``VAULT_ADDR``. Keep both names aligned before constructing
    the default manager so package-mode runtimes stay portable.
    """

    vault_addr = str(os.getenv("VAULT_ADDR", "") or "").strip()
    vault_address = str(os.getenv("VAULT_ADDRESS", "") or "").strip()

    if not vault_addr and vault_address:
        os.environ["VAULT_ADDR"] = vault_address

    if not vault_address and vault_addr:
        os.environ["VAULT_ADDRESS"] = vault_addr


def build_clickhouse_env(
    secret: Mapping[str, Any],
    *,
    database: Optional[str] = None,
    dataset_table_separator: str = "__",
) -> Dict[str, str]:
    """Map a ClickHouse secret dict to dlt destination env vars."""
    host = str(_coalesce(secret.get("host"), secret.get("hostname"), default="")).strip()
    port = int(_coalesce(secret.get("port"), default=9000))
    http_port = int(_coalesce(secret.get("http_port"), secret.get("httpPort"), default=8123))
    secure = _as_bool(_coalesce(secret.get("secure"), secret.get("tls"), default=False))
    username = str(_coalesce(secret.get("username"), secret.get("user"), secret.get("login"), default="")).strip()
    password = str(_coalesce(secret.get("password"), secret.get("pass"), default="")).strip()
    db = str(_coalesce(database, secret.get("database"), default="")).strip()

    if not host:
        raise ValueError("ClickHouse secret must contain 'host'")
    if not db:
        raise ValueError("ClickHouse database is required (set in secret or override)")

    return {
        "DESTINATION__CLICKHOUSE__DATASET_TABLE_SEPARATOR": dataset_table_separator,
        "DESTINATION__CLICKHOUSE__CREDENTIALS__HOST": host,
        "DESTINATION__CLICKHOUSE__CREDENTIALS__PORT": str(port),
        "DESTINATION__CLICKHOUSE__CREDENTIALS__HTTP_PORT": str(http_port),
        "DESTINATION__CLICKHOUSE__CREDENTIALS__SECURE": "1" if secure else "0",
        "DESTINATION__CLICKHOUSE__CREDENTIALS__DATABASE": db,
        "DESTINATION__CLICKHOUSE__CREDENTIALS__USERNAME": username,
        "DESTINATION__CLICKHOUSE__CREDENTIALS__PASSWORD": password,
    }


def build_sql_database_env(
    secret: Mapping[str, Any],
    *,
    drivername: Optional[str] = None,
    database: Optional[str] = None,
) -> Dict[str, str]:
    """Map a generic SQL DB secret dict to dlt sql_database env vars."""
    drv = str(_coalesce(drivername, secret.get("drivername"), secret.get("driver"), default="")).strip()
    host = str(_coalesce(secret.get("host"), secret.get("hostname"), default="")).strip()
    port = int(_coalesce(secret.get("port"), default=0) or 0)
    username = str(_coalesce(secret.get("username"), secret.get("user"), secret.get("login"), default="")).strip()
    password = str(_coalesce(secret.get("password"), secret.get("pass"), default="")).strip()
    db = str(_coalesce(database, secret.get("database"), secret.get("schema"), default="")).strip()
    dsn = str(_coalesce(secret.get("dsn"), default="")).strip()

    if not drv:
        raise ValueError("SQL secret must contain 'drivername' (or provide override)")
    if not host:
        raise ValueError("SQL secret must contain 'host'")
    if not username or not password:
        raise ValueError("SQL secret must contain 'username' and 'password'")

    env = {
        "SOURCES__SQL_DATABASE__CREDENTIALS__DRIVERNAME": drv,
        "SOURCES__SQL_DATABASE__CREDENTIALS__HOST": host,
        "SOURCES__SQL_DATABASE__CREDENTIALS__USERNAME": username,
        "SOURCES__SQL_DATABASE__CREDENTIALS__PASSWORD": password,
    }
    if port:
        env["SOURCES__SQL_DATABASE__CREDENTIALS__PORT"] = str(port)
    if db:
        env["SOURCES__SQL_DATABASE__CREDENTIALS__DATABASE"] = db
    if dsn:
        env["SOURCES__SQL_DATABASE__CREDENTIALS__DSN"] = dsn
    return env


def build_mongodb_env(secret: Mapping[str, Any]) -> Dict[str, str]:
    """Build MongoDB connection URL from Vault secret"""
    url = str(
        _coalesce(
            secret.get("connection_url"),
            secret.get("connectionUrl"),
            secret.get("uri"),
            secret.get("url"),
            default="",
        )
    ).strip()

    if not url:
        host = (secret.get("host") or "").strip()
        if not host:
            raise ValueError(
                "Mongo secret must contain 'connection_url' (or 'uri'/'url') "
                "or at least 'host' (and optionally port, user, password, database)"
            )
        port = secret.get("port") or 27017
        user = (secret.get("user") or "").strip()
        password = (secret.get("password") or "").strip()
        database = (secret.get("database") or "").strip()

        if user and password:
            user_esc = quote_plus(user)
            password_esc = quote_plus(password)
            url = f"mongodb://{user_esc}:{password_esc}@{host}:{port}"
        else:
            url = f"mongodb://{host}:{port}"

        if database:
            url = f"{url}/{database}"

    has_direct_connection = re.search(r'[?&]directconnection=', url, re.IGNORECASE)
    
    if not has_direct_connection:
        if "?" not in url:
            url = f"{url}?directConnection=true"
        else:
            url = f"{url}&directConnection=true"

    return {
        "SOURCES__MONGODB__CONNECTION_URL": url,
    }


def build_kafka_env(
    secret: Mapping[str, Any],
    *,
    env_prefix: str = "KAFKA__",
) -> Dict[str, str]:
    """Map a Kafka secret dict to ENV vars.

    This framework intentionally keeps Kafka settings in *generic* ENV vars
    (not tied to a specific integration), so that future custom API/Kafka
    sources can reuse the same approach.

    Supported secret keys (aliases):
        - bootstrap servers: bootstrap_servers, bootstrapServers, brokers, servers
        - security protocol: security_protocol, securityProtocol, protocol
        - sasl mechanism: sasl_mechanism, saslMechanism, mechanism
        - sasl username: sasl_username, saslUser, username, user
        - sasl password: sasl_password, password, pass
        - ssl file paths: ssl_cafile/certfile/keyfile (and common aliases)
        - ssl_check_hostname: ssl_check_hostname

    All values are mapped as strings.
    """

    pfx = normalize_env_prefix(env_prefix, default="KAFKA__")

    # bootstrap servers
    bs = _coalesce(
        secret.get("bootstrap_servers"),
        secret.get("bootstrapServers"),
        secret.get("brokers"),
        secret.get("servers"),
        secret.get("bootstrap"),
        default=None,
    )
    if isinstance(bs, (list, tuple)):
        bs_str = ",".join([str(x).strip() for x in bs if str(x).strip()])
    else:
        bs_str = str(bs).strip() if bs is not None else ""

    security_protocol = _coalesce(
        secret.get("security_protocol"),
        secret.get("securityProtocol"),
        secret.get("protocol"),
        default=None,
    )
    sasl_mechanism = _coalesce(
        secret.get("sasl_mechanism"),
        secret.get("saslMechanism"),
        secret.get("mechanism"),
        default=None,
    )
    sasl_username = _coalesce(
        secret.get("sasl_username"),
        secret.get("saslUser"),
        secret.get("username"),
        secret.get("user"),
        default=None,
    )
    sasl_password = _coalesce(
        secret.get("sasl_password"),
        secret.get("password"),
        secret.get("pass"),
        default=None,
    )

    ssl_cafile = _coalesce(secret.get("ssl_cafile"), secret.get("cafile"), secret.get("sslCaFile"), default=None)
    ssl_certfile = _coalesce(
        secret.get("ssl_certfile"), secret.get("certfile"), secret.get("sslCertFile"), default=None
    )
    ssl_keyfile = _coalesce(secret.get("ssl_keyfile"), secret.get("keyfile"), secret.get("sslKeyFile"), default=None)

    ssl_check_hostname = secret.get("ssl_check_hostname")

    env: Dict[str, str] = {}
    if bs_str:
        env[f"{pfx}BOOTSTRAP_SERVERS"] = bs_str

    if security_protocol is not None and str(security_protocol).strip() != "":
        env[f"{pfx}SECURITY_PROTOCOL"] = str(security_protocol).strip()
    if sasl_mechanism is not None and str(sasl_mechanism).strip() != "":
        env[f"{pfx}SASL_MECHANISM"] = str(sasl_mechanism).strip()
    if sasl_username is not None and str(sasl_username).strip() != "":
        env[f"{pfx}SASL_USERNAME"] = str(sasl_username).strip()
    if sasl_password is not None and str(sasl_password).strip() != "":
        env[f"{pfx}SASL_PASSWORD"] = str(sasl_password).strip()

    if ssl_cafile is not None and str(ssl_cafile).strip() != "":
        env[f"{pfx}SSL_CAFILE"] = str(ssl_cafile).strip()
    if ssl_certfile is not None and str(ssl_certfile).strip() != "":
        env[f"{pfx}SSL_CERTFILE"] = str(ssl_certfile).strip()
    if ssl_keyfile is not None and str(ssl_keyfile).strip() != "":
        env[f"{pfx}SSL_KEYFILE"] = str(ssl_keyfile).strip()

    if ssl_check_hostname is not None and str(ssl_check_hostname).strip() != "":
        env[f"{pfx}SSL_CHECK_HOSTNAME"] = "1" if _as_bool(ssl_check_hostname, default=True) else "0"

    return env


def build_keycloak_env(secret: Mapping[str, Any]) -> Dict[str, str]:
    """Map a Keycloak secret dict to env vars used by custom sources.

    Expected secret keys (flexible aliases are supported):
      - token_url / tokenUrl / url
      - client_id / clientId
      - client_secret / clientSecret (optional)
      - username (optional for client_credentials)
      - password (optional for client_credentials)
      - grant_type / grantType (default: password)
      - scope (default: openid)
      - verify_ssl / verifySsl / verify (default: true)
      - timeout_seconds / timeoutSeconds (default: 30)

    Returns env keys under `SOURCES__KEYCLOAK__*`.
    """

    token_url = str(
        _coalesce(secret.get("token_url"), secret.get("tokenUrl"), secret.get("url"), default="")
    ).strip()
    client_id = str(_coalesce(secret.get("client_id"), secret.get("clientId"), default="")).strip()
    client_secret = str(
        _coalesce(secret.get("client_secret"), secret.get("clientSecret"), default="")
    ).strip()
    username = str(_coalesce(secret.get("username"), secret.get("user"), default="")).strip()
    password = str(_coalesce(secret.get("password"), secret.get("pass"), default="")).strip()
    grant_type = str(
        _coalesce(secret.get("grant_type"), secret.get("grantType"), default="password")
    ).strip()
    scope = str(_coalesce(secret.get("scope"), default="openid")).strip()
    verify_ssl = _as_bool(
        _coalesce(secret.get("verify_ssl"), secret.get("verifySsl"), secret.get("verify"), default=True),
        default=True,
    )
    timeout_seconds = int(
        _coalesce(secret.get("timeout_seconds"), secret.get("timeoutSeconds"), default=30)
        or 30
    )

    if not token_url:
        raise ValueError("Keycloak secret must contain 'token_url'")
    if not client_id:
        raise ValueError("Keycloak secret must contain 'client_id'")

    env: Dict[str, str] = {
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


@contextlib.contextmanager
def temporary_environ(env: Mapping[str, str]) -> Iterator[None]:
    """Temporarily set os.environ keys for the current process."""
    old: Dict[str, Optional[str]] = {}
    for k, v in (env or {}).items():
        old[k] = os.environ.get(k)
        os.environ[k] = str(v)
    try:
        yield
    finally:
        for k, prev in old.items():
            if prev is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = prev


def parse_csv_or_json_list(value: str) -> list[str]:
    """Parse either JSON list or comma-separated string into list[str]."""
    s = (value or "").strip()
    if not s:
        return []
    if s.startswith("["):
        try:
            arr = json.loads(s)
            if isinstance(arr, list):
                return [str(x).strip() for x in arr if str(x).strip()]
        except Exception:
            pass
    return [p.strip() for p in s.split(",") if p.strip()]
