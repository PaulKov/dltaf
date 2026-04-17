"""Kafka configuration helpers.

The framework uses Kafka in some custom sources (API trigger -> Kafka wait).

Design goals
------------
1) Allow secrets to be injected from Vault into ENV for the task process.
2) If Vault is not configured/available, allow fallback to Airflow Variables.
3) Keep the runtime code for Kafka consumer creation consistent and reusable.

This module is dependency-free and does not import Airflow.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence

from dlt_utils.vault_env import normalize_env_prefix, parse_csv_or_json_list


def _str_or_none(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def _bool_or_none(v: Any) -> Optional[bool]:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, int):
        return v != 0
    s = str(v).strip().lower()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return None


def parse_bootstrap_servers(value: Any) -> List[str]:
    """Parse bootstrap servers from either list or CSV/JSON string."""
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [str(x).strip() for x in value if str(x).strip()]
    s = str(value).strip()
    if not s:
        return []
    return parse_csv_or_json_list(s)


@dataclass(frozen=True)
class KafkaConsumerSecurity:
    """Security-related configuration for kafka-python KafkaConsumer."""

    security_protocol: Optional[str] = None
    sasl_mechanism: Optional[str] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = None
    ssl_cafile: Optional[str] = None
    ssl_certfile: Optional[str] = None
    ssl_keyfile: Optional[str] = None
    ssl_check_hostname: Optional[bool] = None

    def consumer_kwargs(self) -> Dict[str, Any]:
        """Return kwargs compatible with kafka.KafkaConsumer."""

        out: Dict[str, Any] = {}

        if self.security_protocol:
            out["security_protocol"] = self.security_protocol

        if self.sasl_mechanism:
            out["sasl_mechanism"] = self.sasl_mechanism

        # kafka-python expects sasl_plain_username/password
        if self.sasl_username:
            out["sasl_plain_username"] = self.sasl_username
        if self.sasl_password:
            out["sasl_plain_password"] = self.sasl_password

        if self.ssl_cafile:
            out["ssl_cafile"] = self.ssl_cafile
        if self.ssl_certfile:
            out["ssl_certfile"] = self.ssl_certfile
        if self.ssl_keyfile:
            out["ssl_keyfile"] = self.ssl_keyfile

        if self.ssl_check_hostname is not None:
            out["ssl_check_hostname"] = self.ssl_check_hostname

        return out


@dataclass(frozen=True)
class KafkaConnectionConfig:
    """Kafka connection config used by custom sources."""

    bootstrap_servers: List[str]
    security: KafkaConsumerSecurity


def kafka_connection_from_env(prefix: str = "KAFKA__") -> KafkaConnectionConfig:
    """Read Kafka connection settings from ENV.

    Expected keys (with prefix):
        - BOOTSTRAP_SERVERS (CSV or JSON list)
        - SECURITY_PROTOCOL
        - SASL_MECHANISM
        - SASL_USERNAME
        - SASL_PASSWORD
        - SSL_CAFILE / SSL_CERTFILE / SSL_KEYFILE
        - SSL_CHECK_HOSTNAME (bool-like)
    """

    pfx = normalize_env_prefix(prefix, default="KAFKA__")

    bootstrap_servers = parse_csv_or_json_list(os.getenv(f"{pfx}BOOTSTRAP_SERVERS", ""))

    security = KafkaConsumerSecurity(
        security_protocol=_str_or_none(os.getenv(f"{pfx}SECURITY_PROTOCOL", "")),
        sasl_mechanism=_str_or_none(os.getenv(f"{pfx}SASL_MECHANISM", "")),
        sasl_username=_str_or_none(os.getenv(f"{pfx}SASL_USERNAME", "")),
        sasl_password=_str_or_none(os.getenv(f"{pfx}SASL_PASSWORD", "")),
        ssl_cafile=_str_or_none(os.getenv(f"{pfx}SSL_CAFILE", "")),
        ssl_certfile=_str_or_none(os.getenv(f"{pfx}SSL_CERTFILE", "")),
        ssl_keyfile=_str_or_none(os.getenv(f"{pfx}SSL_KEYFILE", "")),
        ssl_check_hostname=_bool_or_none(os.getenv(f"{pfx}SSL_CHECK_HOSTNAME", "")),
    )

    return KafkaConnectionConfig(bootstrap_servers=bootstrap_servers, security=security)


def kafka_connection_from_manifest_or_env(
    kafka_cfg: Optional[Any],
    *,
    env_prefix: str = "KAFKA__",
    default_bootstrap_servers: Optional[Sequence[str]] = None,
) -> KafkaConnectionConfig:
    """Resolve Kafka connection config.

    Precedence:
        1) Explicit values in manifest.source.kafka (bootstrap_servers, security fields)
        2) ENV (usually injected from Vault/Airflow Variables)
        3) default_bootstrap_servers (optional)

    Args:
        kafka_cfg: manifest "source.kafka" mapping (may be None)
        env_prefix: ENV prefix for kafka settings
        default_bootstrap_servers: optional default list (last resort)
    """

    kafka_cfg = kafka_cfg or {}
    if not isinstance(kafka_cfg, Mapping):
        dump = getattr(kafka_cfg, "model_dump", None)
        if callable(dump):
            kafka_cfg = dump(mode="python", by_alias=True)
        else:
            raise TypeError(f"kafka_cfg must be a mapping-like object, got: {type(kafka_cfg)}")

    env_cfg = kafka_connection_from_env(env_prefix)

    bs = parse_bootstrap_servers(kafka_cfg.get("bootstrap_servers"))
    if not bs:
        bs = list(env_cfg.bootstrap_servers)
    if (not bs) and default_bootstrap_servers:
        bs = [str(x).strip() for x in default_bootstrap_servers if str(x).strip()]

    # Security fields: manifest overrides env
    sec = KafkaConsumerSecurity(
        security_protocol=_str_or_none(kafka_cfg.get("security_protocol"))
        or _str_or_none(kafka_cfg.get("protocol"))
        or env_cfg.security.security_protocol,
        sasl_mechanism=_str_or_none(kafka_cfg.get("sasl_mechanism"))
        or _str_or_none(kafka_cfg.get("mechanism"))
        or env_cfg.security.sasl_mechanism,
        sasl_username=_str_or_none(kafka_cfg.get("sasl_username"))
        or _str_or_none(kafka_cfg.get("username"))
        or env_cfg.security.sasl_username,
        sasl_password=_str_or_none(kafka_cfg.get("sasl_password"))
        or _str_or_none(kafka_cfg.get("password"))
        or env_cfg.security.sasl_password,
        ssl_cafile=_str_or_none(kafka_cfg.get("ssl_cafile")) or env_cfg.security.ssl_cafile,
        ssl_certfile=_str_or_none(kafka_cfg.get("ssl_certfile")) or env_cfg.security.ssl_certfile,
        ssl_keyfile=_str_or_none(kafka_cfg.get("ssl_keyfile")) or env_cfg.security.ssl_keyfile,
        ssl_check_hostname=_bool_or_none(kafka_cfg.get("ssl_check_hostname"))
        if kafka_cfg.get("ssl_check_hostname") is not None
        else env_cfg.security.ssl_check_hostname,
    )

    return KafkaConnectionConfig(bootstrap_servers=bs, security=sec)
