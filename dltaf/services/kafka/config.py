from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence

from dltaf.services.inputs import parse_csv_or_json_list


def normalize_env_prefix(prefix: Any, *, default: str = "") -> str:
    if prefix is None:
        return default
    value = str(prefix).strip()
    if not value:
        return default
    return f"{value.rstrip('_')}__"


def _str_or_none(value: Any) -> Optional[str]:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _bool_or_none(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value != 0
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    return None


def _as_mapping(obj: Any) -> Mapping[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, Mapping):
        return obj
    dump = getattr(obj, "model_dump", None)
    if callable(dump):
        return dump(mode="python", by_alias=True)
    raise TypeError(f"kafka_cfg must be a mapping-like object, got: {type(obj)}")


def parse_bootstrap_servers(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [str(item).strip() for item in value if str(item).strip()]
    return parse_csv_or_json_list(str(value))


@dataclass(frozen=True)
class KafkaConsumerSecurity:
    """Security-related kwargs for kafka-python consumers."""

    security_protocol: Optional[str] = None
    sasl_mechanism: Optional[str] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = None
    ssl_cafile: Optional[str] = None
    ssl_certfile: Optional[str] = None
    ssl_keyfile: Optional[str] = None
    ssl_check_hostname: Optional[bool] = None

    def consumer_kwargs(self) -> Dict[str, Any]:
        kwargs: Dict[str, Any] = {}
        if self.security_protocol:
            kwargs["security_protocol"] = self.security_protocol
        if self.sasl_mechanism:
            kwargs["sasl_mechanism"] = self.sasl_mechanism
        if self.sasl_username:
            kwargs["sasl_plain_username"] = self.sasl_username
        if self.sasl_password:
            kwargs["sasl_plain_password"] = self.sasl_password
        if self.ssl_cafile:
            kwargs["ssl_cafile"] = self.ssl_cafile
        if self.ssl_certfile:
            kwargs["ssl_certfile"] = self.ssl_certfile
        if self.ssl_keyfile:
            kwargs["ssl_keyfile"] = self.ssl_keyfile
        if self.ssl_check_hostname is not None:
            kwargs["ssl_check_hostname"] = self.ssl_check_hostname
        return kwargs


@dataclass(frozen=True)
class KafkaConnectionConfig:
    """Resolved Kafka bootstrap and security settings."""

    bootstrap_servers: List[str]
    security: KafkaConsumerSecurity


def kafka_connection_from_env(
    prefix: str = "KAFKA__",
    *,
    env: Mapping[str, str] | None = None,
) -> KafkaConnectionConfig:
    environ = env or os.environ
    normalized_prefix = normalize_env_prefix(prefix, default="KAFKA__")
    return KafkaConnectionConfig(
        bootstrap_servers=parse_csv_or_json_list(environ.get(f"{normalized_prefix}BOOTSTRAP_SERVERS", "")),
        security=KafkaConsumerSecurity(
            security_protocol=_str_or_none(environ.get(f"{normalized_prefix}SECURITY_PROTOCOL")),
            sasl_mechanism=_str_or_none(environ.get(f"{normalized_prefix}SASL_MECHANISM")),
            sasl_username=_str_or_none(environ.get(f"{normalized_prefix}SASL_USERNAME")),
            sasl_password=_str_or_none(environ.get(f"{normalized_prefix}SASL_PASSWORD")),
            ssl_cafile=_str_or_none(environ.get(f"{normalized_prefix}SSL_CAFILE")),
            ssl_certfile=_str_or_none(environ.get(f"{normalized_prefix}SSL_CERTFILE")),
            ssl_keyfile=_str_or_none(environ.get(f"{normalized_prefix}SSL_KEYFILE")),
            ssl_check_hostname=_bool_or_none(environ.get(f"{normalized_prefix}SSL_CHECK_HOSTNAME")),
        ),
    )


def kafka_connection_from_manifest_or_env(
    kafka_cfg: Optional[Any],
    *,
    env_prefix: str = "KAFKA__",
    default_bootstrap_servers: Optional[Sequence[str]] = None,
    env: Mapping[str, str] | None = None,
) -> KafkaConnectionConfig:
    config = _as_mapping(kafka_cfg)
    env_config = kafka_connection_from_env(env_prefix, env=env)

    bootstrap_servers = parse_bootstrap_servers(config.get("bootstrap_servers"))
    if not bootstrap_servers:
        bootstrap_servers = list(env_config.bootstrap_servers)
    if not bootstrap_servers and default_bootstrap_servers:
        bootstrap_servers = [str(item).strip() for item in default_bootstrap_servers if str(item).strip()]

    return KafkaConnectionConfig(
        bootstrap_servers=bootstrap_servers,
        security=KafkaConsumerSecurity(
            security_protocol=_str_or_none(config.get("security_protocol"))
            or _str_or_none(config.get("protocol"))
            or env_config.security.security_protocol,
            sasl_mechanism=_str_or_none(config.get("sasl_mechanism"))
            or _str_or_none(config.get("mechanism"))
            or env_config.security.sasl_mechanism,
            sasl_username=_str_or_none(config.get("sasl_username"))
            or _str_or_none(config.get("username"))
            or env_config.security.sasl_username,
            sasl_password=_str_or_none(config.get("sasl_password"))
            or _str_or_none(config.get("password"))
            or env_config.security.sasl_password,
            ssl_cafile=_str_or_none(config.get("ssl_cafile")) or env_config.security.ssl_cafile,
            ssl_certfile=_str_or_none(config.get("ssl_certfile")) or env_config.security.ssl_certfile,
            ssl_keyfile=_str_or_none(config.get("ssl_keyfile")) or env_config.security.ssl_keyfile,
            ssl_check_hostname=_bool_or_none(config.get("ssl_check_hostname"))
            if config.get("ssl_check_hostname") is not None
            else env_config.security.ssl_check_hostname,
        ),
    )
