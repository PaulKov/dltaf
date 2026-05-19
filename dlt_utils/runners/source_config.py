"""Typed source config parsing for runners.

Stage 10: move per-runner config parsing to Pydantic models.

Rationale
---------
We already validate manifests with Pydantic schema models (core/manifest_schema.py).
Runners previously re-parsed raw YAML mappings using ad-hoc string/int/bool casts.

This helper allows each runner to:
  - parse `manifest['source']` into a typed model (e.g. SourceUploaderB057)
  - get consistent type coercion for env-driven values ("600" -> 600, "false" -> False)
  - keep error messages readable and pinned to the runner/kind

The runtime remains backward compatible:
  - runners still accept a `manifest: Mapping[str, Any]`
  - we keep allow-extra behavior from schema models for faster iteration
"""

from __future__ import annotations

from typing import Any, Mapping, Optional, Sequence, Type, TypeVar

from pydantic import BaseModel, ValidationError

from dlt_utils.kafka_config import KafkaConnectionConfig, kafka_connection_from_manifest_or_env

T = TypeVar("T", bound=BaseModel)


def parse_source_config(manifest: Mapping[str, Any], model: Type[T]) -> T:
    """Parse and validate manifest['source'] into a typed Pydantic model.

    Args:
        manifest: resolved manifest mapping
        model: pydantic model class (e.g. SourcePKBConclusion)

    Raises:
        ValueError: if source config doesn't match the model
    """

    source = manifest.get("source") or {}
    try:
        return model.model_validate(source)
    except ValidationError as e:
        kind = None
        try:
            kind = str((source or {}).get("kind") or "").strip() or None
        except Exception:
            kind = None
        prefix = f"Invalid source config{f' for kind={kind}' if kind else ''}: "
        # str(e) is already a compact human-readable multi-line error summary
        raise ValueError(prefix + str(e)) from e


def resolve_kafka_connection(
    manifest: Mapping[str, Any],
    kafka_cfg: Optional[Any],
    *,
    env_prefix_default: str = "KAFKA__",
    default_bootstrap_servers: Optional[Sequence[str]] = None,
) -> KafkaConnectionConfig:
    """Resolve Kafka connection for a source.

    Precedence:
      1) source.kafka (bootstrap_servers/security)
      2) resolved ENV (Vault -> Variables) with prefix from `connections.kafka.env_prefix`
      3) default_bootstrap_servers (optional)

    This helper exists to keep runners small and consistent.
    """

    connections_cfg = manifest.get("connections") or {}
    kafka_conn_cfg = connections_cfg.get("kafka") or {}
    env_prefix = env_prefix_default
    if isinstance(kafka_conn_cfg, Mapping):
        env_prefix = str(kafka_conn_cfg.get("env_prefix") or env_prefix_default)

    return kafka_connection_from_manifest_or_env(
        kafka_cfg,
        env_prefix=env_prefix,
        default_bootstrap_servers=default_bootstrap_servers,
    )
