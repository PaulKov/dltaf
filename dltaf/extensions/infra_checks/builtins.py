from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Mapping, Optional

from dltaf.extensions.infra_checks.online import clickhouse_online_check, kafka_online_check
from dltaf.services.kafka import kafka_connection_from_manifest_or_env


@dataclass
class ClickHouseConnectivityCheck:
    name: str = "clickhouse"
    description: str = "ClickHouse connectivity (TCP + optional SELECT 1)."

    def applies(self, manifest: Mapping[str, Any], ctx: Any) -> bool:
        pipeline_cfg = manifest.get("pipeline") or {}
        return str((pipeline_cfg.get("destination") or "")).strip() == "clickhouse"

    def run(self, manifest: Mapping[str, Any], ctx: Any, *, timeout_seconds: float) -> Mapping[str, Any]:
        return clickhouse_online_check(timeout_seconds=float(timeout_seconds))

    def evaluate(self, result: Any) -> Mapping[str, List[str]]:
        errors: List[str] = []
        warnings: List[str] = []

        if not isinstance(result, Mapping):
            errors.append("ClickHouse online check returned an invalid result.")
            return {"errors": errors, "warnings": warnings}

        reason = str(result.get("reason") or "").strip()
        if reason == "not_configured":
            errors.append("ClickHouse is not configured; online check skipped.")
            return {"errors": errors, "warnings": warnings}

        tcp = result.get("tcp")
        if isinstance(tcp, Mapping) and not bool(tcp.get("ok")):
            errors.append("ClickHouse TCP connectivity check failed (host/port unreachable).")

        client = result.get("client")
        if isinstance(client, Mapping):
            if bool(client.get("attempted")) and not bool(client.get("ok")):
                errors.append(
                    "ClickHouse query connectivity check failed (SELECT 1). "
                    "Check credentials/network/SSL settings."
                )
            if (not bool(client.get("attempted"))) and str(client.get("error") or "").startswith(
                "dependency_missing"
            ):
                warnings.append(
                    "clickhouse-connect is not installed; online ClickHouse query check was skipped."
                )

        return {"errors": errors, "warnings": warnings}


@dataclass
class KafkaConnectivityCheck:
    name: str = "kafka"
    default_env_prefix: str = "KAFKA__"
    description: str = "Kafka connectivity (TCP + optional metadata fetch)."

    def applies(self, manifest: Mapping[str, Any], ctx: Any) -> bool:
        source_cfg = manifest.get("source") or {}
        if not isinstance(source_cfg, Mapping):
            return False
        return isinstance(source_cfg.get("kafka"), Mapping)

    def run(self, manifest: Mapping[str, Any], ctx: Any, *, timeout_seconds: float) -> Mapping[str, Any]:
        source_cfg = manifest.get("source") or {}
        connections_cfg = manifest.get("connections") or {}
        kafka_cfg = {}
        if isinstance(source_cfg, Mapping) and isinstance(source_cfg.get("kafka"), Mapping):
            kafka_cfg = dict(source_cfg.get("kafka") or {})

        env_prefix = self.default_env_prefix
        if isinstance(connections_cfg, Mapping):
            conn = connections_cfg.get("kafka")
            if isinstance(conn, Mapping) and conn.get("env_prefix"):
                env_prefix = str(conn.get("env_prefix"))

        conn = kafka_connection_from_manifest_or_env(kafka_cfg, env_prefix=env_prefix)

        topic: Optional[str] = None
        if isinstance(kafka_cfg, Mapping):
            raw_topic = kafka_cfg.get("topic")
            if raw_topic is not None:
                topic = str(raw_topic).strip() or None

        return kafka_online_check(
            bootstrap_servers=list(conn.bootstrap_servers),
            topic=topic,
            consumer_kwargs=conn.security.consumer_kwargs(),
            timeout_seconds=float(timeout_seconds),
        )

    def evaluate(self, result: Any) -> Mapping[str, List[str]]:
        errors: List[str] = []
        warnings: List[str] = []

        if not isinstance(result, Mapping):
            errors.append("Kafka online check returned an invalid result.")
            return {"errors": errors, "warnings": warnings}

        reason = str(result.get("reason") or "").strip()
        if reason == "bootstrap_servers_empty":
            errors.append("Kafka bootstrap_servers are not configured; online check skipped.")
            return {"errors": errors, "warnings": warnings}

        tcp = result.get("tcp")
        if isinstance(tcp, Mapping) and not bool(tcp.get("ok")):
            errors.append("Kafka TCP connectivity check failed (no brokers reachable).")

        meta = result.get("metadata")
        if isinstance(meta, Mapping):
            if bool(meta.get("attempted")) and not bool(meta.get("ok")):
                errors.append(
                    "Kafka metadata check failed (topic/cluster metadata not available). "
                    "Check topic name, auth, and network."
                )
            if (not bool(meta.get("attempted"))) and str(meta.get("error") or "").startswith(
                "dependency_missing"
            ):
                warnings.append(
                    "kafka-python is not installed; Kafka metadata check was skipped (TCP only)."
                )

        return {"errors": errors, "warnings": warnings}


def build_builtin_infra_checks() -> List[Any]:
    return [ClickHouseConnectivityCheck(), KafkaConnectivityCheck()]


__all__ = [
    "ClickHouseConnectivityCheck",
    "KafkaConnectivityCheck",
    "build_builtin_infra_checks",
]
