"""Best-effort online connectivity checks for dry-run strict mode."""

from __future__ import annotations

import os
import socket
import time
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple


def _strip_scheme(endpoint: str) -> str:
    value = str(endpoint).strip()
    if "://" in value:
        return value.split("://", 1)[1]
    return value


def _parse_host_port(endpoint: str, *, default_port: int) -> Tuple[str, int]:
    value = _strip_scheme(endpoint)
    if not value:
        raise ValueError("empty endpoint")
    if value.startswith("[") and "]" in value:
        host = value[1 : value.index("]")]
        rest = value[value.index("]") + 1 :]
        return host, int(rest[1:] if rest.startswith(":") else default_port)
    if ":" in value:
        host, port = value.rsplit(":", 1)
        return host, int(port)
    return value, int(default_port)


def tcp_check_endpoints(
    endpoints: Sequence[str],
    *,
    timeout_seconds: float,
    default_port: int,
) -> Dict[str, Any]:
    results: list[dict[str, Any]] = []
    ok_any = False

    for endpoint in [str(item).strip() for item in endpoints or () if str(item).strip()]:
        host = ""
        port = int(default_port)
        error: Optional[str] = None
        ok = False
        started = time.time()

        try:
            host, port = _parse_host_port(endpoint, default_port=default_port)
            sock = socket.create_connection((host, int(port)), timeout=float(timeout_seconds))
            try:
                ok = True
            finally:
                sock.close()
        except Exception as exc:
            error = f"{exc.__class__.__name__}: {exc}"

        latency_ms = int(max(0.0, (time.time() - started) * 1000.0))
        ok_any = ok_any or ok
        results.append(
            {
                "endpoint": endpoint,
                "host": host,
                "port": int(port),
                "ok": bool(ok),
                "latency_ms": latency_ms,
                "error": error,
            }
        )

    return {"ok": bool(ok_any), "timeout_seconds": float(timeout_seconds), "results": results}


def clickhouse_online_check(*, timeout_seconds: float = 5.0) -> Dict[str, Any]:
    host = os.getenv("DESTINATION__CLICKHOUSE__CREDENTIALS__HOST")
    if not host:
        return {"attempted": False, "ok": False, "reason": "not_configured"}

    port = int(os.getenv("DESTINATION__CLICKHOUSE__CREDENTIALS__HTTP_PORT") or "8123")
    secure = os.getenv("DESTINATION__CLICKHOUSE__CREDENTIALS__SECURE") == "1"
    endpoint = f"{host}:{port}"
    tcp = tcp_check_endpoints([endpoint], timeout_seconds=float(timeout_seconds), default_port=port)
    client_check: Dict[str, Any] = {
        "attempted": False,
        "ok": False,
        "error": None,
        "latency_ms": None,
    }

    try:
        import inspect

        import clickhouse_connect  # type: ignore

        kwargs: Dict[str, Any] = {}
        try:
            signature = inspect.signature(clickhouse_connect.get_client)
            if "connect_timeout" in signature.parameters:
                kwargs["connect_timeout"] = float(timeout_seconds)
            if "send_receive_timeout" in signature.parameters:
                kwargs["send_receive_timeout"] = float(timeout_seconds)
        except Exception:
            kwargs = {}

        username = os.getenv("DESTINATION__CLICKHOUSE__CREDENTIALS__USERNAME") or "default"
        password = os.getenv("DESTINATION__CLICKHOUSE__CREDENTIALS__PASSWORD") or ""
        database = os.getenv("DESTINATION__CLICKHOUSE__CREDENTIALS__DATABASE") or "default"
        started = time.time()
        client_check["attempted"] = True
        client = clickhouse_connect.get_client(
            host=str(host),
            port=int(port),
            username=str(username),
            password=str(password),
            database=str(database),
            secure=bool(secure),
            **kwargs,
        )
        try:
            if hasattr(client, "command"):
                client.command("SELECT 1")
            else:
                client.query("SELECT 1")
            client_check["ok"] = True
        finally:
            client_check["latency_ms"] = int(max(0.0, (time.time() - started) * 1000.0))
            try:
                if hasattr(client, "close"):
                    client.close()
            except Exception:
                pass
    except ImportError as exc:
        client_check["error"] = f"dependency_missing: {exc.__class__.__name__}"
    except Exception as exc:
        client_check["attempted"] = True
        client_check["error"] = f"{exc.__class__.__name__}: {exc}"

    return {
        "attempted": True,
        "ok": bool(tcp.get("ok")) and bool(client_check.get("ok") or not client_check.get("attempted")),
        "endpoint": endpoint,
        "secure": bool(secure),
        "tcp": tcp,
        "client": client_check,
    }


def kafka_online_check(
    *,
    bootstrap_servers: Sequence[str],
    topic: Optional[str] = None,
    consumer_kwargs: Optional[Mapping[str, Any]] = None,
    timeout_seconds: float = 5.0,
) -> Dict[str, Any]:
    servers = [str(item).strip() for item in bootstrap_servers or () if str(item).strip()]
    if not servers:
        return {"attempted": False, "ok": False, "reason": "bootstrap_servers_empty"}

    tcp = tcp_check_endpoints(servers, timeout_seconds=float(timeout_seconds), default_port=9092)
    metadata: Dict[str, Any] = {
        "attempted": False,
        "ok": False,
        "topic": str(topic).strip() if topic else None,
        "partitions": None,
        "topics_count": None,
        "latency_ms": None,
        "error": None,
    }

    try:
        from kafka import KafkaConsumer  # type: ignore

        config: Dict[str, Any] = dict(consumer_kwargs or {})
        try:
            default_config = getattr(KafkaConsumer, "DEFAULT_CONFIG", {})
            if isinstance(default_config, dict):
                for key, value in {
                    "api_version_auto_timeout_ms": int(float(timeout_seconds) * 1000.0),
                    "request_timeout_ms": int(float(timeout_seconds) * 1000.0),
                    "metadata_max_age_ms": int(float(timeout_seconds) * 1000.0),
                }.items():
                    if key in default_config and key not in config:
                        config[key] = value
        except Exception:
            pass

        started = time.time()
        metadata["attempted"] = True
        consumer = KafkaConsumer(
            bootstrap_servers=list(servers),
            enable_auto_commit=False,
            auto_offset_reset="latest",
            **config,
        )
        try:
            deadline = time.time() + max(0.1, float(timeout_seconds))
            topic_name = str(topic).strip() if topic else ""
            while time.time() < deadline:
                try:
                    consumer.poll(timeout_ms=0)
                except Exception:
                    pass
                if topic_name:
                    parts = consumer.partitions_for_topic(topic_name)
                    if parts:
                        metadata["ok"] = True
                        metadata["partitions"] = sorted(int(part) for part in parts)
                        break
                else:
                    try:
                        topics = consumer.topics()
                    except Exception:
                        topics = None
                    if topics:
                        metadata["ok"] = True
                        metadata["topics_count"] = len(list(topics))
                        break
                time.sleep(0.2)
            if not metadata["ok"]:
                metadata["error"] = f"metadata_timeout: {timeout_seconds}s"
        finally:
            metadata["latency_ms"] = int(max(0.0, (time.time() - started) * 1000.0))
            try:
                consumer.close()
            except Exception:
                pass
    except ImportError as exc:
        metadata["error"] = f"dependency_missing: {exc.__class__.__name__}"
    except Exception as exc:
        metadata["attempted"] = True
        metadata["error"] = f"{exc.__class__.__name__}: {exc}"

    return {
        "attempted": True,
        "ok": bool(tcp.get("ok")) and bool(metadata.get("ok") or not metadata.get("attempted")),
        "bootstrap_servers": list(servers),
        "tcp": tcp,
        "metadata": metadata,
    }
