"""Online connectivity checks used by `--dry-run-online`.

Stage 16
--------
Adds an *optional* online mode for dry-run that can verify that external
dependencies are reachable *without side effects*:

- ClickHouse: TCP connectivity and (optionally) `SELECT 1` via clickhouse-connect
- Kafka: TCP connectivity to bootstrap servers and (optionally) metadata fetch

Design goals
------------
- Best-effort: failures must not crash import-time or normal runs.
- Safe output: never include secret values (passwords, tokens, etc.).
- Minimal dependencies: optional libraries are imported lazily.
"""

from __future__ import annotations

import os
import socket
import time
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple


def _strip_scheme(endpoint: str) -> str:
    s = str(endpoint).strip()
    if "://" in s:
        return s.split("://", 1)[1]
    return s


def _parse_host_port(endpoint: str, *, default_port: int) -> Tuple[str, int]:
    """Parse host:port with basic IPv6 bracket support."""

    s = _strip_scheme(endpoint)
    if not s:
        raise ValueError("empty endpoint")

    # IPv6 in brackets: [::1]:9092
    if s.startswith("[") and "]" in s:
        host = s[1 : s.index("]")]
        rest = s[s.index("]") + 1 :]
        if rest.startswith(":"):
            port = int(rest[1:])
        else:
            port = int(default_port)
        return host, port

    # host:port
    if ":" in s:
        host, port_str = s.rsplit(":", 1)
        return host, int(port_str)

    return s, int(default_port)


def tcp_check_endpoints(
    endpoints: Sequence[str],
    *,
    timeout_seconds: float,
    default_port: int,
) -> Dict[str, Any]:
    """Check TCP connectivity to a list of endpoints.

    Returns a safe dict:
      {
        "ok": bool,
        "timeout_seconds": float,
        "results": [
          {"endpoint": "host:port", "host": "host", "port": 123, "ok": bool, "latency_ms": int, "error": str|None}
        ]
      }

    Notes:
      - This does not validate auth/SSL; it only checks TCP reachability.
    """

    out: Dict[str, Any] = {
        "ok": False,
        "timeout_seconds": float(timeout_seconds),
        "results": [],
    }

    results: List[Dict[str, Any]] = []
    ok_any = False

    for ep in list(endpoints or []):
        ep_s = str(ep).strip()
        if not ep_s:
            continue

        host = ""
        port = int(default_port)
        parse_error: Optional[str] = None

        try:
            host, port = _parse_host_port(ep_s, default_port=default_port)
        except Exception as e:
            parse_error = f"{e.__class__.__name__}: {e}"

        started = time.time()
        ok = False
        err: Optional[str] = None
        latency_ms = 0

        if parse_error is not None:
            ok = False
            err = parse_error
        else:
            try:
                sock = socket.create_connection((host, int(port)), timeout=float(timeout_seconds))
                try:
                    ok = True
                finally:
                    try:
                        sock.close()
                    except Exception:
                        pass
            except Exception as e:
                ok = False
                err = f"{e.__class__.__name__}: {e}"
            finally:
                latency_ms = int(max(0.0, (time.time() - started) * 1000.0))

        if ok:
            ok_any = True

        results.append(
            {
                "endpoint": ep_s,
                "host": host,
                "port": int(port),
                "ok": bool(ok),
                "latency_ms": int(latency_ms),
                "error": err,
            }
        )

    out["ok"] = bool(ok_any)
    out["results"] = results
    return out


def clickhouse_online_check(*, timeout_seconds: float = 5.0) -> Dict[str, Any]:
    """Check ClickHouse connectivity (best-effort).

    Data sources:
      - Reads destination creds from ENV (already injected by secrets resolver)

    Returns a safe dict (no secrets).
    """

    host = os.getenv("DESTINATION__CLICKHOUSE__CREDENTIALS__HOST")
    if not host:
        return {
            "attempted": False,
            "ok": False,
            "reason": "not_configured",
        }

    port = int(os.getenv("DESTINATION__CLICKHOUSE__CREDENTIALS__HTTP_PORT") or "8123")
    secure = os.getenv("DESTINATION__CLICKHOUSE__CREDENTIALS__SECURE") == "1"

    endpoint = f"{host}:{port}"

    tcp = tcp_check_endpoints([endpoint], timeout_seconds=float(timeout_seconds), default_port=int(port))

    # Optional higher-level check via clickhouse-connect.
    client_check: Dict[str, Any] = {"attempted": False, "ok": False, "error": None, "latency_ms": None}

    try:
        import inspect

        import clickhouse_connect  # type: ignore

        kwargs: Dict[str, Any] = {}
        try:
            sig = inspect.signature(clickhouse_connect.get_client)
            if "connect_timeout" in sig.parameters:
                kwargs["connect_timeout"] = float(timeout_seconds)
            if "send_receive_timeout" in sig.parameters:
                kwargs["send_receive_timeout"] = float(timeout_seconds)
        except Exception:
            # If introspection fails, we proceed without timeout kwargs.
            kwargs = {}

        client_check["attempted"] = True

        # username/password/database are read from ENV but must NOT be returned.
        username = os.getenv("DESTINATION__CLICKHOUSE__CREDENTIALS__USERNAME") or "default"
        password = os.getenv("DESTINATION__CLICKHOUSE__CREDENTIALS__PASSWORD") or ""
        database = os.getenv("DESTINATION__CLICKHOUSE__CREDENTIALS__DATABASE") or "default"

        started = time.time()
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
            # Safe query
            if hasattr(client, "command"):
                client.command("SELECT 1")
            else:
                # fall back: query API
                client.query("SELECT 1")
            client_check["ok"] = True
        finally:
            client_check["latency_ms"] = int(max(0.0, (time.time() - started) * 1000.0))
            try:
                if hasattr(client, "close"):
                    client.close()
            except Exception:
                pass

    except ImportError as e:
        client_check["attempted"] = False
        client_check["ok"] = False
        client_check["error"] = f"dependency_missing: {e.__class__.__name__}"
    except Exception as e:
        client_check["attempted"] = True
        client_check["ok"] = False
        client_check["error"] = f"{e.__class__.__name__}: {e}"

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
    """Check Kafka connectivity (best-effort).

    What we check:
      - TCP reachability to each bootstrap server
      - If kafka-python is available: metadata fetch (topic partitions or topics list)

    Returns safe dict (no secrets).
    """

    bs = [str(x).strip() for x in (bootstrap_servers or []) if str(x).strip()]
    if not bs:
        return {
            "attempted": False,
            "ok": False,
            "reason": "bootstrap_servers_empty",
        }

    tcp = tcp_check_endpoints(bs, timeout_seconds=float(timeout_seconds), default_port=9092)

    meta: Dict[str, Any] = {
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

        cfg: Dict[str, Any] = {}
        cfg.update(dict(consumer_kwargs or {}))

        # Add safe timeouts if supported by this kafka-python version.
        try:
            default_cfg = getattr(KafkaConsumer, "DEFAULT_CONFIG", {})
            if isinstance(default_cfg, dict):
                for k, v in {
                    "api_version_auto_timeout_ms": int(float(timeout_seconds) * 1000.0),
                    "request_timeout_ms": int(float(timeout_seconds) * 1000.0),
                    "metadata_max_age_ms": int(float(timeout_seconds) * 1000.0),
                }.items():
                    if k in default_cfg and k not in cfg:
                        cfg[k] = v
        except Exception:
            pass

        started = time.time()
        meta["attempted"] = True

        consumer = KafkaConsumer(
            bootstrap_servers=list(bs),
            enable_auto_commit=False,
            auto_offset_reset="latest",
            **cfg,
        )

        try:
            deadline = time.time() + max(0.1, float(timeout_seconds))
            t = str(topic).strip() if topic else ""

            while time.time() < deadline:
                try:
                    consumer.poll(timeout_ms=0)
                except Exception:
                    pass

                if t:
                    parts = consumer.partitions_for_topic(t)
                    if parts:
                        meta["ok"] = True
                        meta["partitions"] = sorted([int(p) for p in parts])
                        break
                else:
                    try:
                        topics = consumer.topics()
                    except Exception:
                        topics = None
                    if topics:
                        meta["ok"] = True
                        meta["topics_count"] = int(len(list(topics)))
                        break

                time.sleep(0.2)

            if not meta["ok"]:
                meta["error"] = f"metadata_timeout: {timeout_seconds}s"

        finally:
            meta["latency_ms"] = int(max(0.0, (time.time() - started) * 1000.0))
            try:
                consumer.close()
            except Exception:
                pass

    except ImportError as e:
        meta["attempted"] = False
        meta["ok"] = False
        meta["error"] = f"dependency_missing: {e.__class__.__name__}"
    except Exception as e:
        meta["attempted"] = True
        meta["ok"] = False
        meta["error"] = f"{e.__class__.__name__}: {e}"

    # Overall ok: TCP must pass at least once; metadata is best-effort
    # (if attempted, it should also pass).
    ok = bool(tcp.get("ok")) and bool(meta.get("ok") or not meta.get("attempted"))

    return {
        "attempted": True,
        "ok": bool(ok),
        "bootstrap_servers": list(bs),
        "tcp": tcp,
        "metadata": meta,
    }
