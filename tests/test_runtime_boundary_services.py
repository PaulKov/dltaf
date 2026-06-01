from __future__ import annotations

import importlib
import os
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest


def test_value_list_resolver_supports_inline_file_env_and_clickhouse(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dltaf.services.inputs import resolve_values_list

    manifest_path = tmp_path / "manifest.yaml"
    manifest_path.write_text("source: {}\n", encoding="utf-8")
    values_file = tmp_path / "values.txt"
    values_file.write_text("100\n200, 300\n100\n", encoding="utf-8")
    manifest = {"__manifest_path__": str(manifest_path)}

    assert resolve_values_list({"values": ["100", "200", "100", ""]}, manifest) == [
        "100",
        "200",
    ]
    assert resolve_values_list({"from_file": "values.txt"}, manifest) == ["100", "200", "300"]

    monkeypatch.setenv("DLTAF_TEST_VALUES", '["900", "901", "900"]')
    assert resolve_values_list({"from_env": "DLTAF_TEST_VALUES"}, manifest) == ["900", "901"]

    captured: dict[str, str] = {}

    def fetcher(query: str) -> list[str]:
        captured["query"] = query
        return ["42", "43", "42"]

    assert resolve_values_list(
        {
            "from_clickhouse": {
                "database": "analytics",
                "table": "bins",
                "column": "bin",
                "where": "active = 1",
                "limit": 10,
            }
        },
        manifest,
        allow_clickhouse=True,
        clickhouse_fetcher=fetcher,
    ) == ["42", "43"]
    assert captured["query"] == "SELECT DISTINCT bin FROM analytics.bins WHERE active = 1 LIMIT 10"


def test_value_list_resolver_accepts_pydantic_style_config() -> None:
    from dltaf.services.inputs import resolve_values_list

    cfg = SimpleNamespace(model_dump=lambda **_: {"values": ["A", "B", "A"]})

    assert resolve_values_list(cfg, {"__manifest_path__": "/tmp/manifest.yaml"}) == ["A", "B"]


def test_clickhouse_fetcher_is_injectable(monkeypatch: pytest.MonkeyPatch) -> None:
    from dltaf.services.clickhouse import fetch_first_column_values, get_clickhouse_client

    monkeypatch.setenv("DESTINATION__CLICKHOUSE__CREDENTIALS__HOST", "clickhouse.local")
    monkeypatch.setenv("DESTINATION__CLICKHOUSE__CREDENTIALS__HTTP_PORT", "8124")
    monkeypatch.setenv("DESTINATION__CLICKHOUSE__CREDENTIALS__USERNAME", "svc")
    monkeypatch.setenv("DESTINATION__CLICKHOUSE__CREDENTIALS__PASSWORD", "secret")
    monkeypatch.setenv("DESTINATION__CLICKHOUSE__CREDENTIALS__DATABASE", "analytics")
    monkeypatch.setenv("DESTINATION__CLICKHOUSE__CREDENTIALS__SECURE", "1")

    calls: dict[str, Any] = {}

    class FakeClient:
        def query(self, query: str) -> Any:
            calls["query"] = query
            return SimpleNamespace(result_rows=[("100",), ("200",), (None,)])

    def factory(**kwargs: Any) -> FakeClient:
        calls["factory_kwargs"] = kwargs
        return FakeClient()

    client = get_clickhouse_client(client_factory=factory)

    assert calls["factory_kwargs"] == {
        "host": "clickhouse.local",
        "port": 8124,
        "username": "svc",
        "password": "secret",
        "secure": True,
        "database": "analytics",
    }
    assert fetch_first_column_values("select bin from source", client=client) == ["100", "200"]
    assert calls["query"] == "select bin from source"


def test_vault_secret_resolution_supports_structured_refs_and_injected_getter() -> None:
    from dltaf.services.secrets import VaultSecretRef, resolve_vault_secret

    calls: list[tuple[str, str, str | None]] = []

    def getter(mount_point: str, path: str, *, kv_version: str | None = None) -> dict[str, Any]:
        calls.append((mount_point, path, kv_version))
        return {"host": "vault-host", "port": 8123}

    secret = resolve_vault_secret(
        {"ref": "dwh:prod/clickhouse", "kv_version": "2"},
        overrides={"database": "analytics"},
        getter=getter,
    )

    assert calls == [("dwh", "prod/clickhouse", "2")]
    assert secret == {"host": "vault-host", "port": 8123, "database": "analytics"}
    assert VaultSecretRef("dwh", "prod/clickhouse", "2").kv_version == "2"


def test_secrets_provider_resolves_connections_without_legacy_modules() -> None:
    from dltaf.app.services import SecretsProvider

    calls: list[tuple[str, str, str | None]] = []

    def getter(mount_point: str, path: str, *, kv_version: str | None = None) -> dict[str, Any]:
        calls.append((mount_point, path, kv_version))
        if path == "prod/postgres":
            return {
                "drivername": "postgresql+psycopg2",
                "host": "postgres.local",
                "port": 5432,
                "username": "etl",
                "password": "secret",
                "database": "source_db",
            }
        if path == "prod/clickhouse":
            return {
                "host": "clickhouse.local",
                "http_port": 8123,
                "username": "dwh",
                "password": "secret",
            }
        if path == "prod/kafka":
            return {
                "bootstrap_servers": ["broker-1:9092", "broker-2:9092"],
                "security_protocol": "SASL_SSL",
            }
        raise AssertionError(path)

    resolved = SecretsProvider(vault_getter=getter).resolve_connections(
        {
            "source": {
                "kind": "postgres",
                "vault": {"ref": "dwh:prod/postgres", "kv_version": "2"},
            },
            "destination": {
                "kind": "clickhouse",
                "vault": {"ref": "dwh:prod/clickhouse", "kv_version": "2"},
                "overrides": {"database": "analytics", "dataset_table_separator": "__"},
            },
            "kafka": {
                "kind": "kafka",
                "vault": {"ref": "dwh:prod/kafka", "kv_version": "2"},
            },
        }
    )

    assert calls == [
        ("dwh", "prod/postgres", "2"),
        ("dwh", "prod/clickhouse", "2"),
        ("dwh", "prod/kafka", "2"),
    ]
    assert resolved.to_env_dict()["SOURCES__SQL_DATABASE__CREDENTIALS__HOST"] == "postgres.local"
    assert (
        resolved.to_env_dict()["DESTINATION__CLICKHOUSE__CREDENTIALS__DATABASE"]
        == "analytics"
    )
    assert resolved.to_env_dict()["KAFKA__BOOTSTRAP_SERVERS"] == "broker-1:9092,broker-2:9092"
    assert (
        resolved.sources()["DESTINATION__CLICKHOUSE__CREDENTIALS__DATABASE"]
        == "override:connections.destination.overrides.database"
    )


def test_native_kafka_config_resolves_manifest_over_env(monkeypatch: pytest.MonkeyPatch) -> None:
    from dltaf.services.kafka import kafka_connection_from_manifest_or_env

    monkeypatch.setenv("KAFKA__BOOTSTRAP_SERVERS", "env-1:9092,env-2:9092")
    monkeypatch.setenv("KAFKA__SECURITY_PROTOCOL", "SASL_SSL")
    monkeypatch.setenv("KAFKA__SASL_MECHANISM", "SCRAM-SHA-512")
    monkeypatch.setenv("KAFKA__SASL_USERNAME", "env-user")

    resolved = kafka_connection_from_manifest_or_env(
        {
            "bootstrap_servers": ["manifest:9092"],
            "sasl_username": "manifest-user",
            "ssl_check_hostname": "false",
        }
    )

    assert resolved.bootstrap_servers == ["manifest:9092"]
    assert resolved.security.security_protocol == "SASL_SSL"
    assert resolved.security.sasl_mechanism == "SCRAM-SHA-512"
    assert resolved.security.sasl_username == "manifest-user"
    assert resolved.security.ssl_check_hostname is False


def test_new_runtime_boundary_services_do_not_import_dlt_utils() -> None:
    modules = [
        "dltaf.services.inputs",
        "dltaf.services.inputs.list_resolution",
        "dltaf.services.clickhouse.client",
        "dltaf.services.kafka.config",
        "dltaf.services.secrets.vault",
        "dltaf.services.secrets.connections",
        "dltaf.app.services",
        "dltaf.extensions.infra_checks.builtins",
        "dltaf.extensions.infra_checks.online",
        "dltaf.services.execution.executor",
        "dltaf.services.execution.planner",
    ]

    for module_name in modules:
        module = importlib.import_module(module_name)
        source = Path(module.__file__ or "").read_text(encoding="utf-8")
        assert "from dlt_utils" not in source
        assert "import dlt_utils" not in source


def test_temporary_environ_is_native() -> None:
    from dltaf.services.execution.environment import temporary_environ

    os.environ.pop("DLTAF_TMP_ENV_TEST", None)
    with temporary_environ({"DLTAF_TMP_ENV_TEST": "inside"}):
        assert os.environ["DLTAF_TMP_ENV_TEST"] == "inside"
    assert "DLTAF_TMP_ENV_TEST" not in os.environ
