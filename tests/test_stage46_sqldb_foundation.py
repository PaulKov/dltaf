from __future__ import annotations

from dltaf.extensions.runners.builtins import build_builtin_runners
from dltaf.integrations.sqldb.normalizer import build_sqldb_execution_manifest, normalize_sqldb_source
from dltaf.services.manifests.schema import SourceSQLDB, validate_manifest_schema


def test_sqldb_schema_accepts_canonical_catalog() -> None:
    source = SourceSQLDB.model_validate(
        {
            "kind": "sqldb",
            "dialect": "generic",
            "mode": "catalog",
            "catalog": {"schema": "public", "tables": ["orders"]},
        }
    )
    assert str(source.kind) == "sqldb"
    assert source.dialect.value == "generic"
    assert source.mode.value == "catalog"


def test_sqldb_schema_accepts_oracle_preset_alias() -> None:
    source = SourceSQLDB.model_validate(
        {
            "kind": "oracle",
            "query": {"queries": [{"name": "q1", "sql_file": "sql/q1.sql"}]},
        }
    )
    assert str(source.kind) == "oracle"
    assert source.dialect.value == "oracle"
    assert source.mode.value == "query"


def test_normalize_legacy_sql_database_to_sqldb_catalog() -> None:
    normalized = normalize_sqldb_source(
        {
            "kind": "sql_database",
            "schema": "public",
            "tables": ["orders"],
            "reflection_level": "full",
        }
    )
    assert normalized["kind"] == "sqldb"
    assert normalized["dialect"] == "generic"
    assert normalized["mode"] == "catalog"
    assert normalized["catalog"]["schema"] == "public"
    assert normalized["catalog"]["tables"] == ["orders"]


def test_normalize_legacy_oracle_custom_sql_to_sqldb_query() -> None:
    normalized = normalize_sqldb_source(
        {
            "kind": "oracle_custom_sql",
            "queries": [{"name": "q1", "sql_file": "sql/q1.sql"}],
            "init_sql": "alter session set nls_date_format = 'YYYY-MM-DD'",
        }
    )
    assert normalized["kind"] == "sqldb"
    assert normalized["dialect"] == "oracle"
    assert normalized["mode"] == "query"
    assert normalized["query"]["queries"][0]["sql_file"] == "sql/q1.sql"
    assert "init_sql" in normalized["dialect_options"]


def test_build_execution_manifest_for_catalog_alias() -> None:
    manifest = {
        "version": 1,
        "pipeline": {"name": "dlt__test__to__clickhouse__raw", "destination": "clickhouse", "dataset": "raw"},
        "source": {
            "kind": "sqldb",
            "mode": "catalog",
            "dialect": "generic",
            "catalog": {"schema": "public", "tables": ["orders"]},
        },
    }
    execution = build_sqldb_execution_manifest(manifest)
    assert execution["source"]["kind"] == "sql_database"
    assert execution["source"]["schema"] == "public"


def test_runner_registry_contains_public_builtins_only() -> None:
    kinds = {runner.kind for runner in build_builtin_runners()}
    assert {"sqldb", "oracle", "oracle_custom_sql", "sql_database", "mongodb"} <= kinds
    assert "pkb_conclusion" not in kinds
    assert "uploader_b057" not in kinds


def test_manifest_validation_accepts_sqldb_manifest() -> None:
    manifest = {
        "version": 1,
        "pipeline": {
            "name": "dlt__sqldb__to__clickhouse__raw",
            "destination": "clickhouse",
            "dataset": "raw",
        },
        "source": {
            "kind": "sqldb",
            "dialect": "generic",
            "mode": "catalog",
            "catalog": {"schema": "public", "tables": ["orders"]},
        },
        "airflow": {
            "dag_id": "dlt__sqldb__to__clickhouse__raw",
            "schedule": "0 6 * * *",
            "start_date": "2026-01-01",
            "catchup": False,
        },
    }
    validated = validate_manifest_schema(manifest, strict=True, strict_source=True)
    assert validated.pipeline.name == "dlt__sqldb__to__clickhouse__raw"
