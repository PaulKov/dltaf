from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

import pytest
from sqlalchemy.exc import InvalidRequestError

from dltaf.app.runtime import RunContext, RunOptions
from dltaf.integrations.sqldb.config import SqlDbConfigParser
from dltaf.integrations.sqldb.partial_success import (
    resolve_partial_success_policy,
    run_catalog_with_partial_success,
)
from dltaf.services.manifests.validator import validate_manifest
from dlt_utils.core.run_result import RunResult, TableRunStats
from dlt_utils.runners.common import ReplaceProtectedExecutionError


def _ctx() -> RunContext:
    return RunContext(
        run_id="stage50-test",
        started_at=datetime.now(timezone.utc),
        manifest_path=Path("manifest.yaml"),
        pipeline_name="dlt__postgres_cabinet__to__clickhouse__spr",
        source_kind="sql_database",
        destination="clickhouse",
        dataset="spr",
        logger=logging.getLogger("stage50"),
        options=RunOptions(),
    )


def _manifest(*, source_kind: str = "sql_database", source_mode: str | None = None) -> dict:
    source: dict = {
        "kind": source_kind,
        "name": "postgres_spr",
        "schema": "spr",
        "tables": ["spr_country", "spr_zayav_status"],
        "reflection_level": "full",
    }
    if source_kind == "sqldb":
        source = {
            "kind": "sqldb",
            "dialect": "generic",
            "mode": source_mode or "catalog",
            "name": "postgres_spr",
            "catalog": {
                "schema": "spr",
                "tables": ["spr_country", "spr_zayav_status"],
                "reflection_level": "full",
            },
        }

    return {
        "version": 1,
        "pipeline": {
            "name": "dlt__postgres_cabinet__to__clickhouse__spr",
            "destination": "clickhouse",
            "dataset": "spr",
        },
        "run": {
            "write_disposition": "merge",
            "partial_success": {
                "mode": "any_success",
                "tolerate_errors": "any_per_table",
            },
        },
        "source": source,
        "airflow": {
            "dag_id": "dlt__postgres_cabinet__to__clickhouse__spr",
            "schedule": "0 3 * * *",
            "start_date": "2025-01-01",
            "catchup": False,
        },
    }


def test_manifest_validation_accepts_partial_success_for_sql_database() -> None:
    manifest = _manifest()
    manifest["run"]["observability"] = {"verbosity": "verbose", "dlt_progress": "summary_only"}
    validate_manifest(manifest, strict_source=True, enforce_filename_match=False)
    policy = resolve_partial_success_policy(manifest)
    assert policy is not None
    assert policy.mode == "any_success"
    assert policy.tolerate_errors == "any_per_table"


def test_manifest_validation_accepts_partial_success_for_sqldb_catalog() -> None:
    manifest = _manifest(source_kind="sqldb", source_mode="catalog")
    validate_manifest(manifest, strict_source=True, enforce_filename_match=False)


def test_manifest_validation_rejects_partial_success_for_sqldb_query() -> None:
    manifest = _manifest(source_kind="sqldb", source_mode="query")
    manifest["source"]["query"] = {"queries": [{"name": "q1", "sql": "select 1"}]}
    manifest["source"].pop("catalog", None)
    with pytest.raises(ValueError, match="run.partial_success is supported only"):
        validate_manifest(manifest, strict_source=True, enforce_filename_match=False)


def test_run_catalog_with_partial_success_tolerates_missing_table(monkeypatch) -> None:
    manifest = _manifest()
    cfg = SqlDbConfigParser().parse(manifest, _ctx())

    monkeypatch.setattr(
        "dltaf.integrations.sqldb.partial_success.build_single_catalog_table_source_factory",
        lambda config, schema_name, source_table: (lambda: f"SOURCE::{source_table}"),
    )
    monkeypatch.setattr(
        "dltaf.integrations.sqldb.partial_success._collect_clickhouse_stats",
        lambda dataset, table: {"rows_count": 10, "bytes_count": 1024},
    )

    def fake_run(*, pipeline, source_factory, manifest, write_disposition, annotate_errors):
        table_name = manifest["source"]["tables"][0]
        assert annotate_errors is True
        if table_name == "spr_zayav_status":
            raise ReplaceProtectedExecutionError(
                stage="source",
                cause=InvalidRequestError("Could not reflect: requested table(s) not available"),
            )
        return {
            "load_packages": [
                {
                    "jobs": [
                        {
                            "table_name": "country",
                            "rows": 12,
                            "status": "completed",
                        }
                    ]
                }
            ]
        }

    monkeypatch.setattr("dltaf.integrations.sqldb.partial_success.run_with_replace_protection", fake_run)

    result = run_catalog_with_partial_success(
        config=cfg,
        ctx=_ctx(),
        pipeline=object(),
        write_disposition="merge",
    )

    assert isinstance(result, RunResult)
    assert result.status == "partial_success"
    assert len(result.table_stats) == 2
    assert any(item.status == "success" for item in result.table_stats)
    assert any(item.status == "failed" for item in result.table_stats)
    assert "tolerated" in (result.message or "")


def test_run_catalog_with_partial_success_fails_when_policy_not_satisfied(monkeypatch) -> None:
    manifest = _manifest()
    cfg = SqlDbConfigParser().parse(manifest, _ctx())

    monkeypatch.setattr(
        "dltaf.integrations.sqldb.partial_success.build_single_catalog_table_source_factory",
        lambda config, schema_name, source_table: (lambda: f"SOURCE::{source_table}"),
    )
    monkeypatch.setattr(
        "dltaf.integrations.sqldb.partial_success._collect_clickhouse_stats",
        lambda dataset, table: {"rows_count": 0, "bytes_count": 0},
    )
    monkeypatch.setattr(
        "dltaf.integrations.sqldb.partial_success.run_with_replace_protection",
        lambda **kwargs: (_ for _ in ()).throw(
            ReplaceProtectedExecutionError(
                stage="source",
                cause=InvalidRequestError("Could not reflect: requested table(s) not available"),
            )
        ),
    )

    result = run_catalog_with_partial_success(
        config=cfg,
        ctx=_ctx(),
        pipeline=object(),
        write_disposition="merge",
    )

    assert result.status == "failed"
    assert "zero tables loaded" in (result.message or "")


def test_partial_success_policy_threshold_requires_limits() -> None:
    with pytest.raises(ValueError, match="min_success_tables and/or min_success_ratio"):
        validate_manifest(
            {
                **_manifest(),
                "run": {
                    "write_disposition": "merge",
                    "partial_success": {
                        "mode": "threshold",
                        "tolerate_errors": "source_only",
                    },
                },
            },
            strict_source=True,
            enforce_filename_match=False,
        )


def test_failed_run_result_can_be_returned_to_hook_pipeline() -> None:
    result = RunResult(
        status="failed",
        payload=None,
        table_stats=(
            TableRunStats(
                source_table="spr_country",
                target_table="country",
                started_at=datetime.now(timezone.utc),
                finished_at=datetime.now(timezone.utc),
                duration_seconds=1.0,
                status="failed",
                error_kind="CONFIG:invalid_config",
                error_message="boom",
            ),
        ),
        message="policy failed",
    )
    assert result.status == "failed"
    assert result.message == "policy failed"
