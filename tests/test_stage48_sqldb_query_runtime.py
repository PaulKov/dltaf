from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType

from dltaf.app.runtime import RunContext, RunOptions
from dltaf.extensions.runners.builtins import build_builtin_runners
from dltaf.integrations.oracle_custom_sql.runner import OracleCustomSQLRunner
from dltaf.integrations.sqldb.config import (
    ResolvedSqlDbQueryRuntime,
    SqlDbConfigParser,
    SqlDbOracleConnectionConfig,
)
from dltaf.integrations.sqldb.dialects.oracle import OracleDialect
from dltaf.integrations.sqldb.modes.query import QueryMode
from dltaf.integrations.sqldb.runner import SqlDbRunner


def _ctx() -> RunContext:
    return RunContext(
        run_id="stage48-test",
        started_at=datetime.now(timezone.utc),
        manifest_path=Path("manifest.yaml"),
        pipeline_name="dlt__sqldb__to__clickhouse__raw",
        source_kind="sqldb",
        destination="clickhouse",
        dataset="raw",
        logger=logging.getLogger("stage48"),
        options=RunOptions(),
    )


def test_oracle_custom_sql_runner_is_alias_wrapper() -> None:
    assert issubclass(OracleCustomSQLRunner, SqlDbRunner)
    assert OracleCustomSQLRunner.kind == "oracle_custom_sql"
    kinds = {runner.kind for runner in build_builtin_runners()}
    assert "oracle_custom_sql" in kinds


def test_query_mode_runs_with_native_sqldb_path(monkeypatch) -> None:
    manifest = {
        "version": 1,
        "pipeline": {
            "name": "dlt__sqldb__to__clickhouse__raw",
            "destination": "clickhouse",
            "dataset": "raw",
        },
        "run": {"write_disposition": "merge"},
        "source": {
            "kind": "sqldb",
            "dialect": "oracle",
            "mode": "query",
            "name": "sqldb_oracle_query",
            "query": {
                "queries": [
                    {
                        "name": "q1",
                        "sql": "select :dt_from as dt from dual",
                        "primary_key": ["dt"],
                        "params": {"dt_from": "2025-01-01"},
                    }
                ],
                "fetch_batch_size": 1000,
            },
            "dialect_options": {"init_sql": "alter session set nls_date_format = 'YYYY-MM-DD'"},
        },
    }
    cfg = SqlDbConfigParser().parse(manifest, _ctx())
    captured: dict[str, object] = {}

    class FakePipeline:
        pass

    def fake_pipeline(**kwargs):
        captured["pipeline_kwargs"] = kwargs
        return FakePipeline()

    def fake_runtime(parsed_cfg):
        captured["runtime_mode"] = parsed_cfg.mode
        return ResolvedSqlDbQueryRuntime(
            source_name="sqldb_oracle_query",
            fetch_batch_size=1000,
            init_sql="alter session set current_schema = test",
            thick_mode=None,
            connection=SqlDbOracleConnectionConfig(
                host="db",
                port=1521,
                username="u",
                password="p",
                database="xe",
                dsn=None,
            ),
            queries=(),
        )

    def fake_build_factory(runtime, *, logger_, dialect):
        captured["runtime_source_name"] = runtime.source_name
        captured["dialect_name"] = dialect.name
        return lambda: "SOURCE"

    def fake_run_with_replace_protection(*, pipeline, source_factory, manifest, write_disposition):
        captured["manifest_kind"] = manifest["source"]["kind"]
        captured["write_disposition"] = write_disposition
        captured["source_factory_value"] = source_factory()
        return "LOAD_INFO"

    fake_dlt = ModuleType("dlt")
    fake_dlt.pipeline = fake_pipeline
    monkeypatch.setitem(sys.modules, "dlt", fake_dlt)
    monkeypatch.setattr("dltaf.integrations.sqldb.modes.query.build_oracle_query_source_factory", fake_build_factory)
    monkeypatch.setattr("dltaf.integrations.sqldb.modes.query.run_with_replace_protection", fake_run_with_replace_protection)
    monkeypatch.setattr("dltaf.integrations.sqldb.modes.query.SqlDbConfigParser.build_query_runtime", lambda self, parsed_cfg: fake_runtime(parsed_cfg))

    result = QueryMode().run(cfg, _ctx(), OracleDialect())
    assert result == "LOAD_INFO"
    assert captured["runtime_mode"] == "query"
    assert captured["dialect_name"] == "oracle"
    assert captured["manifest_kind"] == "oracle_custom_sql"
    assert captured["write_disposition"] == "merge"
    assert captured["source_factory_value"] == "SOURCE"
    assert captured["pipeline_kwargs"]["pipeline_name"] == "dlt__sqldb__to__clickhouse__raw"


def test_legacy_oracle_custom_sql_manifest_runs_through_sqldb(monkeypatch) -> None:
    manifest = {
        "version": 1,
        "pipeline": {
            "name": "dlt__oracle__to__clickhouse__raw",
            "destination": "clickhouse",
            "dataset": "raw",
        },
        "run": {"write_disposition": "append"},
        "source": {
            "kind": "oracle_custom_sql",
            "name": "legacy_oracle_query",
            "queries": [{"name": "q1", "sql": "select 1 as id from dual"}],
        },
    }
    captured: dict[str, object] = {}

    def fake_execute(self, config, ctx):
        captured["requested_kind"] = config.requested_kind
        captured["mode"] = config.mode
        captured["dialect"] = config.dialect
        return "OK"

    monkeypatch.setattr("dltaf.integrations.sqldb.workflow.SqlDbWorkflow.execute", fake_execute)
    result = OracleCustomSQLRunner().run(manifest, _ctx())
    assert result == "OK"
    assert captured["requested_kind"] == "oracle_custom_sql"
    assert captured["mode"] == "query"
    assert captured["dialect"] == "oracle"
