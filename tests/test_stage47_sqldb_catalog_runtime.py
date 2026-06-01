from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType

from dltaf.app.runtime import RunContext, RunOptions
from dltaf.integrations.sql_database.runner import SQLDatabaseRunner
from dltaf.integrations.sqldb.config import SqlDbConfigParser
from dltaf.integrations.sqldb.dialects.generic import GenericSqlDialect
from dltaf.integrations.sqldb.modes.catalog import CatalogMode
from dltaf.integrations.sqldb.runner import SqlDbRunner


def _ctx() -> RunContext:
    return RunContext(
        run_id="stage47-test",
        started_at=datetime.now(timezone.utc),
        manifest_path=Path("manifest.yaml"),
        pipeline_name="dlt__sqldb__to__clickhouse__raw",
        source_kind="sqldb",
        destination="clickhouse",
        dataset="raw",
        logger=logging.getLogger("stage47"),
        options=RunOptions(),
    )


def test_sql_database_runner_is_alias_wrapper() -> None:
    assert issubclass(SQLDatabaseRunner, SqlDbRunner)
    assert SQLDatabaseRunner.kind == "sql_database"


def test_catalog_mode_runs_with_native_sqldb_path(monkeypatch) -> None:
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
            "dialect": "generic",
            "mode": "catalog",
            "name": "sqldb_catalog",
            "catalog": {"schema": "public", "tables": ["orders"]},
        },
    }
    cfg = SqlDbConfigParser().parse(manifest, _ctx())
    captured: dict[str, object] = {}

    class FakePipeline:
        pass

    def fake_pipeline(**kwargs):
        captured["pipeline_kwargs"] = kwargs
        return FakePipeline()

    def fake_build_factory(parsed_cfg):
        captured["factory_config_kind"] = parsed_cfg.source.kind
        return lambda: "SOURCE"

    def fake_run_with_replace_protection(*, pipeline, source_factory, manifest, write_disposition):
        captured["manifest_kind"] = manifest["source"]["kind"]
        captured["write_disposition"] = write_disposition
        captured["source_factory_value"] = source_factory()
        return "LOAD_INFO"

    fake_dlt = ModuleType("dlt")
    fake_dlt.pipeline = fake_pipeline
    monkeypatch.setitem(sys.modules, "dlt", fake_dlt)
    monkeypatch.setattr("dltaf.integrations.sqldb.modes.catalog.build_catalog_source_factory", fake_build_factory)
    monkeypatch.setattr("dltaf.integrations.sqldb.modes.catalog.run_with_replace_protection", fake_run_with_replace_protection)

    result = CatalogMode().run(cfg, _ctx(), GenericSqlDialect())
    assert result == "LOAD_INFO"
    assert captured["factory_config_kind"] == "sqldb"
    assert captured["manifest_kind"] == "sql_database"
    assert captured["write_disposition"] == "merge"
    assert captured["source_factory_value"] == "SOURCE"
    assert captured["pipeline_kwargs"]["pipeline_name"] == "dlt__sqldb__to__clickhouse__raw"


def test_legacy_sql_database_manifest_runs_through_sqldb(monkeypatch) -> None:
    manifest = {
        "version": 1,
        "pipeline": {
            "name": "dlt__sql_database__to__clickhouse__raw",
            "destination": "clickhouse",
            "dataset": "raw",
        },
        "run": {"write_disposition": "append"},
        "source": {
            "kind": "sql_database",
            "name": "legacy_sql",
            "schema": "public",
            "tables": ["orders"],
        },
    }
    captured: dict[str, object] = {}

    class FakePipeline:
        pass

    def fake_pipeline(**kwargs):
        return FakePipeline()

    def fake_build_factory(parsed_cfg):
        captured["mode"] = str(parsed_cfg.mode)
        captured["dialect"] = str(parsed_cfg.dialect)
        return lambda: "SOURCE"

    def fake_run_with_replace_protection(*, pipeline, source_factory, manifest, write_disposition):
        captured["manifest_kind"] = manifest["source"]["kind"]
        return "OK"

    fake_dlt = ModuleType("dlt")
    fake_dlt.pipeline = fake_pipeline
    monkeypatch.setitem(sys.modules, "dlt", fake_dlt)
    monkeypatch.setattr("dltaf.integrations.sqldb.modes.catalog.build_catalog_source_factory", fake_build_factory)
    monkeypatch.setattr("dltaf.integrations.sqldb.modes.catalog.run_with_replace_protection", fake_run_with_replace_protection)

    result = SQLDatabaseRunner().run(manifest, _ctx())
    assert result == "OK"
    assert captured["mode"] == "catalog"
    assert captured["dialect"] == "generic"
    assert captured["manifest_kind"] == "sql_database"
