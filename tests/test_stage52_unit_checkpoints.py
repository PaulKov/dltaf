from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

from dltaf.app.runtime import RunContext, RunOptions
from dlt_utils.core.run_result import UnitRunStats
from dlt_utils.core.unit_checkpoints import (
    ClickHouseUnitCheckpointStore,
    build_resumed_unit_stat,
    build_unit_resume_key,
    resolve_unit_checkpoint_config,
)
from dlt_utils.hooks.audit_run import AuditRunHook


def _ctx() -> RunContext:
    return RunContext(
        run_id="framework-run-id",
        started_at=datetime.now(timezone.utc),
        manifest_path=Path("manifest.yaml"),
        pipeline_name="dlt__uploader__to__clickhouse__b057",
        source_kind="uploader_b057",
        destination="clickhouse",
        dataset="uploader_b057",
        logger=logging.getLogger("unit-checkpoints-test"),
        options=RunOptions(),
    )


def _unit(**kwargs) -> UnitRunStats:
    now = datetime.now(timezone.utc)
    defaults = dict(
        unit_kind="project_period",
        unit_id="BIN=180240007079|projectId=1|year=2026|period=FULL_YEAR",
        ordinal=1,
        started_at=now,
        finished_at=now,
        duration_seconds=2.5,
        status="success",
        stage="load_ready",
        rows_emitted=1,
        retry_count=0,
        warnings_count=0,
        external_id="message-1",
        outcome_code="loaded",
        details={"requested_bin": "180240007079", "requested_project_id": 1},
        load_uuid="airflow-run-1",
        batch_key="2026-desc",
        resume_key="project_period:BIN=180240007079|projectId=1|year=2026|period=FULL_YEAR",
    )
    defaults.update(kwargs)
    return UnitRunStats(**defaults)


def test_checkpoint_config_uses_airflow_run_id_as_default_load_uuid(monkeypatch) -> None:
    monkeypatch.setenv("AIRFLOW_CTX_DAG_RUN_ID", "scheduled__2026-05-20T03:00:00+00:00")
    manifest = {
        "run": {
            "checkpoint": {
                "enabled": True,
                "batch_key": "b057-window-2026-2023",
            }
        }
    }

    config = resolve_unit_checkpoint_config(manifest, _ctx(), default_batch_key="fallback")

    assert config.enabled is True
    assert config.load_uuid == "scheduled__2026-05-20T03:00:00+00:00"
    assert config.batch_key == "b057-window-2026-2023"
    assert config.table_name == "_pipeline_unit_checkpoints"


def test_build_unit_resume_key_is_stable_and_explicit() -> None:
    assert build_unit_resume_key("bin", "961040001237") == "bin:961040001237"
    assert (
        build_unit_resume_key(
            "project_period",
            "BIN=180240007079|projectId=1|year=2026|period=FULL_YEAR",
        )
        == "project_period:BIN=180240007079|projectId=1|year=2026|period=FULL_YEAR"
    )


class _FakeQueryResult:
    def __init__(self, rows) -> None:
        self.result_rows = rows


class _FakeClient:
    def __init__(self) -> None:
        self.commands: list[str] = []
        self.inserts: list[tuple[str, list[list[object]], list[str]]] = []
        self.query_sql: list[str] = []
        self.rows: list[tuple[str, str, str]] = []

    def command(self, sql: str) -> None:
        self.commands.append(sql)

    def insert(self, table: str, rows, column_names):
        self.inserts.append((table, list(rows), list(column_names)))

    def query(self, sql: str):
        self.query_sql.append(sql)
        return _FakeQueryResult(self.rows)


def test_clickhouse_checkpoint_store_persists_and_loads_rows_payload() -> None:
    client = _FakeClient()
    store = ClickHouseUnitCheckpointStore(client=client, database="uploader_b057")
    unit = _unit()
    rows = [{"generatorRequest": {"id": 42}, "requested_bin": "180240007079"}]

    store.record_unit(
        ctx=_ctx(),
        load_uuid="airflow-run-1",
        batch_key="2026-desc",
        resume_key=unit.resume_key or "",
        unit_stat=unit,
        rows=rows,
    )

    assert any("_pipeline_unit_checkpoints" in command for command in client.commands)
    insert = client.inserts[-1]
    assert insert[0] == "uploader_b057._pipeline_unit_checkpoints"
    assert "rows_json" in insert[2]

    rows_json = insert[1][0][insert[2].index("rows_json")]
    unit_stats_json = insert[1][0][insert[2].index("unit_stats_json")]
    client.rows = [(unit.resume_key, unit_stats_json, rows_json)]

    loaded = store.load_completed_units(
        pipeline_name="dlt__uploader__to__clickhouse__b057",
        load_uuid="airflow-run-1",
        batch_key="2026-desc",
    )

    assert list(loaded) == [unit.resume_key]
    record = loaded[unit.resume_key or ""]
    assert record.rows == rows
    assert record.unit_stat.unit_id == unit.unit_id
    assert record.unit_stat.details == unit.details


def test_resumed_unit_stat_keeps_cleanup_details_and_marks_checkpoint_resume() -> None:
    unit = _unit()
    record = ClickHouseUnitCheckpointStore.to_record(
        resume_key=unit.resume_key or "",
        unit_stats_json=unit.to_json(),
        rows_json='[{"ok": true}]',
    )

    resumed = build_resumed_unit_stat(record, ordinal=7)

    assert resumed.status == "success"
    assert resumed.stage == "checkpoint_resume"
    assert resumed.outcome_code == "resumed"
    assert resumed.ordinal == 7
    assert resumed.rows_emitted == 1
    assert resumed.details == unit.details
    assert resumed.load_uuid == unit.load_uuid
    assert resumed.batch_key == unit.batch_key
    assert resumed.resume_key == unit.resume_key


def test_audit_run_hook_writes_checkpoint_identity_columns(monkeypatch) -> None:
    client = _FakeClient()
    hook = AuditRunHook()

    monkeypatch.setattr("dlt_utils.hooks.audit_run.get_clickhouse_client", lambda: client)
    monkeypatch.setenv("DESTINATION__CLICKHOUSE__CREDENTIALS__DATABASE", "uploader_b057")

    from dlt_utils.core.run_result import RunResult

    hook.post_run({}, _ctx(), RunResult(status="success", unit_stats=(_unit(),)))

    unit_insert = next(item for item in client.inserts if item[0] == "uploader_b057._pipeline_run_units")
    assert {"load_uuid", "batch_key", "resume_key"}.issubset(set(unit_insert[2]))
    row = unit_insert[1][0]
    assert row[unit_insert[2].index("load_uuid")] == "airflow-run-1"
    assert row[unit_insert[2].index("batch_key")] == "2026-desc"
    assert row[unit_insert[2].index("resume_key")] == _unit().resume_key
