from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

from dltaf.app.runtime import RunContext, RunOptions
from dltaf import UnitProgressLogger as PublicUnitProgressLogger
from dltaf import UnitRunStats as PublicUnitRunStats
from dltaf import build_unit_rollup as public_build_unit_rollup
from dltaf import build_run_result as public_build_run_result
from dltaf import is_unit_business_outcome as public_is_unit_business_outcome
from dlt_utils.core.unit_observability import is_unit_business_outcome, is_unit_technical_failure
from dlt_utils.core.run_result import RunResult, UnitRunStats
from dlt_utils.hooks.audit_run import AuditRunHook
from dlt_utils.hooks.runtime_summary import RuntimeSummaryHook


def _ctx() -> RunContext:
    return RunContext(
        run_id="unit-observability-test",
        started_at=datetime.now(timezone.utc),
        manifest_path=Path("manifest.yaml"),
        pipeline_name="dlt__pkb_conclusion__to__clickhouse__adata",
        source_kind="pkb_conclusion",
        destination="clickhouse",
        dataset="adata",
        logger=logging.getLogger("unit-observability-test"),
        options=RunOptions(observability_verbosity="verbose", dlt_progress="summary_only"),
    )


def _unit(status: str = "success", *, ordinal: int = 1, unit_id: str = "961040001237") -> UnitRunStats:
    now = datetime.now(timezone.utc)
    return UnitRunStats(
        unit_kind="bin",
        unit_id=unit_id,
        ordinal=ordinal,
        started_at=now,
        finished_at=now,
        duration_seconds=1.25,
        status=status,
        stage="final_get" if status == "success" else "kafka_wait",
        rows_emitted=7 if status == "success" else 0,
        retry_count=2,
        warnings_count=0 if status == "success" else 1,
        external_id="req-1",
        outcome_code="approved_loaded" if status == "success" else "declined",
        error_kind=None if status == "success" else "decision_declined",
        error_message=None if status == "success" else "decision is 'DECLINED'",
        details={"requested_report": True},
    )


def test_public_dltaf_namespace_exports_runtime_result_contracts() -> None:
    assert PublicUnitRunStats is UnitRunStats
    logger = PublicUnitProgressLogger(
        logging.getLogger("public-export-test"),
        unit_kind="bin",
        total_units=1,
    )
    assert isinstance(logger, PublicUnitProgressLogger)
    assert public_build_unit_rollup((_unit(),)).succeeded_units == 1
    assert callable(public_build_run_result)
    assert public_is_unit_business_outcome(_unit("business_declined"))


def test_unit_rollup_separates_business_and_technical_outcomes() -> None:
    rollup = public_build_unit_rollup(
        (
            _unit("success", ordinal=1),
            _unit("business_declined", ordinal=2),
            _unit("technical_failed", ordinal=3),
        )
    )

    assert rollup.succeeded_units == 1
    assert rollup.business_units == 1
    assert rollup.technical_failed_units == 1
    assert rollup.failed_units == 1
    assert is_unit_business_outcome(_unit("business_declined"))
    assert is_unit_technical_failure(_unit("technical_failed"))


def test_unit_progress_logs_business_outcome_without_warning(caplog) -> None:
    logger = PublicUnitProgressLogger(
        logging.getLogger("unit-business-outcome-test"),
        unit_kind="bin",
        total_units=1,
        verbosity="verbose",
    )

    with caplog.at_level(logging.INFO):
        logger.record(_unit("business_declined"))
        logger.finish((_unit("business_declined"),))

    assert "status=business_declined" in caplog.text
    assert "business=1" in caplog.text
    assert not [record for record in caplog.records if record.levelno >= logging.WARNING]


def test_runtime_summary_logs_unit_summary(caplog) -> None:
    hook = RuntimeSummaryHook()
    ctx = _ctx()
    result = RunResult(
        status="partial_success",
        unit_stats=(_unit("success", ordinal=1), _unit("failed", ordinal=2, unit_id="970000000001")),
        message="PKB rollup: total_bins=2 succeeded_bins=1 failed_bins=1",
        duration_seconds=3.0,
    )

    with caplog.at_level(logging.INFO):
        hook.pre_run({"run": {"observability": {"verbosity": "verbose", "dlt_progress": "summary_only"}}}, ctx)
        hook.post_run({}, ctx, result)

    assert "Runtime observability: verbosity=verbose dlt_progress=summary_only" in caplog.text
    assert "Per-unit summary:" in caplog.text
    assert "Unit rollup:" in caplog.text
    assert "PKB rollup: total_bins=2" in caplog.text


class _FakeClient:
    def __init__(self) -> None:
        self.commands: list[str] = []
        self.inserts: list[tuple[str, list[list[object]], list[str]]] = []

    def command(self, sql: str) -> None:
        self.commands.append(sql)

    def insert(self, table: str, rows, column_names):
        self.inserts.append((table, list(rows), list(column_names)))


def test_audit_run_hook_writes_unit_rows(monkeypatch) -> None:
    client = _FakeClient()
    hook = AuditRunHook()
    ctx = _ctx()
    result = RunResult(
        status="partial_success",
        payload={"ok": True},
        unit_stats=(_unit("success", ordinal=1), _unit("failed", ordinal=2)),
        duration_seconds=2.0,
    )

    monkeypatch.setattr("dlt_utils.hooks.audit_run.get_clickhouse_client", lambda: client)
    monkeypatch.setenv("DESTINATION__CLICKHOUSE__CREDENTIALS__DATABASE", "adata")

    hook.post_run({}, ctx, result)

    assert any(table == "adata._pipeline_runs" for table, _, _ in client.inserts)
    assert any(table == "adata._pipeline_run_units" for table, _, _ in client.inserts)
    unit_insert = next(item for item in client.inserts if item[0] == "adata._pipeline_run_units")
    assert len(unit_insert[1]) == 2
