from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Mapping, Optional

from dlt_utils.clickhouse_helpers import get_clickhouse_client
from dltaf.app.runtime import RunContext
from dlt_utils.core.error_taxonomy import classify_exception
from dltaf.services.execution.redaction import safe_exception_message, safe_json, safe_str
from dlt_utils.core.run_result import LoadMetrics, RunResult, UnitRunStats, extract_load_metrics


def _quote_ident(name: str) -> str:
    """Safely quote ClickHouse identifier with backticks."""

    return "`" + str(name).replace("`", "``") + "`"


def _metrics_to_json(metrics: Optional[LoadMetrics]) -> Optional[str]:
    if metrics is None:
        return None
    try:
        return json.dumps(metrics.to_dict(include_none=False), ensure_ascii=False)
    except Exception:
        try:
            return json.dumps(asdict(metrics), ensure_ascii=False)
        except Exception:
            return None


@dataclass
class AuditRunHook:
    """Write an audit row for each pipeline run into ClickHouse.

    The hook is best-effort:
    - If ClickHouse client can't be created (no creds / no dependency), it silently skips.
    - If insert fails, it logs a warning but never fails the run.

    Table:
        <database>._pipeline_runs

    Database is taken from ClickHouse destination creds env:
        DESTINATION__CLICKHOUSE__CREDENTIALS__DATABASE
    falling back to ctx.dataset.

    Notes:
        - Never logs secret values.
        - Stores env provenance (ctx.env_sources) as JSON.

    Stage 14:
        - Hooks receive structured RunResult in post_run.
        - We store normalized metrics (packages/jobs/tables/rows) for successful loads.
        - For plan/dry-run modes, we store plan_json.

    Stage 16:
        - `--dry-run-online` is tracked via ctx.options.dry_run_online and stored in a column.
    """

    name: str = "audit_run"
    table_name: str = "_pipeline_runs"
    units_table_name: str = "_pipeline_run_units"

    def pre_run(self, manifest: Mapping[str, Any], ctx: RunContext) -> None:
        return

    def post_run(self, manifest: Mapping[str, Any], ctx: RunContext, result: Any) -> None:
        status = "success"
        if isinstance(result, RunResult):
            status = result.status
        self._write_row(manifest, ctx, status=status, result=result, exc=None)

    def on_error(self, manifest: Mapping[str, Any], ctx: RunContext, exc: Exception) -> None:
        self._write_row(manifest, ctx, status="failed", result=None, exc=exc)

    def _write_row(
        self,
        manifest: Mapping[str, Any],
        ctx: RunContext,
        *,
        status: str,
        result: Optional[Any],
        exc: Optional[Exception],
    ) -> None:
        try:
            client = get_clickhouse_client()
            if client is None:
                ctx.logger.debug("AuditRunHook: ClickHouse client not available; skipping")
                return

            db = os.getenv("DESTINATION__CLICKHOUSE__CREDENTIALS__DATABASE") or ctx.dataset or "default"
            table = self.table_name
            units_table = self.units_table_name

            self._ensure_tables(client, db=db, table=table, units_table=units_table)

            # Structured result support
            payload_obj: Any = result
            plan_obj: Optional[Mapping[str, Any]] = None
            metrics_obj: Optional[LoadMetrics] = None

            finished_at = datetime.utcnow()
            duration_s = ctx.elapsed_seconds(finished_at)

            if isinstance(result, RunResult):
                payload_obj = result.payload
                plan_obj = result.plan
                metrics_obj = result.load_metrics
                finished_at = result.finished_at
                duration_s = float(result.duration_seconds)

            # If execution failed in dry-run strict mode, the exception may carry a plan.
            if plan_obj is None and exc is not None and hasattr(exc, "plan"):
                try:
                    maybe_plan = getattr(exc, "plan")
                    if isinstance(maybe_plan, Mapping):
                        plan_obj = maybe_plan
                except Exception:
                    plan_obj = None

            # Fallback: attempt to extract metrics from raw payload
            if metrics_obj is None and payload_obj is not None and status == "success":
                metrics_obj = extract_load_metrics(payload_obj)

            error_type = exc.__class__.__name__ if exc else None
            error_message = safe_exception_message(exc, limit=1500) if exc else None

            error_class = None
            error_code = None
            if exc is not None:
                try:
                    err_info = classify_exception(exc)
                    error_class, error_code = err_info.to_tuple()
                except Exception:
                    error_class, error_code = None, None

            env_sources_json = json.dumps(ctx.env_sources or {}, ensure_ascii=False)

            plan_json: Optional[str] = None
            if plan_obj is not None:
                plan_json = safe_json(plan_obj)

            load_metrics_json = _metrics_to_json(metrics_obj)

            packages_count = metrics_obj.packages_count if metrics_obj else None
            jobs_count = metrics_obj.jobs_count if metrics_obj else None
            failed_jobs_count = metrics_obj.failed_jobs_count if metrics_obj else None
            tables_count = metrics_obj.tables_count if metrics_obj else None
            rows_count = metrics_obj.rows_count if metrics_obj else None

            result_type = type(payload_obj).__name__ if payload_obj is not None else None
            # Keep repr for debugging (truncated), but audit queries should use normalized columns.
            result_repr = safe_str(payload_obj, limit=2000) if payload_obj is not None else None

            cols = [
                "run_id",
                "pipeline_name",
                "manifest_path",
                "source_kind",
                "destination",
                "dataset",
                "started_at",
                "finished_at",
                "duration_seconds",
                "status",
                "write_disposition",
                "validate_only",
                "explain_config",
                "dry_run",
                "dry_run_online",
                "plan",
                "env_sources_json",
                "plan_json",
                "load_metrics_json",
                "packages_count",
                "jobs_count",
                "failed_jobs_count",
                "tables_count",
                "rows_count",
                "result_type",
                "result_repr",
                "error_type",
                "error_class",
                "error_code",
                "error_message",
            ]

            values = [
                ctx.run_id,
                ctx.pipeline_name,
                str(ctx.manifest_path),
                ctx.source_kind,
                ctx.destination,
                ctx.dataset,
                ctx.started_at,
                finished_at,
                float(duration_s),
                str(status),
                ctx.options.write_disposition,
                1 if ctx.options.validate_only else 0,
                1 if ctx.options.explain_config else 0,
                1 if ctx.options.dry_run else 0,
                1 if getattr(ctx.options, "dry_run_online", False) else 0,
                1 if ctx.options.plan else 0,
                env_sources_json,
                plan_json,
                load_metrics_json,
                packages_count,
                jobs_count,
                failed_jobs_count,
                tables_count,
                rows_count,
                result_type,
                result_repr,
                error_type,
                error_class,
                error_code,
                error_message,
            ]

            # clickhouse-connect supports insert(table, data, column_names=[...])
            if hasattr(client, "insert"):
                client.insert(f"{db}.{table}", [values], column_names=cols)
                if isinstance(result, RunResult) and result.unit_stats:
                    self._write_unit_rows(
                        client=client,
                        db=db,
                        table=units_table,
                        ctx=ctx,
                        result=result,
                    )
            else:  # pragma: no cover
                ctx.logger.warning("AuditRunHook: ClickHouse client has no insert(); skipping")

        except Exception as e:
            # Best-effort: do not fail the run
            ctx.logger.warning("AuditRunHook failed: %s", safe_exception_message(e), exc_info=True)

    def _ensure_tables(self, client: Any, *, db: str, table: str, units_table: str) -> None:
        """Create database + audit table if they do not exist.

        If the table exists but schema is older, we add missing columns via ALTER.
        """

        # Ensure database exists
        client.command(f"CREATE DATABASE IF NOT EXISTS {_quote_ident(db)}")

        ddl = f"""
CREATE TABLE IF NOT EXISTS {_quote_ident(db)}.{_quote_ident(table)} (
  run_id String,
  pipeline_name String,
  manifest_path String,
  source_kind LowCardinality(String),
  destination LowCardinality(String),
  dataset String,
  started_at DateTime64(3, 'UTC'),
  finished_at DateTime64(3, 'UTC'),
  duration_seconds Float64,
  status LowCardinality(String),
  write_disposition Nullable(String),
  validate_only UInt8,
  explain_config UInt8,
  dry_run UInt8,
  dry_run_online UInt8,
  plan UInt8,
  env_sources_json String,
  plan_json Nullable(String),
  load_metrics_json Nullable(String),
  packages_count Nullable(UInt32),
  jobs_count Nullable(UInt32),
  failed_jobs_count Nullable(UInt32),
  tables_count Nullable(UInt32),
  rows_count Nullable(UInt64),
  result_type Nullable(String),
  result_repr Nullable(String),
  error_type Nullable(String),
  error_class Nullable(String),
  error_code Nullable(String),
  error_message Nullable(String)
) ENGINE = MergeTree
ORDER BY (pipeline_name, started_at, run_id)
""".strip()

        client.command(ddl)

        # Ensure forward-compatible columns for already existing tables.
        alters = [
            "ADD COLUMN IF NOT EXISTS plan_json Nullable(String)",
            "ADD COLUMN IF NOT EXISTS load_metrics_json Nullable(String)",
            "ADD COLUMN IF NOT EXISTS dry_run_online UInt8",
            "ADD COLUMN IF NOT EXISTS packages_count Nullable(UInt32)",
            "ADD COLUMN IF NOT EXISTS jobs_count Nullable(UInt32)",
            "ADD COLUMN IF NOT EXISTS failed_jobs_count Nullable(UInt32)",
            "ADD COLUMN IF NOT EXISTS tables_count Nullable(UInt32)",
            "ADD COLUMN IF NOT EXISTS rows_count Nullable(UInt64)",
            "ADD COLUMN IF NOT EXISTS error_class Nullable(String)",
            "ADD COLUMN IF NOT EXISTS error_code Nullable(String)",
        ]

        for a in alters:
            client.command(f"ALTER TABLE {_quote_ident(db)}.{_quote_ident(table)} {a}")

        units_ddl = f"""
CREATE TABLE IF NOT EXISTS {_quote_ident(db)}.{_quote_ident(units_table)} (
  run_id String,
  pipeline_name String,
  manifest_path String,
  source_kind LowCardinality(String),
  unit_kind LowCardinality(String),
  unit_id String,
  ordinal UInt32,
  started_at DateTime64(3, 'UTC'),
  finished_at DateTime64(3, 'UTC'),
  duration_seconds Float64,
  status LowCardinality(String),
  stage LowCardinality(String),
  rows_emitted Nullable(UInt64),
  retry_count Nullable(UInt32),
  warnings_count Nullable(UInt32),
  external_id Nullable(String),
  outcome_code Nullable(String),
  error_kind Nullable(String),
  error_message Nullable(String),
  details_json Nullable(String)
) ENGINE = MergeTree
ORDER BY (pipeline_name, started_at, run_id, unit_kind, ordinal)
""".strip()
        client.command(units_ddl)

    def _write_unit_rows(
        self,
        *,
        client: Any,
        db: str,
        table: str,
        ctx: RunContext,
        result: RunResult,
    ) -> None:
        rows: list[list[Any]] = []
        cols = [
            "run_id",
            "pipeline_name",
            "manifest_path",
            "source_kind",
            "unit_kind",
            "unit_id",
            "ordinal",
            "started_at",
            "finished_at",
            "duration_seconds",
            "status",
            "stage",
            "rows_emitted",
            "retry_count",
            "warnings_count",
            "external_id",
            "outcome_code",
            "error_kind",
            "error_message",
            "details_json",
        ]

        for item in result.unit_stats:
            rows.append(self._map_unit_row(ctx=ctx, item=item))

        if rows:
            client.insert(f"{db}.{table}", rows, column_names=cols)

    def _map_unit_row(self, *, ctx: RunContext, item: UnitRunStats) -> list[Any]:
        details_json: Optional[str] = None
        if item.details:
            try:
                details_json = json.dumps(dict(item.details), ensure_ascii=False)
            except Exception:
                details_json = safe_json(dict(item.details))

        return [
            ctx.run_id,
            ctx.pipeline_name,
            str(ctx.manifest_path),
            ctx.source_kind,
            item.unit_kind,
            item.unit_id,
            int(item.ordinal),
            item.started_at,
            item.finished_at,
            float(item.duration_seconds),
            item.status,
            item.stage,
            item.rows_emitted,
            item.retry_count,
            item.warnings_count,
            item.external_id,
            item.outcome_code,
            item.error_kind,
            item.error_message,
            details_json,
        ]
