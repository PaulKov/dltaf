from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Sequence

from dlt_utils.clickhouse_helpers import get_clickhouse_client
from dltaf.services.execution.redaction_service import RedactionService


@dataclass(frozen=True)
class AuditRunUnitRow:
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    duration_seconds: object
    status: str
    stage: str
    unit_kind: str
    unit_id: str
    ordinal: object
    rows_emitted: object
    retry_count: object
    warnings_count: object
    external_id: str
    outcome_code: str
    error_text: str
    run_id: str


def add_runs_units_arguments(parser) -> None:
    parser.add_argument("--run-id", default=None, help="Filter by exact run_id")
    parser.add_argument("--pipeline", default=None, help="Filter by pipeline_name")
    parser.add_argument("--status", default=None, help="Filter by unit status")
    parser.add_argument("--unit-kind", default=None, help="Filter by unit_kind")
    parser.add_argument("--limit", type=int, default=100, help="How many unit rows to show")
    parser.add_argument(
        "--database",
        default=None,
        help="ClickHouse database (defaults to DESTINATION__CLICKHOUSE__CREDENTIALS__DATABASE)",
    )
    parser.add_argument("--table", default="_pipeline_run_units", help="Audit unit table name")


class AuditRunUnitsService:
    def __init__(self, *, logger) -> None:
        self.logger = logger
        self.redaction = RedactionService()

    @staticmethod
    def _fmt_dt(v: Optional[datetime]) -> str:
        if v is None:
            return "-"
        try:
            return v.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return str(v)

    @staticmethod
    def _fmt_int(v: object) -> str:
        if v is None:
            return "-"
        try:
            return str(int(v))
        except Exception:
            return str(v)

    @staticmethod
    def _quote_string_fallback(value: str) -> str:
        return "'" + value.replace("'", "''") + "'"

    def _compose_where(self, *, client, args: argparse.Namespace) -> str:
        filters: list[str] = []
        for column, value in (
            ("run_id", getattr(args, "run_id", None)),
            ("pipeline_name", getattr(args, "pipeline", None)),
            ("status", getattr(args, "status", None)),
            ("unit_kind", getattr(args, "unit_kind", None)),
        ):
            if not value:
                continue
            if hasattr(client, "format_query_value"):
                rendered = client.format_query_value(str(value))
            else:
                rendered = self._quote_string_fallback(str(value))
            filters.append(f"{column} = {rendered}")
        if not filters:
            return ""
        return "WHERE " + " AND ".join(filters)

    def _fetch_rows(self, *, client, database: str, table: str, args: argparse.Namespace) -> Sequence[tuple]:
        where = self._compose_where(client=client, args=args)
        query = f"""
SELECT
  started_at,
  finished_at,
  duration_seconds,
  status,
  stage,
  unit_kind,
  unit_id,
  ordinal,
  rows_emitted,
  retry_count,
  warnings_count,
  external_id,
  outcome_code,
  error_kind,
  error_message,
  run_id
FROM `{database}`.`{table}`
{where}
ORDER BY started_at DESC, ordinal ASC
LIMIT {int(args.limit)}
""".strip()
        result = client.query(query)
        return tuple(result.result_rows or [])

    def _map_rows(self, rows: Sequence[tuple]) -> Sequence[AuditRunUnitRow]:
        mapped: list[AuditRunUnitRow] = []
        for row in rows:
            (
                started_at,
                finished_at,
                duration_seconds,
                status,
                stage,
                unit_kind,
                unit_id,
                ordinal,
                rows_emitted,
                retry_count,
                warnings_count,
                external_id,
                outcome_code,
                error_kind,
                error_message,
                run_id,
            ) = row
            error = ""
            if error_kind or error_message:
                safe_msg = self.redaction.text(str(error_message or ""))
                error = f"{error_kind or ''}: {safe_msg}".strip(": ")
                if len(error) > 160:
                    error = error[:160] + "…"
            mapped.append(
                AuditRunUnitRow(
                    started_at=started_at,
                    finished_at=finished_at,
                    duration_seconds=duration_seconds,
                    status=str(status),
                    stage=str(stage),
                    unit_kind=str(unit_kind),
                    unit_id=str(unit_id),
                    ordinal=ordinal,
                    rows_emitted=rows_emitted,
                    retry_count=retry_count,
                    warnings_count=warnings_count,
                    external_id=str(external_id or ""),
                    outcome_code=str(outcome_code or ""),
                    error_text=error or "-",
                    run_id=str(run_id),
                )
            )
        return tuple(mapped)

    def emit_rows(self, rows: Sequence[AuditRunUnitRow]) -> None:
        headers = [
            "started_at",
            "finished_at",
            "duration_s",
            "status",
            "stage",
            "unit_kind",
            "unit_id",
            "ordinal",
            "rows",
            "retries",
            "warnings",
            "external_id",
            "outcome",
            "run_id",
            "error",
        ]
        print("\t".join(headers))
        for row in rows:
            try:
                duration_rendered = f"{float(row.duration_seconds):.3f}"
            except Exception:
                duration_rendered = str(row.duration_seconds)
            print(
                "\t".join(
                    [
                        self._fmt_dt(row.started_at),
                        self._fmt_dt(row.finished_at),
                        duration_rendered,
                        row.status,
                        row.stage,
                        row.unit_kind,
                        row.unit_id,
                        self._fmt_int(row.ordinal),
                        self._fmt_int(row.rows_emitted),
                        self._fmt_int(row.retry_count),
                        self._fmt_int(row.warnings_count),
                        row.external_id or "-",
                        row.outcome_code or "-",
                        row.run_id,
                        row.error_text,
                    ]
                )
            )

    def run(self, args: argparse.Namespace) -> int:
        client = get_clickhouse_client()
        if client is None:
            print(
                "ClickHouse client is not available. Ensure clickhouse-connect (or dlt[clickhouse]) is installed "
                "and DESTINATION__CLICKHOUSE__CREDENTIALS__* env vars are set."
            )
            return 1
        database = args.database or os.getenv("DESTINATION__CLICKHOUSE__CREDENTIALS__DATABASE") or "default"
        rows_raw = self._fetch_rows(
            client=client,
            database=str(database),
            table=str(args.table),
            args=args,
        )
        rows = self._map_rows(rows_raw)
        if not rows:
            print("No rows found")
            return 0
        self.emit_rows(rows)
        return 0


__all__ = ["AuditRunUnitRow", "AuditRunUnitsService", "add_runs_units_arguments"]
