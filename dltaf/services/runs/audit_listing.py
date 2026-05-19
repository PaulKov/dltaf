from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Sequence

from dlt_utils.clickhouse_helpers import get_clickhouse_client
from dltaf.services.execution.redaction_service import RedactionService


@dataclass(frozen=True)
class AuditRunsRow:
    started_at: Optional[datetime]
    status: str
    duration_seconds: object
    packages_count: object
    tables_count: object
    rows_count: object
    run_id: str
    source_kind: str
    destination: str
    error_text: str


def add_runs_list_arguments(parser) -> None:
    parser.add_argument("--pipeline", default=None, help="Filter by pipeline_name")
    parser.add_argument("--limit", type=int, default=20, help="How many rows to show")
    parser.add_argument(
        "--database",
        default=None,
        help="ClickHouse database (defaults to DESTINATION__CLICKHOUSE__CREDENTIALS__DATABASE)",
    )
    parser.add_argument("--table", default="_pipeline_runs", help="Audit table name")


class AuditRunsService:
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

    def _compose_where(self, *, client, pipeline: str | None) -> str:
        if not pipeline:
            return ""
        if hasattr(client, "format_query_value"):
            return f"WHERE pipeline_name = {client.format_query_value(str(pipeline))}"
        return f"WHERE pipeline_name = {self._quote_string_fallback(str(pipeline))}"

    def _fetch_rows(self, *, client, database: str, table: str, pipeline: str | None, limit: int) -> Sequence[tuple]:
        where = self._compose_where(client=client, pipeline=pipeline)
        query = f"""
SELECT
  started_at,
  status,
  duration_seconds,
  packages_count,
  tables_count,
  rows_count,
  run_id,
  source_kind,
  destination,
  error_type,
  error_class,
  error_code,
  error_message
FROM `{database}`.`{table}`
{where}
ORDER BY started_at DESC
LIMIT {int(limit)}
""".strip()
        result = client.query(query)
        return tuple(result.result_rows or [])

    def _map_rows(self, rows: Sequence[tuple]) -> Sequence[AuditRunsRow]:
        mapped: list[AuditRunsRow] = []
        for row in rows:
            (
                started_at,
                status,
                duration_seconds,
                packages_count,
                tables_count,
                rows_count,
                run_id,
                source_kind,
                destination,
                error_type,
                error_class,
                error_code,
                error_message,
            ) = row
            error = ""
            if error_type or error_class or error_code or error_message:
                prefix = "/".join([x for x in [error_class, error_code, error_type] if x])
                safe_msg = self.redaction.text(str(error_message or ""))
                error = f"{prefix}: {safe_msg}".strip(": ")
                if len(error) > 160:
                    error = error[:160] + "…"
            mapped.append(
                AuditRunsRow(
                    started_at=started_at,
                    status=str(status),
                    duration_seconds=duration_seconds,
                    packages_count=packages_count,
                    tables_count=tables_count,
                    rows_count=rows_count,
                    run_id=str(run_id),
                    source_kind=str(source_kind),
                    destination=str(destination),
                    error_text=error or "-",
                )
            )
        return tuple(mapped)

    def emit_rows(self, rows: Sequence[AuditRunsRow]) -> None:
        headers = [
            "started_at",
            "status",
            "duration_s",
            "packages",
            "tables",
            "rows",
            "run_id",
            "kind",
            "dest",
            "error",
        ]
        print("\t".join(headers))
        for row in rows:
            duration = row.duration_seconds
            try:
                duration_rendered = f"{float(duration):.3f}"
            except Exception:
                duration_rendered = str(duration)
            print(
                "\t".join(
                    [
                        self._fmt_dt(row.started_at),
                        row.status,
                        duration_rendered,
                        self._fmt_int(row.packages_count),
                        self._fmt_int(row.tables_count),
                        self._fmt_int(row.rows_count),
                        row.run_id,
                        row.source_kind,
                        row.destination,
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
            pipeline=args.pipeline,
            limit=int(args.limit),
        )
        rows = self._map_rows(rows_raw)
        if not rows:
            print("No rows found")
            return 0
        self.emit_rows(rows)
        return 0


__all__ = [
    'AuditRunsRow',
    'AuditRunsService',
    'add_runs_list_arguments',
]
