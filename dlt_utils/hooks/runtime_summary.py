from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from prettytable import PrettyTable

from dltaf.app.runtime import RunContext
from dlt_utils.core.run_result import RunResult, TableRunStats


def _color(value: str, color_code: str) -> str:
    return f"\033[{color_code}m{value}\033[0m"


def _status_label(status: str) -> str:
    normalized = str(status or "").strip().lower()
    if normalized == "success":
        return _color("SUCCESS", "32")
    if normalized == "partial_success":
        return _color("PARTIAL", "33")
    if normalized == "failed":
        return _color("FAILED", "31")
    return _color(normalized.upper() or "UNKNOWN", "36")


def _fmt_int(value: Any) -> str:
    if value is None:
        return "n/a"
    return f"{int(value):,}".replace(",", " ")


def _fmt_float(value: Any, digits: int = 2) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):.{digits}f}"


def _fmt_pct(value: Any) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):.2f}%"


def _build_table_stats_table(items: Sequence[TableRunStats]) -> PrettyTable:
    table = PrettyTable()
    table.field_names = [
        "source",
        "target",
        "status",
        "sec",
        "rows",
        "before",
        "after",
        "Δrows",
        "Δ%",
        "rps",
        "GiB before",
        "GiB after",
        "ΔGiB",
        "GiB/s",
        "error",
    ]
    table.align = "l"
    table.max_width["error"] = 48

    for item in items:
        table.add_row(
            [
                item.source_table,
                item.target_table,
                _status_label(item.status),
                _fmt_float(item.duration_seconds, 3),
                _fmt_int(item.rows_processed),
                _fmt_int(item.rows_before),
                _fmt_int(item.rows_after),
                _fmt_int(item.delta_rows),
                _fmt_pct(item.delta_pct),
                _fmt_float(item.rps, 2),
                _fmt_float(item.size_gb_before, 3),
                _fmt_float(item.size_gb_after, 3),
                _fmt_float(item.delta_gb, 3),
                _fmt_float(item.gbps, 3),
                item.error_message or "",
            ]
        )
    return table


def _build_rollup_table(result: RunResult) -> PrettyTable:
    metrics = result.load_metrics
    succeeded = len([item for item in result.table_stats if item.status == "success"])
    failed = len([item for item in result.table_stats if item.status != "success"])

    table = PrettyTable()
    table.field_names = [
        "run_status",
        "tables_ok",
        "tables_failed",
        "packages",
        "jobs",
        "failed_jobs",
        "rows_total",
        "duration_s",
    ]
    table.align = "l"
    table.add_row(
        [
            _status_label(result.status),
            str(succeeded) if result.table_stats else "n/a",
            str(failed) if result.table_stats else "n/a",
            _fmt_int(metrics.packages_count if metrics else None),
            _fmt_int(metrics.jobs_count if metrics else None),
            _fmt_int(metrics.failed_jobs_count if metrics else None),
            _fmt_int(metrics.rows_count if metrics else None),
            _fmt_float(result.duration_seconds, 3),
        ]
    )
    return table


@dataclass
class RuntimeSummaryHook:
    """Compact colored runtime summary for all built-in runs."""

    name: str = "runtime_summary"

    def pre_run(self, manifest: Mapping[str, Any], ctx: RunContext) -> None:
        run_cfg = manifest.get("run") or {}
        partial_cfg = (run_cfg.get("partial_success") or {}) if isinstance(run_cfg, Mapping) else {}
        if partial_cfg:
            ctx.logger.info(
                "Runtime policy: partial_success mode=%s tolerate_errors=%s",
                partial_cfg.get("mode"),
                partial_cfg.get("tolerate_errors"),
            )
        else:
            ctx.logger.info("Runtime policy: strict success")

    def post_run(self, manifest: Mapping[str, Any], ctx: RunContext, result: Any) -> None:
        if not isinstance(result, RunResult):
            return

        if result.message:
            ctx.logger.info("Run summary message: %s", result.message)

        for warning in result.warnings or ():
            ctx.logger.warning("Run warning: %s", warning)

        if result.table_stats:
            ctx.logger.info("Per-table summary:\n%s", _build_table_stats_table(result.table_stats))

        ctx.logger.info("Run rollup:\n%s", _build_rollup_table(result))

    def on_error(self, manifest: Mapping[str, Any], ctx: RunContext, exc: Exception) -> None:
        return

