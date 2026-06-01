"""Structured run result and load metrics.

Stage 14
--------
We keep the external API backward compatible (runner returns whatever it returned
before), but hooks (audit/logging) receive a *structured* RunResult.

Why?
- Make audit logs queryable (packages/jobs/tables/rows counts) instead of storing
  only repr(result).
- Provide a stable envelope for future integrations (plan/dry-run modes,
  structured warnings, etc.).

Implementation notes
--------------------
- This module must stay dependency-free: do NOT import dlt.
- Metrics extraction is best-effort and relies on duck-typing to support
  different dlt versions.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence, Set


@dataclass(frozen=True)
class LoadMetrics:
    """Normalized metrics extracted from a dlt load result."""

    packages_count: Optional[int] = None
    jobs_count: Optional[int] = None
    failed_jobs_count: Optional[int] = None
    tables_count: Optional[int] = None
    rows_count: Optional[int] = None

    def to_dict(self, *, include_none: bool = False) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            "packages_count": self.packages_count,
            "jobs_count": self.jobs_count,
            "failed_jobs_count": self.failed_jobs_count,
            "tables_count": self.tables_count,
            "rows_count": self.rows_count,
        }
        if include_none:
            return d
        return {k: v for k, v in d.items() if v is not None}


@dataclass(frozen=True)
class TableRunStats:
    """Best-effort per-table execution metrics."""

    source_table: str
    target_table: str
    started_at: datetime
    finished_at: datetime
    duration_seconds: float
    status: str
    error_kind: Optional[str] = None
    error_message: Optional[str] = None
    rows_processed: Optional[int] = None
    rows_inserted: Optional[int] = None
    rows_updated: Optional[int] = None
    rows_deleted: Optional[int] = None
    rows_before: Optional[int] = None
    rows_after: Optional[int] = None
    delta_rows: Optional[int] = None
    delta_pct: Optional[float] = None
    size_gb_before: Optional[float] = None
    size_gb_after: Optional[float] = None
    delta_gb: Optional[float] = None
    rps: Optional[float] = None
    gbps: Optional[float] = None

    def to_dict(self, *, include_none: bool = False) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            "source_table": self.source_table,
            "target_table": self.target_table,
            "started_at": self.started_at.isoformat() + "Z",
            "finished_at": self.finished_at.isoformat() + "Z",
            "duration_seconds": self.duration_seconds,
            "status": self.status,
            "error_kind": self.error_kind,
            "error_message": self.error_message,
            "rows_processed": self.rows_processed,
            "rows_inserted": self.rows_inserted,
            "rows_updated": self.rows_updated,
            "rows_deleted": self.rows_deleted,
            "rows_before": self.rows_before,
            "rows_after": self.rows_after,
            "delta_rows": self.delta_rows,
            "delta_pct": self.delta_pct,
            "size_gb_before": self.size_gb_before,
            "size_gb_after": self.size_gb_after,
            "delta_gb": self.delta_gb,
            "rps": self.rps,
            "gbps": self.gbps,
        }
        if include_none:
            return d
        return {k: v for k, v in d.items() if v is not None}


@dataclass(frozen=True)
class UnitRunStats:
    """Best-effort per-unit execution metrics for API-style runners."""

    unit_kind: str
    unit_id: str
    ordinal: int
    started_at: datetime
    finished_at: datetime
    duration_seconds: float
    status: str
    stage: str
    rows_emitted: Optional[int] = None
    retry_count: Optional[int] = None
    warnings_count: Optional[int] = None
    external_id: Optional[str] = None
    outcome_code: Optional[str] = None
    error_kind: Optional[str] = None
    error_message: Optional[str] = None
    details: Optional[Mapping[str, Any]] = None
    load_uuid: Optional[str] = None
    batch_key: Optional[str] = None
    resume_key: Optional[str] = None

    def to_dict(self, *, include_none: bool = False) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            "unit_kind": self.unit_kind,
            "unit_id": self.unit_id,
            "ordinal": self.ordinal,
            "started_at": self.started_at.isoformat() + "Z",
            "finished_at": self.finished_at.isoformat() + "Z",
            "duration_seconds": self.duration_seconds,
            "status": self.status,
            "stage": self.stage,
            "rows_emitted": self.rows_emitted,
            "retry_count": self.retry_count,
            "warnings_count": self.warnings_count,
            "external_id": self.external_id,
            "outcome_code": self.outcome_code,
            "error_kind": self.error_kind,
            "error_message": self.error_message,
            "details": dict(self.details or {}) if self.details is not None else None,
            "load_uuid": self.load_uuid,
            "batch_key": self.batch_key,
            "resume_key": self.resume_key,
        }
        if include_none:
            return d
        return {k: v for k, v in d.items() if v is not None}

    def to_json(self, *, include_none: bool = False) -> str:
        """Serialize the unit stats as stable JSON for audit/checkpoint storage."""

        return json.dumps(self.to_dict(include_none=include_none), ensure_ascii=False)


@dataclass(frozen=True)
class RunResult:
    """Structured run result passed to hooks.

    Attributes:
        status: one of: success | partial_success | failed | planned | dry_run | dry_run_online
        payload: original result object returned by the runner (often dlt LoadInfo)
        plan: plan dict for plan/dry-run modes
        load_metrics: best-effort normalized metrics for successful loads
        finished_at: UTC timestamp
        duration_seconds: elapsed seconds
    """

    status: str
    payload: Any = None
    plan: Optional[Mapping[str, Any]] = None
    load_metrics: Optional[LoadMetrics] = None
    warnings: Sequence[str] = field(default_factory=tuple)
    table_stats: Sequence[TableRunStats] = field(default_factory=tuple)
    unit_stats: Sequence[UnitRunStats] = field(default_factory=tuple)
    message: Optional[str] = None

    finished_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    duration_seconds: float = 0.0

    def safe_json(self, *, indent: Optional[int] = None) -> str:
        """Safe JSON for logs/audit.

        - Does not include secret values (we never store ctx.env values).
        - Does not attempt to fully serialize payload.
        """

        obj = {
            "status": self.status,
            "message": self.message,
            "duration_seconds": self.duration_seconds,
            "finished_at": self.finished_at.isoformat() + "Z",
            "load_metrics": self.load_metrics.to_dict() if self.load_metrics else None,
            "warnings": list(self.warnings or ()),
            "table_stats": [table.to_dict() for table in self.table_stats or ()],
            "unit_stats": [unit.to_dict() for unit in self.unit_stats or ()],
            "plan": self.plan,
            "payload_type": type(self.payload).__name__ if self.payload is not None else None,
        }
        return json.dumps(obj, ensure_ascii=False, indent=indent)


def _get(obj: Any, names: Sequence[str]) -> Any:
    """Get an attribute or mapping key."""

    if obj is None:
        return None

    if isinstance(obj, Mapping):
        for n in names:
            if n in obj:
                return obj.get(n)
        return None

    for n in names:
        if hasattr(obj, n):
            try:
                return getattr(obj, n)
            except Exception:
                continue
    return None


def _iter_items(v: Any) -> Iterable[Any]:
    if v is None:
        return []
    if isinstance(v, Mapping):
        return list(v.values())
    if isinstance(v, (list, tuple, set)):
        return list(v)
    # single item
    return [v]


def _int_or_none(v: Any) -> Optional[int]:
    if v is None:
        return None
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, (int, float)):
        try:
            return int(v)
        except Exception:
            return None
    try:
        s = str(v).strip()
        if not s:
            return None
        return int(float(s))
    except Exception:
        return None


def _job_failed(job: Any) -> bool:
    status = _get(job, ["state", "status", "job_state", "load_state"])
    if status is None:
        return False
    s = str(status).strip().lower()
    return s in {"failed", "error", "errors", "failure"}


def _job_table_name(job: Any) -> Optional[str]:
    v = _get(job, ["table_name", "table", "resource", "name"])
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _job_rows(job: Any) -> Optional[int]:
    for key in ["rows", "row_count", "inserted_rows", "loaded_rows", "records"]:
        v = _get(job, [key])
        n = _int_or_none(v)
        if n is not None:
            return n
    return None


def extract_load_metrics(payload: Any) -> Optional[LoadMetrics]:
    """Best-effort extraction of normalized metrics from a dlt run result.

    Supports multiple dlt versions by using duck-typing.

    If payload does not look like a dlt LoadInfo, returns None.
    """

    if payload is None:
        return None

    packages = _get(payload, ["load_packages", "packages", "loads", "load_infos"])
    loads_ids = _get(payload, ["loads_ids", "load_ids", "loads"])

    packages_list = list(_iter_items(packages)) if packages is not None else []

    # Determine packages_count.
    packages_count: Optional[int] = None
    if packages_list:
        packages_count = len(packages_list)
    else:
        # loads_ids may be a list of strings
        if isinstance(loads_ids, (list, tuple, set)):
            packages_count = len(list(loads_ids))

    jobs_count = 0
    failed_jobs_count = 0
    tables: Set[str] = set()
    rows_count = 0
    rows_seen = False

    for pkg in packages_list:
        jobs = _get(pkg, ["jobs", "load_jobs", "job_infos", "load_job_infos"])
        for job in _iter_items(jobs):
            jobs_count += 1
            if _job_failed(job):
                failed_jobs_count += 1
            tn = _job_table_name(job)
            if tn:
                tables.add(tn)
            r = _job_rows(job)
            if r is not None:
                rows_seen = True
                rows_count += int(r)

    tables_count: Optional[int] = len(tables) if tables else None

    # If we couldn't find anything meaningful, return None.
    if packages_count is None and jobs_count == 0 and tables_count is None and not rows_seen:
        return None

    return LoadMetrics(
        packages_count=packages_count,
        jobs_count=jobs_count if jobs_count else None,
        failed_jobs_count=failed_jobs_count if jobs_count else None,
        tables_count=tables_count,
        rows_count=rows_count if rows_seen else None,
    )


def merge_load_metrics(metrics_items: Sequence[Optional[LoadMetrics]]) -> Optional[LoadMetrics]:
    meaningful = [item for item in metrics_items if item is not None]
    if not meaningful:
        return None

    packages_count = sum(int(item.packages_count or 0) for item in meaningful)
    jobs_count = sum(int(item.jobs_count or 0) for item in meaningful)
    failed_jobs_count = sum(int(item.failed_jobs_count or 0) for item in meaningful)
    tables_count = sum(int(item.tables_count or 0) for item in meaningful)
    rows_count = sum(int(item.rows_count or 0) for item in meaningful)

    return LoadMetrics(
        packages_count=packages_count or None,
        jobs_count=jobs_count or None,
        failed_jobs_count=failed_jobs_count or None,
        tables_count=tables_count or None,
        rows_count=rows_count or None,
    )


def build_run_result(
    *,
    ctx: Any,
    manifest: Mapping[str, Any],
    payload: Any,
    status: str,
    finished_at: Optional[datetime] = None,
    warnings: Optional[Sequence[str]] = None,
    table_stats: Optional[Sequence[TableRunStats]] = None,
    unit_stats: Optional[Sequence[UnitRunStats]] = None,
    message: Optional[str] = None,
    load_metrics: Optional[LoadMetrics] = None,
) -> RunResult:
    """Build a structured RunResult envelope."""

    fa = finished_at or datetime.now(timezone.utc)

    duration_s = 0.0
    try:
        duration_s = float(getattr(ctx, "elapsed_seconds")(fa))
    except Exception:
        duration_s = 0.0

    plan: Optional[Mapping[str, Any]] = None

    if status in {"planned", "dry_run", "dry_run_online"} and isinstance(payload, Mapping):
        plan = payload

    if load_metrics is None and status in {"success", "partial_success"}:
        load_metrics = extract_load_metrics(payload)

    return RunResult(
        status=str(status),
        payload=payload,
        plan=plan,
        load_metrics=load_metrics,
        warnings=tuple(warnings or ()),
        table_stats=tuple(table_stats or ()),
        unit_stats=tuple(unit_stats or ()),
        message=message,
        finished_at=fa,
        duration_seconds=float(duration_s),
    )
