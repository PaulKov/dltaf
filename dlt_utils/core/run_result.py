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
from datetime import datetime
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
class RunResult:
    """Structured run result passed to hooks.

    Attributes:
        status: one of: success | failed | planned | dry_run | dry_run_online
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

    finished_at: datetime = field(default_factory=datetime.utcnow)
    duration_seconds: float = 0.0

    def safe_json(self, *, indent: Optional[int] = None) -> str:
        """Safe JSON for logs/audit.

        - Does not include secret values (we never store ctx.env values).
        - Does not attempt to fully serialize payload.
        """

        obj = {
            "status": self.status,
            "duration_seconds": self.duration_seconds,
            "finished_at": self.finished_at.isoformat() + "Z",
            "load_metrics": self.load_metrics.to_dict() if self.load_metrics else None,
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


def build_run_result(
    *,
    ctx: Any,
    manifest: Mapping[str, Any],
    payload: Any,
    status: str,
    finished_at: Optional[datetime] = None,
) -> RunResult:
    """Build a structured RunResult envelope."""

    fa = finished_at or datetime.utcnow()

    duration_s = 0.0
    try:
        duration_s = float(getattr(ctx, "elapsed_seconds")(fa))
    except Exception:
        duration_s = 0.0

    plan: Optional[Mapping[str, Any]] = None
    load_metrics: Optional[LoadMetrics] = None

    if status in {"planned", "dry_run", "dry_run_online"} and isinstance(payload, Mapping):
        plan = payload

    if status == "success":
        load_metrics = extract_load_metrics(payload)

    return RunResult(
        status=str(status),
        payload=payload,
        plan=plan,
        load_metrics=load_metrics,
        finished_at=fa,
        duration_seconds=float(duration_s),
    )
