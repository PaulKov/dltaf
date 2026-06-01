from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from statistics import mean
from typing import Mapping, Optional, Sequence

from .run_result import UnitRunStats

BUSINESS_OUTCOME_STATUSES = frozenset(
    {
        "business_declined",
        "business_skipped",
        "period_unavailable",
    }
)
SUCCESS_STATUSES = frozenset({"success"})


@dataclass(frozen=True)
class UnitRollup:
    total_units: int
    succeeded_units: int
    failed_units: int
    rows_emitted: int
    avg_duration_seconds: Optional[float]
    p50_duration_seconds: Optional[float]
    p95_duration_seconds: Optional[float]
    business_units: int = 0
    technical_failed_units: int = 0


def normalize_unit_status(value: object) -> str:
    """Return the stable lowercase unit status used for observability rollups."""

    return str(value or "").strip().lower()


def is_unit_success(item: UnitRunStats) -> bool:
    """Return True when the unit emitted a technically successful result."""

    return normalize_unit_status(item.status) in SUCCESS_STATUSES


def is_unit_business_outcome(item: UnitRunStats) -> bool:
    """Return True for expected business declines/skips that are not incidents."""

    status = normalize_unit_status(item.status)
    return status in BUSINESS_OUTCOME_STATUSES or status.startswith("business_")


def is_unit_technical_failure(item: UnitRunStats) -> bool:
    """Return True for retryable/incident-worthy unit failures."""

    return not is_unit_success(item) and not is_unit_business_outcome(item)


def _percentile(sorted_values: Sequence[float], pct: float) -> Optional[float]:
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    idx = max(0, min(len(sorted_values) - 1, round((len(sorted_values) - 1) * pct)))
    return float(sorted_values[idx])


def build_unit_rollup(items: Sequence[UnitRunStats]) -> UnitRollup:
    durations = sorted(float(item.duration_seconds) for item in items if item.duration_seconds is not None)
    rows_emitted = sum(int(item.rows_emitted or 0) for item in items)
    succeeded_units = sum(1 for item in items if is_unit_success(item))
    business_units = sum(1 for item in items if is_unit_business_outcome(item))
    technical_failed_units = sum(1 for item in items if is_unit_technical_failure(item))
    return UnitRollup(
        total_units=len(items),
        succeeded_units=succeeded_units,
        failed_units=technical_failed_units,
        rows_emitted=rows_emitted,
        avg_duration_seconds=float(mean(durations)) if durations else None,
        p50_duration_seconds=_percentile(durations, 0.50),
        p95_duration_seconds=_percentile(durations, 0.95),
        business_units=business_units,
        technical_failed_units=technical_failed_units,
    )


@dataclass
class UnitProgressLogger:
    logger: logging.Logger
    unit_kind: str
    total_units: int
    verbosity: str = "compact"
    audit_table: Optional[str] = None
    checkpoint_every: int = 25
    checkpoint_seconds: float = 30.0
    external_label: str = "external_id"
    outcome_label: str = "outcome"

    def __post_init__(self) -> None:
        self._started_at = time.monotonic()
        self._last_checkpoint_at = self._started_at
        self._completed = 0
        self._succeeded = 0
        self._business = 0
        self._failed = 0
        self._rows_emitted = 0

    def announce_start(self, *, details: Optional[Mapping[str, object]] = None) -> None:
        details_parts = []
        if self.audit_table:
            details_parts.append(f"audit_table={self.audit_table}")
        for key, value in (details or {}).items():
            if value is None or str(value).strip() == "":
                continue
            details_parts.append(f"{key}={value}")
        suffix = f" ({', '.join(details_parts)})" if details_parts else ""
        self.logger.info(
            "Unit observability started: kind=%s total=%s verbosity=%s%s",
            self.unit_kind,
            self.total_units,
            self.verbosity,
            suffix,
        )

    def record(self, item: UnitRunStats) -> None:
        self._completed += 1
        if is_unit_success(item):
            self._succeeded += 1
        elif is_unit_business_outcome(item):
            self._business += 1
        else:
            self._failed += 1
        self._rows_emitted += int(item.rows_emitted or 0)

        if self.verbosity == "verbose":
            log_fn = self.logger.warning if is_unit_technical_failure(item) else self.logger.info
            log_fn(self._format_verbose_line(item))
        elif self._should_emit_checkpoint():
            self._emit_checkpoint()

    def finish(self, items: Sequence[UnitRunStats]) -> None:
        rollup = build_unit_rollup(items)
        self.logger.info(
            "Unit observability finished: total=%s ok=%s business=%s failed=%s rows=%s avg=%.3fs p50=%.3fs p95=%.3fs",
            rollup.total_units,
            rollup.succeeded_units,
            rollup.business_units,
            rollup.failed_units,
            rollup.rows_emitted,
            float(rollup.avg_duration_seconds or 0.0),
            float(rollup.p50_duration_seconds or 0.0),
            float(rollup.p95_duration_seconds or 0.0),
        )

    def _format_verbose_line(self, item: UnitRunStats) -> str:
        details = [
            f"[{item.ordinal}/{self.total_units}]",
            f"{item.unit_kind.upper()}={item.unit_id}",
            f"status={item.status}",
            f"stage={item.stage}",
            f"rows={int(item.rows_emitted or 0)}",
            f"sec={float(item.duration_seconds):.3f}",
        ]
        if item.external_id:
            details.append(f"{self.external_label}={item.external_id}")
        if item.outcome_code:
            details.append(f"{self.outcome_label}={item.outcome_code}")
        if item.retry_count is not None:
            details.append(f"retries={int(item.retry_count)}")
        if item.error_kind:
            details.append(f"error_kind={item.error_kind}")
        if item.error_message:
            details.append(f"error={item.error_message}")
        return "Unit result: " + " ".join(details)

    def _should_emit_checkpoint(self) -> bool:
        if self._completed <= 0:
            return False
        if self._completed == self.total_units:
            return True
        if self._completed % max(1, int(self.checkpoint_every)) == 0:
            return True
        now = time.monotonic()
        return (now - self._last_checkpoint_at) >= float(self.checkpoint_seconds)

    def _emit_checkpoint(self) -> None:
        now = time.monotonic()
        elapsed = max(0.0, now - self._started_at)
        self._last_checkpoint_at = now
        self.logger.info(
            "Unit progress: completed=%s/%s ok=%s business=%s failed=%s rows=%s elapsed=%.3fs",
            self._completed,
            self.total_units,
            self._succeeded,
            self._business,
            self._failed,
            self._rows_emitted,
            elapsed,
        )


__all__ = [
    "BUSINESS_OUTCOME_STATUSES",
    "SUCCESS_STATUSES",
    "UnitProgressLogger",
    "UnitRollup",
    "build_unit_rollup",
    "is_unit_business_outcome",
    "is_unit_success",
    "is_unit_technical_failure",
    "normalize_unit_status",
]
