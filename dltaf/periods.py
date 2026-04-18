from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Sequence


PERIOD_SEQUENCE: tuple[str, ...] = (
    "QUARTER_1",
    "HALF_YEAR_1",
    "NINE_MONTH",
    "FULL_YEAR",
)
_PERIOD_TO_INDEX = {period: index for index, period in enumerate(PERIOD_SEQUENCE)}


@dataclass(frozen=True)
class PeriodPoint:
    """Canonical reporting period point used by windowed runners."""

    year: int
    period: str

    def as_tuple(self) -> tuple[int, str]:
        return self.year, self.period


def normalize_period_name(value: str) -> str:
    normalized = str(value or "").strip().upper()
    if normalized not in _PERIOD_TO_INDEX:
        raise ValueError(
            "period must be one of "
            f"{list(PERIOD_SEQUENCE)!r}, got {value!r}"
        )
    return normalized


def period_sort_key(year: int, period: str) -> tuple[int, int]:
    return int(year), _PERIOD_TO_INDEX[normalize_period_name(period)]


def default_window_end(
    *,
    current_year: Optional[int] = None,
    default_period: str = "FULL_YEAR",
) -> PeriodPoint:
    year = int(current_year or datetime.now(timezone.utc).year)
    return PeriodPoint(year=year, period=normalize_period_name(default_period))


def expand_period_window(
    *,
    start_year: int,
    start_period: str,
    end_year: Optional[int] = None,
    end_period: Optional[str] = None,
    current_year: Optional[int] = None,
    default_end_period: str = "FULL_YEAR",
) -> tuple[PeriodPoint, ...]:
    """Expand an inclusive reporting-period window.

    When end bounds are omitted, the window runs until the default end period of
    the current year. The function is intentionally generic and contains no
    source-specific semantics such as "all periods" flags.
    """

    normalized_start = PeriodPoint(
        year=int(start_year),
        period=normalize_period_name(start_period),
    )

    if end_year is None and end_period is None:
        normalized_end = default_window_end(
            current_year=current_year,
            default_period=default_end_period,
        )
    else:
        resolved_end_year = int(end_year if end_year is not None else start_year)
        resolved_end_period = normalize_period_name(end_period or start_period)
        normalized_end = PeriodPoint(
            year=resolved_end_year,
            period=resolved_end_period,
        )

    if period_sort_key(*normalized_end.as_tuple()) < period_sort_key(*normalized_start.as_tuple()):
        raise ValueError(
            "period window end must not be earlier than start: "
            f"start={normalized_start.as_tuple()} end={normalized_end.as_tuple()}"
        )

    items: list[PeriodPoint] = []
    year = normalized_start.year
    period_index = _PERIOD_TO_INDEX[normalized_start.period]
    end_key = period_sort_key(*normalized_end.as_tuple())

    while (year, period_index) <= end_key:
        items.append(PeriodPoint(year=year, period=PERIOD_SEQUENCE[period_index]))
        period_index += 1
        if period_index >= len(PERIOD_SEQUENCE):
            year += 1
            period_index = 0

    return tuple(items)


def format_period_window(items: Sequence[PeriodPoint]) -> str:
    if not items:
        return "empty-window"
    first = items[0]
    last = items[-1]
    if len(items) == 1:
        return f"{first.year}:{first.period}"
    return f"{first.year}:{first.period}..{last.year}:{last.period}"


__all__ = [
    "PERIOD_SEQUENCE",
    "PeriodPoint",
    "default_window_end",
    "expand_period_window",
    "format_period_window",
    "normalize_period_name",
    "period_sort_key",
]
