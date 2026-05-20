from __future__ import annotations

import pytest

from dltaf import PeriodPoint, expand_period_window, format_period_window, normalize_period_name


def test_expand_period_window_uses_current_year_default_end() -> None:
    items = expand_period_window(
        start_year=2024,
        start_period="NINE_MONTH",
        current_year=2025,
    )

    assert items == (
        PeriodPoint(year=2024, period="NINE_MONTH"),
        PeriodPoint(year=2024, period="FULL_YEAR"),
        PeriodPoint(year=2025, period="QUARTER_1"),
        PeriodPoint(year=2025, period="HALF_YEAR_1"),
        PeriodPoint(year=2025, period="NINE_MONTH"),
        PeriodPoint(year=2025, period="FULL_YEAR"),
    )
    assert format_period_window(items) == "2024:NINE_MONTH..2025:FULL_YEAR"


def test_expand_period_window_supports_same_period_start_end() -> None:
    items = expand_period_window(
        start_year=2026,
        start_period="QUARTER_1",
        end_year=2026,
        end_period="QUARTER_1",
    )

    assert items == (PeriodPoint(year=2026, period="QUARTER_1"),)


def test_expand_period_window_rejects_inverted_bounds() -> None:
    with pytest.raises(ValueError, match="must not be earlier than start"):
        expand_period_window(
            start_year=2026,
            start_period="FULL_YEAR",
            end_year=2026,
            end_period="QUARTER_1",
        )


def test_normalize_period_name_is_case_insensitive() -> None:
    assert normalize_period_name("quarter_1") == "QUARTER_1"
