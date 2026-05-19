"""Public package surface for `dltaf`.

`dltaf` is the canonical framework namespace. Existing legacy modules
(`dlt_utils`, `cli`, `dag_builder`, ...) remain available as compatibility
facades, but new consumer code should import framework contracts from here.
"""

from dlt_utils.core.run_result import (
    LoadMetrics,
    RunResult,
    TableRunStats,
    UnitRunStats,
    build_run_result,
    extract_load_metrics,
)
from dlt_utils.core.unit_observability import UnitProgressLogger, UnitRollup, build_unit_rollup
from dltaf.airflow.runtime_params import (
    build_runtime_overrides,
    extract_param_defaults,
    load_runtime_overrides_from_manifest,
)
from dltaf.periods import (
    PERIOD_SEQUENCE,
    PeriodPoint,
    default_window_end,
    expand_period_window,
    format_period_window,
    normalize_period_name,
    period_sort_key,
)

__all__ = [
    "__version__",
    "LoadMetrics",
    "PERIOD_SEQUENCE",
    "PeriodPoint",
    "RunResult",
    "TableRunStats",
    "UnitProgressLogger",
    "UnitRollup",
    "UnitRunStats",
    "build_runtime_overrides",
    "build_run_result",
    "build_unit_rollup",
    "default_window_end",
    "expand_period_window",
    "extract_param_defaults",
    "extract_load_metrics",
    "format_period_window",
    "load_runtime_overrides_from_manifest",
    "normalize_period_name",
    "period_sort_key",
]
__version__ = "0.2.7"
