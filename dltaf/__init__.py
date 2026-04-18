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

__all__ = [
    "__version__",
    "LoadMetrics",
    "RunResult",
    "TableRunStats",
    "UnitProgressLogger",
    "UnitRollup",
    "UnitRunStats",
    "build_run_result",
    "build_unit_rollup",
    "extract_load_metrics",
]
__version__ = "0.2.6"
