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
from dlt_utils.core.unit_checkpoints import (
    UnitCheckpointConfig,
    UnitCheckpointRecord,
    build_resumed_unit_stat,
    build_unit_resume_key,
    load_checkpoint_records,
    record_unit_checkpoint,
    resolve_unit_checkpoint_config,
)
from dltaf.airflow.runtime_params import (
    build_runtime_overrides,
    extract_param_defaults,
    load_runtime_overrides_from_manifest,
)
from dltaf.execution import (
    PartitionedExecutionConfig,
    execute_partitioned_units,
    normalize_parallel_limit,
    resolve_unit_partition_value,
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
    "PartitionedExecutionConfig",
    "RunResult",
    "TableRunStats",
    "UnitProgressLogger",
    "UnitRollup",
    "UnitRunStats",
    "UnitCheckpointConfig",
    "UnitCheckpointRecord",
    "build_runtime_overrides",
    "build_resumed_unit_stat",
    "build_run_result",
    "build_unit_resume_key",
    "build_unit_rollup",
    "default_window_end",
    "execute_partitioned_units",
    "expand_period_window",
    "extract_param_defaults",
    "extract_load_metrics",
    "format_period_window",
    "load_runtime_overrides_from_manifest",
    "load_checkpoint_records",
    "normalize_period_name",
    "normalize_parallel_limit",
    "period_sort_key",
    "record_unit_checkpoint",
    "resolve_unit_partition_value",
    "resolve_unit_checkpoint_config",
]
__version__ = "0.2.13"
