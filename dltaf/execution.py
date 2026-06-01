"""Generic execution helpers for API-style unit runners.

Private integrations often process a sequence of logical units: BINs,
projects, tenants, periods, files, or any combination of those dimensions. The
framework should not know those business dimensions. It only needs a reusable
way to bound concurrency by an arbitrary unit field selected by the runner.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, fields, is_dataclass
from typing import Any, Callable, Dict, Iterable, Iterator, Mapping, Optional, Tuple, TypeVar


UnitT = TypeVar("UnitT")
ResultT = TypeVar("ResultT")


DEFAULT_PARTITION_ALIASES = {
    "unit": "unit_id",
}


@dataclass(frozen=True)
class PartitionedExecutionConfig:
    """Concurrency settings for unit-level runners.

    Attributes:
        max_parallel_units: Global worker ceiling.
        max_parallel_partitions: Maximum number of distinct partition values
            that may run at the same time.
        partition_key: Unit attribute/detail key used as the partition value.
    """

    max_parallel_units: int = 1
    max_parallel_partitions: int = 1
    partition_key: str = "unit_id"


def normalize_parallel_limit(
    value: Any,
    *,
    default: int,
    field_name: str,
    maximum: Optional[int] = None,
) -> int:
    """Parse and validate a positive parallelism limit."""

    raw = default if value in {None, ""} else value
    try:
        parsed = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be an integer") from exc

    if parsed < 1:
        raise ValueError(f"{field_name} must be >= 1")
    if maximum is not None and parsed > maximum:
        raise ValueError(f"{field_name} must be <= {maximum}")
    return parsed


def _unit_mapping(unit: Any) -> Dict[str, Any]:
    data: Dict[str, Any] = {}

    if isinstance(unit, Mapping):
        data.update(unit)

    if is_dataclass(unit):
        for field in fields(unit):
            data.setdefault(field.name, getattr(unit, field.name))

    if hasattr(unit, "__dict__"):
        for key, value in vars(unit).items():
            if not key.startswith("_"):
                data.setdefault(key, value)

    to_details = getattr(unit, "to_details", None)
    if callable(to_details):
        details = to_details()
        if isinstance(details, Mapping):
            data.update(details)

    for attr in ("unit_id", "id", "name"):
        if hasattr(unit, attr):
            data.setdefault(attr, getattr(unit, attr))

    return data


def resolve_unit_partition_value(
    unit: Any,
    partition_key: str,
    *,
    aliases: Optional[Mapping[str, str]] = None,
) -> str:
    """Resolve a partition value from a unit object or mapping.

    The key can point to a dataclass field, plain attribute, mapping key, or
    a value exposed by ``unit.to_details()``. This keeps business dimensions
    outside the framework while still giving runners a standard scheduler.
    """

    raw_key = str(partition_key or "unit_id").strip()
    key_aliases = {**DEFAULT_PARTITION_ALIASES, **dict(aliases or {})}
    key = key_aliases.get(raw_key, raw_key)
    data = _unit_mapping(unit)

    if key not in data:
        available = ", ".join(sorted(str(item) for item in data))
        raise ValueError(
            f"Unknown unit partition key {raw_key!r}. Available keys: {available}"
        )

    value = data[key]
    return str(value) if value is not None else "<null>"


def execute_partitioned_units(
    units: Iterable[UnitT],
    worker: Callable[[UnitT], ResultT],
    *,
    max_parallel_units: int,
    max_parallel_partitions: int,
    partition_key: str = "unit_id",
    partition_aliases: Optional[Mapping[str, str]] = None,
    partition_value_getter: Optional[Callable[[UnitT], str]] = None,
) -> Iterator[Tuple[UnitT, ResultT]]:
    """Execute units while bounding concurrency by a configurable partition.

    Units sharing the same partition value are processed sequentially inside one
    worker. Different partitions may run concurrently up to the configured
    limits. Results are yielded as partitions finish.
    """

    unit_list = list(units)
    if not unit_list:
        return

    unit_limit = normalize_parallel_limit(
        max_parallel_units,
        default=1,
        field_name="max_parallel_units",
    )
    partition_limit = normalize_parallel_limit(
        max_parallel_partitions,
        default=1,
        field_name="max_parallel_partitions",
    )

    grouped: Dict[str, list[UnitT]] = {}
    for unit in unit_list:
        partition = (
            str(partition_value_getter(unit))
            if partition_value_getter is not None
            else resolve_unit_partition_value(
                unit,
                partition_key,
                aliases=partition_aliases,
            )
        )
        grouped.setdefault(partition, []).append(unit)

    worker_count = max(1, min(unit_limit, partition_limit, len(grouped)))

    def process_partition(partition_units: Iterable[UnitT]) -> list[Tuple[UnitT, ResultT]]:
        return [(unit, worker(unit)) for unit in partition_units]

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = [
            executor.submit(process_partition, partition_units)
            for partition_units in grouped.values()
        ]
        for future in as_completed(futures):
            yield from future.result()
