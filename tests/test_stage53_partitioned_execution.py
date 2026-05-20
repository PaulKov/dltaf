from __future__ import annotations

import threading
import time
from dataclasses import dataclass

import pytest

from dltaf import execute_partitioned_units, resolve_unit_partition_value


@dataclass(frozen=True)
class ApiUnit:
    unit_id: str
    tenant: str
    project_id: int

    def to_details(self):
        return {
            "tenant": self.tenant,
            "project_id": self.project_id,
        }


def test_execute_partitioned_units_limits_active_configured_partitions() -> None:
    lock = threading.Lock()
    active_tenants: set[str] = set()
    max_active_tenants = 0

    def worker(unit: ApiUnit) -> str:
        nonlocal max_active_tenants
        with lock:
            active_tenants.add(unit.tenant)
            max_active_tenants = max(max_active_tenants, len(active_tenants))
        time.sleep(0.03)
        with lock:
            active_tenants.remove(unit.tenant)
        return unit.unit_id

    units = [
        ApiUnit(unit_id=f"{tenant}-{idx}", tenant=tenant, project_id=idx)
        for tenant in ("a", "b", "c", "d")
        for idx in range(2)
    ]

    results = list(
        execute_partitioned_units(
            units,
            worker,
            max_parallel_units=8,
            max_parallel_partitions=2,
            partition_key="tenant",
        )
    )

    assert sorted(result for _, result in results) == sorted(unit.unit_id for unit in units)
    assert max_active_tenants <= 2


def test_resolve_unit_partition_value_supports_unit_details_and_aliases() -> None:
    unit = ApiUnit(unit_id="u-1", tenant="tenant-a", project_id=42)

    assert resolve_unit_partition_value(unit, "tenant") == "tenant-a"
    assert resolve_unit_partition_value(unit, "unit") == "u-1"
    assert resolve_unit_partition_value(unit, "project", aliases={"project": "project_id"}) == "42"


def test_resolve_unit_partition_value_rejects_unknown_keys() -> None:
    unit = ApiUnit(unit_id="u-1", tenant="tenant-a", project_id=42)

    with pytest.raises(ValueError, match="Unknown unit partition key"):
        resolve_unit_partition_value(unit, "missing")
