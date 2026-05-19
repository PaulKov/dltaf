"""Contract testing helpers.

Stage 28
--------
Adds a CI-friendly workflow for payload contracts by validating
contract samples.

Notes:
- Error details must be safe (no payload values).
- This module does not fetch any external data; it only validates local JSON.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dlt_utils.contracts.payload_contract import (
    PayloadContract,
    PayloadContractConfig,
    _format_validation_errors,
    _iter_validation_errors_safe,
    resolve_payload_contract_sample_paths,
    resolve_payload_contract_schema_path,
)


@dataclass(frozen=True)
class SampleCheckResult:
    sample_path: str
    ok: bool
    errors: List[str]


@dataclass(frozen=True)
class ContractCheckResult:
    contract_name: str
    schema_ref: Optional[str]
    ok: bool
    samples: List[SampleCheckResult]
    errors_count: int


def load_json_file(path: Path) -> Tuple[Optional[Any], Optional[str]]:
    """Load JSON from file.

    Returns (obj, error). Error is a safe string (no payload echo).
    """

    try:
        text = path.read_text(encoding="utf-8")
    except Exception as e:
        return None, f"read_failed: {e}"

    try:
        return json.loads(text), None
    except Exception as e:
        return None, f"json_parse_failed: {e}"


def validate_payload(contract: PayloadContract, payload: Any, *, max_errors: int) -> List[str]:
    """Validate payload and return safe error list (empty if valid)."""

    errs = list(_iter_validation_errors_safe(contract.validator, payload))
    if not errs:
        return []
    return _format_validation_errors(errs, limit=max_errors)


def resolve_contract_samples(
    cfg: PayloadContractConfig,
    *,
    manifest_path: Path,
) -> Tuple[Optional[Path], List[Path]]:
    """Resolve schema path (if file-based) and sample file paths."""

    schema_path = resolve_payload_contract_schema_path(cfg, manifest_path=manifest_path)
    samples = resolve_payload_contract_sample_paths(
        cfg,
        manifest_path=manifest_path,
        schema_path=schema_path,
    )
    return schema_path, samples


def check_contract_samples(
    contract: PayloadContract,
    *,
    cfg: PayloadContractConfig,
    manifest_path: Path,
    max_errors: Optional[int] = None,
    allow_no_samples: bool = False,
) -> ContractCheckResult:
    """Validate contract samples.

    Returns a structured result for CLI/CI consumption.
    """

    limit = int(max_errors) if max_errors is not None else int(contract.max_errors)
    limit = max(1, min(200, limit))

    _schema_path, sample_paths = resolve_contract_samples(cfg, manifest_path=manifest_path)

    sample_results: List[SampleCheckResult] = []

    if not sample_paths:
        ok = bool(allow_no_samples)
        errors_count = 0 if ok else 1
        if not ok:
            sample_results.append(
                SampleCheckResult(sample_path="<none>", ok=False, errors=["no_samples_found"])
            )
        return ContractCheckResult(
            contract_name=contract.name,
            schema_ref=contract.schema_ref,
            ok=ok,
            samples=sample_results,
            errors_count=errors_count,
        )

    total_errors = 0
    for sp in sample_paths:
        payload, load_err = load_json_file(sp)
        if load_err:
            total_errors += 1
            sample_results.append(SampleCheckResult(sample_path=str(sp), ok=False, errors=[load_err]))
            continue

        errs = validate_payload(contract, payload, max_errors=limit)
        if errs:
            total_errors += 1
            sample_results.append(SampleCheckResult(sample_path=str(sp), ok=False, errors=errs))
        else:
            sample_results.append(SampleCheckResult(sample_path=str(sp), ok=True, errors=[]))

    return ContractCheckResult(
        contract_name=contract.name,
        schema_ref=contract.schema_ref,
        ok=(total_errors == 0),
        samples=sample_results,
        errors_count=total_errors,
    )


def result_to_dict(res: ContractCheckResult) -> Dict[str, Any]:
    return {
        "contract": res.contract_name,
        "schema": res.schema_ref,
        "ok": res.ok,
        "errors_count": res.errors_count,
        "samples": [
            {"path": s.sample_path, "ok": s.ok, "errors": list(s.errors)} for s in res.samples
        ],
    }
