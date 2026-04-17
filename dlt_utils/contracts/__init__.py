"""Payload/data contracts utilities.

Stage 27 introduces **payload contracts** to detect schema drift in external
systems (API / Kafka payloads) early.

Stage 28 adds:
- contract samples (one or many)
- capture mode (store failing payload samples safely)
- a CI workflow (`dlt-contract-test`)

Public entrypoints:
- :class:`~dlt_utils.contracts.payload_contract.PayloadContract`
- :func:`~dlt_utils.contracts.payload_contract.build_payload_contract`
- :func:`~dlt_utils.contracts.contract_testing.check_contract_samples`
"""

from dlt_utils.contracts.contract_testing import (
    ContractCheckResult,
    SampleCheckResult,
    check_contract_samples,
    result_to_dict,
)
from dlt_utils.contracts.payload_contract import (
    PayloadContract,
    PayloadContractCaptureConfig,
    PayloadContractConfig,
    PayloadContractMode,
    PayloadContractRuntimeContext,
    PayloadContractViolation,
    build_payload_contract,
    discover_samples_for_schema,
    resolve_payload_contract_sample_paths,
    resolve_payload_contract_schema_path,
)

__all__ = [
    "PayloadContract",
    "PayloadContractCaptureConfig",
    "PayloadContractConfig",
    "PayloadContractMode",
    "PayloadContractRuntimeContext",
    "PayloadContractViolation",
    "build_payload_contract",
    "resolve_payload_contract_schema_path",
    "discover_samples_for_schema",
    "resolve_payload_contract_sample_paths",
    "SampleCheckResult",
    "ContractCheckResult",
    "check_contract_samples",
    "result_to_dict",
]
