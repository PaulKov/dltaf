"""Error taxonomy and classification.

Goal
----
Provide a stable, queryable set of error categories/codes across:
- audit logs (ClickHouse _pipeline_runs)
- CLI/CI logs

Design principles
-----------------
- Keep the taxonomy small and stable (categories are low-cardinality).
- Store a *code* for more detail while keeping the *category* stable.
- Never include secret values in error info.
- Be dependency-light and robust (classification should never raise).
"""

from __future__ import annotations

import socket
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional


class ErrorCategory(str, Enum):
    CONFIG = "CONFIG"
    SECRETS = "SECRETS"
    AUTH = "AUTH"
    CONNECTIVITY = "CONNECTIVITY"
    HTTP = "HTTP"
    TIMEOUT = "TIMEOUT"
    DATA = "DATA"
    POLICY = "POLICY"
    DEPENDENCY = "DEPENDENCY"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class ErrorInfo:
    """Normalized error info.

    Attributes:
        category: high-level stable category
        code: more specific stable code (within the category)
        retryable: whether an automated retry could make sense
        details: small extra info safe for audit (no secrets)
    """

    category: str
    code: str
    retryable: bool = False
    details: Optional[str] = None

    def to_tuple(self) -> tuple[str, str]:
        return (self.category, self.code)


def _contains(s: str, needles: set[str]) -> bool:
    ss = (s or "").lower()
    return any(n.lower() in ss for n in needles)


def _class_name(obj: Any) -> str:
    try:
        return obj.__class__.__name__
    except Exception:
        return "Unknown"


def classify_exception(exc: BaseException) -> ErrorInfo:
    """Classify exception into a stable taxonomy.

    The function must be safe: it never raises.
    """

    try:
        # Local imports keep this module lightweight.
        from pydantic import ValidationError

        from dlt_utils.adapters.http.requests_client import HttpRequestError, HttpResponseError
        from dlt_utils.contracts.payload_contract import PayloadContractViolation
        from dltaf.services.execution.planner import DryRunStrictError
        from dlt_utils.patterns.kafka_async_job import (
            KafkaAsyncJobDecodeError,
            KafkaAsyncJobTimeoutError,
            KafkaAsyncJobTriggerError,
        )

        # --- Most specific: known custom exceptions ---
        if isinstance(exc, DryRunStrictError):
            return ErrorInfo(category=ErrorCategory.CONFIG.value, code="dry_run_strict_failed", retryable=False)

        if isinstance(exc, KafkaAsyncJobTimeoutError):
            return ErrorInfo(category=ErrorCategory.TIMEOUT.value, code="kafka_wait_timeout", retryable=True)

        if isinstance(exc, KafkaAsyncJobDecodeError):
            return ErrorInfo(category=ErrorCategory.DATA.value, code="kafka_message_decode", retryable=False)

        if isinstance(exc, KafkaAsyncJobTriggerError):
            return ErrorInfo(category=ErrorCategory.HTTP.value, code="async_trigger_failed", retryable=True)

        if isinstance(exc, PayloadContractViolation):
            return ErrorInfo(
                category=ErrorCategory.DATA.value,
                code="payload_contract_violation",
                retryable=False,
            )

        if isinstance(exc, HttpRequestError):
            # Transport-level error. Usually retryable.
            return ErrorInfo(category=ErrorCategory.CONNECTIVITY.value, code="http_transport", retryable=True)

        if isinstance(exc, HttpResponseError):
            status = int(getattr(exc, "status_code", 0) or 0)
            if status in {401, 403}:
                return ErrorInfo(category=ErrorCategory.AUTH.value, code="http_auth", retryable=False)
            if status == 429:
                return ErrorInfo(category=ErrorCategory.HTTP.value, code="http_rate_limit", retryable=True)
            if 500 <= status <= 599:
                return ErrorInfo(category=ErrorCategory.HTTP.value, code="http_5xx", retryable=True)
            if 400 <= status <= 499:
                return ErrorInfo(category=ErrorCategory.HTTP.value, code="http_4xx", retryable=False)
            return ErrorInfo(category=ErrorCategory.HTTP.value, code="http_unexpected", retryable=False)

        if isinstance(exc, ValidationError):
            return ErrorInfo(category=ErrorCategory.CONFIG.value, code="manifest_schema_validation", retryable=False)

        # --- Policy hook / known message patterns ---
        msg = ""
        try:
            msg = str(exc)
        except Exception:
            msg = ""

        if "ReplacePolicyHook" in msg:
            return ErrorInfo(category=ErrorCategory.POLICY.value, code="replace_blocked", retryable=False)

        # Missing dependencies (common in optional adapters)
        if _contains(msg, {"kafka-python", "clickhouse-connect", "clickhouse_connect"}):
            return ErrorInfo(category=ErrorCategory.DEPENDENCY.value, code="dependency_missing", retryable=False)

        # --- Timeouts ---
        if isinstance(exc, (TimeoutError, socket.timeout)):
            return ErrorInfo(category=ErrorCategory.TIMEOUT.value, code="timeout", retryable=True)
        if _contains(msg, {"timeout", "timed out"}):
            return ErrorInfo(category=ErrorCategory.TIMEOUT.value, code="timeout", retryable=True)

        # --- Auth patterns (non-HTTP) ---
        if _contains(msg, {"unauthorized", "forbidden", "invalid credentials", "authentication"}):
            return ErrorInfo(category=ErrorCategory.AUTH.value, code="auth_failed", retryable=False)

        # --- Secrets patterns ---
        if _contains(msg, {"vault", "airflow variable", "airflow_variables", "secret"}):
            # This is heuristic: we prefer to separate secrets/config from runtime.
            return ErrorInfo(category=ErrorCategory.SECRETS.value, code="secrets_resolution", retryable=False)

        # --- Connectivity patterns ---
        if _contains(msg, {"connection refused", "name or service not known", "temporary failure", "no route"}):
            return ErrorInfo(category=ErrorCategory.CONNECTIVITY.value, code="network", retryable=True)

        # --- Data patterns ---
        if _contains(msg, {"json", "decode", "parse", "schema"}):
            # Avoid classifying manifest schema errors here (handled above).
            return ErrorInfo(category=ErrorCategory.DATA.value, code="data_validation", retryable=False)

        # --- Config patterns ---
        if isinstance(exc, (ValueError, KeyError)):
            return ErrorInfo(category=ErrorCategory.CONFIG.value, code="invalid_config", retryable=False)

        # Fallback
        return ErrorInfo(category=ErrorCategory.UNKNOWN.value, code="unknown", retryable=False, details=_class_name(exc))

    except Exception:
        # If anything goes wrong in the classifier, do not block the run.
        return ErrorInfo(category=ErrorCategory.UNKNOWN.value, code="classifier_failed", retryable=False)
