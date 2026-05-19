from __future__ import annotations

from dataclasses import dataclass

from dlt_utils.core.error_taxonomy import ErrorCategory, ErrorInfo, classify_exception


@dataclass(frozen=True)
class ErrorTaxonomyService:
    """Thin service wrapper around the stable error taxonomy implementation."""

    def classify(self, exc: BaseException) -> ErrorInfo:
        return classify_exception(exc)


__all__ = ["ErrorCategory", "ErrorInfo", "ErrorTaxonomyService", "classify_exception"]
