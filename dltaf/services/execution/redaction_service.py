from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dltaf.services.execution.redaction import (
    RedactionOptions,
    redact_obj,
    redact_text,
    safe_exception_message,
    safe_json,
    safe_str,
)


@dataclass(frozen=True)
class RedactionService:
    """Central access point for runtime-safe redaction helpers."""

    options: RedactionOptions = RedactionOptions()

    def redact(self, obj: Any) -> Any:
        return redact_obj(obj, options=self.options)

    def text(self, value: str) -> str:
        return str(redact_text(value))

    def safe_string(self, obj: Any) -> str:
        return safe_str(obj)

    def safe_json(self, obj: Any) -> str:
        return safe_json(obj)

    def safe_exception(self, exc: BaseException) -> str:
        return safe_exception_message(exc)


__all__ = [
    "RedactionOptions",
    "RedactionService",
    "redact_obj",
    "redact_text",
    "safe_exception_message",
    "safe_json",
    "safe_str",
]
