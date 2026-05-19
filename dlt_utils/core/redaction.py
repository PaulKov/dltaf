"""Compatibility shim for centralized redaction helpers.

Native implementation now lives in :mod:`dltaf.services.execution.redaction`.
"""

from dltaf.app.deprecation import warn_legacy_module

warn_legacy_module(__name__, replacement="dltaf.services.execution.redaction")

from dltaf.services.execution.redaction import (
    REDACTED,
    RedactionOptions,
    is_sensitive_key,
    redact_obj,
    redact_text,
    safe_exception_message,
    safe_json,
    safe_str,
)

__all__ = [
    "REDACTED",
    "RedactionOptions",
    "is_sensitive_key",
    "redact_obj",
    "redact_text",
    "safe_exception_message",
    "safe_json",
    "safe_str",
]
