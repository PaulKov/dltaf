"""HTTP adapters.

This package provides a small, dependency-light HTTP client wrapper used by
custom API integrations.

Goals:
- Standardize retries/backoff/timeouts
- Centralize logging (without leaking sensitive headers)
- Keep API integrations readable
"""

from .requests_client import RequestsHttpClient, HttpRequestError, HttpResponseError

__all__ = [
    "RequestsHttpClient",
    "HttpRequestError",
    "HttpResponseError",
]
