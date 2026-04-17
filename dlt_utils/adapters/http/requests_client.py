from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, Sequence, Set

import requests

from dltaf.services.execution.redaction import redact_text


DEFAULT_RETRY_STATUS_CODES: Set[int] = {429, 500, 502, 503, 504}


@dataclass(frozen=True)
class HttpResponseError(RuntimeError):
    """HTTP request completed but returned an unexpected response."""

    method: str
    url: str
    status_code: int
    body_snippet: str

    def __str__(self) -> str:  # pragma: no cover
        return (
            f"HTTP {self.method} {self.url} failed with status={self.status_code}, "
            f"body={self.body_snippet!r}"
        )


@dataclass(frozen=True)
class HttpRequestError(RuntimeError):
    """HTTP request failed due to network/timeout/transport errors."""

    method: str
    url: str
    error: str

    def __str__(self) -> str:  # pragma: no cover
        return f"HTTP {self.method} {self.url} request error: {self.error}"


class RequestsHttpClient:
    """A thin wrapper around requests.Session with retries/backoff.

    This is intentionally small. It is NOT a general-purpose REST SDK.

    Notes on logging & safety:
    - We never log request headers (to avoid leaking Authorization).
    - On errors we log only a short response body snippet.
    """

    def __init__(
        self,
        *,
        session: Optional[requests.Session] = None,
        base_url: str = "",
        default_headers: Optional[Mapping[str, str]] = None,
        timeout_seconds: int = 30,
        verify_ssl: bool = True,
        retries: int = 3,
        backoff_seconds: float = 1.0,
        retry_status_codes: Optional[Set[int]] = None,
        logger: Any = None,
    ) -> None:
        self._session = session or requests.Session()
        self._own_session = session is None
        self._base_url = (base_url or "").rstrip("/")
        self._default_headers: Dict[str, str] = dict(default_headers or {})
        self._timeout_seconds = int(timeout_seconds)
        self._verify_ssl = bool(verify_ssl)
        self._retries = max(0, int(retries))
        self._backoff_seconds = float(backoff_seconds)
        self._retry_status_codes = set(retry_status_codes or DEFAULT_RETRY_STATUS_CODES)
        self._logger = logger

    def close(self) -> None:
        if self._own_session:
            try:
                self._session.close()
            except Exception:
                pass

    def _full_url(self, path_or_url: str) -> str:
        s = str(path_or_url)
        if s.startswith("http://") or s.startswith("https://"):
            return s
        if not s.startswith("/"):
            s = "/" + s
        return f"{self._base_url}{s}" if self._base_url else s

    def _sleep_backoff(self, attempt: int) -> None:
        # Exponential backoff: backoff * 2^(attempt-1)
        delay = self._backoff_seconds * (2 ** max(0, attempt - 1))
        time.sleep(delay)

    def request_json(
        self,
        method: str,
        path_or_url: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
        json_body: Optional[Any] = None,
        data: Optional[Mapping[str, Any]] = None,
        headers: Optional[Mapping[str, str]] = None,
        timeout_seconds: Optional[int] = None,
        verify_ssl: Optional[bool] = None,
        expected_status_codes: Optional[Sequence[int]] = None,
        retries: Optional[int] = None,
    ) -> Any:
        """Perform HTTP request and parse JSON response.

        Raises:
            HttpRequestError: transport-level error (timeout, DNS, etc.)
            HttpResponseError: non-expected HTTP status or invalid JSON
        """

        m = str(method or "GET").upper()
        url = self._full_url(path_or_url)
        safe_url = str(redact_text(url))
        timeout = self._timeout_seconds if timeout_seconds is None else int(timeout_seconds)
        verify = self._verify_ssl if verify_ssl is None else bool(verify_ssl)
        max_retries = self._retries if retries is None else max(0, int(retries))

        expected = set(expected_status_codes or [])

        last_exc: Optional[Exception] = None
        for attempt in range(1, max_retries + 2):
            try:
                hdrs: Optional[Dict[str, str]] = None
                if self._default_headers or headers:
                    hdrs = dict(self._default_headers)
                    if headers:
                        hdrs.update(dict(headers))

                resp = self._session.request(
                    m,
                    url,
                    params=dict(params) if params else None,
                    json=json_body,
                    data=dict(data) if data else None,
                    headers=hdrs,
                    timeout=timeout,
                    verify=verify,
                )

                status = int(resp.status_code)

                if expected and status not in expected:
                    # still retry for retryable statuses
                    if status in self._retry_status_codes and attempt <= max_retries:
                        self._log_retry(m, url, attempt, f"status={status}")
                        self._sleep_backoff(attempt)
                        continue
                    raise HttpResponseError(
                        method=m,
                        url=safe_url,
                        status_code=status,
                        body_snippet=str(redact_text((resp.text or "")[:300])),
                    )

                # If no explicit expected statuses: accept any 2xx.
                if not expected and not (200 <= status < 300):
                    if status in self._retry_status_codes and attempt <= max_retries:
                        self._log_retry(m, url, attempt, f"status={status}")
                        self._sleep_backoff(attempt)
                        continue
                    raise HttpResponseError(
                        method=m,
                        url=safe_url,
                        status_code=status,
                        body_snippet=str(redact_text((resp.text or "")[:300])),
                    )

                try:
                    return resp.json()
                except Exception as e:
                    # JSON decode errors are not retryable by default.
                    raise HttpResponseError(
                        method=m,
                        url=safe_url,
                        status_code=status,
                        body_snippet=str(redact_text((resp.text or "")[:300])),
                    ) from e

            except requests.RequestException as e:
                last_exc = e
                if attempt <= max_retries:
                    self._log_retry(m, safe_url, attempt, f"error={type(e).__name__}")
                    self._sleep_backoff(attempt)
                    continue
                raise HttpRequestError(method=m, url=safe_url, error=str(redact_text(str(e)))) from e

        # should never happen
        raise HttpRequestError(
            method=m,
            url=safe_url,
            error=str(redact_text(str(last_exc) if last_exc else "unknown")),
        )

    def get_json(
        self,
        path_or_url: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
        headers: Optional[Mapping[str, str]] = None,
        timeout_seconds: Optional[int] = None,
        verify_ssl: Optional[bool] = None,
        expected_status_codes: Optional[Sequence[int]] = None,
        retries: Optional[int] = None,
    ) -> Any:
        return self.request_json(
            "GET",
            path_or_url,
            params=params,
            headers=headers,
            timeout_seconds=timeout_seconds,
            verify_ssl=verify_ssl,
            expected_status_codes=expected_status_codes,
            retries=retries,
        )

    def post_json(
        self,
        path_or_url: str,
        *,
        body: Optional[Any] = None,
        params: Optional[Mapping[str, Any]] = None,
        headers: Optional[Mapping[str, str]] = None,
        timeout_seconds: Optional[int] = None,
        verify_ssl: Optional[bool] = None,
        expected_status_codes: Optional[Sequence[int]] = None,
        retries: Optional[int] = None,
    ) -> Any:
        return self.request_json(
            "POST",
            path_or_url,
            params=params,
            json_body=body,
            headers=headers,
            timeout_seconds=timeout_seconds,
            verify_ssl=verify_ssl,
            expected_status_codes=expected_status_codes,
            retries=retries,
        )

    def post_form_json(
        self,
        path_or_url: str,
        *,
        form: Mapping[str, Any],
        headers: Optional[Mapping[str, str]] = None,
        timeout_seconds: Optional[int] = None,
        verify_ssl: Optional[bool] = None,
        expected_status_codes: Optional[Sequence[int]] = None,
        retries: Optional[int] = None,
    ) -> Any:
        hdrs: Dict[str, str] = {"Content-Type": "application/x-www-form-urlencoded"}
        if headers:
            hdrs.update(dict(headers))

        return self.request_json(
            "POST",
            path_or_url,
            data=form,
            headers=hdrs,
            timeout_seconds=timeout_seconds,
            verify_ssl=verify_ssl,
            expected_status_codes=expected_status_codes,
            retries=retries,
        )

    def _log_retry(self, method: str, url: str, attempt: int, reason: str) -> None:
        if not self._logger:
            return
        try:
            request_id = (
                self._default_headers.get("X-Request-ID")
                or self._default_headers.get("X-Correlation-ID")
                or ""
            )
            self._logger.warning(
                "HTTP retry: %s %s attempt=%s reason=%s request_id=%s",
                method,
                str(redact_text(url)),
                attempt,
                reason,
                request_id,
            )
        except Exception:
            pass
