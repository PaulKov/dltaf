from __future__ import annotations

import re
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from .policy import (
    _BASIC_RE,
    _BEARER_RE,
    _JSON_KV_RE,
    _KV_RE,
    _SENSITIVE_QUERY_KEYS,
    _URL_USERINFO_RE,
    REDACTED,
    is_sensitive_key,
)


def redact_text(text):
    if not isinstance(text, str):
        return text
    s = text
    try:
        s = _BEARER_RE.sub(lambda m: f"Bearer {REDACTED}", s)
        s = _BASIC_RE.sub(lambda m: f"Basic {REDACTED}", s)
        s = _JSON_KV_RE.sub(lambda m: f"{m.group(1)}\"{REDACTED}\"", s)
        s = _KV_RE.sub(lambda m: f"{m.group(1)}={REDACTED}", s)
        return _redact_urls_in_text(s)
    except Exception:
        return text


def _redact_urls_in_text(text: str) -> str:
    out = _URL_USERINFO_RE.sub(lambda m: f"{m.group(1)}{REDACTED}@", text)
    url_candidates = re.findall(r"https?://[^\s\)\]\}\>\"']+", out)
    for url in set(url_candidates):
        redacted = _redact_url(url)
        if redacted != url:
            out = out.replace(url, redacted)
    return out


def _redact_url(url: str) -> str:
    try:
        parts = urlsplit(url)
        netloc = parts.netloc
        if "@" in netloc:
            hostport = netloc.split("@", 1)[1]
            netloc = f"{REDACTED}@{hostport}"
        query = parts.query
        if parts.query:
            q = []
            for k, v in parse_qsl(parts.query, keep_blank_values=True):
                if is_sensitive_key(k) or str(k).lower() in _SENSITIVE_QUERY_KEYS:
                    q.append((k, REDACTED))
                else:
                    q.append((k, v))
            query = urlencode(q, doseq=True)
        return urlunsplit((parts.scheme, netloc, parts.path, query, parts.fragment))
    except Exception:
        return url
