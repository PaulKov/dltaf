from __future__ import annotations

import re
from dataclasses import dataclass

REDACTED = "***redacted***"

_SENSITIVE_KEY_RE = re.compile(
    r"(pass(word)?|passwd|pwd|secret|token|bearer|authorization|cookie|session|api[_-]?key|apikey|access[_-]?token|refresh[_-]?token|client[_-]?secret|private[_-]?key|ssl[_-]?key|cert(ificate)?|sasl|vault[_-]?token)",
    re.IGNORECASE,
)
_SENSITIVE_QUERY_KEYS = {
    "access_token", "refresh_token", "token", "id_token", "client_secret", "password", "passwd", "apikey", "api_key", "signature", "sig",
}
_BEARER_RE = re.compile(r"(?i)\bBearer\s+([A-Za-z0-9\-._~+/]+=*)")
_BASIC_RE = re.compile(r"(?i)\bBasic\s+([A-Za-z0-9+/=]+)")
_JSON_KV_RE = re.compile(r'(?i)("(?:access_token|refresh_token|token|id_token|client_secret|password|api_key|apikey|authorization)"\s*:\s*)"(.*?)"')
_KV_RE = re.compile(r"(?i)\b(access_token|refresh_token|token|id_token|client_secret|password|passwd|api_key|apikey)\b\s*=\s*([^\s&;]+)")
_URL_USERINFO_RE = re.compile(r"(?i)(https?://)([^\s/@:]+)(:([^\s/@]+))?@")


@dataclass(frozen=True)
class RedactionOptions:
    max_depth: int = 8
    max_list: int = 100
    max_str: int = 1024
    redact_keys: bool = True
    redact_text: bool = True


def is_sensitive_key(key: str) -> bool:
    try:
        return bool(_SENSITIVE_KEY_RE.search(str(key)))
    except Exception:
        return False
