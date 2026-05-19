from __future__ import annotations

import json
from typing import Any

from .objects import redact_obj
from .text import redact_text


def safe_str(obj: Any, *, limit: int = 2000) -> str:
    s: str
    try:
        if isinstance(obj, (dict, list, tuple)):
            s = json.dumps(redact_obj(obj), ensure_ascii=False, sort_keys=True)
        else:
            s = repr(obj)
            s = redact_text(s)
    except Exception:
        try:
            s = redact_text(str(obj))
        except Exception:
            s = "<unstringifiable>"
    return s[:limit] + ("…" if len(s) > limit else "")


def safe_json(obj: Any, *, limit: int = 20000) -> str:
    s: str
    try:
        s = json.dumps(redact_obj(obj), ensure_ascii=False, sort_keys=True)
    except Exception:
        s = safe_str(obj, limit=limit)
    return s[:limit] + ("…" if len(s) > limit else "")


def safe_exception_message(exc: BaseException, *, limit: int = 2000) -> str:
    s: str
    try:
        s = str(exc)
    except Exception:
        s = repr(exc)
    s = redact_text(s)
    return s[:limit] + ("…" if len(s) > limit else "")
