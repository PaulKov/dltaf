from __future__ import annotations

from typing import Any, Dict, Mapping, Optional

from .policy import RedactionOptions, REDACTED, is_sensitive_key
from .text import redact_text


def redact_obj(obj: Any, *, options: Optional[RedactionOptions] = None, _depth: int = 0) -> Any:
    opts = options or RedactionOptions()
    try:
        if _depth > int(opts.max_depth):
            return "__truncated_depth__"
        if isinstance(obj, Mapping):
            out: Dict[str, Any] = {}
            for k, v in obj.items():
                kk = str(k)
                if bool(opts.redact_keys) and is_sensitive_key(kk):
                    out[kk] = REDACTED
                else:
                    out[kk] = redact_obj(v, options=opts, _depth=_depth + 1)
            return out
        if isinstance(obj, (list, tuple)):
            seq = list(obj)
            if len(seq) > int(opts.max_list):
                head = [redact_obj(v, options=opts, _depth=_depth + 1) for v in seq[: int(opts.max_list)]]
                head.append(f"__truncated_list__({len(seq)}->{int(opts.max_list)})")
                return head
            return [redact_obj(v, options=opts, _depth=_depth + 1) for v in seq]
        if isinstance(obj, str):
            s = str(redact_text(obj) if bool(opts.redact_text) else obj)
            if len(s) > int(opts.max_str):
                return s[: int(opts.max_str)] + "…"
            return s
        if obj is None or isinstance(obj, (int, float, bool)):
            return obj
        s = redact_text(repr(obj)) if bool(opts.redact_text) else repr(obj)
        if isinstance(s, str) and len(s) > int(opts.max_str):
            return s[: int(opts.max_str)] + "…"
        return s
    except Exception:
        try:
            return redact_text(str(obj)) if bool(opts.redact_text) else str(obj)
        except Exception:
            return "<unredactable>"
