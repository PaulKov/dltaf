from __future__ import annotations

import re


def normalize_slug(value: str) -> str:
    s = str(value or "").strip()
    if not s:
        return ""
    s = s.replace("-", "_")
    s = re.sub(r"[^0-9a-zA-Z_]+", "_", s)
    s = s.strip("_")
    if s and s[0].isdigit():
        s = f"p_{s}"
    return s.lower()


__all__ = ["normalize_slug"]
