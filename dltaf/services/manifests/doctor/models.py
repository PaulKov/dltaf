from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple


@dataclass(frozen=True)
class Issue:
    level: str
    code: str
    message: str
    fix: Optional[Tuple[str, str, str]] = None


__all__ = ["Issue"]
