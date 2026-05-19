from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from dltaf.app.runtime import RunContext


class _DltProgressNoiseFilter(logging.Filter):
    _PATTERNS: Sequence[re.Pattern[str]] = (
        re.compile(r"^-{5,}\s*(Extract|Normalize|Load)\b", re.IGNORECASE),
        re.compile(r"^(Resources|Files|Jobs):\s", re.IGNORECASE),
        re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_:-]*:\s+\d+\s+\|\s+Time:", re.IGNORECASE),
    )

    def filter(self, record: logging.LogRecord) -> bool:
        if not str(record.name or "").startswith("dlt"):
            return True
        if record.levelno >= logging.WARNING:
            return True
        try:
            message = record.getMessage()
        except Exception:
            return True
        if "BIN=" in message or "requestId=" in message:
            return True
        return not any(pattern.search(message) for pattern in self._PATTERNS)


@dataclass
class DltProgressControlHook:
    """Reduce noisy built-in dlt progress logs when manifest requests summary_only."""

    name: str = "dlt_progress_control"
    _filter: _DltProgressNoiseFilter = field(default_factory=_DltProgressNoiseFilter)
    _handlers: list[logging.Handler] = field(default_factory=list)

    def pre_run(self, manifest: Mapping[str, Any], ctx: RunContext) -> None:
        if str(ctx.options.dlt_progress or "default").strip().lower() != "summary_only":
            return
        seen: set[int] = set()
        for logger_name in ("", "dlt"):
            current = logging.getLogger(logger_name)
            for handler in current.handlers:
                if id(handler) in seen:
                    continue
                handler.addFilter(self._filter)
                self._handlers.append(handler)
                seen.add(id(handler))
        ctx.logger.info("DLT progress noise filter enabled (mode=summary_only)")

    def post_run(self, manifest: Mapping[str, Any], ctx: RunContext, result: Any) -> None:
        self._cleanup()

    def on_error(self, manifest: Mapping[str, Any], ctx: RunContext, exc: Exception) -> None:
        self._cleanup()

    def _cleanup(self) -> None:
        while self._handlers:
            handler = self._handlers.pop()
            try:
                handler.removeFilter(self._filter)
            except Exception:
                continue


__all__ = ["DltProgressControlHook"]
