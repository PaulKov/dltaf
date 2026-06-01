"""Kafka wait-loop primitives shared by API-style integrations.

Private integrations keep business-specific message matching and decoding.
The framework owns the repetitive lifecycle concerns: timeout accounting,
poll cadence, progress heartbeats, and consistent start/timeout log lines.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Callable, Optional, Protocol, TypeVar


ResultT = TypeVar("ResultT")


class ClockFunc(Protocol):
    def __call__(self) -> float:
        """Return current monotonic-ish time in seconds."""


@dataclass(frozen=True)
class KafkaWaitLoopConfig:
    """Configuration for a bounded Kafka polling loop.

    `workflow_label` is intentionally human-facing because it appears in logs,
    for example `PKB` or `B057`. `correlation_label` names the id being waited
    on, for example `requestId` or `messageId`.
    """

    workflow_label: str
    correlation_label: str
    timeout_seconds: int
    poll_interval_seconds: float
    progress_interval_seconds: float = 0

    def normalized(self) -> "KafkaWaitLoopConfig":
        return KafkaWaitLoopConfig(
            workflow_label=str(self.workflow_label or "DLTAF").strip() or "DLTAF",
            correlation_label=str(self.correlation_label or "correlationId").strip()
            or "correlationId",
            timeout_seconds=max(0, int(self.timeout_seconds or 0)),
            poll_interval_seconds=max(0.05, float(self.poll_interval_seconds or 0.05)),
            progress_interval_seconds=max(0.0, float(self.progress_interval_seconds or 0.0)),
        )


def wait_for_kafka_match(
    *,
    poll_once: Callable[[int], Optional[ResultT]],
    config: KafkaWaitLoopConfig,
    unit_id: str,
    correlation_id: str,
    logger: logging.Logger | None = None,
    clock: ClockFunc = time.time,
) -> Optional[ResultT]:
    """Poll Kafka until `poll_once` returns a match or the timeout expires.

    Args:
        poll_once: Callable receiving `timeout_ms` for a single poll iteration.
        config: Wait-loop labels and timing settings.
        unit_id: Human-readable unit id for logs.
        correlation_id: Human-readable request/message id for logs.
        logger: Optional logger. Defaults to this module logger.
        clock: Injectable clock for deterministic tests.
    """

    cfg = config.normalized()
    active_logger = logger or logging.getLogger(__name__)
    unit_label = str(unit_id or "n/a")
    correlation_value = str(correlation_id or "n/a")
    start_time = clock()
    end_time = start_time + cfg.timeout_seconds
    last_progress_at = start_time

    active_logger.info(
        "%s Kafka wait started: unit=%s %s=%s timeout=%ss",
        cfg.workflow_label,
        unit_label,
        cfg.correlation_label,
        correlation_value,
        cfg.timeout_seconds,
    )

    while clock() < end_time:
        remaining_seconds = max(0.0, end_time - clock())
        timeout_ms = int(min(cfg.poll_interval_seconds, remaining_seconds) * 1000)
        timeout_ms = max(50, timeout_ms)
        result = poll_once(timeout_ms)
        if result is not None:
            return result

        now = clock()
        if cfg.progress_interval_seconds > 0 and now - last_progress_at >= cfg.progress_interval_seconds:
            elapsed = max(0, int(now - start_time))
            remaining = max(0, int(end_time - now))
            active_logger.info(
                "%s Kafka wait pending: unit=%s %s=%s elapsed=%ss remaining=%ss",
                cfg.workflow_label,
                unit_label,
                cfg.correlation_label,
                correlation_value,
                elapsed,
                remaining,
            )
            last_progress_at = now

    active_logger.warning(
        "%s Kafka wait timed out: unit=%s %s=%s timeout=%ss",
        cfg.workflow_label,
        unit_label,
        cfg.correlation_label,
        correlation_value,
        cfg.timeout_seconds,
    )
    return None


__all__ = ["KafkaWaitLoopConfig", "wait_for_kafka_match"]
