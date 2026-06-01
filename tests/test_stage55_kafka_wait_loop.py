from __future__ import annotations

import logging
from dataclasses import dataclass

from dltaf.services.kafka import KafkaWaitLoopConfig, wait_for_kafka_match


@dataclass(frozen=True)
class MatchResult:
    payload: str


class FakeClock:
    def __init__(self) -> None:
        self.now = 1_000.0

    def time(self) -> float:
        return self.now

    def advance_ms(self, timeout_ms: int) -> None:
        self.now += max(0.1, timeout_ms / 1000)


def test_wait_for_kafka_match_returns_first_match_and_logs_lifecycle(caplog) -> None:
    clock = FakeClock()
    attempts: list[int] = []

    def poll_once(timeout_ms: int) -> MatchResult | None:
        attempts.append(timeout_ms)
        clock.advance_ms(timeout_ms)
        if len(attempts) == 2:
            return MatchResult(payload="ok")
        return None

    caplog.set_level(logging.INFO, logger="test.kafka.wait")

    result = wait_for_kafka_match(
        poll_once=poll_once,
        config=KafkaWaitLoopConfig(
            workflow_label="PKB",
            correlation_label="requestId",
            timeout_seconds=10,
            poll_interval_seconds=1,
            progress_interval_seconds=1,
        ),
        unit_id="BIN=123",
        correlation_id="req-1",
        logger=logging.getLogger("test.kafka.wait"),
        clock=clock.time,
    )

    assert result == MatchResult(payload="ok")
    assert attempts == [1000, 1000]
    messages = [record.getMessage() for record in caplog.records]
    assert any("PKB Kafka wait started: unit=BIN=123 requestId=req-1 timeout=10s" in msg for msg in messages)
    assert any("PKB Kafka wait pending: unit=BIN=123 requestId=req-1" in msg for msg in messages)


def test_wait_for_kafka_match_returns_none_after_timeout_and_logs_warning(caplog) -> None:
    clock = FakeClock()

    def poll_once(timeout_ms: int) -> None:
        clock.advance_ms(timeout_ms)
        return None

    caplog.set_level(logging.INFO, logger="test.kafka.wait")

    result = wait_for_kafka_match(
        poll_once=poll_once,
        config=KafkaWaitLoopConfig(
            workflow_label="B057",
            correlation_label="messageId",
            timeout_seconds=3,
            poll_interval_seconds=1,
            progress_interval_seconds=1,
        ),
        unit_id="BIN=123|projectId=1",
        correlation_id="msg-1",
        logger=logging.getLogger("test.kafka.wait"),
        clock=clock.time,
    )

    assert result is None
    messages = [record.getMessage() for record in caplog.records]
    assert any("B057 Kafka wait timed out" in msg for msg in messages)
    assert any("messageId=msg-1" in msg for msg in messages)
