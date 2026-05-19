from __future__ import annotations

from typing import Protocol, Optional

from dlt_utils.adapters.kafka.types import KafkaMessage


class KafkaWaiter(Protocol):
    """Wait for a Kafka message by key.

    Notes:
        - The implementation must NOT auto-commit offsets.
        - The recommended usage is:

            waiter.prepare(topic, start_from="end")
            correlation_id = trigger()
            msg = waiter.wait_for_key(topic, correlation_id, ...)

        so we don't miss messages published quickly after the trigger.
    """

    def prepare(self, topic: str, *, start_from: str = "end") -> None:
        """Prepare consumer for waiting.

        Typically:
        - ensure metadata is loaded
        - assign partitions
        - seek to end/beginning

        Args:
            topic: topic name
            start_from: "end" (default) or "beginning"
        """

    def wait_for_key(
        self,
        topic: str,
        key: str,
        *,
        timeout_s: int,
        poll_interval_s: float,
    ) -> Optional[KafkaMessage]:
        """Return the first message with matching key or None on timeout."""

    def close(self) -> None:
        """Close underlying consumer (best-effort)."""
