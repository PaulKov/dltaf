"""Kafka adapters.

Stage 4 goal
------------
Provide a single, reusable implementation for the common pattern used in
custom dlt sources:

    trigger HTTP job -> wait Kafka message by key (correlation id)

Why:
- avoid copy-paste of KafkaConsumer setup/poll loop
- standardize error handling and logging
- fix a subtle reliability issue: missing very fast messages

Key idea
--------
Call `KafkaWaiter.prepare(topic, start_from="end")` *before* triggering the job.
The waiter will assign partitions and seek to the end so only new messages are
consumed.

Current implementation uses `kafka-python`.
"""

from __future__ import annotations

from dlt_utils.adapters.kafka.base import KafkaWaiter
from dlt_utils.adapters.kafka.types import KafkaMessage

# kafka-python is an optional dependency for environments that only need DB pulls.
# In production for Kafka-based async integrations it MUST be installed.
try:  # pragma: no cover
    from dlt_utils.adapters.kafka.kafka_python import KafkaPythonWaiter
except ModuleNotFoundError:  # pragma: no cover

    class KafkaPythonWaiter:  # type: ignore
        def __init__(self, *args, **kwargs):  # noqa: ANN001
            raise RuntimeError(
                "Kafka support requires dependency 'kafka-python'. "
                "Install project dependencies or add kafka-python to your environment."
            )

__all__ = [
    "KafkaWaiter",
    "KafkaPythonWaiter",
    "KafkaMessage",
]
