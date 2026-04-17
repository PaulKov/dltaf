"""Reusable patterns for dlt-based integrations.

This package contains small, well-tested building blocks that reduce
copy/paste across integrations.

Public API is exported via this module to keep imports stable.
"""

from dlt_utils.patterns.kafka_async_job import (
    KafkaAsyncJobSettings,
    KafkaAsyncJobResult,
    KafkaJsonAsyncJob,
    KafkaJsonAsyncJobRunner,
)
from dlt_utils.patterns.polling import poll_until

__all__ = [
    "KafkaAsyncJobSettings",
    "KafkaAsyncJobResult",
    "KafkaJsonAsyncJob",
    "KafkaJsonAsyncJobRunner",
    "poll_until",
]
