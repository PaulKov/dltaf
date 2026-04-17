from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError, NoBrokersAvailable

from dlt_utils.adapters.kafka.base import KafkaWaiter
from dlt_utils.adapters.kafka.types import KafkaMessage
from dltaf.services.execution.redaction import safe_exception_message


@dataclass
class _PreparedTopic:
    topic: str
    partitions: List[TopicPartition]


class KafkaPythonWaiter(KafkaWaiter):
    """KafkaWaiter implementation using kafka-python.

    This class is intentionally small and focused:
    - create a single consumer instance
    - manually assign topic partitions
    - seek to end/beginning on demand
    - poll and match messages by key

    The implementation is optimized for the *request/response via Kafka* pattern,
    not for streaming consumption.
    """

    def __init__(
        self,
        *,
        bootstrap_servers: Sequence[str],
        consumer_kwargs: Optional[Dict[str, Any]] = None,
        logger: Optional[logging.Logger] = None,
        metadata_timeout_s: float = 10.0,
    ) -> None:
        self._bootstrap_servers = [str(x).strip() for x in bootstrap_servers if str(x).strip()]
        self._consumer_kwargs = dict(consumer_kwargs or {})
        self._logger = logger or logging.getLogger(__name__)
        self._metadata_timeout_s = float(metadata_timeout_s)

        self._consumer: Optional[KafkaConsumer] = None
        self._prepared: Optional[_PreparedTopic] = None

    def __enter__(self) -> "KafkaPythonWaiter":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        self.close()

    def _ensure_consumer(self) -> Optional[KafkaConsumer]:
        if self._consumer is not None:
            return self._consumer

        if not self._bootstrap_servers:
            self._logger.error("Kafka bootstrap_servers is empty")
            return None

        try:
            self._consumer = KafkaConsumer(
                bootstrap_servers=list(self._bootstrap_servers),
                enable_auto_commit=False,
                auto_offset_reset="latest",
                **self._consumer_kwargs,
            )
            return self._consumer
        except NoBrokersAvailable as e:
            self._logger.error(
                "NoBrokersAvailable when connecting to Kafka: %s",
                safe_exception_message(e),
            )
            return None
        except KafkaError as e:
            self._logger.error("KafkaError when connecting to Kafka: %s", safe_exception_message(e))
            return None
        except Exception as e:
            self._logger.error(
                "Unexpected error when connecting to Kafka: %s",
                safe_exception_message(e),
            )
            return None

    def _wait_partitions(self, consumer: KafkaConsumer, topic: str) -> Optional[List[int]]:
        deadline = time.time() + self._metadata_timeout_s
        partitions: Optional[List[int]] = None

        while time.time() < deadline:
            # Poll 0ms to update metadata
            try:
                consumer.poll(timeout_ms=0)
            except Exception:
                # ignore - metadata may still be loading
                pass

            parts = consumer.partitions_for_topic(topic)
            if parts:
                partitions = sorted(int(p) for p in parts)
                break

            time.sleep(0.2)

        if not partitions:
            self._logger.error(
                "Kafka topic=%s: failed to load partitions within %.1fs",
                topic,
                self._metadata_timeout_s,
            )
            return None

        return partitions

    def prepare(self, topic: str, *, start_from: str = "end") -> None:
        consumer = self._ensure_consumer()
        if consumer is None:
            return

        topic = str(topic).strip()
        if not topic:
            raise ValueError("topic is empty")

        parts = self._wait_partitions(consumer, topic)
        if not parts:
            return

        tps = [TopicPartition(topic, p) for p in parts]
        consumer.assign(tps)

        if start_from == "end":
            consumer.seek_to_end(*tps)
        elif start_from == "beginning":
            consumer.seek_to_beginning(*tps)
        else:
            raise ValueError(f"Unsupported start_from: {start_from!r}")

        self._prepared = _PreparedTopic(topic=topic, partitions=tps)

    @staticmethod
    def _decode_key(raw_key: Optional[bytes]) -> Optional[str]:
        if raw_key is None:
            return None
        if isinstance(raw_key, (bytes, bytearray)):
            return raw_key.decode("utf-8", errors="replace")
        return str(raw_key)

    def wait_for_key(
        self,
        topic: str,
        key: str,
        *,
        timeout_s: int,
        poll_interval_s: float,
    ) -> Optional[KafkaMessage]:
        consumer = self._ensure_consumer()
        if consumer is None:
            return None

        topic = str(topic).strip()
        key = str(key).strip()
        if not topic:
            raise ValueError("topic is empty")
        if not key:
            raise ValueError("key is empty")

        # Ensure we are prepared for this topic.
        if self._prepared is None or self._prepared.topic != topic:
            # Default to end: safest for request/response pattern.
            self.prepare(topic, start_from="end")

        end_time = time.time() + max(0, int(timeout_s))

        # Keep poll intervals reasonable.
        poll_ms = int(max(50.0, float(poll_interval_s) * 1000.0))

        while time.time() < end_time:
            msg_pack = consumer.poll(timeout_ms=poll_ms)
            for _, messages in msg_pack.items():
                for msg in messages:
                    msg_key = self._decode_key(msg.key)
                    if msg_key != key:
                        continue

                    ts_ms: Optional[int] = None
                    try:
                        # kafka-python message: timestamp is (type, milliseconds)
                        if msg.timestamp is not None:
                            ts_ms = int(msg.timestamp)
                    except Exception:
                        ts_ms = None

                    return KafkaMessage(
                        topic=topic,
                        partition=int(msg.partition),
                        offset=int(msg.offset),
                        key=msg_key or "",
                        value=bytes(msg.value) if msg.value is not None else b"",
                        timestamp_ms=ts_ms,
                    )

        self._logger.error(
            "Kafka timeout: topic=%s key=%s timeout_s=%s",
            topic,
            key,
            timeout_s,
        )
        return None

    def wait_for_any(
        self,
        topic: str,
        keys: Sequence[str],
        *,
        timeout_s: int,
        poll_interval_s: float,
    ) -> Optional[KafkaMessage]:
        """Return the first message with key in `keys` or None on timeout.

        This is a small extension used for batch workflows where multiple
        correlation ids are in-flight at the same time.
        """

        consumer = self._ensure_consumer()
        if consumer is None:
            return None

        topic = str(topic).strip()
        if not topic:
            raise ValueError("topic is empty")

        key_set = {str(k).strip() for k in (keys or []) if str(k).strip()}
        if not key_set:
            raise ValueError("keys is empty")

        # Ensure we are prepared for this topic.
        if self._prepared is None or self._prepared.topic != topic:
            self.prepare(topic, start_from="end")

        end_time = time.time() + max(0, int(timeout_s))
        poll_ms = int(max(50.0, float(poll_interval_s) * 1000.0))

        while time.time() < end_time:
            msg_pack = consumer.poll(timeout_ms=poll_ms)
            for _, messages in msg_pack.items():
                for msg in messages:
                    msg_key = self._decode_key(msg.key)
                    if not msg_key or msg_key not in key_set:
                        continue

                    ts_ms: Optional[int] = None
                    try:
                        if msg.timestamp is not None:
                            ts_ms = int(msg.timestamp)
                    except Exception:
                        ts_ms = None

                    return KafkaMessage(
                        topic=topic,
                        partition=int(msg.partition),
                        offset=int(msg.offset),
                        key=msg_key,
                        value=bytes(msg.value) if msg.value is not None else b"",
                        timestamp_ms=ts_ms,
                    )

        self._logger.error(
            "Kafka timeout: topic=%s keys=%s timeout_s=%s",
            topic,
            len(key_set),
            timeout_s,
        )
        return None

    def close(self) -> None:
        if self._consumer is None:
            return
        try:
            self._consumer.close()
        except Exception as e:
            self._logger.warning("Failed to close KafkaConsumer: %s", safe_exception_message(e))
        finally:
            self._consumer = None
            self._prepared = None
