from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Callable, Optional, Sequence

from dlt_utils.adapters.kafka.types import KafkaMessage
from dlt_utils.adapters.kafka.base import KafkaWaiter
from dlt_utils.contracts import PayloadContract
from dltaf.services.execution.redaction import safe_exception_message


@dataclass(frozen=True)
class KafkaAsyncJobSettings:
    """Settings for Kafka-based async jobs."""

    topic: str
    timeout_seconds: int
    poll_interval_seconds: float
    start_from: str = "end"  # end|beginning


@dataclass(frozen=True)
class KafkaAsyncJobResult:
    """Result of a Kafka async job."""

    correlation_id: str
    message: KafkaMessage
    payload: Any
    raw_text: str


@dataclass(frozen=True)
class KafkaJsonAsyncJob:
    """Declarative description of a Kafka JSON async job.

    This wrapper reduces duplication across integrations by encapsulating:
    - trigger function
    - correlation_id field name (for logs)
    - Kafka settings
    """

    name: str
    settings: KafkaAsyncJobSettings
    correlation_id_name: str
    trigger: Callable[[], Optional[str]]
    payload_contract: Optional[PayloadContract] = None


class KafkaAsyncJobError(RuntimeError):
    pass


class KafkaAsyncJobTriggerError(KafkaAsyncJobError):
    pass


class KafkaAsyncJobTimeoutError(KafkaAsyncJobError):
    pass


class KafkaAsyncJobDecodeError(KafkaAsyncJobError):
    pass


class KafkaJsonAsyncJobRunner:
    """Template Method for: prepare -> trigger -> wait Kafka by key -> decode JSON.

    This runner is designed for request/response via Kafka patterns.

    Usage:
        runner = KafkaJsonAsyncJobRunner(waiter, logger)
        res = runner.run_safe(settings, trigger, correlation_id_name="messageId")

    Or with a declarative job description:
        job = KafkaJsonAsyncJob(...)
        res = runner.run_job_safe(job)

    The runner returns None on recoverable errors.

    Note:
        If a payload contract is provided with mode=deny, the runner raises
        :class:`~dlt_utils.contracts.payload_contract.PayloadContractViolation`.
    """

    def __init__(self, *, waiter: KafkaWaiter, logger: Optional[logging.Logger] = None) -> None:
        self._waiter = waiter
        self._logger = logger or logging.getLogger(__name__)

    def run_job_safe(self, job: KafkaJsonAsyncJob) -> Optional[KafkaAsyncJobResult]:
        return self.run_safe(
            settings=job.settings,
            trigger=job.trigger,
            correlation_id_name=job.correlation_id_name,
            job_name=job.name,
            payload_contract=job.payload_contract,
        )

    def run_safe(
        self,
        *,
        settings: KafkaAsyncJobSettings,
        trigger: Callable[[], Optional[str]],
        correlation_id_name: str,
        payload_contract: Optional[PayloadContract] = None,
        job_name: str = "kafka_async_job",
    ) -> Optional[KafkaAsyncJobResult]:
        """Execute Kafka async job.

        Returns KafkaAsyncJobResult or None on error/timeout.
        """

        topic = str(settings.topic).strip()
        if not topic:
            self._logger.error("%s: topic is empty", job_name)
            return None

        # Prepare consumer BEFORE trigger to avoid missing very fast messages.
        try:
            self._waiter.prepare(topic, start_from=settings.start_from)
        except Exception as e:
            self._logger.error(
                "%s: failed to prepare Kafka consumer: %s",
                job_name,
                safe_exception_message(e),
            )
            # Still proceed: waiter may choose to prepare lazily.

        correlation_id: Optional[str]
        try:
            correlation_id = trigger()
        except Exception as e:
            self._logger.error("%s: trigger failed: %s", job_name, safe_exception_message(e))
            return None

        if not correlation_id:
            self._logger.error("%s: trigger returned empty correlation id (%s)", job_name, correlation_id_name)
            return None

        correlation_id = str(correlation_id).strip()
        if not correlation_id:
            self._logger.error("%s: trigger returned blank correlation id (%s)", job_name, correlation_id_name)
            return None

        self._logger.info(
            "%s: waiting Kafka message, %s=%s, topic=%s, timeout=%ss",
            job_name,
            correlation_id_name,
            correlation_id,
            topic,
            int(settings.timeout_seconds),
        )

        msg = self._waiter.wait_for_key(
            topic,
            correlation_id,
            timeout_s=int(settings.timeout_seconds),
            poll_interval_s=float(settings.poll_interval_seconds),
        )

        if msg is None:
            self._logger.error(
                "%s: Kafka timeout, %s=%s topic=%s",
                job_name,
                correlation_id_name,
                correlation_id,
                topic,
            )
            return None

        raw_text: str
        try:
            raw_text = msg.value.decode("utf-8", errors="replace")
        except Exception:
            raw_text = ""

        try:
            payload = json.loads(raw_text) if raw_text else None
        except Exception as e:
            self._logger.error(
                "%s: failed to decode Kafka JSON, %s=%s: %s",
                job_name,
                correlation_id_name,
                correlation_id,
                safe_exception_message(e),
            )
            return None

        # Stage 27: validate payload contract (schema drift protection).
        if payload_contract is not None:
            payload_contract.validate(
                payload,
                logger=self._logger,
                context=f"job={job_name} topic={topic} {correlation_id_name}={correlation_id}",
            )

        return KafkaAsyncJobResult(
            correlation_id=correlation_id,
            message=msg,
            payload=payload,
            raw_text=raw_text,
        )

    def wait_for_any_safe(
        self,
        *,
        settings: KafkaAsyncJobSettings,
        keys: Sequence[str],
        correlation_id_name: str,
        payload_contract: Optional[PayloadContract] = None,
        job_name: str = "kafka_async_job",
        timeout_seconds: Optional[int] = None,
    ) -> Optional[KafkaAsyncJobResult]:
        """Wait for the first Kafka message for *any* correlation id in `keys`.

        This method is used for batch workflows where multiple async requests are
        in-flight at the same time (controlled parallelism).

        Notes:
            - Returns None only on timeout or missing waiter support.
            - If JSON decoding fails, returns a result with payload=None.
            - If a payload contract is provided with mode=deny, a contract
              violation is raised (same semantics as `run_safe`).
        """

        topic = str(settings.topic).strip()
        if not topic:
            self._logger.error("%s: topic is empty", job_name)
            return None

        key_list = [str(k).strip() for k in (keys or []) if str(k).strip()]
        if not key_list:
            self._logger.error("%s: keys list is empty", job_name)
            return None

        eff_timeout = int(timeout_seconds if timeout_seconds is not None else settings.timeout_seconds)

        waiter_any = getattr(self._waiter, "wait_for_any", None)
        if not callable(waiter_any):
            # Fallback: wait for the first key only.
            self._logger.warning(
                "%s: waiter has no wait_for_any(); falling back to wait_for_key for the first key",
                job_name,
            )
            msg = self._waiter.wait_for_key(
                topic,
                key_list[0],
                timeout_s=eff_timeout,
                poll_interval_s=float(settings.poll_interval_seconds),
            )
        else:
            msg = waiter_any(
                topic,
                key_list,
                timeout_s=eff_timeout,
                poll_interval_s=float(settings.poll_interval_seconds),
            )

        if msg is None:
            return None

        correlation_id = str(msg.key or "").strip()
        if not correlation_id:
            # Should not happen, but keep it safe.
            correlation_id = "<empty>"

        raw_text: str
        try:
            raw_text = msg.value.decode("utf-8", errors="replace")
        except Exception:
            raw_text = ""

        payload: Any
        try:
            payload = json.loads(raw_text) if raw_text else None
        except Exception as e:
            self._logger.error(
                "%s: failed to decode Kafka JSON, %s=%s: %s",
                job_name,
                correlation_id_name,
                correlation_id,
                safe_exception_message(e),
            )
            payload = None

        if payload_contract is not None and payload is not None:
            payload_contract.validate(
                payload,
                logger=self._logger,
                context=f"job={job_name} topic={topic} {correlation_id_name}={correlation_id}",
            )

        return KafkaAsyncJobResult(
            correlation_id=correlation_id,
            message=msg,
            payload=payload,
            raw_text=raw_text,
        )
