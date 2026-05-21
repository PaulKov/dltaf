"""Generic retry/backoff primitives for unit-level runtime integrations.

The framework owns retry mechanics, while private integrations decide which
business outcomes are retryable. This keeps API/Kafka connectors explicit:
timeouts can be retried, but connect/auth/decode failures can remain immediate
unit failures when that is safer.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable, Generic, Optional, Protocol, TypeVar


ResultT = TypeVar("ResultT")


class SleepFunc(Protocol):
    """Small protocol for injecting sleep in tests and long-running workers."""

    def __call__(self, delay_seconds: float) -> None:
        """Sleep for the provided delay."""


@dataclass(frozen=True)
class RetryPolicy:
    """Capped exponential retry policy.

    `attempts` is the total call budget and includes the first operation call.
    For example, `attempts=3` means one initial call and at most two retries.
    """

    attempts: int = 1
    initial_delay_seconds: float = 0.0
    backoff_multiplier: float = 2.0
    max_delay_seconds: float = 60.0

    def normalized(self) -> "RetryPolicy":
        """Return a safe policy with bounded, non-negative values."""

        attempts = max(1, int(self.attempts or 1))
        initial_delay = max(0.0, float(self.initial_delay_seconds or 0.0))
        multiplier = max(0.0, float(self.backoff_multiplier or 0.0))
        max_delay = max(0.0, float(self.max_delay_seconds or 0.0))
        return RetryPolicy(
            attempts=attempts,
            initial_delay_seconds=initial_delay,
            backoff_multiplier=multiplier,
            max_delay_seconds=max_delay,
        )

    def delays(self) -> tuple[float, ...]:
        """Return delays before retry calls.

        The number of delays is `attempts - 1`. A zero delay is valid and means
        "retry immediately"; callers may still inject a no-op sleeper in tests.
        """

        policy = self.normalized()
        if policy.attempts <= 1:
            return ()

        delay = min(policy.initial_delay_seconds, policy.max_delay_seconds)
        values: list[float] = []
        for _ in range(policy.attempts - 1):
            values.append(delay)
            if policy.backoff_multiplier <= 0:
                delay = min(delay, policy.max_delay_seconds)
            else:
                delay = min(delay * policy.backoff_multiplier, policy.max_delay_seconds)
        return tuple(values)

    @property
    def max_wait_seconds(self) -> float:
        """Maximum wall-clock delay added by the retry policy itself."""

        return sum(self.delays())


@dataclass(frozen=True)
class RetryOutcome(Generic[ResultT]):
    """Result of executing an operation under a retry policy."""

    result: ResultT
    attempts: int
    retry_count: int


def status_in(*retryable_statuses: str) -> Callable[[object], bool]:
    """Build a predicate that retries objects whose `.status` is in the set."""

    normalized = {status.strip().lower() for status in retryable_statuses if status.strip()}

    def predicate(result: object) -> bool:
        return str(getattr(result, "status", "")).strip().lower() in normalized

    return predicate


def run_with_retry(
    operation: Callable[[int], ResultT],
    *,
    policy: RetryPolicy,
    should_retry: Callable[[ResultT], bool],
    sleep: Optional[SleepFunc] = None,
    on_retry: Optional[Callable[[int, int, float, ResultT], None]] = None,
) -> RetryOutcome[ResultT]:
    """Run `operation(attempt)` until it succeeds or retry budget is exhausted.

    Args:
        operation: Callable receiving 1-based attempt number.
        policy: Total-attempt retry policy.
        should_retry: Predicate for retryable results.
        sleep: Injectable sleeper; defaults to `time.sleep`.
        on_retry: Optional callback `(next_attempt, total_attempts, delay, result)`.
    """

    normalized_policy = policy.normalized()
    delays = normalized_policy.delays()
    sleeper = sleep or time.sleep
    last_result: ResultT | None = None

    for attempt in range(1, normalized_policy.attempts + 1):
        result = operation(attempt)
        last_result = result
        if not should_retry(result):
            return RetryOutcome(result=result, attempts=attempt, retry_count=attempt - 1)
        if attempt >= normalized_policy.attempts:
            return RetryOutcome(result=result, attempts=attempt, retry_count=attempt - 1)

        delay = delays[attempt - 1] if attempt - 1 < len(delays) else 0.0
        if on_retry is not None:
            on_retry(attempt + 1, normalized_policy.attempts, delay, result)
        if delay > 0:
            sleeper(delay)

    # Defensive fallback; the loop always returns because attempts is normalized to >= 1.
    if last_result is None:  # pragma: no cover
        raise RuntimeError("retry operation did not run")
    return RetryOutcome(
        result=last_result,
        attempts=normalized_policy.attempts,
        retry_count=max(0, normalized_policy.attempts - 1),
    )


__all__ = [
    "RetryOutcome",
    "RetryPolicy",
    "run_with_retry",
    "status_in",
]
