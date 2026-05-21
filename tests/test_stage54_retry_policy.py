from __future__ import annotations

from dataclasses import dataclass

from dltaf import RetryPolicy, run_with_retry, status_in


@dataclass(frozen=True)
class OperationResult:
    status: str


def test_retry_policy_delays_are_capped_and_attempts_include_first_call() -> None:
    policy = RetryPolicy(
        attempts=5,
        initial_delay_seconds=30,
        backoff_multiplier=2,
        max_delay_seconds=60,
    )

    assert policy.normalized().attempts == 5
    assert policy.delays() == (30.0, 60.0, 60.0, 60.0)


def test_run_with_retry_retries_timeout_results_until_success() -> None:
    results = [
        OperationResult("timeout"),
        OperationResult("timeout"),
        OperationResult("approved"),
    ]
    sleeps: list[float] = []
    attempts: list[int] = []

    outcome = run_with_retry(
        lambda attempt: attempts.append(attempt) or results[attempt - 1],
        policy=RetryPolicy(attempts=3, initial_delay_seconds=5, backoff_multiplier=2),
        should_retry=status_in("timeout"),
        sleep=sleeps.append,
    )

    assert outcome.result == OperationResult("approved")
    assert outcome.attempts == 3
    assert outcome.retry_count == 2
    assert attempts == [1, 2, 3]
    assert sleeps == [5.0, 10.0]


def test_run_with_retry_does_not_retry_non_retryable_results() -> None:
    sleeps: list[float] = []

    outcome = run_with_retry(
        lambda attempt: OperationResult("connect_error"),
        policy=RetryPolicy(attempts=3, initial_delay_seconds=5),
        should_retry=status_in("timeout"),
        sleep=sleeps.append,
    )

    assert outcome.result == OperationResult("connect_error")
    assert outcome.attempts == 1
    assert outcome.retry_count == 0
    assert sleeps == []


def test_run_with_retry_returns_last_retryable_result_when_budget_is_exhausted() -> None:
    outcome = run_with_retry(
        lambda attempt: OperationResult("timeout"),
        policy=RetryPolicy(attempts=3, initial_delay_seconds=0),
        should_retry=status_in("timeout"),
        sleep=lambda delay: None,
    )

    assert outcome.result == OperationResult("timeout")
    assert outcome.attempts == 3
    assert outcome.retry_count == 2
