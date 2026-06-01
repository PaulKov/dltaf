from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Callable, Generic, Optional, TypeVar

T = TypeVar("T")


@dataclass(frozen=True)
class PollResult(Generic[T]):
    value: Optional[T]
    attempts_used: int
    success: bool


def poll_until(
    fn: Callable[[], Optional[T]],
    is_done: Callable[[Optional[T]], bool],
    *,
    attempts: int,
    sleep_seconds: float,
    logger: Optional[logging.Logger] = None,
    description: str = "poll_until",
    sleep_before_first: bool = True,
) -> PollResult[T]:
    """Poll `fn` until `is_done(value)` becomes True or attempts are exhausted.

    This is a small helper used across integrations for *eventual consistency*
    cases (e.g., external job completed, but the report becomes available slightly later).

    The function is intentionally deterministic:
    - fixed sleep interval
    - no jitter (can be added later if needed)

    Parameters
    ----------
    fn:
        Function that returns the current value (or None when unavailable).
    is_done:
        Predicate to determine completion.
    attempts:
        Max number of polling attempts.
    sleep_seconds:
        Sleep interval between attempts.
    sleep_before_first:
        If True, sleeps before the first call. Useful when you know the system
        needs some time after a triggering event.

    Returns
    -------
    PollResult
        Contains last value, attempts used, and success flag.
    """

    log = logger or logging.getLogger(__name__)

    attempts_int = max(0, int(attempts))
    sleep_s = max(0.0, float(sleep_seconds))

    last_value: Optional[T] = None

    for i in range(attempts_int):
        if sleep_before_first or i > 0:
            if sleep_s > 0:
                time.sleep(sleep_s)

        try:
            last_value = fn()
        except Exception as e:
            log.warning("%s: poll attempt %s/%s failed: %s", description, i + 1, attempts_int, e)
            last_value = None

        if is_done(last_value):
            return PollResult(value=last_value, attempts_used=i + 1, success=True)

    log.info("%s: polling exhausted (attempts=%s)", description, attempts_int)
    return PollResult(value=last_value, attempts_used=attempts_int, success=False)
