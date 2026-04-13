"""
RetryPolicy — composable retry configuration for enterprise integration.

Separates retry configuration from the caller, making policies testable,
reusable, and injectable without subclassing.
"""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


class RetryExhausted(Exception):
    """Raised when all retry attempts are exhausted."""

    def __init__(self, attempts: int, last_error: BaseException) -> None:
        self.attempts = attempts
        self.last_error = last_error
        super().__init__(f"Exhausted {attempts} retry attempts: {last_error}")


@dataclass
class RetryPolicy:
    """
    Configurable retry policy with exponential backoff and full jitter.

    Usage::

        policy = RetryPolicy(max_attempts=3, base_delay=0.1, max_delay=10.0)
        result = policy.execute(my_function, arg1, arg2)
    """

    max_attempts: int = 3
    base_delay: float = 1.0  # seconds
    max_delay: float = 60.0  # seconds
    multiplier: float = 2.0
    jitter: bool = True
    retryable_exceptions: tuple[type[BaseException], ...] = (Exception,)

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.base_delay < 0:
            raise ValueError("base_delay must be >= 0")
        if self.max_delay < self.base_delay:
            raise ValueError("max_delay must be >= base_delay")
        if self.multiplier < 1.0:
            raise ValueError("multiplier must be >= 1.0")

    def delay_for(self, attempt: int) -> float:
        """Calculate sleep duration for a given attempt number (0-indexed)."""
        delay = min(self.base_delay * (self.multiplier**attempt), self.max_delay)
        if self.jitter:
            delay = random.uniform(0, delay)
        return delay

    def execute(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """
        Execute fn(*args, **kwargs) with retry on retryable_exceptions.
        Raises RetryExhausted if all attempts fail.
        """
        last_error: BaseException = RuntimeError("no attempts made")
        for attempt in range(self.max_attempts):
            try:
                return fn(*args, **kwargs)
            except self.retryable_exceptions as exc:
                last_error = exc
                if attempt < self.max_attempts - 1:
                    sleep_time = self.delay_for(attempt)
                    logger.debug(
                        "Attempt %d/%d failed (%s); retrying in %.3fs",
                        attempt + 1,
                        self.max_attempts,
                        exc,
                        sleep_time,
                    )
                    time.sleep(sleep_time)
        raise RetryExhausted(self.max_attempts, last_error)

    @classmethod
    def no_retry(cls) -> RetryPolicy:
        """Single-attempt policy — fails immediately on first error."""
        return cls(max_attempts=1, base_delay=0.0)

    @classmethod
    def aggressive(cls) -> RetryPolicy:
        """5 attempts, starting at 0.5s, doubling, capped at 30s."""
        return cls(max_attempts=5, base_delay=0.5, max_delay=30.0)
