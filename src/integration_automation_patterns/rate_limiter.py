"""
rate_limiter.py — Token-bucket rate limiter for enterprise webhook and event processing.

Provides ``TokenBucketRateLimiter``, a thread-safe token-bucket implementation
suitable for controlling outbound webhook delivery rate, API call bursting,
and event-driven integration rate management.

Token bucket semantics:
  - Bucket holds ``capacity`` tokens.
  - Tokens refill at ``refill_rate`` tokens per second.
  - Each ``acquire()`` call consumes ``cost`` tokens.
  - If tokens are unavailable, the call either blocks (``blocking=True``) or
    raises ``RateLimitExceeded`` (``blocking=False``).

Usage::

    from integration_automation_patterns.rate_limiter import (
        TokenBucketRateLimiter,
        RateLimitExceeded,
    )

    # Allow bursts of up to 100 calls, refilling at 10 calls/second
    limiter = TokenBucketRateLimiter(capacity=100, refill_rate=10.0)

    # Non-blocking — raises if no tokens
    try:
        limiter.acquire()
        send_webhook(event)
    except RateLimitExceeded:
        queue_for_retry(event)

    # Blocking — waits until tokens are available (max_wait_seconds cap)
    limiter.acquire(blocking=True, max_wait_seconds=5.0)
    send_webhook(event)

    # Async variant
    await limiter.async_acquire()
"""

from __future__ import annotations

import asyncio
import time
from threading import Lock


class RateLimitExceeded(Exception):
    """
    Raised when a non-blocking ``TokenBucketRateLimiter.acquire()`` call
    cannot be satisfied because the bucket is empty.

    Attributes:
        tokens_available: Current token count when the limit was hit.
        cost: The requested token cost that could not be satisfied.
    """

    def __init__(self, tokens_available: float, cost: float) -> None:
        self.tokens_available = tokens_available
        self.cost = cost
        super().__init__(
            f"Rate limit exceeded: requested {cost} tokens but only "
            f"{tokens_available:.2f} available."
        )


class TokenBucketRateLimiter:
    """
    Thread-safe token-bucket rate limiter for enterprise integration patterns.

    The token bucket algorithm allows controlled bursting (up to ``capacity``
    tokens) while enforcing a long-term average rate (``refill_rate``
    tokens/second). This is the standard pattern for rate-limiting webhook
    delivery, API calls, and event publishing in enterprise systems.

    Args:
        capacity: Maximum number of tokens the bucket can hold (burst ceiling).
        refill_rate: Tokens added per second (long-term average throughput).
        initial_tokens: Starting token count. Defaults to ``capacity`` (full).

    Example — limit webhook delivery to 50/s with 200 burst::

        limiter = TokenBucketRateLimiter(capacity=200, refill_rate=50.0)
    """

    def __init__(
        self,
        capacity: float,
        refill_rate: float,
        initial_tokens: float | None = None,
    ) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        if refill_rate <= 0:
            raise ValueError("refill_rate must be positive")
        self._capacity = capacity
        self._refill_rate = refill_rate
        self._tokens = capacity if initial_tokens is None else min(initial_tokens, capacity)
        self._last_refill = time.monotonic()
        self._lock = Lock()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @property
    def capacity(self) -> float:
        """Maximum token capacity (burst ceiling)."""
        return self._capacity

    @property
    def refill_rate(self) -> float:
        """Token refill rate in tokens per second."""
        return self._refill_rate

    def acquire(
        self,
        cost: float = 1.0,
        blocking: bool = False,
        max_wait_seconds: float = 30.0,
    ) -> float:
        """
        Acquire ``cost`` tokens from the bucket.

        Args:
            cost: Number of tokens to consume. Must be > 0.
            blocking: If ``True``, block until tokens are available
                      (up to ``max_wait_seconds``). If ``False`` (default),
                      raise ``RateLimitExceeded`` immediately when empty.
            max_wait_seconds: Upper bound on blocking wait time.
                              Raises ``RateLimitExceeded`` if exceeded.

        Returns:
            The actual wait time in seconds (0.0 if tokens were immediately
            available).

        Raises:
            RateLimitExceeded: When ``blocking=False`` and tokens are
                               insufficient, or ``blocking=True`` and
                               ``max_wait_seconds`` is exceeded.
            ValueError: If ``cost <= 0`` or ``cost > capacity``.
        """
        if cost <= 0:
            raise ValueError("cost must be positive")
        if cost > self._capacity:
            raise ValueError(f"cost {cost} exceeds bucket capacity {self._capacity}")

        deadline = time.monotonic() + max_wait_seconds
        total_wait = 0.0

        while True:
            with self._lock:
                self._refill()
                if self._tokens >= cost:
                    self._tokens -= cost
                    return total_wait

                if not blocking:
                    raise RateLimitExceeded(tokens_available=self._tokens, cost=cost)

                # Calculate how long until enough tokens are available
                deficit = cost - self._tokens
                wait_needed = deficit / self._refill_rate

            remaining = deadline - time.monotonic()
            if wait_needed > remaining:
                raise RateLimitExceeded(tokens_available=self._tokens, cost=cost)

            sleep_time = min(wait_needed, remaining, 0.1)  # cap individual sleeps
            time.sleep(sleep_time)
            total_wait += sleep_time

    async def async_acquire(
        self,
        cost: float = 1.0,
        max_wait_seconds: float = 30.0,
    ) -> float:
        """
        Async variant of ``acquire()``. Always non-blocking on the event loop
        (uses ``asyncio.sleep`` instead of ``time.sleep``).

        Args:
            cost: Number of tokens to consume.
            max_wait_seconds: Maximum time to wait.

        Returns:
            Total wait time in seconds.

        Raises:
            RateLimitExceeded: If ``max_wait_seconds`` is exceeded.
        """
        if cost <= 0:
            raise ValueError("cost must be positive")

        deadline = time.monotonic() + max_wait_seconds
        total_wait = 0.0

        while True:
            with self._lock:
                self._refill()
                if self._tokens >= cost:
                    self._tokens -= cost
                    return total_wait

                deficit = cost - self._tokens
                wait_needed = deficit / self._refill_rate

            remaining = deadline - time.monotonic()
            if wait_needed > remaining:
                raise RateLimitExceeded(tokens_available=self._tokens, cost=cost)

            sleep_time = min(wait_needed, remaining, 0.05)
            await asyncio.sleep(sleep_time)
            total_wait += sleep_time

    def try_acquire(self, cost: float = 1.0) -> bool:
        """
        Non-blocking attempt to acquire tokens. Returns ``True`` if successful,
        ``False`` if the bucket is empty. Does not raise.

        Args:
            cost: Number of tokens to consume.

        Returns:
            ``True`` if tokens were acquired, ``False`` otherwise.
        """
        try:
            self.acquire(cost=cost, blocking=False)
            return True
        except RateLimitExceeded:
            return False

    def tokens_available(self) -> float:
        """
        Return current token count after applying any pending refill.
        Thread-safe read.
        """
        with self._lock:
            self._refill()
            return self._tokens

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _refill(self) -> None:
        """
        Add tokens based on time elapsed since the last refill.
        Must be called while holding ``self._lock``.
        """
        now = time.monotonic()
        elapsed = now - self._last_refill
        added = elapsed * self._refill_rate
        self._tokens = min(self._capacity, self._tokens + added)
        self._last_refill = now
