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
from collections import deque
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
        super().__init__(f"Rate limit exceeded: requested {cost} tokens but only {tokens_available:.2f} available.")


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

                # Snapshot inside the lock to avoid data race when raising outside it
                tokens_snapshot = self._tokens

                if not blocking:
                    raise RateLimitExceeded(tokens_available=tokens_snapshot, cost=cost)

                # Calculate how long until enough tokens are available
                deficit = cost - tokens_snapshot
                wait_needed = deficit / self._refill_rate

            remaining = deadline - time.monotonic()
            if wait_needed > remaining:
                raise RateLimitExceeded(tokens_available=tokens_snapshot, cost=cost)

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

                tokens_snapshot = self._tokens
                deficit = cost - tokens_snapshot
                wait_needed = deficit / self._refill_rate

            remaining = deadline - time.monotonic()
            if wait_needed > remaining:
                raise RateLimitExceeded(tokens_available=tokens_snapshot, cost=cost)

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


# ---------------------------------------------------------------------------
# SlidingWindowRateLimiter
# ---------------------------------------------------------------------------


class SlidingWindowRateLimiter:
    """
    Thread-safe sliding-window rate limiter for enterprise CRM API rate windows.

    Enterprise CRM and ERP APIs (Salesforce, HubSpot, Workday, ServiceNow)
    enforce **sliding-window** limits: a maximum number of requests within a
    rolling time window (e.g. 1,000,000 Salesforce API calls per 24-hour window).

    Unlike the token bucket (which models burst rate), the sliding window tracks
    the exact timestamps of recent requests and enforces a hard count limit
    within the rolling window.

    Args:
        limit: Maximum number of requests allowed in the window.
        window_seconds: Size of the rolling window in seconds.
                        Example: ``86400`` for a 24-hour window.

    Example — Salesforce daily API budget::

        # 100,000 calls per rolling 24-hour window
        sf_limiter = SlidingWindowRateLimiter(limit=100_000, window_seconds=86400)

        if sf_limiter.try_acquire():
            result = sf.query("SELECT Id FROM Account")
        else:
            raise APIBudgetExhausted("Salesforce daily limit reached")

    Example — HubSpot per-10-second burst::

        hubspot_limiter = SlidingWindowRateLimiter(limit=10, window_seconds=10)
    """

    def __init__(self, limit: int, window_seconds: float) -> None:
        if limit <= 0:
            raise ValueError("limit must be positive")
        if window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        self._limit = limit
        self._window = window_seconds
        self._timestamps: deque[float] = deque()
        self._lock = Lock()

    @property
    def limit(self) -> int:
        """Maximum requests allowed in the window."""
        return self._limit

    @property
    def window_seconds(self) -> float:
        """Window size in seconds."""
        return self._window

    def acquire(
        self,
        blocking: bool = False,
        max_wait_seconds: float = 30.0,
    ) -> float:
        """
        Acquire one request slot in the sliding window.

        Args:
            blocking: If ``True``, block until a slot opens (up to
                      ``max_wait_seconds``). If ``False`` (default), raise
                      ``RateLimitExceeded`` immediately if the window is full.
            max_wait_seconds: Upper bound on blocking wait time.

        Returns:
            Actual wait time in seconds (0.0 if slot was immediately available).

        Raises:
            RateLimitExceeded: If non-blocking and window is full, or if
                               ``max_wait_seconds`` exceeded.
        """
        deadline = time.monotonic() + max_wait_seconds
        total_wait = 0.0

        while True:
            with self._lock:
                now = time.monotonic()
                self._evict(now)
                current_count = len(self._timestamps)
                if current_count < self._limit:
                    self._timestamps.append(now)
                    return total_wait

                # Snapshot inside the lock to avoid data race when raising outside it
                slots_available = self._limit - current_count

                if not blocking:
                    raise RateLimitExceeded(
                        tokens_available=slots_available,
                        cost=1.0,
                    )

                # Oldest timestamp determines when the next slot opens
                oldest = self._timestamps[0]
                wait_needed = (oldest + self._window) - now

            remaining = deadline - time.monotonic()
            if wait_needed > remaining:
                raise RateLimitExceeded(
                    tokens_available=slots_available,
                    cost=1.0,
                )

            sleep_time = min(wait_needed, remaining, 0.1)
            time.sleep(sleep_time)
            total_wait += sleep_time

    async def async_acquire(self, max_wait_seconds: float = 30.0) -> float:
        """
        Async variant of ``acquire()``. Uses ``asyncio.sleep`` to yield the
        event loop while waiting.

        Args:
            max_wait_seconds: Maximum wait time.

        Returns:
            Total wait time in seconds.
        """
        deadline = time.monotonic() + max_wait_seconds
        total_wait = 0.0

        while True:
            with self._lock:
                now = time.monotonic()
                self._evict(now)
                current_count = len(self._timestamps)
                if current_count < self._limit:
                    self._timestamps.append(now)
                    return total_wait

                slots_available = self._limit - current_count
                oldest = self._timestamps[0]
                wait_needed = (oldest + self._window) - now

            remaining = deadline - time.monotonic()
            if wait_needed > remaining:
                raise RateLimitExceeded(
                    tokens_available=slots_available,
                    cost=1.0,
                )

            sleep_time = min(wait_needed, remaining, 0.05)
            await asyncio.sleep(sleep_time)
            total_wait += sleep_time

    def try_acquire(self) -> bool:
        """
        Non-blocking attempt. Returns ``True`` if a slot was acquired,
        ``False`` if the window is full.
        """
        try:
            self.acquire(blocking=False)
            return True
        except RateLimitExceeded:
            return False

    def utilization(self) -> float:
        """
        Current window utilization as a fraction of the limit (0.0–1.0).

        Useful for monitoring and back-pressure decisions.

        Example::

            if limiter.utilization() > 0.9:
                alert("Salesforce API budget at 90% — throttle integrations")
        """
        with self._lock:
            self._evict(time.monotonic())
            return len(self._timestamps) / self._limit

    def requests_in_window(self) -> int:
        """Return count of requests within the current rolling window."""
        with self._lock:
            self._evict(time.monotonic())
            return len(self._timestamps)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _evict(self, now: float) -> None:
        """Remove timestamps older than the window. Must hold ``_lock``."""
        cutoff = now - self._window
        # Timestamps are appended in order; evict from the front in O(1) per pop
        while self._timestamps and self._timestamps[0] <= cutoff:
            self._timestamps.popleft()
