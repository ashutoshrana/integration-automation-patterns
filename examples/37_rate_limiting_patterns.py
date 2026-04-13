"""
37_rate_limiting_patterns.py — API Rate Limiting and Throttling Patterns

Pure-Python implementation of five rate-limiting and throttling primitives using
only the standard library (``collections``, ``dataclasses``, ``threading``,
``time``, ``typing``).  No external dependencies are required.

    Pattern 1 — SlidingWindowRateLimiter:
                Accurate per-client sliding window rate limiter backed by a
                ``collections.deque``.  :meth:`allow` returns ``True`` when the
                number of requests recorded within the last *window_seconds* is
                below *max_requests*; it appends the current timestamp and
                returns ``False`` once the cap is reached.  :meth:`remaining`
                returns the number of additional requests the client may make
                right now.  :meth:`reset_at` returns the Unix timestamp at which
                the oldest in-window request will expire and a new slot will open.
                All operations are protected by a ``threading.Lock`` for safe
                concurrent use.

    Pattern 2 — TokenBucketLimiter:
                Classic token-bucket algorithm allowing short bursts up to the
                bucket *capacity* while sustaining a long-run *rate*
                tokens-per-second.  Tokens refill continuously between calls
                (calculated lazily via ``time.monotonic()`` elapsed time).
                :meth:`consume` deducts *tokens* when sufficient tokens are
                available and returns ``True``; returns ``False`` without
                modifying state when the bucket lacks enough tokens.
                :meth:`available_tokens` returns the current (post-refill)
                token count for the given client.  Thread-safe.

    Pattern 3 — AdaptiveThrottler:
                Adapts the allowed request rate based on observed downstream
                error rate using the AIMD (Additive Increase, Multiplicative
                Decrease) algorithm.  :meth:`record_success` nudges the rate up
                by a fixed ``increase_step`` (default 1.0 rps) toward
                *max_rps*.  :meth:`record_failure` halves the current rate
                (multiplicative decrease) floored at *min_rps*.  :meth:`allow`
                uses an internal token-bucket style gate—tokens accumulate at
                the current RPS and one is consumed per allowed request.

    Pattern 4 — QuotaManager:
                Multi-tier quota enforcement with independent per-second,
                per-minute, and per-day windows.  :meth:`consume` returns
                ``True`` only when **all three** tiers have remaining capacity
                and atomically decrements each counter.  :meth:`status` returns
                a ``{tier: (used, limit, reset_at)}`` mapping for the client.
                :meth:`reset` clears all tier state for a client.  Thread-safe.

    Pattern 5 — CircuitBreakerRateLimiter:
                Combines a sliding-window rate limiter with a three-state
                circuit breaker (CLOSED → OPEN → HALF_OPEN → CLOSED).  The
                circuit opens after *failure_threshold* consecutive failures and
                remains open for *recovery_timeout* seconds before entering
                HALF_OPEN, where a single successful probe closes it again.
                :meth:`call` raises :class:`RateLimitExceeded` when the rate
                limit fires, :class:`CircuitOpen` when the circuit is OPEN, and
                propagates any exception raised by *fn* (counting it as a
                failure).  :attr:`state` exposes the current circuit state
                string.

No external dependencies required.

Run:
    python examples/37_rate_limiting_patterns.py
"""

from __future__ import annotations

import time
from collections import defaultdict, deque
from collections.abc import Callable
from dataclasses import dataclass, field
from threading import Lock
from typing import Any

# ===========================================================================
# Exceptions
# ===========================================================================


class RateLimitExceeded(Exception):
    """Raised when a rate limit is exceeded."""


class CircuitOpen(Exception):
    """Raised when the circuit breaker is in the OPEN state."""


# ===========================================================================
# Pattern 1 — SlidingWindowRateLimiter
# ===========================================================================


class SlidingWindowRateLimiter:
    """Accurate sliding-window rate limiter per client.

    Example::

        limiter = SlidingWindowRateLimiter(max_requests=5, window_seconds=1.0)
        for _ in range(5):
            assert limiter.allow("user-1") is True
        assert limiter.allow("user-1") is False   # 6th request → rejected
    """

    def __init__(self, max_requests: int, window_seconds: float) -> None:
        if max_requests < 1:
            raise ValueError("max_requests must be >= 1")
        if window_seconds <= 0:
            raise ValueError("window_seconds must be > 0")
        self._max = max_requests
        self._window = window_seconds
        # client_id → deque of monotonic timestamps
        self._windows: dict[str, deque[float]] = defaultdict(deque)
        self._lock = Lock()

    def _evict(self, dq: deque[float], now: float) -> None:
        """Remove timestamps older than the window (called under lock)."""
        cutoff = now - self._window
        while dq and dq[0] <= cutoff:
            dq.popleft()

    def allow(self, client_id: str) -> bool:
        """Return ``True`` and record the request if within rate limit.

        Args:
            client_id: Opaque string identifying the caller.

        Returns:
            ``True`` when the request is allowed; ``False`` when the limit
            has been reached.
        """
        now = time.monotonic()
        with self._lock:
            dq = self._windows[client_id]
            self._evict(dq, now)
            if len(dq) >= self._max:
                return False
            dq.append(now)
            return True

    def remaining(self, client_id: str) -> int:
        """Return how many more requests the client may make right now.

        Args:
            client_id: Opaque string identifying the caller.

        Returns:
            Number of remaining allowed requests in the current window.
        """
        now = time.monotonic()
        with self._lock:
            dq = self._windows[client_id]
            self._evict(dq, now)
            return max(0, self._max - len(dq))

    def reset_at(self, client_id: str) -> float:
        """Return the Unix timestamp when the oldest in-window request expires.

        If the client has no recorded requests the current wall-clock time is
        returned (i.e. a slot is available immediately).

        Args:
            client_id: Opaque string identifying the caller.

        Returns:
            Unix timestamp (``time.time()`` scale) of next slot opening.
        """
        mono_now = time.monotonic()
        wall_now = time.time()
        with self._lock:
            dq = self._windows[client_id]
            self._evict(dq, mono_now)
            if not dq:
                return wall_now
            # Oldest entry in deque expires window_seconds after it was recorded.
            oldest_mono = dq[0]
            mono_expire = oldest_mono + self._window
            # Convert monotonic delta to wall time.
            return wall_now + (mono_expire - mono_now)


# ===========================================================================
# Pattern 2 — TokenBucketLimiter
# ===========================================================================


@dataclass
class _BucketState:
    """Mutable per-client token bucket state."""

    tokens: float
    last_refill: float = field(default_factory=time.monotonic)


class TokenBucketLimiter:
    """Token-bucket rate limiter allowing short bursts up to *capacity* tokens.

    Tokens refill at *rate* tokens per second, capped at *capacity*.

    Example::

        tb = TokenBucketLimiter(rate=10.0, capacity=20.0)
        assert tb.consume("svc-a", tokens=5.0) is True
        assert tb.available_tokens("svc-a") >= 15.0
    """

    def __init__(self, rate: float, capacity: float) -> None:
        if rate <= 0:
            raise ValueError("rate must be > 0")
        if capacity <= 0:
            raise ValueError("capacity must be > 0")
        self._rate = rate
        self._capacity = capacity
        self._buckets: dict[str, _BucketState] = {}
        self._lock = Lock()

    def _get_or_create(self, client_id: str) -> _BucketState:
        """Return existing bucket state or create a full bucket (called under lock)."""
        if client_id not in self._buckets:
            self._buckets[client_id] = _BucketState(tokens=self._capacity)
        return self._buckets[client_id]

    def _refill(self, state: _BucketState) -> None:
        """Add accrued tokens since last refill (called under lock)."""
        now = time.monotonic()
        elapsed = now - state.last_refill
        state.tokens = min(self._capacity, state.tokens + elapsed * self._rate)
        state.last_refill = now

    def consume(self, client_id: str, tokens: float = 1.0) -> bool:
        """Attempt to consume *tokens* from the client's bucket.

        Args:
            client_id: Opaque string identifying the caller.
            tokens:    Number of tokens to consume (default 1.0).

        Returns:
            ``True`` if tokens were available and consumed; ``False`` otherwise.
        """
        if tokens <= 0:
            raise ValueError("tokens must be > 0")
        with self._lock:
            state = self._get_or_create(client_id)
            self._refill(state)
            if state.tokens < tokens:
                return False
            state.tokens -= tokens
            return True

    def available_tokens(self, client_id: str) -> float:
        """Return the current (post-refill) token count for the client.

        Args:
            client_id: Opaque string identifying the caller.

        Returns:
            Current token count as a float.
        """
        with self._lock:
            state = self._get_or_create(client_id)
            self._refill(state)
            return state.tokens


# ===========================================================================
# Pattern 3 — AdaptiveThrottler
# ===========================================================================


class AdaptiveThrottler:
    """AIMD adaptive throttler that adjusts RPS based on downstream error rate.

    Additive Increase: each success nudges the rate up by *increase_step* rps.
    Multiplicative Decrease: each failure halves the current rate (floor = min_rps).

    Example::

        throttler = AdaptiveThrottler(initial_rps=10.0, min_rps=1.0, max_rps=50.0)
        throttler.record_failure()
        assert throttler.current_rps == 5.0
        throttler.record_success()
        assert throttler.current_rps == 6.0
    """

    def __init__(
        self,
        initial_rps: float,
        min_rps: float,
        max_rps: float,
        increase_step: float = 1.0,
    ) -> None:
        if min_rps <= 0:
            raise ValueError("min_rps must be > 0")
        if max_rps < min_rps:
            raise ValueError("max_rps must be >= min_rps")
        if initial_rps < min_rps or initial_rps > max_rps:
            raise ValueError("initial_rps must be in [min_rps, max_rps]")
        self._rps = initial_rps
        self._min_rps = min_rps
        self._max_rps = max_rps
        self._increase_step = increase_step
        # Token-bucket state for allow()
        self._tokens: float = initial_rps
        self._last_refill: float = time.monotonic()
        self._lock = Lock()

    @property
    def current_rps(self) -> float:
        """Current allowed requests per second."""
        with self._lock:
            return self._rps

    def record_success(self) -> None:
        """Increase RPS by *increase_step* toward *max_rps*."""
        with self._lock:
            self._rps = min(self._max_rps, self._rps + self._increase_step)

    def record_failure(self) -> None:
        """Halve RPS (multiplicative decrease), floored at *min_rps*."""
        with self._lock:
            self._rps = max(self._min_rps, self._rps * 0.5)

    def _refill_tokens(self) -> None:
        """Accumulate tokens up to current_rps capacity (called under lock)."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self._rps, self._tokens + elapsed * self._rps)
        self._last_refill = now

    def allow(self) -> bool:
        """Return ``True`` if a request is allowed at the current RPS.

        Uses an internal token bucket seeded from the current RPS so that
        short bursts remain aligned with the adaptive rate.

        Returns:
            ``True`` when a request token is available; ``False`` otherwise.
        """
        with self._lock:
            self._refill_tokens()
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return True
            return False


# ===========================================================================
# Pattern 4 — QuotaManager
# ===========================================================================


@dataclass
class _TierState:
    """Mutable per-client state for one quota tier."""

    used: int = 0
    window_start: float = field(default_factory=time.monotonic)


class QuotaManager:
    """Multi-tier quota enforcement (per-second, per-minute, per-day).

    :meth:`consume` only allows a request when **all three** tiers have
    remaining capacity and atomically decrements each.

    Example::

        qm = QuotaManager(per_second=5, per_minute=100, per_day=1000)
        assert qm.consume("user-a") is True
        status = qm.status("user-a")
        assert status["second"][0] == 1   # 1 used out of 5
    """

    _TIERS: tuple[tuple[str, str], ...] = (
        ("second", "per_second"),
        ("minute", "per_minute"),
        ("day", "per_day"),
    )
    _TIER_SECONDS: dict[str, float] = {"second": 1.0, "minute": 60.0, "day": 86400.0}

    def __init__(self, per_second: int, per_minute: int, per_day: int) -> None:
        for name, val in (("per_second", per_second), ("per_minute", per_minute), ("per_day", per_day)):
            if val < 1:
                raise ValueError(f"{name} must be >= 1")
        self._limits: dict[str, int] = {
            "second": per_second,
            "minute": per_minute,
            "day": per_day,
        }
        # client_id → {tier → _TierState}
        self._state: dict[str, dict[str, _TierState]] = defaultdict(
            lambda: {t: _TierState() for t in ("second", "minute", "day")}
        )
        self._lock = Lock()

    def _reset_expired(self, tiers: dict[str, _TierState], now: float) -> None:
        """Reset any tier whose window has elapsed (called under lock)."""
        for tier, state in tiers.items():
            if now - state.window_start >= self._TIER_SECONDS[tier]:
                state.used = 0
                state.window_start = now

    def consume(self, client_id: str) -> bool:
        """Consume one unit from all tiers if all have capacity.

        Args:
            client_id: Opaque string identifying the caller.

        Returns:
            ``True`` if all tiers had capacity (all decremented); ``False``
            if any tier was exhausted (none decremented).
        """
        now = time.monotonic()
        with self._lock:
            tiers = self._state[client_id]
            self._reset_expired(tiers, now)
            for tier, state in tiers.items():
                if state.used >= self._limits[tier]:
                    return False
            for state in tiers.values():
                state.used += 1
            return True

    def status(self, client_id: str) -> dict[str, tuple[int, int, float]]:
        """Return current quota status for the client.

        Returns a ``{tier: (used, limit, reset_at)}`` mapping where
        *reset_at* is a Unix wall-clock timestamp.

        Args:
            client_id: Opaque string identifying the caller.
        """
        now_mono = time.monotonic()
        now_wall = time.time()
        with self._lock:
            tiers = self._state[client_id]
            self._reset_expired(tiers, now_mono)
            result: dict[str, tuple[int, int, float]] = {}
            for tier, state in tiers.items():
                window_s = self._TIER_SECONDS[tier]
                mono_expire = state.window_start + window_s
                reset_wall = now_wall + (mono_expire - now_mono)
                result[tier] = (state.used, self._limits[tier], reset_wall)
            return result

    def reset(self, client_id: str) -> None:
        """Reset all quota tiers for *client_id*.

        Args:
            client_id: Opaque string identifying the caller.
        """
        with self._lock:
            if client_id in self._state:
                for state in self._state[client_id].values():
                    state.used = 0
                    state.window_start = time.monotonic()


# ===========================================================================
# Pattern 5 — CircuitBreakerRateLimiter
# ===========================================================================

_STATE_CLOSED = "CLOSED"
_STATE_OPEN = "OPEN"
_STATE_HALF_OPEN = "HALF_OPEN"


class CircuitBreakerRateLimiter:
    """Sliding-window rate limiter fused with a three-state circuit breaker.

    Circuit states:

    * ``CLOSED`` — normal operation; failures accumulate.
    * ``OPEN``   — all calls rejected immediately with :class:`CircuitOpen`.
    * ``HALF_OPEN`` — one probe call allowed; success → CLOSED, failure → OPEN.

    Example::

        cb = CircuitBreakerRateLimiter(
            max_requests=10, window_seconds=1.0,
            failure_threshold=3, recovery_timeout=5.0,
        )
        result = cb.call(lambda: "ok", client_id="svc")
        assert result == "ok"
        assert cb.state == "CLOSED"
    """

    def __init__(
        self,
        max_requests: int,
        window_seconds: float,
        failure_threshold: int,
        recovery_timeout: float,
    ) -> None:
        self._limiter = SlidingWindowRateLimiter(max_requests, window_seconds)
        self._failure_threshold = failure_threshold
        self._recovery_timeout = recovery_timeout
        self._cb_state: str = _STATE_CLOSED
        self._failure_count: int = 0
        self._opened_at: float = 0.0
        self._lock = Lock()

    @property
    def state(self) -> str:
        """Current circuit breaker state: ``CLOSED``, ``OPEN``, or ``HALF_OPEN``."""
        with self._lock:
            return self._cb_state

    def _check_and_maybe_transition(self, now: float) -> str:
        """Return the effective state, transitioning OPEN→HALF_OPEN if timeout elapsed (called under lock)."""
        if self._cb_state == _STATE_OPEN:
            if now - self._opened_at >= self._recovery_timeout:
                self._cb_state = _STATE_HALF_OPEN
        return self._cb_state

    def call(self, fn: Callable[[], Any], client_id: str = "default") -> Any:
        """Execute *fn* subject to rate limiting and circuit-breaker protection.

        Args:
            fn:        Zero-argument callable to execute.
            client_id: Identifier used for the rate limiter window.

        Returns:
            The return value of *fn*.

        Raises:
            RateLimitExceeded: When the sliding-window rate limit is reached.
            CircuitOpen:       When the circuit is in the OPEN state.
            Exception:         Any exception raised by *fn* (also counted as a
                               circuit-breaker failure).
        """
        now = time.monotonic()

        with self._lock:
            effective = self._check_and_maybe_transition(now)
            if effective == _STATE_OPEN:
                raise CircuitOpen("Circuit breaker is OPEN — call rejected")
            # Rate check (doesn't need the CB lock held, but keep it simple)
            if not self._limiter.allow(client_id):
                raise RateLimitExceeded(f"Rate limit exceeded for client '{client_id}'")

        try:
            result = fn()
        except Exception:
            with self._lock:
                self._failure_count += 1
                if self._cb_state == _STATE_HALF_OPEN:
                    # Failed probe — reopen
                    self._cb_state = _STATE_OPEN
                    self._opened_at = time.monotonic()
                    self._failure_count = 0
                elif self._failure_count >= self._failure_threshold:
                    self._cb_state = _STATE_OPEN
                    self._opened_at = time.monotonic()
                    self._failure_count = 0
            raise

        with self._lock:
            if self._cb_state == _STATE_HALF_OPEN:
                # Successful probe — close the circuit
                self._cb_state = _STATE_CLOSED
                self._failure_count = 0
            else:
                # Successful call in CLOSED state resets consecutive failure count
                self._failure_count = 0

        return result


# ===========================================================================
# Self-test / demo (run directly)
# ===========================================================================

if __name__ == "__main__":
    print("=== SlidingWindowRateLimiter ===")
    sw = SlidingWindowRateLimiter(max_requests=3, window_seconds=1.0)
    for i in range(4):
        allowed = sw.allow("demo")
        print(f"  request {i + 1}: allowed={allowed}, remaining={sw.remaining('demo')}")

    print("\n=== TokenBucketLimiter ===")
    tb = TokenBucketLimiter(rate=5.0, capacity=10.0)
    for i in range(12):
        ok = tb.consume("demo")
        print(f"  consume {i + 1}: ok={ok}, tokens={tb.available_tokens('demo'):.2f}")

    print("\n=== AdaptiveThrottler ===")
    at = AdaptiveThrottler(initial_rps=10.0, min_rps=1.0, max_rps=50.0)
    print(f"  initial rps={at.current_rps}")
    at.record_failure()
    print(f"  after failure rps={at.current_rps}")
    at.record_success()
    print(f"  after success rps={at.current_rps}")

    print("\n=== QuotaManager ===")
    qm = QuotaManager(per_second=2, per_minute=10, per_day=100)
    for i in range(3):
        ok = qm.consume("demo")
        print(f"  consume {i + 1}: ok={ok}")
    s = qm.status("demo")
    print(f"  status: {s}")

    print("\n=== CircuitBreakerRateLimiter ===")
    cb = CircuitBreakerRateLimiter(
        max_requests=5, window_seconds=1.0, failure_threshold=2, recovery_timeout=60.0
    )
    print(f"  state={cb.state}")
    try:
        cb.call(lambda: (_ for _ in ()).throw(RuntimeError("downstream fail")))
    except RuntimeError:
        pass
    try:
        cb.call(lambda: (_ for _ in ()).throw(RuntimeError("downstream fail again")))
    except RuntimeError:
        pass
    print(f"  after 2 failures state={cb.state}")
    try:
        cb.call(lambda: "probe", client_id="demo")
    except CircuitOpen as e:
        print(f"  probe blocked: {e}")

    print("\nAll demos complete.")
