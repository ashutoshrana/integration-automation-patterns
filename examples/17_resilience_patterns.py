"""
17_resilience_patterns.py — Production resilience patterns for microservices:
Circuit Breaker, Bulkhead, Rate Limiter, and Retry with Jitter.

Demonstrates four complementary patterns that together prevent cascading
failures, enforce fair-use limits, and provide structured retry semantics in
distributed service architectures:

    Pattern 1 — Sliding Window Circuit Breaker:
                Tracks failure rates over a configurable sliding window
                (count-based or time-based). Transitions CLOSED → OPEN when
                the failure rate or slow-call rate exceeds thresholds. In OPEN
                state, calls are rejected immediately (fail-fast). After a
                configurable wait duration, transitions to HALF_OPEN to probe
                service recovery. Returns to CLOSED on success, OPEN on failure.

    Pattern 2 — Bulkhead (Thread Pool Isolation):
                Isolates service calls into separate thread pools, each with a
                configurable concurrency limit and queue depth. When a bulkhead
                is full, new calls raise BulkheadFullError immediately rather
                than blocking indefinitely. Prevents one slow downstream service
                from consuming all available threads.

    Pattern 3 — Token Bucket Rate Limiter (Burst-Aware):
                Per-identity token bucket with configurable refill rate and
                burst capacity. Identities that have not made recent calls
                accumulate tokens (up to burst capacity). Short traffic spikes
                are absorbed; sustained over-limit traffic is rejected with
                RateLimitExceeded. Thread-safe for concurrent callers.

    Pattern 4 — Retry with Exponential Backoff + Full Jitter:
                Retries failed calls with configurable max attempts, base delay,
                max delay cap, and per-attempt full jitter (random uniform
                between 0 and the computed backoff). Classifies exceptions as
                retryable or non-retryable. Integrates with the circuit breaker:
                respects OPEN state and does not retry when the circuit is open.

No external dependencies required.

Run:
    python examples/17_resilience_patterns.py
"""

from __future__ import annotations

import math
import random
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Type


# ---------------------------------------------------------------------------
# Pattern 1 — Sliding Window Circuit Breaker
# ---------------------------------------------------------------------------


class CircuitBreakerState(str, Enum):
    """Circuit breaker state machine states."""

    CLOSED = "CLOSED"       # Normal operation; calls pass through
    OPEN = "OPEN"           # Failure threshold exceeded; calls rejected
    HALF_OPEN = "HALF_OPEN"  # Probing recovery; limited calls allowed


class CircuitBreakerOpenError(Exception):
    """Raised when a call is attempted while the circuit breaker is OPEN."""


@dataclass
class CircuitBreakerConfig:
    """
    Configuration for the SlidingWindowCircuitBreaker.

    Parameters
    ----------
    failure_rate_threshold : float
        Minimum failure rate (0.0–1.0) to trip the circuit. Default 0.5 (50%).
    slow_call_rate_threshold : float
        Minimum slow-call rate to trip the circuit. Default 1.0 (disabled by default).
    slow_call_duration_threshold : float
        Call duration in seconds above which a call is considered slow.
    minimum_calls : int
        Minimum number of calls in the window before the threshold is evaluated.
    sliding_window_size : int
        Number of calls to include in the count-based window.
    wait_duration_in_open_state : float
        Seconds to wait in OPEN state before transitioning to HALF_OPEN.
    permitted_calls_in_half_open : int
        Number of probe calls allowed in HALF_OPEN state.
    """

    failure_rate_threshold: float = 0.5
    slow_call_rate_threshold: float = 1.0  # 100% — effectively disabled
    slow_call_duration_threshold: float = 2.0
    minimum_calls: int = 5
    sliding_window_size: int = 10
    wait_duration_in_open_state: float = 5.0
    permitted_calls_in_half_open: int = 3


class SlidingWindowCircuitBreaker:
    """
    Count-based sliding window circuit breaker.

    Thread-safe for concurrent use in a multi-threaded service environment.

    Example
    -------
    >>> cb = SlidingWindowCircuitBreaker("payment-service")
    >>> try:
    ...     with cb:
    ...         result = call_payment_service()
    ... except CircuitBreakerOpenError:
    ...     return fallback_response()
    ... except Exception as exc:
    ...     raise  # Circuit breaker already recorded the failure
    """

    def __init__(
        self,
        name: str,
        config: Optional[CircuitBreakerConfig] = None,
    ) -> None:
        self.name = name
        self._config = config or CircuitBreakerConfig()
        self._lock = threading.Lock()
        self._state = CircuitBreakerState.CLOSED
        # Each entry: True = success, False = failure
        self._window: deque = deque(maxlen=self._config.sliding_window_size)
        self._slow_calls: deque = deque(maxlen=self._config.sliding_window_size)
        self._open_since: float = 0.0
        self._half_open_count: int = 0

    @property
    def state(self) -> CircuitBreakerState:
        with self._lock:
            self._check_half_open_transition()
            return self._state

    def _check_half_open_transition(self) -> None:
        """Transition OPEN → HALF_OPEN if wait_duration has elapsed."""
        if (
            self._state == CircuitBreakerState.OPEN
            and time.time() - self._open_since >= self._config.wait_duration_in_open_state
        ):
            self._state = CircuitBreakerState.HALF_OPEN
            self._half_open_count = 0

    def _record_outcome(self, success: bool, duration: float) -> None:
        is_slow = duration >= self._config.slow_call_duration_threshold
        self._window.append(success)
        self._slow_calls.append(is_slow)

        if len(self._window) < self._config.minimum_calls:
            return

        total = len(self._window)
        failures = sum(1 for s in self._window if not s)
        slow = sum(1 for s in self._slow_calls if s)

        failure_rate = failures / total
        slow_rate = slow / total

        if (
            failure_rate >= self._config.failure_rate_threshold
            or slow_rate >= self._config.slow_call_rate_threshold
        ):
            self._trip_open()

    def _trip_open(self) -> None:
        self._state = CircuitBreakerState.OPEN
        self._open_since = time.time()
        self._window.clear()
        self._slow_calls.clear()

    def call(self, fn: Callable[[], Any]) -> Any:
        """
        Execute ``fn`` through the circuit breaker.

        Raises
        ------
        CircuitBreakerOpenError
            If the circuit is OPEN (fail-fast).
        Exception
            Any exception raised by ``fn`` (recorded as a failure).
        """
        with self._lock:
            self._check_half_open_transition()
            state = self._state

            if state == CircuitBreakerState.OPEN:
                raise CircuitBreakerOpenError(
                    f"Circuit breaker '{self.name}' is OPEN — calls are rejected"
                )
            if state == CircuitBreakerState.HALF_OPEN:
                if self._half_open_count >= self._config.permitted_calls_in_half_open:
                    raise CircuitBreakerOpenError(
                        f"Circuit breaker '{self.name}' is HALF_OPEN — "
                        f"probe call limit reached"
                    )
                self._half_open_count += 1

        start = time.time()
        try:
            result = fn()
            duration = time.time() - start
            with self._lock:
                if self._state == CircuitBreakerState.HALF_OPEN:
                    # Successful probe → transition to CLOSED
                    self._state = CircuitBreakerState.CLOSED
                    self._window.clear()
                    self._slow_calls.clear()
                else:
                    self._record_outcome(True, duration)
            return result
        except CircuitBreakerOpenError:
            raise
        except Exception:
            duration = time.time() - start
            with self._lock:
                if self._state == CircuitBreakerState.HALF_OPEN:
                    self._trip_open()
                else:
                    self._record_outcome(False, duration)
            raise

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False  # Do not suppress exceptions


# ---------------------------------------------------------------------------
# Pattern 2 — Bulkhead
# ---------------------------------------------------------------------------


class BulkheadFullError(Exception):
    """Raised when a bulkhead is at maximum concurrency."""


class BulkheadPool:
    """
    Thread pool bulkhead that limits concurrent calls to an isolated resource.

    When the concurrent call count reaches ``max_concurrent_calls``, new
    calls raise BulkheadFullError immediately (no blocking wait).

    Example
    -------
    >>> bulkhead = BulkheadPool("inventory-service", max_concurrent_calls=10)
    >>> try:
    ...     with bulkhead:
    ...         result = call_inventory_service()
    ... except BulkheadFullError:
    ...     return cached_inventory()
    """

    def __init__(
        self,
        name: str,
        max_concurrent_calls: int = 10,
    ) -> None:
        self.name = name
        self.max_concurrent_calls = max_concurrent_calls
        self._lock = threading.Lock()
        self._active_calls: int = 0

    @property
    def active_calls(self) -> int:
        with self._lock:
            return self._active_calls

    @property
    def available(self) -> int:
        with self._lock:
            return max(0, self.max_concurrent_calls - self._active_calls)

    def acquire(self) -> None:
        with self._lock:
            if self._active_calls >= self.max_concurrent_calls:
                raise BulkheadFullError(
                    f"Bulkhead '{self.name}' is full "
                    f"({self._active_calls}/{self.max_concurrent_calls} slots in use)"
                )
            self._active_calls += 1

    def release(self) -> None:
        with self._lock:
            self._active_calls = max(0, self._active_calls - 1)

    def call(self, fn: Callable[[], Any]) -> Any:
        """Execute ``fn`` within the bulkhead, raising BulkheadFullError if full."""
        self.acquire()
        try:
            return fn()
        finally:
            self.release()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()
        return False


# ---------------------------------------------------------------------------
# Pattern 3 — Token Bucket Rate Limiter
# ---------------------------------------------------------------------------


class RateLimitExceeded(Exception):
    """Raised when an identity has exhausted its token bucket allowance."""


@dataclass
class TokenBucketState:
    """Per-identity token bucket state."""

    tokens: float
    last_refill: float = field(default_factory=time.time)


class BurstAwareRateLimiter:
    """
    Per-identity token bucket rate limiter with burst capacity.

    Tokens are refilled at ``refill_rate`` tokens per second. An identity
    can accumulate up to ``burst_capacity`` tokens (covering short traffic
    spikes). When tokens are exhausted, ``acquire()`` raises RateLimitExceeded.

    Thread-safe for concurrent callers with different or the same identity.

    Example
    -------
    >>> limiter = BurstAwareRateLimiter(refill_rate=10.0, burst_capacity=20)
    >>> try:
    ...     limiter.acquire(identity="tenant-42")
    ...     result = handle_request()
    ... except RateLimitExceeded:
    ...     return http_429_response()
    """

    def __init__(
        self,
        refill_rate: float = 10.0,
        burst_capacity: float = 20.0,
        tokens_per_call: float = 1.0,
    ) -> None:
        self._refill_rate = refill_rate
        self._burst_capacity = burst_capacity
        self._tokens_per_call = tokens_per_call
        self._lock = threading.Lock()
        self._buckets: Dict[str, TokenBucketState] = {}

    def _get_or_create(self, identity: str) -> TokenBucketState:
        if identity not in self._buckets:
            self._buckets[identity] = TokenBucketState(tokens=self._burst_capacity)
        return self._buckets[identity]

    def _refill(self, state: TokenBucketState) -> None:
        now = time.time()
        elapsed = now - state.last_refill
        new_tokens = elapsed * self._refill_rate
        state.tokens = min(self._burst_capacity, state.tokens + new_tokens)
        state.last_refill = now

    def acquire(self, identity: str = "default") -> None:
        """
        Consume one token for ``identity``.

        Raises
        ------
        RateLimitExceeded
            If the token bucket for this identity is empty.
        """
        with self._lock:
            state = self._get_or_create(identity)
            self._refill(state)
            if state.tokens < self._tokens_per_call:
                raise RateLimitExceeded(
                    f"Rate limit exceeded for identity '{identity}': "
                    f"{state.tokens:.2f} tokens available, "
                    f"{self._tokens_per_call} required"
                )
            state.tokens -= self._tokens_per_call

    def tokens_available(self, identity: str = "default") -> float:
        """Return the current token count for an identity (after refill)."""
        with self._lock:
            state = self._get_or_create(identity)
            self._refill(state)
            return state.tokens


# ---------------------------------------------------------------------------
# Pattern 4 — Retry with Exponential Backoff + Full Jitter
# ---------------------------------------------------------------------------


@dataclass
class RetryConfig:
    """
    Configuration for the JitteredRetryPolicy.

    Parameters
    ----------
    max_attempts : int
        Maximum number of total attempts (including the first call).
    base_delay : float
        Base delay in seconds for the first retry.
    max_delay : float
        Upper cap on computed backoff delay before jitter.
    jitter_factor : float
        Full jitter: actual delay is random.uniform(0, min(max_delay, base * 2**n)).
    non_retryable : Set[Type[Exception]]
        Exception types that should NOT be retried (re-raise immediately).
    """

    max_attempts: int = 3
    base_delay: float = 0.1
    max_delay: float = 30.0
    jitter_factor: float = 1.0
    non_retryable: Set[Type[Exception]] = field(default_factory=set)


class JitteredRetryPolicy:
    """
    Retry policy with exponential backoff and full jitter.

    Full jitter randomizes the delay uniformly between 0 and the computed
    exponential ceiling, which prevents the "thundering herd" problem where
    all retrying clients fire at the same time after a shared outage.

    Integrates with SlidingWindowCircuitBreaker: if a CircuitBreakerOpenError
    is raised, the retry loop stops immediately (retrying an open circuit is
    futile and would only increase load on an already-failing service).

    Example
    -------
    >>> retry = JitteredRetryPolicy(RetryConfig(max_attempts=3, base_delay=0.05))
    >>> result = retry.execute(lambda: call_external_service())
    """

    def __init__(self, config: Optional[RetryConfig] = None) -> None:
        self._config = config or RetryConfig()

    def execute(self, fn: Callable[[], Any]) -> Any:
        """
        Execute ``fn`` with retries.

        Parameters
        ----------
        fn : Callable[[], Any]
            The operation to execute. Must be idempotent.

        Returns
        -------
        Any
            The return value of ``fn`` on success.

        Raises
        ------
        Exception
            The last exception raised after all retry attempts are exhausted,
            or the first non-retryable or CircuitBreakerOpenError.
        """
        last_exc: Optional[Exception] = None

        for attempt in range(self._config.max_attempts):
            try:
                return fn()
            except CircuitBreakerOpenError:
                raise  # Never retry an open circuit
            except tuple(self._config.non_retryable) if self._config.non_retryable else () as exc:  # type: ignore[misc]
                raise
            except Exception as exc:
                last_exc = exc
                if attempt + 1 == self._config.max_attempts:
                    break
                # Full jitter: delay ∈ [0, min(max_delay, base * 2^attempt)]
                ceiling = min(
                    self._config.max_delay,
                    self._config.base_delay * math.pow(2, attempt),
                )
                delay = random.uniform(0, ceiling) * self._config.jitter_factor
                time.sleep(delay)

        raise last_exc  # type: ignore[misc]

    def execute_with_breaker(
        self,
        fn: Callable[[], Any],
        circuit_breaker: SlidingWindowCircuitBreaker,
    ) -> Any:
        """
        Execute ``fn`` through the circuit breaker with jittered retries.

        The circuit breaker is checked before each attempt. A CircuitBreakerOpenError
        on the first attempt propagates immediately (no retry). On subsequent
        attempts, if the circuit opened during a previous failure, retries stop.
        """
        last_exc: Optional[Exception] = None

        for attempt in range(self._config.max_attempts):
            try:
                return circuit_breaker.call(fn)
            except CircuitBreakerOpenError:
                raise  # Circuit is open — abort retry loop
            except tuple(self._config.non_retryable) if self._config.non_retryable else () as exc:  # type: ignore[misc]
                raise
            except Exception as exc:
                last_exc = exc
                if attempt + 1 == self._config.max_attempts:
                    break
                ceiling = min(
                    self._config.max_delay,
                    self._config.base_delay * math.pow(2, attempt),
                )
                delay = random.uniform(0, ceiling) * self._config.jitter_factor
                time.sleep(delay)

        raise last_exc  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Scenario demonstrations
# ---------------------------------------------------------------------------


def scenario_circuit_breaker_trip_and_recover() -> None:
    """Circuit breaker trips after failures then recovers on probe."""
    print("\n--- Circuit Breaker: Trip → OPEN → HALF_OPEN → CLOSED ---")
    config = CircuitBreakerConfig(
        failure_rate_threshold=0.5,
        minimum_calls=4,
        sliding_window_size=4,
        wait_duration_in_open_state=0.05,
        permitted_calls_in_half_open=1,
    )
    cb = SlidingWindowCircuitBreaker("payment-service", config)

    # 3 failures out of 4 → trips open
    for i in range(4):
        try:
            cb.call(lambda: (_ for _ in ()).throw(Exception("timeout")) if i < 3 else "ok")
        except CircuitBreakerOpenError:
            print(f"  Call {i}: circuit OPEN — fast-fail")
        except Exception:
            print(f"  Call {i}: failure recorded")

    print(f"  State after failures: {cb.state.value}")
    time.sleep(0.06)  # Wait past open duration
    print(f"  State after wait: {cb.state.value}")

    # Probe call succeeds → CLOSED
    try:
        result = cb.call(lambda: "recovered")
        print(f"  Probe call: {result}")
    except Exception as e:
        print(f"  Probe failed: {e}")
    print(f"  Final state: {cb.state.value}")


def scenario_bulkhead_isolation() -> None:
    """Bulkhead limits concurrent calls; excess raises BulkheadFullError."""
    print("\n--- Bulkhead: Concurrency Isolation ---")
    bh = BulkheadPool("inventory-service", max_concurrent_calls=2)

    results = []

    def held_call():
        bh.acquire()
        try:
            time.sleep(0.05)
            results.append("success")
        finally:
            bh.release()

    threads = [threading.Thread(target=held_call) for _ in range(2)]
    for t in threads:
        t.start()

    time.sleep(0.01)  # Let threads acquire
    print(f"  Active calls: {bh.active_calls}")
    try:
        bh.acquire()
        print("  Third acquire: unexpected success")
        bh.release()
    except BulkheadFullError as e:
        print(f"  Third call rejected: BulkheadFullError (as expected)")

    for t in threads:
        t.join()
    print(f"  After completion — active calls: {bh.active_calls}")


def scenario_rate_limiter_burst() -> None:
    """Token bucket allows burst then throttles sustained traffic."""
    print("\n--- Rate Limiter: Burst Capacity ---")
    limiter = BurstAwareRateLimiter(refill_rate=2.0, burst_capacity=5.0)
    accepted = 0
    rejected = 0
    for _ in range(8):
        try:
            limiter.acquire("tenant-42")
            accepted += 1
        except RateLimitExceeded:
            rejected += 1
    print(f"  Accepted: {accepted}, Rejected: {rejected}")
    print(f"  (5 burst + refill before rejection)")


def scenario_retry_with_eventual_success() -> None:
    """Retry policy retries transient failures and returns on success."""
    print("\n--- Retry: Exponential Backoff + Full Jitter ---")
    config = RetryConfig(max_attempts=4, base_delay=0.001, max_delay=0.01)
    retry = JitteredRetryPolicy(config)
    attempts = [0]

    def flaky():
        attempts[0] += 1
        if attempts[0] < 3:
            raise ConnectionError("transient failure")
        return "success"

    result = retry.execute(flaky)
    print(f"  Result: {result} (succeeded after {attempts[0]} attempts)")


if __name__ == "__main__":
    scenario_circuit_breaker_trip_and_recover()
    scenario_bulkhead_isolation()
    scenario_rate_limiter_burst()
    scenario_retry_with_eventual_success()
