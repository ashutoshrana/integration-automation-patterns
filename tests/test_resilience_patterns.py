"""
Tests for 17_resilience_patterns.py

Four resilience patterns for microservices:
  1. SlidingWindowCircuitBreaker — CLOSED/OPEN/HALF_OPEN state machine
  2. BulkheadPool — concurrency isolation with BulkheadFullError
  3. BurstAwareRateLimiter — per-identity token bucket with burst capacity
  4. JitteredRetryPolicy — exponential backoff + full jitter
"""

import importlib.util
import sys
import threading
import time
import types
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Module loading
# ---------------------------------------------------------------------------

_MOD_PATH = (
    Path(__file__).parent.parent / "examples" / "17_resilience_patterns.py"
)


def _load_module():
    module_name = "resilience_patterns"
    if module_name in sys.modules:
        return sys.modules[module_name]
    spec = importlib.util.spec_from_file_location(module_name, _MOD_PATH)
    mod = types.ModuleType(module_name)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def m():
    return _load_module()


# ---------------------------------------------------------------------------
# Pattern 1 — SlidingWindowCircuitBreaker
# ---------------------------------------------------------------------------


def _fast_cb(m, failure_rate=0.5, min_calls=4, window=4, wait=0.05, half_open=1):
    config = m.CircuitBreakerConfig(
        failure_rate_threshold=failure_rate,
        minimum_calls=min_calls,
        sliding_window_size=window,
        wait_duration_in_open_state=wait,
        permitted_calls_in_half_open=half_open,
    )
    return m.SlidingWindowCircuitBreaker("test-svc", config)


class TestSlidingWindowCircuitBreaker:

    def test_initial_state_is_closed(self, m):
        cb = _fast_cb(m)
        assert cb.state == m.CircuitBreakerState.CLOSED

    def test_successful_call_passes_through(self, m):
        cb = _fast_cb(m)
        result = cb.call(lambda: 42)
        assert result == 42

    def test_failure_recorded_without_tripping_below_minimum(self, m):
        cb = _fast_cb(m, min_calls=5, window=5)
        for _ in range(3):
            try:
                cb.call(lambda: (_ for _ in ()).throw(Exception("err")))
            except Exception:
                pass
        assert cb.state == m.CircuitBreakerState.CLOSED

    def test_circuit_trips_open_after_threshold(self, m):
        cb = _fast_cb(m, failure_rate=0.5, min_calls=4, window=4)
        for i in range(4):
            try:
                cb.call(lambda: (_ for _ in ()).throw(Exception("fail")))
            except (Exception, m.CircuitBreakerOpenError):
                pass
        assert cb.state == m.CircuitBreakerState.OPEN

    def test_open_circuit_rejects_immediately(self, m):
        cb = _fast_cb(m)
        # Trip the circuit
        for _ in range(4):
            try:
                cb.call(lambda: (_ for _ in ()).throw(Exception("fail")))
            except Exception:
                pass
        with pytest.raises(m.CircuitBreakerOpenError):
            cb.call(lambda: "should not run")

    def test_transitions_to_half_open_after_wait(self, m):
        cb = _fast_cb(m, wait=0.05)
        for _ in range(4):
            try:
                cb.call(lambda: (_ for _ in ()).throw(Exception("fail")))
            except Exception:
                pass
        time.sleep(0.07)
        assert cb.state == m.CircuitBreakerState.HALF_OPEN

    def test_successful_probe_closes_circuit(self, m):
        cb = _fast_cb(m, wait=0.02)
        for _ in range(4):
            try:
                cb.call(lambda: (_ for _ in ()).throw(Exception("fail")))
            except Exception:
                pass
        time.sleep(0.03)
        cb.call(lambda: "probe")
        assert cb.state == m.CircuitBreakerState.CLOSED

    def test_failed_probe_reopens_circuit(self, m):
        cb = _fast_cb(m, wait=0.02)
        for _ in range(4):
            try:
                cb.call(lambda: (_ for _ in ()).throw(Exception("fail")))
            except Exception:
                pass
        time.sleep(0.03)
        try:
            cb.call(lambda: (_ for _ in ()).throw(Exception("still failing")))
        except Exception:
            pass
        assert cb.state == m.CircuitBreakerState.OPEN

    def test_half_open_probe_limit_enforced(self, m):
        cb = _fast_cb(m, wait=0.02, half_open=1)
        for _ in range(4):
            try:
                cb.call(lambda: (_ for _ in ()).throw(Exception("fail")))
            except Exception:
                pass
        time.sleep(0.03)
        # First probe allowed
        try:
            cb.call(lambda: (_ for _ in ()).throw(Exception("probe fail")))
        except Exception:
            pass
        # Second probe rejected — circuit should have reopened
        with pytest.raises((m.CircuitBreakerOpenError, Exception)):
            cb.call(lambda: "second probe")

    def test_context_manager_does_not_suppress_exceptions(self, m):
        cb = _fast_cb(m)
        with pytest.raises(ValueError):
            with cb:
                raise ValueError("test error")


# ---------------------------------------------------------------------------
# Pattern 2 — BulkheadPool
# ---------------------------------------------------------------------------


class TestBulkheadPool:

    def test_successful_call_within_limit(self, m):
        bh = m.BulkheadPool("svc", max_concurrent_calls=2)
        result = bh.call(lambda: "ok")
        assert result == "ok"

    def test_acquire_increments_active_count(self, m):
        bh = m.BulkheadPool("svc", max_concurrent_calls=5)
        bh.acquire()
        assert bh.active_calls == 1
        bh.release()

    def test_release_decrements_active_count(self, m):
        bh = m.BulkheadPool("svc", max_concurrent_calls=5)
        bh.acquire()
        bh.release()
        assert bh.active_calls == 0

    def test_exceeds_limit_raises_bulkhead_full(self, m):
        bh = m.BulkheadPool("svc", max_concurrent_calls=1)
        bh.acquire()
        with pytest.raises(m.BulkheadFullError):
            bh.acquire()
        bh.release()

    def test_available_count_decreases_on_acquire(self, m):
        bh = m.BulkheadPool("svc", max_concurrent_calls=3)
        assert bh.available == 3
        bh.acquire()
        assert bh.available == 2
        bh.release()

    def test_context_manager_releases_on_exception(self, m):
        bh = m.BulkheadPool("svc", max_concurrent_calls=2)
        try:
            with bh:
                raise RuntimeError("test")
        except RuntimeError:
            pass
        assert bh.active_calls == 0

    def test_concurrent_threads_respect_limit(self, m):
        bh = m.BulkheadPool("svc", max_concurrent_calls=2)
        results = {"rejected": 0, "accepted": 0}
        lock = threading.Lock()

        def worker():
            try:
                with bh:
                    time.sleep(0.05)
                    with lock:
                        results["accepted"] += 1
            except m.BulkheadFullError:
                with lock:
                    results["rejected"] += 1

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        time.sleep(0.01)  # Let threads try to acquire
        for t in threads:
            t.join()

        assert results["accepted"] + results["rejected"] == 4
        assert results["rejected"] >= 1  # At least one was rejected

    def test_zero_active_after_all_complete(self, m):
        bh = m.BulkheadPool("svc", max_concurrent_calls=3)
        for _ in range(3):
            with bh:
                pass
        assert bh.active_calls == 0


# ---------------------------------------------------------------------------
# Pattern 3 — BurstAwareRateLimiter
# ---------------------------------------------------------------------------


class TestBurstAwareRateLimiter:

    def test_acquires_within_burst(self, m):
        limiter = m.BurstAwareRateLimiter(refill_rate=1.0, burst_capacity=5.0)
        for _ in range(5):
            limiter.acquire("tenant-1")  # Should not raise

    def test_exceeds_burst_raises_rate_limit(self, m):
        limiter = m.BurstAwareRateLimiter(refill_rate=0.01, burst_capacity=3.0)
        for _ in range(3):
            limiter.acquire("tenant-2")
        with pytest.raises(m.RateLimitExceeded):
            limiter.acquire("tenant-2")

    def test_different_identities_independent(self, m):
        limiter = m.BurstAwareRateLimiter(refill_rate=0.01, burst_capacity=1.0)
        limiter.acquire("tenant-A")
        with pytest.raises(m.RateLimitExceeded):
            limiter.acquire("tenant-A")
        # tenant-B is independent — should succeed
        limiter.acquire("tenant-B")

    def test_tokens_available_decreases_on_acquire(self, m):
        limiter = m.BurstAwareRateLimiter(refill_rate=0.01, burst_capacity=10.0)
        before = limiter.tokens_available("t1")
        limiter.acquire("t1")
        after = limiter.tokens_available("t1")
        assert after < before

    def test_refill_over_time(self, m):
        limiter = m.BurstAwareRateLimiter(refill_rate=100.0, burst_capacity=5.0)
        for _ in range(5):
            limiter.acquire("t2")
        # After exhaustion, wait briefly for refill
        time.sleep(0.05)  # 0.05s × 100 tokens/s = 5 new tokens
        tokens = limiter.tokens_available("t2")
        assert tokens > 0

    def test_burst_capacity_is_upper_bound(self, m):
        limiter = m.BurstAwareRateLimiter(refill_rate=1000.0, burst_capacity=5.0)
        # Even after long wait, tokens should not exceed burst_capacity
        time.sleep(0.01)
        tokens = limiter.tokens_available("t3")
        assert tokens <= 5.0

    def test_thread_safe_concurrent_acquires(self, m):
        limiter = m.BurstAwareRateLimiter(refill_rate=0.0, burst_capacity=10.0)
        accepted = []
        rejected = []
        lock = threading.Lock()

        def acquire():
            try:
                limiter.acquire("shared")
                with lock:
                    accepted.append(1)
            except m.RateLimitExceeded:
                with lock:
                    rejected.append(1)

        threads = [threading.Thread(target=acquire) for _ in range(15)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(accepted) == 10
        assert len(rejected) == 5


# ---------------------------------------------------------------------------
# Pattern 4 — JitteredRetryPolicy
# ---------------------------------------------------------------------------


class TestJitteredRetryPolicy:

    def test_success_on_first_attempt(self, m):
        retry = m.JitteredRetryPolicy(m.RetryConfig(max_attempts=3, base_delay=0.001))
        result = retry.execute(lambda: "ok")
        assert result == "ok"

    def test_retries_and_succeeds(self, m):
        attempts = [0]
        retry = m.JitteredRetryPolicy(m.RetryConfig(max_attempts=4, base_delay=0.001, max_delay=0.01))

        def flaky():
            attempts[0] += 1
            if attempts[0] < 3:
                raise ConnectionError("transient")
            return "success"

        result = retry.execute(flaky)
        assert result == "success"
        assert attempts[0] == 3

    def test_raises_after_max_attempts(self, m):
        retry = m.JitteredRetryPolicy(m.RetryConfig(max_attempts=3, base_delay=0.001, max_delay=0.005))
        with pytest.raises(RuntimeError, match="always fails"):
            retry.execute(lambda: (_ for _ in ()).throw(RuntimeError("always fails")))

    def test_non_retryable_exception_not_retried(self, m):
        attempts = [0]
        config = m.RetryConfig(
            max_attempts=5,
            base_delay=0.001,
            non_retryable={ValueError},
        )
        retry = m.JitteredRetryPolicy(config)

        def fn():
            attempts[0] += 1
            raise ValueError("non-retryable")

        with pytest.raises(ValueError):
            retry.execute(fn)
        assert attempts[0] == 1  # Not retried

    def test_circuit_breaker_open_not_retried(self, m):
        cb = _fast_cb(m, wait=5.0)  # Long wait so it stays OPEN
        # Trip the circuit
        for _ in range(4):
            try:
                cb.call(lambda: (_ for _ in ()).throw(Exception("fail")))
            except Exception:
                pass

        retry = m.JitteredRetryPolicy(m.RetryConfig(max_attempts=5, base_delay=0.001))
        attempts = [0]

        def fn():
            attempts[0] += 1
            return cb.call(lambda: "ok")

        with pytest.raises(m.CircuitBreakerOpenError):
            retry.execute(fn)
        assert attempts[0] == 1  # CircuitBreakerOpenError stops retries

    def test_execute_with_breaker_success(self, m):
        cb = _fast_cb(m)
        retry = m.JitteredRetryPolicy(m.RetryConfig(max_attempts=3, base_delay=0.001))
        result = retry.execute_with_breaker(lambda: "via breaker", cb)
        assert result == "via breaker"

    def test_execute_with_breaker_retries_transient(self, m):
        cb = _fast_cb(m, min_calls=10, window=10)  # Hard to trip
        attempts = [0]
        retry = m.JitteredRetryPolicy(m.RetryConfig(max_attempts=4, base_delay=0.001, max_delay=0.01))

        def flaky():
            attempts[0] += 1
            if attempts[0] < 3:
                raise IOError("transient io")
            return "recovered"

        result = retry.execute_with_breaker(flaky, cb)
        assert result == "recovered"

    def test_max_attempts_one_means_no_retries(self, m):
        attempts = [0]
        retry = m.JitteredRetryPolicy(m.RetryConfig(max_attempts=1, base_delay=0.001))

        def fn():
            attempts[0] += 1
            raise RuntimeError("fail")

        with pytest.raises(RuntimeError):
            retry.execute(fn)
        assert attempts[0] == 1

    def test_retry_config_defaults(self, m):
        config = m.RetryConfig()
        assert config.max_attempts == 3
        assert config.base_delay == 0.1
        assert config.max_delay == 30.0
