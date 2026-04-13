"""
Tests for 37_rate_limiting_patterns.py

Covers all five patterns (SlidingWindowRateLimiter, TokenBucketLimiter,
AdaptiveThrottler, QuotaManager, CircuitBreakerRateLimiter) with 75 test
cases using only the standard library.  No external dependencies required.
"""

from __future__ import annotations

import importlib.util
import sys
import threading
import time
import types
from pathlib import Path

import pytest

_MOD_PATH = Path(__file__).parent.parent / "examples" / "37_rate_limiting_patterns.py"


def _load_module() -> types.ModuleType:
    module_name = "rate_limiting_patterns_37"
    if module_name in sys.modules:
        return sys.modules[module_name]
    spec = importlib.util.spec_from_file_location(module_name, _MOD_PATH)
    mod = types.ModuleType(module_name)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


@pytest.fixture(scope="module")
def mod() -> types.ModuleType:
    return _load_module()


# ===========================================================================
# SlidingWindowRateLimiter
# ===========================================================================


class TestSlidingWindowRateLimiter:
    # --- basic allow/deny ---

    def test_allows_up_to_max(self, mod):
        lim = mod.SlidingWindowRateLimiter(max_requests=3, window_seconds=5.0)
        results = [lim.allow("c") for _ in range(3)]
        assert all(results)

    def test_denies_over_max(self, mod):
        lim = mod.SlidingWindowRateLimiter(max_requests=2, window_seconds=5.0)
        lim.allow("c")
        lim.allow("c")
        assert lim.allow("c") is False

    def test_different_clients_are_independent(self, mod):
        lim = mod.SlidingWindowRateLimiter(max_requests=1, window_seconds=5.0)
        assert lim.allow("a") is True
        assert lim.allow("b") is True
        assert lim.allow("a") is False

    def test_allows_after_window_expires(self, mod):
        lim = mod.SlidingWindowRateLimiter(max_requests=1, window_seconds=0.05)
        assert lim.allow("c") is True
        assert lim.allow("c") is False
        time.sleep(0.07)
        assert lim.allow("c") is True

    def test_allow_returns_bool_true(self, mod):
        lim = mod.SlidingWindowRateLimiter(max_requests=5, window_seconds=1.0)
        result = lim.allow("x")
        assert result is True

    def test_allow_returns_bool_false(self, mod):
        lim = mod.SlidingWindowRateLimiter(max_requests=1, window_seconds=5.0)
        lim.allow("x")
        result = lim.allow("x")
        assert result is False

    # --- remaining ---

    def test_remaining_full_at_start(self, mod):
        lim = mod.SlidingWindowRateLimiter(max_requests=5, window_seconds=5.0)
        assert lim.remaining("new-client") == 5

    def test_remaining_decrements(self, mod):
        lim = mod.SlidingWindowRateLimiter(max_requests=5, window_seconds=5.0)
        lim.allow("c")
        lim.allow("c")
        assert lim.remaining("c") == 3

    def test_remaining_zero_when_exhausted(self, mod):
        lim = mod.SlidingWindowRateLimiter(max_requests=2, window_seconds=5.0)
        lim.allow("c")
        lim.allow("c")
        assert lim.remaining("c") == 0

    def test_remaining_restores_after_window(self, mod):
        lim = mod.SlidingWindowRateLimiter(max_requests=2, window_seconds=0.05)
        lim.allow("c")
        lim.allow("c")
        time.sleep(0.07)
        assert lim.remaining("c") == 2

    # --- reset_at ---

    def test_reset_at_returns_float(self, mod):
        lim = mod.SlidingWindowRateLimiter(max_requests=5, window_seconds=1.0)
        lim.allow("c")
        ts = lim.reset_at("c")
        assert isinstance(ts, float)

    def test_reset_at_is_future(self, mod):
        lim = mod.SlidingWindowRateLimiter(max_requests=5, window_seconds=2.0)
        lim.allow("c")
        assert lim.reset_at("c") > time.time()

    def test_reset_at_empty_client_returns_now(self, mod):
        lim = mod.SlidingWindowRateLimiter(max_requests=5, window_seconds=2.0)
        ts = lim.reset_at("no-requests-yet")
        assert abs(ts - time.time()) < 0.5  # within half a second

    # --- constructor validation ---

    def test_invalid_max_requests_raises(self, mod):
        with pytest.raises(ValueError):
            mod.SlidingWindowRateLimiter(max_requests=0, window_seconds=1.0)

    def test_invalid_window_raises(self, mod):
        with pytest.raises(ValueError):
            mod.SlidingWindowRateLimiter(max_requests=5, window_seconds=0.0)

    # --- thread safety ---

    def test_concurrent_allow_does_not_exceed_limit(self, mod):
        lim = mod.SlidingWindowRateLimiter(max_requests=10, window_seconds=10.0)
        results = []
        lock = threading.Lock()

        def worker():
            ok = lim.allow("shared")
            with lock:
                results.append(ok)

        threads = [threading.Thread(target=worker) for _ in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert results.count(True) == 10
        assert results.count(False) == 10


# ===========================================================================
# TokenBucketLimiter
# ===========================================================================


class TestTokenBucketLimiter:
    # --- basic consume ---

    def test_consume_succeeds_within_capacity(self, mod):
        tb = mod.TokenBucketLimiter(rate=10.0, capacity=10.0)
        assert tb.consume("c") is True

    def test_consume_fails_when_empty(self, mod):
        tb = mod.TokenBucketLimiter(rate=1.0, capacity=5.0)
        for _ in range(5):
            tb.consume("c")
        assert tb.consume("c") is False

    def test_consume_custom_tokens(self, mod):
        tb = mod.TokenBucketLimiter(rate=1.0, capacity=10.0)
        assert tb.consume("c", tokens=7.0) is True
        assert tb.consume("c", tokens=4.0) is False

    def test_consume_zero_tokens_raises(self, mod):
        tb = mod.TokenBucketLimiter(rate=1.0, capacity=10.0)
        with pytest.raises(ValueError):
            tb.consume("c", tokens=0.0)

    def test_refill_over_time(self, mod):
        tb = mod.TokenBucketLimiter(rate=100.0, capacity=10.0)
        for _ in range(10):
            tb.consume("c")
        assert tb.consume("c") is False
        time.sleep(0.05)  # 5 tokens refilled at 100/s
        assert tb.consume("c") is True

    def test_tokens_capped_at_capacity(self, mod):
        tb = mod.TokenBucketLimiter(rate=1000.0, capacity=5.0)
        time.sleep(0.01)
        assert tb.available_tokens("fresh") <= 5.0

    def test_different_clients_independent(self, mod):
        tb = mod.TokenBucketLimiter(rate=1.0, capacity=2.0)
        tb.consume("a")
        tb.consume("a")
        assert tb.consume("a") is False
        assert tb.consume("b") is True

    # --- available_tokens ---

    def test_available_tokens_starts_at_capacity(self, mod):
        tb = mod.TokenBucketLimiter(rate=1.0, capacity=8.0)
        assert tb.available_tokens("new") == pytest.approx(8.0, abs=0.1)

    def test_available_tokens_decreases_after_consume(self, mod):
        tb = mod.TokenBucketLimiter(rate=1.0, capacity=10.0)
        tb.consume("c", tokens=3.0)
        assert tb.available_tokens("c") == pytest.approx(7.0, abs=0.1)

    # --- constructor validation ---

    def test_invalid_rate_raises(self, mod):
        with pytest.raises(ValueError):
            mod.TokenBucketLimiter(rate=0.0, capacity=10.0)

    def test_invalid_capacity_raises(self, mod):
        with pytest.raises(ValueError):
            mod.TokenBucketLimiter(rate=1.0, capacity=0.0)

    # --- thread safety ---

    def test_concurrent_consume_does_not_exceed_capacity(self, mod):
        tb = mod.TokenBucketLimiter(rate=0.01, capacity=5.0)
        successes = []
        lock = threading.Lock()

        def worker():
            ok = tb.consume("shared")
            with lock:
                successes.append(ok)

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert successes.count(True) == 5


# ===========================================================================
# AdaptiveThrottler
# ===========================================================================


class TestAdaptiveThrottler:
    # --- current_rps ---

    def test_initial_rps(self, mod):
        at = mod.AdaptiveThrottler(initial_rps=10.0, min_rps=1.0, max_rps=50.0)
        assert at.current_rps == pytest.approx(10.0)

    def test_record_success_increases_rps(self, mod):
        at = mod.AdaptiveThrottler(initial_rps=10.0, min_rps=1.0, max_rps=50.0, increase_step=2.0)
        at.record_success()
        assert at.current_rps == pytest.approx(12.0)

    def test_record_failure_halves_rps(self, mod):
        at = mod.AdaptiveThrottler(initial_rps=10.0, min_rps=1.0, max_rps=50.0)
        at.record_failure()
        assert at.current_rps == pytest.approx(5.0)

    def test_record_failure_floors_at_min(self, mod):
        at = mod.AdaptiveThrottler(initial_rps=2.0, min_rps=2.0, max_rps=50.0)
        at.record_failure()
        assert at.current_rps == pytest.approx(2.0)

    def test_record_success_caps_at_max(self, mod):
        at = mod.AdaptiveThrottler(initial_rps=50.0, min_rps=1.0, max_rps=50.0, increase_step=5.0)
        at.record_success()
        assert at.current_rps == pytest.approx(50.0)

    def test_multiple_failures_floor(self, mod):
        at = mod.AdaptiveThrottler(initial_rps=16.0, min_rps=1.0, max_rps=50.0)
        for _ in range(10):
            at.record_failure()
        assert at.current_rps == pytest.approx(1.0)

    def test_aimd_cycle(self, mod):
        at = mod.AdaptiveThrottler(initial_rps=10.0, min_rps=1.0, max_rps=50.0, increase_step=1.0)
        at.record_failure()  # → 5.0
        assert at.current_rps == pytest.approx(5.0)
        at.record_success()  # → 6.0
        assert at.current_rps == pytest.approx(6.0)
        at.record_success()  # → 7.0
        assert at.current_rps == pytest.approx(7.0)

    # --- allow ---

    def test_allow_returns_bool(self, mod):
        at = mod.AdaptiveThrottler(initial_rps=100.0, min_rps=1.0, max_rps=200.0)
        result = at.allow()
        assert isinstance(result, bool)

    def test_allow_passes_at_high_rps(self, mod):
        at = mod.AdaptiveThrottler(initial_rps=1000.0, min_rps=1.0, max_rps=2000.0)
        # Should get at least one allowed request immediately
        assert at.allow() is True

    def test_allow_throttles_at_low_rps(self, mod):
        at = mod.AdaptiveThrottler(initial_rps=0.01, min_rps=0.01, max_rps=1.0)
        # Drain the bucket
        at.allow()
        # Subsequent calls should be denied
        denied = sum(1 for _ in range(10) if not at.allow())
        assert denied > 0

    # --- constructor validation ---

    def test_invalid_min_rps_raises(self, mod):
        with pytest.raises(ValueError):
            mod.AdaptiveThrottler(initial_rps=5.0, min_rps=0.0, max_rps=10.0)

    def test_invalid_max_lt_min_raises(self, mod):
        with pytest.raises(ValueError):
            mod.AdaptiveThrottler(initial_rps=5.0, min_rps=10.0, max_rps=5.0)

    def test_initial_out_of_bounds_raises(self, mod):
        with pytest.raises(ValueError):
            mod.AdaptiveThrottler(initial_rps=20.0, min_rps=1.0, max_rps=10.0)


# ===========================================================================
# QuotaManager
# ===========================================================================


class TestQuotaManager:
    # --- consume ---

    def test_consume_within_all_tiers(self, mod):
        qm = mod.QuotaManager(per_second=5, per_minute=100, per_day=1000)
        assert qm.consume("c") is True

    def test_consume_denied_when_second_exhausted(self, mod):
        qm = mod.QuotaManager(per_second=2, per_minute=100, per_day=1000)
        qm.consume("c")
        qm.consume("c")
        assert qm.consume("c") is False

    def test_consume_denied_when_minute_exhausted(self, mod):
        qm = mod.QuotaManager(per_second=100, per_minute=2, per_day=1000)
        qm.consume("c")
        qm.consume("c")
        assert qm.consume("c") is False

    def test_consume_denied_when_day_exhausted(self, mod):
        qm = mod.QuotaManager(per_second=100, per_minute=100, per_day=2)
        qm.consume("c")
        qm.consume("c")
        assert qm.consume("c") is False

    def test_consume_atomic_no_partial_decrement(self, mod):
        """If day tier is full, second and minute should not be decremented."""
        qm = mod.QuotaManager(per_second=10, per_minute=10, per_day=1)
        qm.consume("c")  # day now exhausted
        # Attempt again — should fail without touching second/minute
        ok = qm.consume("c")
        assert ok is False
        # Reset and verify second/minute counters are still at 1 (one success)
        status = qm.status("c")
        assert status["second"][0] == 1
        assert status["minute"][0] == 1

    def test_different_clients_independent(self, mod):
        qm = mod.QuotaManager(per_second=1, per_minute=10, per_day=100)
        qm.consume("a")
        assert qm.consume("a") is False
        assert qm.consume("b") is True

    # --- status ---

    def test_status_keys(self, mod):
        qm = mod.QuotaManager(per_second=5, per_minute=100, per_day=1000)
        s = qm.status("c")
        assert set(s.keys()) == {"second", "minute", "day"}

    def test_status_used_increments(self, mod):
        qm = mod.QuotaManager(per_second=10, per_minute=100, per_day=1000)
        qm.consume("c")
        qm.consume("c")
        s = qm.status("c")
        assert s["second"][0] == 2
        assert s["minute"][0] == 2
        assert s["day"][0] == 2

    def test_status_limits_correct(self, mod):
        qm = mod.QuotaManager(per_second=3, per_minute=60, per_day=500)
        s = qm.status("new")
        assert s["second"][1] == 3
        assert s["minute"][1] == 60
        assert s["day"][1] == 500

    def test_status_reset_at_is_future(self, mod):
        qm = mod.QuotaManager(per_second=5, per_minute=100, per_day=1000)
        qm.consume("c")
        s = qm.status("c")
        now = time.time()
        for tier_data in s.values():
            assert tier_data[2] >= now

    # --- reset ---

    def test_reset_clears_counters(self, mod):
        qm = mod.QuotaManager(per_second=2, per_minute=10, per_day=100)
        qm.consume("c")
        qm.consume("c")
        assert qm.consume("c") is False
        qm.reset("c")
        assert qm.consume("c") is True

    def test_reset_nonexistent_client_safe(self, mod):
        qm = mod.QuotaManager(per_second=5, per_minute=100, per_day=1000)
        qm.reset("ghost")  # should not raise

    # --- constructor validation ---

    def test_invalid_per_second_raises(self, mod):
        with pytest.raises(ValueError):
            mod.QuotaManager(per_second=0, per_minute=10, per_day=100)

    def test_invalid_per_minute_raises(self, mod):
        with pytest.raises(ValueError):
            mod.QuotaManager(per_second=1, per_minute=0, per_day=100)

    def test_invalid_per_day_raises(self, mod):
        with pytest.raises(ValueError):
            mod.QuotaManager(per_second=1, per_minute=10, per_day=0)

    # --- thread safety ---

    def test_concurrent_consume_respects_limit(self, mod):
        qm = mod.QuotaManager(per_second=5, per_minute=1000, per_day=100000)
        successes = []
        lock = threading.Lock()

        def worker():
            ok = qm.consume("shared")
            with lock:
                successes.append(ok)

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert successes.count(True) == 5


# ===========================================================================
# CircuitBreakerRateLimiter
# ===========================================================================


class TestCircuitBreakerRateLimiter:
    def _make(self, mod, max_requests=10, window=5.0, threshold=3, recovery=60.0):
        return mod.CircuitBreakerRateLimiter(
            max_requests=max_requests,
            window_seconds=window,
            failure_threshold=threshold,
            recovery_timeout=recovery,
        )

    # --- normal operation ---

    def test_initial_state_closed(self, mod):
        cb = self._make(mod)
        assert cb.state == "CLOSED"

    def test_successful_call_returns_value(self, mod):
        cb = self._make(mod)
        result = cb.call(lambda: 42)
        assert result == 42

    def test_state_remains_closed_on_success(self, mod):
        cb = self._make(mod)
        cb.call(lambda: "ok")
        assert cb.state == "CLOSED"

    # --- rate limiting ---

    def test_rate_limit_exceeded_raises(self, mod):
        cb = self._make(mod, max_requests=2, window=10.0)
        cb.call(lambda: None)
        cb.call(lambda: None)
        with pytest.raises(mod.RateLimitExceeded):
            cb.call(lambda: None)

    def test_rate_limit_does_not_open_circuit(self, mod):
        cb = self._make(mod, max_requests=1, window=10.0, threshold=3)
        cb.call(lambda: None)
        try:
            cb.call(lambda: None)
        except mod.RateLimitExceeded:
            pass
        assert cb.state == "CLOSED"

    # --- circuit breaker OPEN ---

    def test_circuit_opens_after_threshold_failures(self, mod):
        cb = self._make(mod, threshold=2)

        def boom():
            raise RuntimeError("fail")

        for _ in range(2):
            try:
                cb.call(boom)
            except RuntimeError:
                pass
        assert cb.state == "OPEN"

    def test_open_circuit_rejects_with_circuit_open(self, mod):
        cb = self._make(mod, threshold=1)

        try:
            cb.call(lambda: (_ for _ in ()).throw(ValueError("err")))
        except ValueError:
            pass
        assert cb.state == "OPEN"
        with pytest.raises(mod.CircuitOpen):
            cb.call(lambda: "probe")

    # --- HALF_OPEN recovery ---

    def test_circuit_transitions_to_half_open_after_timeout(self, mod):
        cb = self._make(mod, threshold=1, recovery=0.05)
        try:
            cb.call(lambda: (_ for _ in ()).throw(RuntimeError()))
        except RuntimeError:
            pass
        assert cb.state == "OPEN"
        time.sleep(0.07)
        # Successful probe should close the circuit
        result = cb.call(lambda: "recovered")
        assert result == "recovered"
        assert cb.state == "CLOSED"

    def test_failed_probe_reopens_circuit(self, mod):
        cb = self._make(mod, threshold=1, recovery=0.05)
        try:
            cb.call(lambda: (_ for _ in ()).throw(RuntimeError("first fail")))
        except RuntimeError:
            pass
        time.sleep(0.07)
        assert cb.state in ("OPEN", "HALF_OPEN")
        # Failed probe
        try:
            cb.call(lambda: (_ for _ in ()).throw(RuntimeError("probe fail")))
        except (RuntimeError, mod.CircuitOpen):
            pass
        assert cb.state == "OPEN"

    def test_success_in_closed_resets_failure_count(self, mod):
        """Interleaved success should reset the failure counter."""
        cb = self._make(mod, threshold=3)

        def boom():
            raise RuntimeError("fail")

        cb.call(lambda: None)  # success → reset
        try:
            cb.call(boom)
        except RuntimeError:
            pass
        try:
            cb.call(boom)
        except RuntimeError:
            pass
        # Only 2 failures since last success — should still be CLOSED
        assert cb.state == "CLOSED"

    # --- exception propagation ---

    def test_fn_exception_propagates(self, mod):
        cb = self._make(mod, threshold=10)
        with pytest.raises(KeyError):
            cb.call(lambda: (_ for _ in ()).throw(KeyError("oops")))

    def test_fn_exception_counted_as_failure(self, mod):
        cb = self._make(mod, threshold=2)
        for _ in range(2):
            try:
                cb.call(lambda: (_ for _ in ()).throw(OSError("io")))
            except OSError:
                pass
        assert cb.state == "OPEN"

    # --- state property ---

    def test_state_returns_string(self, mod):
        cb = self._make(mod)
        assert isinstance(cb.state, str)

    def test_state_values_are_valid(self, mod):
        cb = self._make(mod)
        valid = {"CLOSED", "OPEN", "HALF_OPEN"}
        assert cb.state in valid

    # --- custom client_id ---

    def test_custom_client_id_accepted(self, mod):
        cb = self._make(mod, max_requests=5)
        result = cb.call(lambda: "hi", client_id="tenant-42")
        assert result == "hi"

    # --- RateLimitExceeded and CircuitOpen are importable ---

    def test_rate_limit_exceeded_is_exception(self, mod):
        assert issubclass(mod.RateLimitExceeded, Exception)

    def test_circuit_open_is_exception(self, mod):
        assert issubclass(mod.CircuitOpen, Exception)
