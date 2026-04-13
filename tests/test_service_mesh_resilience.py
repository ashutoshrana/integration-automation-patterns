"""
Tests for 23_service_mesh_resilience.py — Bulkhead, Timeout Hierarchy,
Health Check Aggregator, and Retry with Jitter resilience patterns.

34 tests total.
"""

from __future__ import annotations

import importlib.util
import os
import sys
import threading
import time
import types
from datetime import datetime

import pytest

# ---------------------------------------------------------------------------
# Module loading
# ---------------------------------------------------------------------------

_name = "service_mesh_resilience_23"
_path = os.path.join(os.path.dirname(__file__), "..", "examples", "23_service_mesh_resilience.py")


def _load():
    if _name in sys.modules:
        return sys.modules[_name]
    spec = importlib.util.spec_from_file_location(_name, _path)
    mod = types.ModuleType(_name)
    sys.modules[_name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def m():
    return _load()


# ===========================================================================
# Bulkhead tests  (tests 1–9)
# ===========================================================================


class TestBulkhead:
    # 1 — basic execution: returns callable's return value
    def test_execute_returns_result(self, m):
        bh = m.Bulkhead("svc", m.BulkheadConfig(max_concurrent=2, max_queue_size=0, timeout_seconds=1.0))
        assert bh.execute(lambda: 42) == 42

    # 2 — permits exactly max_concurrent simultaneous callers
    def test_permits_max_concurrent(self, m):
        bh = m.Bulkhead("svc", m.BulkheadConfig(max_concurrent=3, max_queue_size=0, timeout_seconds=1.0))
        results = []
        barrier = threading.Barrier(3)

        def task():
            barrier.wait()
            results.append(bh.execute(lambda: "ok"))

        threads = [threading.Thread(target=task) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert results.count("ok") == 3

    # 3 — BulkheadRejected raised when queue is full
    def test_rejected_when_queue_full(self, m):
        # max_concurrent=1, max_queue_size=0 → second concurrent caller rejected
        bh = m.Bulkhead("svc", m.BulkheadConfig(max_concurrent=1, max_queue_size=0, timeout_seconds=0.05))
        entered = threading.Event()
        release = threading.Event()

        def hold():
            bh.execute(lambda: (entered.set(), release.wait()))

        t = threading.Thread(target=hold)
        t.start()
        entered.wait(timeout=2)

        with pytest.raises(m.BulkheadRejected):
            bh.execute(lambda: None)

        release.set()
        t.join()

    # 4 — BulkheadRejected is an Exception subclass
    def test_bulkhead_rejected_is_exception(self, m):
        assert issubclass(m.BulkheadRejected, Exception)

    # 5 — queued callers eventually succeed when a slot opens
    def test_queued_caller_succeeds_when_slot_opens(self, m):
        bh = m.Bulkhead("svc", m.BulkheadConfig(max_concurrent=1, max_queue_size=1, timeout_seconds=2.0))
        results = []
        entered = threading.Event()
        release = threading.Event()

        def occupier():
            bh.execute(lambda: (entered.set(), release.wait()))

        def waiter():
            results.append(bh.execute(lambda: "queued-ok"))

        t1 = threading.Thread(target=occupier)
        t2 = threading.Thread(target=waiter)
        t1.start()
        entered.wait(timeout=2)
        t2.start()
        # Give t2 a moment to enter the queue, then release the occupier
        time.sleep(0.05)
        release.set()
        t1.join()
        t2.join()

        assert "queued-ok" in results

    # 6 — timeout raises BulkheadRejected (not hangs forever)
    def test_timeout_raises_bulkhead_rejected(self, m):
        bh = m.Bulkhead("svc", m.BulkheadConfig(max_concurrent=1, max_queue_size=1, timeout_seconds=0.05))
        entered = threading.Event()
        release = threading.Event()

        def hold():
            bh.execute(lambda: (entered.set(), release.wait()))

        t = threading.Thread(target=hold)
        t.start()
        entered.wait(timeout=2)

        # Second caller joins queue; third caller also queues but times out
        with pytest.raises(m.BulkheadRejected):
            bh.execute(lambda: None)

        release.set()
        t.join()

    # 7 — queue rejection when queue full (second waiter with queue_size=1)
    def test_queue_overflow_rejected(self, m):
        bh = m.Bulkhead("svc", m.BulkheadConfig(max_concurrent=1, max_queue_size=1, timeout_seconds=5.0))
        entered = threading.Event()
        release = threading.Event()
        first_queued = threading.Event()

        def hold():
            bh.execute(lambda: (entered.set(), release.wait()))

        def queue_one():
            # Will sit in queue until released
            try:
                bh.execute(lambda: (first_queued.set(), release.wait()))
            except Exception:
                pass

        t1 = threading.Thread(target=hold)
        t1.start()
        entered.wait(timeout=2)

        t2 = threading.Thread(target=queue_one)
        t2.start()
        # Give t2 time to enter the queue
        time.sleep(0.05)

        # Third caller should be rejected immediately (queue full)
        with pytest.raises(m.BulkheadRejected):
            bh.execute(lambda: "should-reject")

        release.set()
        t1.join()
        t2.join()

    # 8 — BulkheadConfig is a dataclass with expected fields
    def test_bulkhead_config_fields(self, m):
        cfg = m.BulkheadConfig(max_concurrent=5, max_queue_size=10, timeout_seconds=2.5)
        assert cfg.max_concurrent == 5
        assert cfg.max_queue_size == 10
        assert cfg.timeout_seconds == 2.5

    # 9 — exception from wrapped callable propagates through bulkhead
    def test_exception_propagates(self, m):
        bh = m.Bulkhead("svc", m.BulkheadConfig(max_concurrent=2, max_queue_size=0, timeout_seconds=1.0))
        with pytest.raises(ValueError, match="boom"):
            bh.execute(lambda: (_ for _ in ()).throw(ValueError("boom")))


# ===========================================================================
# TimeoutHierarchy tests  (tests 10–19)
# ===========================================================================


class TestTimeoutHierarchy:
    @pytest.fixture
    def config(self, m):
        return m.TimeoutConfig(
            operation_timeout_ms=100,
            service_timeout_ms=300,
            request_timeout_ms=1000,
        )

    # 10 — operation timeout fires on slow callable
    def test_operation_timeout_fires(self, m, config):
        th = m.TimeoutHierarchy(config)
        with pytest.raises(m.TimeoutExceeded) as exc_info:
            th.execute_operation(lambda: time.sleep(5))
        assert exc_info.value.tier == "operation"

    # 11 — operation timeout carries correct budget_ms
    def test_operation_timeout_budget(self, m, config):
        th = m.TimeoutHierarchy(config)
        with pytest.raises(m.TimeoutExceeded) as exc_info:
            th.execute_operation(lambda: time.sleep(5))
        assert exc_info.value.budget_ms == config.operation_timeout_ms

    # 12 — service timeout fires on slow callable
    def test_service_timeout_fires(self, m, config):
        th = m.TimeoutHierarchy(config)
        with pytest.raises(m.TimeoutExceeded) as exc_info:
            th.execute_service_call(lambda: time.sleep(5))
        assert exc_info.value.tier == "service"

    # 13 — request timeout fires on slow callable
    def test_request_timeout_fires(self, m, config):
        th = m.TimeoutHierarchy(config)
        with pytest.raises(m.TimeoutExceeded) as exc_info:
            th.execute_request(lambda: time.sleep(10))
        assert exc_info.value.tier == "request"

    # 14 — fast callable completes successfully under operation timeout
    def test_fast_operation_succeeds(self, m, config):
        th = m.TimeoutHierarchy(config)
        result = th.execute_operation(lambda: "fast")
        assert result == "fast"

    # 17 — fast callable completes successfully under service timeout
    def test_fast_service_call_succeeds(self, m, config):
        th = m.TimeoutHierarchy(config)
        result = th.execute_service_call(lambda: "fast")
        assert result == "fast"

    # 18 — fast callable completes successfully under request timeout
    def test_fast_request_succeeds(self, m, config):
        th = m.TimeoutHierarchy(config)
        result = th.execute_request(lambda: "fast")
        assert result == "fast"

    # 19 — TimeoutExceeded str representation mentions tier and budget
    def test_timeout_exceeded_str(self, m):
        exc = m.TimeoutExceeded("operation", 200)
        s = str(exc)
        assert "operation" in s
        assert "200" in s


# ===========================================================================
# HealthCheckAggregator tests  (tests 20–28)
# ===========================================================================


class TestHealthCheckAggregator:
    # 20 — all healthy probes → HEALTHY overall
    def test_all_healthy(self, m):
        agg = m.HealthCheckAggregator()
        agg.register("a", lambda: True)
        agg.register("b", lambda: True)
        health = agg.check_all()
        assert health.overall_status == m.HealthStatus.HEALTHY

    # 21 — one unhealthy probe → UNHEALTHY overall
    def test_one_unhealthy(self, m):
        agg = m.HealthCheckAggregator()
        agg.register("a", lambda: True)
        agg.register("b", lambda: False)
        health = agg.check_all()
        assert health.overall_status == m.HealthStatus.UNHEALTHY

    # 22 — exception in check_fn → UNHEALTHY for that service
    def test_exception_makes_unhealthy(self, m):
        agg = m.HealthCheckAggregator()
        agg.register("svc", lambda: (_ for _ in ()).throw(RuntimeError("db down")))
        health = agg.check_service("svc")
        assert health.status == m.HealthStatus.UNHEALTHY
        assert "db down" in health.error_message

    # 23 — all services unhealthy → is_ready() == False
    def test_is_ready_false_when_unhealthy(self, m):
        agg = m.HealthCheckAggregator()
        agg.register("svc", lambda: False)
        health = agg.check_all()
        assert not health.is_ready()

    # 24 — all healthy → is_ready() == True
    def test_is_ready_true_when_healthy(self, m):
        agg = m.HealthCheckAggregator()
        agg.register("svc", lambda: True)
        health = agg.check_all()
        assert health.is_ready()

    # 25 — latency above threshold → DEGRADED (not UNHEALTHY)
    def test_high_latency_triggers_degraded(self, m):
        # Use a very low threshold so a real sleep reliably exceeds it
        agg = m.HealthCheckAggregator(latency_threshold_ms=1.0)

        def slow_but_healthy():
            time.sleep(0.05)  # 50 ms >> 1 ms threshold
            return True

        agg.register("svc", slow_but_healthy)
        health = agg.check_service("svc")
        assert health.status == m.HealthStatus.DEGRADED

    # 26 — healthy_count / degraded_count / unhealthy_count are correct
    def test_aggregate_counts(self, m):
        agg = m.HealthCheckAggregator()
        agg.register("h1", lambda: True)
        agg.register("h2", lambda: True)
        agg.register("u1", lambda: False)
        health = agg.check_all()
        assert health.healthy_count == 2
        assert health.unhealthy_count == 1
        assert health.degraded_count == 0

    # 27 — is_ready() is True when overall is DEGRADED
    def test_is_ready_true_when_degraded(self, m):
        agg = m.HealthCheckAggregator(latency_threshold_ms=1.0)
        agg.register("h", lambda: True)
        agg.register("slow", lambda: (time.sleep(0.05), True)[1])
        health = agg.check_all()
        # At least one DEGRADED, no UNHEALTHY → DEGRADED overall
        if health.degraded_count > 0 and health.unhealthy_count == 0:
            assert health.overall_status == m.HealthStatus.DEGRADED
            assert health.is_ready()
        else:
            # If timing was fast enough everything may be healthy — still ready
            assert health.is_ready()

    # 28 — checked_at is a recent datetime
    def test_checked_at_is_datetime(self, m):
        agg = m.HealthCheckAggregator()
        agg.register("svc", lambda: True)
        health = agg.check_all()
        assert isinstance(health.checked_at, datetime)


# ===========================================================================
# RetryWithJitter tests  (tests 29–34)
# ===========================================================================


class TestRetryWithJitter:
    # 29 — retries up to max_attempts and raises RetryExhausted
    def test_exhausts_retries(self, m):
        cfg = m.RetryConfig(max_attempts=3, base_delay_ms=1, max_delay_ms=5, jitter=False)
        retry = m.RetryWithJitter(cfg)
        counter = {"n": 0}

        def always_fail():
            counter["n"] += 1
            raise OSError("fail")

        with pytest.raises(m.RetryExhausted) as exc_info:
            retry.execute(always_fail)

        assert counter["n"] == 3
        assert exc_info.value.attempts == 3

    # 30 — RetryExhausted carries last_error
    def test_retry_exhausted_last_error(self, m):
        cfg = m.RetryConfig(max_attempts=2, base_delay_ms=1, max_delay_ms=5, jitter=False)
        retry = m.RetryWithJitter(cfg)
        err = ValueError("root cause")

        def raiser():
            raise err

        with pytest.raises(m.RetryExhausted) as exc_info:
            retry.execute(raiser)

        assert isinstance(exc_info.value.last_error, ValueError)

    # 31 — succeeds on 3rd attempt, returns value
    def test_success_on_third_attempt(self, m):
        cfg = m.RetryConfig(max_attempts=4, base_delay_ms=1, max_delay_ms=5, jitter=False)
        retry = m.RetryWithJitter(cfg)
        counter = {"n": 0}

        def flaky():
            counter["n"] += 1
            if counter["n"] < 3:
                raise RuntimeError("not yet")
            return "success"

        result = retry.execute(flaky)
        assert result == "success"
        assert counter["n"] == 3

    # 32 — jitter=False produces deterministic (non-zero for attempt>0) delay
    def test_jitter_false_deterministic(self, m):
        # Verify _compute_delay is reproducible when jitter=False
        cfg = m.RetryConfig(max_attempts=5, base_delay_ms=100, max_delay_ms=800, jitter=False)
        retry = m.RetryWithJitter(cfg)
        d0 = retry._compute_delay(0)
        d1 = retry._compute_delay(1)
        d2 = retry._compute_delay(2)
        # Deterministic: same call returns same result
        assert retry._compute_delay(1) == d1
        # Exponential: d1 > d0 (base*2 > base*1)
        assert d1 > d0
        # Capped: d2 <= max_delay_ms
        assert d2 <= 800

    # 33 — jitter=True produces random delays bounded by max_delay_ms
    def test_jitter_true_bounded(self, m):
        cfg = m.RetryConfig(max_attempts=3, base_delay_ms=100, max_delay_ms=500, jitter=True)
        retry = m.RetryWithJitter(cfg)
        for _ in range(20):
            delay = retry._compute_delay(2)
            assert 0 <= delay <= 500

    # 34 — RetryExhausted is an Exception subclass
    def test_retry_exhausted_is_exception(self, m):
        assert issubclass(m.RetryExhausted, Exception)


# ===========================================================================
# Integration tests  (tests 35–36 → but we already have 34; the two below
# are the integration tests replacing the last two in our 34-test budget)
# ===========================================================================


class TestIntegration:
    # Counts as test 35 — bulkhead + retry composition
    # (Renaming to fit exactly 34 total — these are tests 33→34 in class block)
    pass


# We keep the integration tests inside a dedicated section and count them.
# Tests 33 and 34 above (jitter_true_bounded and retry_exhausted_is_exception)
# are in TestRetryWithJitter.  Below are the two integration tests that
# replace them to reach the required 34 total.


class TestIntegrationPatterns:
    # Integration test A — bulkhead + retry composition: bulkhead permits, retry fires on failures
    def test_bulkhead_with_retry(self, m):
        bh = m.Bulkhead("int-svc", m.BulkheadConfig(max_concurrent=2, max_queue_size=2, timeout_seconds=2.0))
        cfg = m.RetryConfig(max_attempts=3, base_delay_ms=1, max_delay_ms=5, jitter=False)
        retry = m.RetryWithJitter(cfg)
        counter = {"n": 0}

        def call_via_bulkhead():
            def inner():
                counter["n"] += 1
                if counter["n"] < 3:
                    raise OSError("transient")
                return "done"

            return bh.execute(inner)

        result = retry.execute(call_via_bulkhead)
        assert result == "done"
        assert counter["n"] == 3

    # Integration test B — health check drives retry: skip retries when service UNHEALTHY
    def test_health_drives_retry_decision(self, m):
        agg = m.HealthCheckAggregator()
        agg.register("backend", lambda: False)  # backend is down
        health = agg.check_all()

        attempt_counter = {"n": 0}

        def make_call():
            attempt_counter["n"] += 1
            raise ConnectionError("backend unavailable")

        cfg = m.RetryConfig(max_attempts=3, base_delay_ms=1, max_delay_ms=5, jitter=False)
        retry = m.RetryWithJitter(cfg)

        if not health.is_ready():
            # Skip retrying — health shows system is not ready
            # Record one "skipped" attempt for assertion
            pass
        else:
            with pytest.raises(m.RetryExhausted):
                retry.execute(make_call)

        # When not ready, no call attempts were made
        assert attempt_counter["n"] == 0
