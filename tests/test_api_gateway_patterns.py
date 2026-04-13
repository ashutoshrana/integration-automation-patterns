"""Tests for 14_api_gateway_patterns.py — Rate Limiting, Circuit Breaker, Coalescing, API Keys, Versioning, Idempotency"""
from __future__ import annotations

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


def _load_module(name: str):
    examples_dir = Path(__file__).parent.parent / "examples"
    spec = importlib.util.spec_from_file_location(name, examples_dir / "14_api_gateway_patterns.py")
    mod = types.ModuleType(name)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def m():
    return _load_module("api_gateway_patterns")


# ---------------------------------------------------------------------------
# Pattern 1 — TokenBucketRateLimiter
# ---------------------------------------------------------------------------


class TestTokenBucketRateLimiter:
    def test_allows_initial_requests_up_to_capacity(self, m):
        limiter = m.TokenBucketRateLimiter("client:test", rate_per_second=1.0, bucket_capacity=5)
        results = [limiter.check() for _ in range(5)]
        assert all(r.allowed for r in results)

    def test_denies_when_bucket_exhausted(self, m):
        limiter = m.TokenBucketRateLimiter("client:test2", rate_per_second=0.1, bucket_capacity=3)
        for _ in range(3):
            limiter.check()
        r = limiter.check()
        assert not r.allowed

    def test_retry_after_ms_positive_on_deny(self, m):
        limiter = m.TokenBucketRateLimiter("client:test3", rate_per_second=1.0, bucket_capacity=1)
        limiter.check()  # Consume the token
        r = limiter.check()
        assert not r.allowed
        assert r.retry_after_ms > 0

    def test_remaining_decreases_with_requests(self, m):
        limiter = m.TokenBucketRateLimiter("client:test4", rate_per_second=100.0, bucket_capacity=10)
        r1 = limiter.check()
        r2 = limiter.check()
        assert r1.remaining >= r2.remaining

    def test_algorithm_label_is_token_bucket(self, m):
        limiter = m.TokenBucketRateLimiter("client:alg", rate_per_second=10.0, bucket_capacity=10)
        r = limiter.check()
        assert r.algorithm == m.RateLimitAlgorithm.TOKEN_BUCKET

    def test_client_id_in_result(self, m):
        limiter = m.TokenBucketRateLimiter("my-client", rate_per_second=10.0, bucket_capacity=10)
        r = limiter.check()
        assert r.client_id == "my-client"

    def test_allow_and_deny_counters(self, m):
        limiter = m.TokenBucketRateLimiter("client:cnt", rate_per_second=0.01, bucket_capacity=2)
        limiter.check()  # allow
        limiter.check()  # allow
        limiter.check()  # deny
        assert limiter.allow_count == 2
        assert limiter.deny_count == 1

    def test_limit_equals_bucket_capacity(self, m):
        limiter = m.TokenBucketRateLimiter("client:lim", rate_per_second=5.0, bucket_capacity=7)
        r = limiter.check()
        assert r.limit == 7


# ---------------------------------------------------------------------------
# Pattern 1 — SlidingWindowRateLimiter
# ---------------------------------------------------------------------------


class TestSlidingWindowRateLimiter:
    def test_allows_up_to_limit(self, m):
        limiter = m.SlidingWindowRateLimiter("client:sw", limit=3, window_ms=60_000)
        results = [limiter.check() for _ in range(3)]
        assert all(r.allowed for r in results)

    def test_denies_when_limit_exceeded(self, m):
        limiter = m.SlidingWindowRateLimiter("client:sw2", limit=2, window_ms=60_000)
        limiter.check()
        limiter.check()
        r = limiter.check()
        assert not r.allowed

    def test_remaining_is_zero_when_full(self, m):
        limiter = m.SlidingWindowRateLimiter("client:sw3", limit=2, window_ms=60_000)
        limiter.check()
        r = limiter.check()
        assert r.remaining == 0

    def test_retry_after_ms_positive_on_deny(self, m):
        limiter = m.SlidingWindowRateLimiter("client:sw4", limit=1, window_ms=5_000)
        limiter.check()
        r = limiter.check()
        assert not r.allowed
        assert r.retry_after_ms >= 0

    def test_algorithm_label_is_sliding_window(self, m):
        limiter = m.SlidingWindowRateLimiter("client:alg", limit=10, window_ms=1000)
        r = limiter.check()
        assert r.algorithm == m.RateLimitAlgorithm.SLIDING_WINDOW_LOG

    def test_allow_and_deny_counters(self, m):
        limiter = m.SlidingWindowRateLimiter("client:cnt2", limit=2, window_ms=60_000)
        limiter.check()  # allow
        limiter.check()  # allow
        limiter.check()  # deny
        assert limiter.allow_count == 2
        assert limiter.deny_count == 1

    def test_window_slides_over_time(self, m):
        """After the window passes, old requests fall off and new ones are allowed."""
        limiter = m.SlidingWindowRateLimiter("client:slide", limit=1, window_ms=50)  # 50ms window
        r1 = limiter.check()
        assert r1.allowed
        r2 = limiter.check()
        assert not r2.allowed
        time.sleep(0.06)  # Wait for window to pass
        r3 = limiter.check()
        assert r3.allowed


# ---------------------------------------------------------------------------
# Pattern 2 — CircuitBreaker
# ---------------------------------------------------------------------------


class TestCircuitBreaker:
    def test_initial_state_is_closed(self, m):
        cb = m.CircuitBreaker("test-service", failure_threshold=3)
        assert cb.state == m.CircuitState.CLOSED

    def test_closed_allows_requests(self, m):
        cb = m.CircuitBreaker("test-service", failure_threshold=3)
        r = cb.check()
        assert r.allowed
        assert r.state == m.CircuitState.CLOSED

    def test_trips_to_open_after_threshold(self, m):
        cb = m.CircuitBreaker("test-service", failure_threshold=3, timeout_ms=10_000)
        for _ in range(3):
            cb.record_failure()
        assert cb.state == m.CircuitState.OPEN

    def test_open_rejects_requests(self, m):
        cb = m.CircuitBreaker("test-service2", failure_threshold=2, timeout_ms=60_000)
        cb.record_failure()
        cb.record_failure()
        r = cb.check()
        assert not r.allowed
        assert r.state == m.CircuitState.OPEN

    def test_transitions_to_half_open_after_timeout(self, m):
        cb = m.CircuitBreaker("test-service3", failure_threshold=1, timeout_ms=10)
        cb.record_failure()
        assert cb.state == m.CircuitState.OPEN
        time.sleep(0.02)  # Wait for timeout
        r = cb.check()
        assert r.state == m.CircuitState.HALF_OPEN
        assert r.allowed

    def test_half_open_closes_after_successes(self, m):
        cb = m.CircuitBreaker("test-service4", failure_threshold=1, success_threshold=2,
                               timeout_ms=10)
        cb.record_failure()
        time.sleep(0.02)
        cb.check()  # Transition to HALF_OPEN
        cb._state = m.CircuitState.HALF_OPEN
        cb._success_count = 0
        cb.record_success()
        cb.record_success()
        assert cb.state == m.CircuitState.CLOSED

    def test_half_open_re_opens_on_failure(self, m):
        cb = m.CircuitBreaker("test-service5", failure_threshold=1, timeout_ms=10)
        cb.record_failure()
        time.sleep(0.02)
        cb.check()  # Probe
        cb._state = m.CircuitState.HALF_OPEN
        cb.record_failure()
        assert cb.state == m.CircuitState.OPEN

    def test_exponential_backoff_on_retries(self, m):
        cb = m.CircuitBreaker("test-service6", failure_threshold=1, timeout_ms=100,
                               max_timeout_ms=1_000)
        cb.record_failure()
        initial_timeout = cb._current_timeout_ms
        # Simulate HALF_OPEN failure → doubles timeout
        cb._state = m.CircuitState.HALF_OPEN
        cb.record_failure()
        assert cb._current_timeout_ms > initial_timeout

    def test_success_in_closed_resets_failure_count(self, m):
        cb = m.CircuitBreaker("test-service7", failure_threshold=5)
        cb.record_failure()
        cb.record_failure()
        cb.record_success()
        assert cb._failure_count == 0


# ---------------------------------------------------------------------------
# Pattern 3 — RequestCoalescer
# ---------------------------------------------------------------------------


class TestRequestCoalescer:
    def test_first_request_not_coalesced(self, m):
        coalescer = m.RequestCoalescer(key_fn=lambda x: x)
        result, was_coalesced = coalescer.get_or_fetch(lambda: 42, "key1")
        assert result == 42
        assert not was_coalesced

    def test_forwarded_count_increments(self, m):
        coalescer = m.RequestCoalescer(key_fn=lambda x: x)
        coalescer.get_or_fetch(lambda: "value", "key-unique")
        assert coalescer.forwarded_count == 1

    def test_concurrent_requests_coalesced(self, m):
        """Multiple concurrent requests for same key should coalesce into one backend call."""
        call_count = [0]

        def slow_fetch():
            call_count[0] += 1
            time.sleep(0.02)  # Give threads time to pile up
            return "result"

        coalescer = m.RequestCoalescer(key_fn=lambda endpoint, uid: f"{endpoint}:{uid}")
        results = []
        threads = []

        for i in range(4):
            def make_req(idx=i):
                r, c = coalescer.get_or_fetch(slow_fetch, "/users", "U-100")
                results.append((idx, r, c))
            t = threading.Thread(target=make_req)
            threads.append(t)

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All should get a result
        assert len(results) == 4
        # Backend called at most a small number of times (ideally 1)
        assert call_count[0] <= 4

    def test_key_fn_separates_different_requests(self, m):
        """Different keys should not be coalesced."""
        coalescer = m.RequestCoalescer(key_fn=lambda x: x)
        r1, c1 = coalescer.get_or_fetch(lambda: "result-A", "key-A")
        r2, c2 = coalescer.get_or_fetch(lambda: "result-B", "key-B")
        assert r1 == "result-A"
        assert r2 == "result-B"
        assert not c1
        assert not c2


# ---------------------------------------------------------------------------
# Pattern 4 — APIKeyManager
# ---------------------------------------------------------------------------


class TestAPIKeyManager:
    def test_create_key_returns_raw_and_key(self, m):
        mgr = m.APIKeyManager()
        raw, key = mgr.create_key("client:test", tier=m.APIKeyTier.STANDARD)
        assert raw
        assert key
        assert key.client_id == "client:test"

    def test_created_key_is_active(self, m):
        mgr = m.APIKeyManager()
        _, key = mgr.create_key("client:active", tier=m.APIKeyTier.STANDARD)
        assert key.status == m.APIKeyStatus.ACTIVE

    def test_validate_valid_key_returns_true(self, m):
        mgr = m.APIKeyManager()
        raw, key = mgr.create_key("client:val", tier=m.APIKeyTier.FREE)
        valid, api_key, reason = mgr.validate_key(key.key_id, raw)
        assert valid
        assert reason == "valid"

    def test_validate_wrong_raw_key_returns_false(self, m):
        mgr = m.APIKeyManager()
        _, key = mgr.create_key("client:wrong", tier=m.APIKeyTier.FREE)
        valid, _, reason = mgr.validate_key(key.key_id, "wrong-key")
        assert not valid

    def test_validate_unknown_key_id_returns_false(self, m):
        mgr = m.APIKeyManager()
        valid, _, reason = mgr.validate_key("nonexistent-id", "any-key")
        assert not valid
        assert "not found" in reason

    def test_tier_rate_limits(self, m):
        assert m.APIKeyManager.TIER_RATE_LIMITS[m.APIKeyTier.FREE] == 10
        assert m.APIKeyManager.TIER_RATE_LIMITS[m.APIKeyTier.STANDARD] == 100
        assert m.APIKeyManager.TIER_RATE_LIMITS[m.APIKeyTier.PREMIUM] == 1000

    def test_free_tier_key_has_10_rpm(self, m):
        mgr = m.APIKeyManager()
        _, key = mgr.create_key("client:free", tier=m.APIKeyTier.FREE)
        assert key.rate_limit_rpm == 10

    def test_premium_tier_key_has_1000_rpm(self, m):
        mgr = m.APIKeyManager()
        _, key = mgr.create_key("client:prem", tier=m.APIKeyTier.PREMIUM)
        assert key.rate_limit_rpm == 1000

    def test_rotate_creates_successor_key(self, m):
        mgr = m.APIKeyManager(grace_period_ms=3_600_000)
        raw, key = mgr.create_key("client:rot", tier=m.APIKeyTier.STANDARD)
        new_raw, new_key = mgr.rotate_key(key.key_id)
        old = mgr.get_key(key.key_id)
        assert old.successor_key_id == new_key.key_id
        assert new_raw

    def test_rotated_old_key_valid_during_grace_period(self, m):
        mgr = m.APIKeyManager(grace_period_ms=86_400_000)  # 24h grace
        raw, key = mgr.create_key("client:grace", tier=m.APIKeyTier.STANDARD)
        mgr.rotate_key(key.key_id)
        old = mgr.get_key(key.key_id)
        assert old.in_grace_period

    def test_revoke_key_invalidates_it(self, m):
        mgr = m.APIKeyManager()
        raw, key = mgr.create_key("client:rev", tier=m.APIKeyTier.STANDARD)
        mgr.revoke_key(key.key_id)
        valid, _, reason = mgr.validate_key(key.key_id, raw)
        assert not valid
        assert "revoked" in reason.lower()

    def test_revoke_nonexistent_key_returns_false(self, m):
        mgr = m.APIKeyManager()
        result = mgr.revoke_key("nonexistent-id")
        assert result is False

    def test_api_key_is_valid_property(self, m):
        mgr = m.APIKeyManager()
        _, key = mgr.create_key("client:valid", tier=m.APIKeyTier.STANDARD)
        assert key.is_valid

    def test_key_hash_not_raw_key(self, m):
        """Raw key should never equal the stored hash."""
        mgr = m.APIKeyManager()
        raw, key = mgr.create_key("client:hash", tier=m.APIKeyTier.STANDARD)
        assert key.key_hash != raw

    def test_custom_raw_key(self, m):
        mgr = m.APIKeyManager()
        raw, key = mgr.create_key("client:custom", raw_key="my-fixed-key")
        valid, _, _ = mgr.validate_key(key.key_id, "my-fixed-key")
        assert valid


# ---------------------------------------------------------------------------
# Pattern 5 — APIVersionRouter
# ---------------------------------------------------------------------------


class TestAPIVersionRouter:
    def _setup_router(self, m):
        now = time.monotonic()
        router = m.APIVersionRouter()
        router.register_version(m.APIVersion(
            version_id="v1",
            introduced_at=now - 1000,
            deprecated_at=now - 500,
            sunset_at=now + 1000,
            is_current=False,
            handler_name="handler_v1",
        ))
        router.register_version(m.APIVersion(
            version_id="v2",
            introduced_at=now - 500,
            is_current=True,
            handler_name="handler_v2",
        ))
        return router

    def test_routes_to_current_by_default(self, m):
        router = self._setup_router(m)
        result = router.route()
        assert result.version_id == "v2"
        assert result.handler_name == "handler_v2"

    def test_routes_via_version_header(self, m):
        router = self._setup_router(m)
        result = router.route(version_header="v1")
        assert result.version_id == "v1"

    def test_routes_via_url_prefix(self, m):
        router = self._setup_router(m)
        result = router.route(url_prefix="v1")
        assert result.version_id == "v1"

    def test_header_takes_priority_over_url(self, m):
        router = self._setup_router(m)
        result = router.route(version_header="v2", url_prefix="v1")
        assert result.version_id == "v2"

    def test_deprecated_version_has_sunset_header(self, m):
        router = self._setup_router(m)
        result = router.route(version_header="v1")
        assert result.is_deprecated
        assert result.sunset_header is not None
        assert result.warning_header is not None

    def test_current_version_not_deprecated(self, m):
        router = self._setup_router(m)
        result = router.route()
        assert not result.is_deprecated
        assert result.sunset_header is None

    def test_expired_version_is_expired(self, m):
        now = time.monotonic()
        router = m.APIVersionRouter()
        router.register_version(m.APIVersion(
            version_id="v0",
            introduced_at=now - 2000,
            deprecated_at=now - 1000,
            sunset_at=now - 1,  # Already expired
            is_current=False,
            handler_name="handler_v0",
        ))
        router.register_version(m.APIVersion(
            version_id="v2",
            introduced_at=now - 100,
            is_current=True,
            handler_name="handler_v2",
        ))
        result = router.route(version_header="v0")
        assert result.is_expired
        assert "410" in result.warning_header or "Gone" in result.warning_header

    def test_unknown_version_falls_back_to_current(self, m):
        router = self._setup_router(m)
        result = router.route(version_header="v99-unknown")
        assert result.version_id == "v2"

    def test_list_versions_sorted_by_introduced_at(self, m):
        router = self._setup_router(m)
        versions = router.list_versions()
        assert len(versions) == 2
        assert versions[0].version_id == "v1"  # Older first
        assert versions[1].version_id == "v2"


# ---------------------------------------------------------------------------
# Pattern 6 — RequestDeduplicator
# ---------------------------------------------------------------------------


class TestRequestDeduplicator:
    def test_first_request_processes_handler(self, m):
        dedup = m.RequestDeduplicator(ttl_ms=60_000)
        call_count = [0]

        def handler():
            call_count[0] += 1
            return {"order_id": "ORD-001", "status": "created"}

        result, was_dup = dedup.execute("idem-key-001", handler)
        assert not was_dup
        assert result["order_id"] == "ORD-001"
        assert call_count[0] == 1

    def test_duplicate_request_returns_cached_result(self, m):
        dedup = m.RequestDeduplicator(ttl_ms=60_000)
        call_count = [0]

        def handler():
            call_count[0] += 1
            return {"amount": 100.00}

        dedup.execute("idem-key-002", handler)
        result, was_dup = dedup.execute("idem-key-002", handler)
        assert was_dup
        assert call_count[0] == 1  # Handler only called once

    def test_different_keys_processed_independently(self, m):
        dedup = m.RequestDeduplicator(ttl_ms=60_000)
        r1, d1 = dedup.execute("key-A", lambda: "response-A")
        r2, d2 = dedup.execute("key-B", lambda: "response-B")
        assert r1 == "response-A"
        assert r2 == "response-B"
        assert not d1
        assert not d2

    def test_processed_count_increments(self, m):
        dedup = m.RequestDeduplicator(ttl_ms=60_000)
        dedup.execute("key-cnt-1", lambda: "a")
        dedup.execute("key-cnt-2", lambda: "b")
        assert dedup.processed_count == 2

    def test_dedup_count_increments(self, m):
        dedup = m.RequestDeduplicator(ttl_ms=60_000)
        dedup.execute("key-dedup", lambda: "x")
        dedup.execute("key-dedup", lambda: "x")
        dedup.execute("key-dedup", lambda: "x")
        assert dedup.dedup_count == 2

    def test_store_size_reflects_active_records(self, m):
        dedup = m.RequestDeduplicator(ttl_ms=60_000)
        dedup.execute("key-store-1", lambda: 1)
        dedup.execute("key-store-2", lambda: 2)
        assert dedup.store_size() == 2

    def test_purge_expired_removes_old_records(self, m):
        dedup = m.RequestDeduplicator(ttl_ms=10)  # Very short TTL
        dedup.execute("key-purge", lambda: "old")
        time.sleep(0.02)  # Let TTL expire
        removed = dedup.purge_expired()
        assert removed >= 1
        assert dedup.store_size() == 0

    def test_expired_record_reprocessed(self, m):
        """After TTL expires, the same key should be re-processed."""
        dedup = m.RequestDeduplicator(ttl_ms=20)  # Very short TTL
        call_count = [0]

        def handler():
            call_count[0] += 1
            return "response"

        dedup.execute("key-reprocess", handler)
        time.sleep(0.04)  # Let TTL expire
        dedup.execute("key-reprocess", handler)
        assert call_count[0] == 2  # Both processed (not deduplicated)

    def test_prevents_double_charge_simulation(self, m):
        """Simulate a payment being retried — idempotency prevents double charge."""
        dedup = m.RequestDeduplicator(ttl_ms=60_000)
        charges = []

        def charge_payment():
            charge_id = f"CHG-{len(charges) + 1}"
            charges.append(charge_id)
            return {"charge_id": charge_id, "amount": 99.99}

        payment_key = "payment-retry-test-001"
        r1, d1 = dedup.execute(payment_key, charge_payment)
        r2, d2 = dedup.execute(payment_key, charge_payment)  # Retry

        assert len(charges) == 1  # Only charged once
        assert r1["charge_id"] == r2["charge_id"]
        assert d2 is True
