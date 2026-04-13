"""
Tests for RetryPolicy — composable retry configuration.

All tests patch time.sleep so the suite runs instantly.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from unittest.mock import patch

import pytest

from integration_automation_patterns.retry_policy import RetryExhausted, RetryPolicy

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _Boom(Exception):
    """Custom non-retryable exception for isolation tests."""


class _Transient(Exception):
    """Custom retryable exception for targeted-exception tests."""


def _always_succeed():
    return "ok"


def _always_fail():
    raise ValueError("boom")


def _fail_n_times(n):
    """Return a callable that fails the first n times, then succeeds."""
    state = {"calls": 0}

    def fn():
        state["calls"] += 1
        if state["calls"] <= n:
            raise ValueError(f"fail #{state['calls']}")
        return "recovered"

    return fn


# ---------------------------------------------------------------------------
# 1. Single attempt — succeeds immediately
# ---------------------------------------------------------------------------


def test_single_attempt_success():
    policy = RetryPolicy(max_attempts=3)
    with patch("time.sleep") as mock_sleep:
        result = policy.execute(_always_succeed)
    assert result == "ok"
    mock_sleep.assert_not_called()


# ---------------------------------------------------------------------------
# 2. All attempts fail — RetryExhausted raised
# ---------------------------------------------------------------------------


def test_all_attempts_fail_raises_retry_exhausted():
    policy = RetryPolicy(max_attempts=3, base_delay=0.01, jitter=False)
    with patch("time.sleep"):
        with pytest.raises(RetryExhausted) as exc_info:
            policy.execute(_always_fail)
    assert exc_info.value.attempts == 3


def test_all_attempts_fail_last_error_preserved():
    original = ValueError("the real error")

    def fn():
        raise original

    policy = RetryPolicy(max_attempts=2, base_delay=0.01, jitter=False)
    with patch("time.sleep"):
        with pytest.raises(RetryExhausted) as exc_info:
            policy.execute(fn)
    assert exc_info.value.last_error is original


# ---------------------------------------------------------------------------
# 3. Second attempt succeeds — returns value, first error swallowed
# ---------------------------------------------------------------------------


def test_second_attempt_succeeds():
    fn = _fail_n_times(1)
    policy = RetryPolicy(max_attempts=3, base_delay=0.01, jitter=False)
    with patch("time.sleep"):
        result = policy.execute(fn)
    assert result == "recovered"


# ---------------------------------------------------------------------------
# 4. max_attempts=1 — fails immediately, no sleep
# ---------------------------------------------------------------------------


def test_max_attempts_one_no_sleep():
    policy = RetryPolicy(max_attempts=1)
    with patch("time.sleep") as mock_sleep:
        with pytest.raises(RetryExhausted):
            policy.execute(_always_fail)
    mock_sleep.assert_not_called()


def test_max_attempts_one_attempt_count():
    policy = RetryPolicy(max_attempts=1)
    with patch("time.sleep"):
        with pytest.raises(RetryExhausted) as exc_info:
            policy.execute(_always_fail)
    assert exc_info.value.attempts == 1


# ---------------------------------------------------------------------------
# 5. delay_for — jitter=True: value in [0, uncapped_delay]
# ---------------------------------------------------------------------------


def test_delay_for_attempt_zero_with_jitter():
    policy = RetryPolicy(max_attempts=3, base_delay=1.0, jitter=True)
    for _ in range(20):
        d = policy.delay_for(0)
        assert 0 <= d <= 1.0


# ---------------------------------------------------------------------------
# 6. delay_for — caps at max_delay
# ---------------------------------------------------------------------------


def test_delay_for_caps_at_max_delay_no_jitter():
    policy = RetryPolicy(max_attempts=10, base_delay=1.0, max_delay=5.0, multiplier=2.0, jitter=False)
    # attempt 5 → 1.0 * 2^5 = 32 → capped at 5.0
    assert policy.delay_for(5) == 5.0


def test_delay_for_caps_at_max_delay_with_jitter():
    policy = RetryPolicy(max_attempts=10, base_delay=1.0, max_delay=5.0, multiplier=2.0, jitter=True)
    for _ in range(20):
        d = policy.delay_for(5)
        assert 0 <= d <= 5.0


# ---------------------------------------------------------------------------
# 7. jitter=False — delay_for is deterministic
# ---------------------------------------------------------------------------


def test_delay_for_no_jitter_is_deterministic():
    policy = RetryPolicy(base_delay=1.0, multiplier=2.0, max_delay=60.0, jitter=False)
    assert policy.delay_for(0) == 1.0
    assert policy.delay_for(1) == 2.0
    assert policy.delay_for(2) == 4.0
    assert policy.delay_for(3) == 8.0


# ---------------------------------------------------------------------------
# 8. multiplier applies — delay grows exponentially
# ---------------------------------------------------------------------------


def test_multiplier_exponential_growth():
    policy = RetryPolicy(base_delay=0.5, multiplier=3.0, max_delay=1000.0, jitter=False)
    assert policy.delay_for(0) == pytest.approx(0.5)
    assert policy.delay_for(1) == pytest.approx(1.5)
    assert policy.delay_for(2) == pytest.approx(4.5)


# ---------------------------------------------------------------------------
# 9–12. Validation — ValueError on bad constructor args
# ---------------------------------------------------------------------------


def test_validation_max_attempts_zero():
    with pytest.raises(ValueError, match="max_attempts must be >= 1"):
        RetryPolicy(max_attempts=0)


def test_validation_max_attempts_negative():
    with pytest.raises(ValueError, match="max_attempts must be >= 1"):
        RetryPolicy(max_attempts=-1)


def test_validation_base_delay_negative():
    with pytest.raises(ValueError, match="base_delay must be >= 0"):
        RetryPolicy(base_delay=-0.1)


def test_validation_max_delay_less_than_base_delay():
    with pytest.raises(ValueError, match="max_delay must be >= base_delay"):
        RetryPolicy(base_delay=5.0, max_delay=1.0)


def test_validation_multiplier_less_than_one():
    with pytest.raises(ValueError, match="multiplier must be >= 1.0"):
        RetryPolicy(multiplier=0.9)


# ---------------------------------------------------------------------------
# 13. retryable_exceptions — non-retryable exception propagates immediately
# ---------------------------------------------------------------------------


def test_non_retryable_exception_propagates_immediately():
    policy = RetryPolicy(
        max_attempts=5,
        base_delay=0.01,
        retryable_exceptions=(ValueError,),
    )

    def fn():
        raise _Boom("not retryable")

    with patch("time.sleep") as mock_sleep:
        with pytest.raises(_Boom):
            policy.execute(fn)
    mock_sleep.assert_not_called()


def test_retryable_exception_is_retried():
    fn = _fail_n_times(2)
    policy = RetryPolicy(
        max_attempts=5,
        base_delay=0.01,
        jitter=False,
        retryable_exceptions=(ValueError,),
    )
    with patch("time.sleep"):
        result = policy.execute(fn)
    assert result == "recovered"


# ---------------------------------------------------------------------------
# 14. RetryPolicy.no_retry() factory
# ---------------------------------------------------------------------------


def test_no_retry_max_attempts_is_one():
    policy = RetryPolicy.no_retry()
    assert policy.max_attempts == 1


def test_no_retry_fails_immediately():
    policy = RetryPolicy.no_retry()
    with patch("time.sleep") as mock_sleep:
        with pytest.raises(RetryExhausted):
            policy.execute(_always_fail)
    mock_sleep.assert_not_called()


# ---------------------------------------------------------------------------
# 15. RetryPolicy.aggressive() factory
# ---------------------------------------------------------------------------


def test_aggressive_max_attempts_is_five():
    policy = RetryPolicy.aggressive()
    assert policy.max_attempts == 5


def test_aggressive_base_delay():
    policy = RetryPolicy.aggressive()
    assert policy.base_delay == pytest.approx(0.5)


def test_aggressive_max_delay():
    policy = RetryPolicy.aggressive()
    assert policy.max_delay == pytest.approx(30.0)


# ---------------------------------------------------------------------------
# 16. execute — args and kwargs passed through correctly
# ---------------------------------------------------------------------------


def test_execute_passes_args_and_kwargs():
    def fn(a, b, *, key):
        return (a, b, key)

    policy = RetryPolicy(max_attempts=1)
    result = policy.execute(fn, 1, 2, key="value")
    assert result == (1, 2, "value")


def test_execute_returns_fn_return_value():
    policy = RetryPolicy(max_attempts=1)
    result = policy.execute(lambda: 42)
    assert result == 42


# ---------------------------------------------------------------------------
# 17. RetryExhausted.attempts and .last_error are accessible
# ---------------------------------------------------------------------------


def test_retry_exhausted_attributes():
    err = ValueError("original")
    exc = RetryExhausted(attempts=4, last_error=err)
    assert exc.attempts == 4
    assert exc.last_error is err


def test_retry_exhausted_str_contains_attempt_count():
    err = RuntimeError("oops")
    exc = RetryExhausted(attempts=3, last_error=err)
    assert "3" in str(exc)


# ---------------------------------------------------------------------------
# 18. sleep is called correct number of times
# ---------------------------------------------------------------------------


def test_sleep_called_between_retries():
    """Sleep should be called max_attempts-1 times (not after the final failure)."""
    policy = RetryPolicy(max_attempts=4, base_delay=0.1, jitter=False)
    with patch("time.sleep") as mock_sleep:
        with pytest.raises(RetryExhausted):
            policy.execute(_always_fail)
    assert mock_sleep.call_count == 3  # 4 attempts → 3 sleeps
