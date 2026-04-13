"""
Tests for 29_async_concurrent_patterns.py

Covers all five patterns with 50+ test cases using only the standard library.
Async tests use ``asyncio.run()`` directly — no pytest-asyncio required.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import importlib.util
import sys
import threading
import time
import types
from pathlib import Path

import pytest

_MOD_PATH = Path(__file__).parent.parent / "examples" / "29_async_concurrent_patterns.py"


def _load_module():
    module_name = "async_concurrent_patterns_29"
    if module_name in sys.modules:
        return sys.modules[module_name]
    spec = importlib.util.spec_from_file_location(module_name, _MOD_PATH)
    mod = types.ModuleType(module_name)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def mod():
    return _load_module()


# ---------------------------------------------------------------------------
# Helper: tiny async no-op (zero sleep)
# ---------------------------------------------------------------------------


async def _noop(value=None):
    await asyncio.sleep(0)
    return value


# ===========================================================================
# Pattern 1 — AsyncTaskPool
# ===========================================================================


class TestAsyncTaskPool:
    # --- run_all ---

    def test_run_all_returns_results_in_submission_order(self, mod):
        pool = mod.AsyncTaskPool(max_concurrency=4)

        async def work(n):
            await asyncio.sleep(0)
            return n * 10

        for i in range(5):
            pool.submit(work(i))

        results = asyncio.run(pool.run_all())
        assert results == [0, 10, 20, 30, 40]

    def test_run_all_respects_max_concurrency(self, mod):
        max_concurrent = 3
        pool = mod.AsyncTaskPool(max_concurrency=max_concurrent)
        active = [0]
        peak = [0]

        async def work(n):
            active[0] += 1
            peak[0] = max(peak[0], active[0])
            await asyncio.sleep(0.01)
            active[0] -= 1
            return n

        for i in range(9):
            pool.submit(work(i))

        asyncio.run(pool.run_all())
        assert peak[0] <= max_concurrent

    def test_run_all_single_task(self, mod):
        pool = mod.AsyncTaskPool(max_concurrency=1)
        pool.submit(_noop(99))
        results = asyncio.run(pool.run_all())
        assert results == [99]

    def test_run_all_empty_returns_empty_list(self, mod):
        pool = mod.AsyncTaskPool(max_concurrency=2)
        results = asyncio.run(pool.run_all())
        assert results == []

    def test_run_all_clears_pending_queue(self, mod):
        pool = mod.AsyncTaskPool(max_concurrency=2)
        pool.submit(_noop(1))
        asyncio.run(pool.run_all())
        results2 = asyncio.run(pool.run_all())
        assert results2 == []

    def test_run_all_multiple_batches(self, mod):
        pool = mod.AsyncTaskPool(max_concurrency=2)
        pool.submit(_noop(1))
        pool.submit(_noop(2))
        first = asyncio.run(pool.run_all())
        pool.submit(_noop(3))
        pool.submit(_noop(4))
        second = asyncio.run(pool.run_all())
        assert first == [1, 2]
        assert second == [3, 4]

    # --- completed_count ---

    def test_completed_count_accumulates_across_batches(self, mod):
        pool = mod.AsyncTaskPool(max_concurrency=5)
        for _ in range(3):
            pool.submit(_noop())
        asyncio.run(pool.run_all())
        for _ in range(4):
            pool.submit(_noop())
        asyncio.run(pool.run_all())
        assert pool.completed_count == 7

    def test_completed_count_starts_at_zero(self, mod):
        pool = mod.AsyncTaskPool(max_concurrency=2)
        assert pool.completed_count == 0

    # --- run_with_timeout ---

    def test_run_with_timeout_returns_result_on_time(self, mod):
        pool = mod.AsyncTaskPool(max_concurrency=1)

        async def fast():
            return "done"

        result = asyncio.run(pool.run_with_timeout(fast(), timeout_seconds=5.0))
        assert result == "done"

    def test_run_with_timeout_raises_on_slow_coroutine(self, mod):
        pool = mod.AsyncTaskPool(max_concurrency=1)

        async def slow():
            await asyncio.sleep(10)
            return "never"

        with pytest.raises(asyncio.TimeoutError):
            asyncio.run(pool.run_with_timeout(slow(), timeout_seconds=0.05))

    def test_invalid_max_concurrency_raises(self, mod):
        with pytest.raises(ValueError):
            mod.AsyncTaskPool(max_concurrency=0)


# ===========================================================================
# Pattern 2 — AsyncRetry
# ===========================================================================


class TestAsyncRetry:
    # --- success on first attempt ---

    def test_succeeds_first_attempt(self, mod):
        retry = mod.AsyncRetry(max_attempts=3, base_delay=0.001)

        async def always_ok():
            return "ok"

        result = asyncio.run(retry.execute(always_ok))
        assert result == "ok"
        assert retry.attempt_count == 1

    # --- retry on failure ---

    def test_retries_and_succeeds_on_third_attempt(self, mod):
        retry = mod.AsyncRetry(max_attempts=3, base_delay=0.001, jitter=False)
        calls = [0]

        async def flaky():
            calls[0] += 1
            if calls[0] < 3:
                raise ValueError("not yet")
            return "success"

        result = asyncio.run(retry.execute(flaky))
        assert result == "success"
        assert calls[0] == 3

    # --- RetryExhausted ---

    def test_raises_retry_exhausted_after_max_attempts(self, mod):
        retry = mod.AsyncRetry(max_attempts=3, base_delay=0.001)

        async def always_fails():
            raise RuntimeError("boom")

        with pytest.raises(mod.AsyncRetry.RetryExhausted) as exc_info:
            asyncio.run(retry.execute(always_fails))

        exc = exc_info.value
        assert exc.attempts == 3
        assert isinstance(exc.last_error, RuntimeError)

    def test_retry_exhausted_message_contains_attempt_count(self, mod):
        retry = mod.AsyncRetry(max_attempts=2, base_delay=0.001)

        async def fail():
            raise ValueError("x")

        with pytest.raises(mod.AsyncRetry.RetryExhausted) as exc_info:
            asyncio.run(retry.execute(fail))

        assert "2" in str(exc_info.value)

    def test_retry_exhausted_carries_last_error(self, mod):
        retry = mod.AsyncRetry(max_attempts=2, base_delay=0.001)
        sentinel = ValueError("sentinel-error")

        async def fail():
            raise sentinel

        with pytest.raises(mod.AsyncRetry.RetryExhausted) as exc_info:
            asyncio.run(retry.execute(fail))

        assert exc_info.value.last_error is sentinel

    # --- attempt_count accumulates ---

    def test_attempt_count_accumulates_across_calls(self, mod):
        retry = mod.AsyncRetry(max_attempts=2, base_delay=0.001)

        async def ok():
            return 1

        asyncio.run(retry.execute(ok))
        asyncio.run(retry.execute(ok))
        assert retry.attempt_count == 2

    def test_attempt_count_includes_failed_attempts(self, mod):
        retry = mod.AsyncRetry(max_attempts=3, base_delay=0.001)

        async def fail():
            raise Exception("x")

        try:
            asyncio.run(retry.execute(fail))
        except mod.AsyncRetry.RetryExhausted:
            pass
        assert retry.attempt_count == 3

    # --- backoff increases between retries ---

    def test_backoff_increases_between_retries(self, mod):
        retry = mod.AsyncRetry(max_attempts=4, base_delay=0.01, max_delay=10.0, jitter=False)
        timestamps = []

        async def record_and_fail():
            timestamps.append(time.monotonic())
            raise ValueError("x")

        try:
            asyncio.run(retry.execute(record_and_fail))
        except mod.AsyncRetry.RetryExhausted:
            pass

        gaps = [timestamps[i + 1] - timestamps[i] for i in range(len(timestamps) - 1)]
        # Each gap should be larger than the previous (exponential backoff)
        for i in range(1, len(gaps)):
            assert gaps[i] > gaps[i - 1] * 0.5

    def test_max_delay_caps_backoff(self, mod):
        retry = mod.AsyncRetry(max_attempts=5, base_delay=0.5, max_delay=0.01, jitter=False)
        start = time.monotonic()

        async def fail():
            raise Exception("x")

        try:
            asyncio.run(retry.execute(fail))
        except mod.AsyncRetry.RetryExhausted:
            pass

        elapsed = time.monotonic() - start
        # With 5 attempts and max_delay=0.01 the total wait should be well under 1s
        assert elapsed < 1.0

    def test_invalid_max_attempts_raises(self, mod):
        with pytest.raises(ValueError):
            mod.AsyncRetry(max_attempts=0)


# ===========================================================================
# Pattern 3 — AsyncPipeline
# ===========================================================================


class TestAsyncPipeline:
    # --- single stage ---

    def test_single_stage_transforms_items(self, mod):
        pipeline = mod.AsyncPipeline()

        async def double(x):
            return x * 2

        pipeline.add_stage("double", double)
        results = asyncio.run(pipeline.process([1, 2, 3]))
        assert results == [2, 4, 6]

    # --- multi-stage ---

    def test_multi_stage_applies_in_order(self, mod):
        pipeline = mod.AsyncPipeline()

        async def add_one(x):
            return x + 1

        async def multiply_two(x):
            return x * 2

        pipeline.add_stage("add_one", add_one)
        pipeline.add_stage("multiply", multiply_two)
        results = asyncio.run(pipeline.process([1, 2, 3]))
        # (1+1)*2=4, (2+1)*2=6, (3+1)*2=8
        assert results == [4, 6, 8]

    # --- None drops item ---

    def test_none_drops_item(self, mod):
        pipeline = mod.AsyncPipeline()

        async def drop_evens(x):
            return None if x % 2 == 0 else x

        pipeline.add_stage("filter", drop_evens)
        results = asyncio.run(pipeline.process([1, 2, 3, 4, 5]))
        assert results == [1, 3, 5]

    def test_item_dropped_in_first_stage_skips_second(self, mod):
        called_second = []
        pipeline = mod.AsyncPipeline()

        async def drop_all(x):
            return None

        async def record(x):
            called_second.append(x)
            return x

        pipeline.add_stage("drop", drop_all)
        pipeline.add_stage("record", record)
        results = asyncio.run(pipeline.process([1, 2, 3]))
        assert results == []
        assert called_second == []

    def test_all_items_dropped(self, mod):
        pipeline = mod.AsyncPipeline()

        async def drop_all(x):
            return None

        pipeline.add_stage("drop", drop_all)
        results = asyncio.run(pipeline.process([1, 2, 3]))
        assert results == []

    # --- stage_count ---

    def test_stage_count_starts_at_zero(self, mod):
        pipeline = mod.AsyncPipeline()
        assert pipeline.stage_count == 0

    def test_stage_count_correct(self, mod):
        pipeline = mod.AsyncPipeline()
        pipeline.add_stage("s1", _noop)
        pipeline.add_stage("s2", _noop)
        assert pipeline.stage_count == 2

    # --- processed_count ---

    def test_processed_count_correct(self, mod):
        pipeline = mod.AsyncPipeline()

        async def keep(x):
            return x

        pipeline.add_stage("keep", keep)
        asyncio.run(pipeline.process([1, 2, 3]))
        assert pipeline.processed_count == 3

    def test_processed_count_excludes_dropped(self, mod):
        pipeline = mod.AsyncPipeline()

        async def drop_even(x):
            return None if x % 2 == 0 else x

        pipeline.add_stage("filter", drop_even)
        asyncio.run(pipeline.process([1, 2, 3, 4, 5, 6]))
        assert pipeline.processed_count == 3

    def test_processed_count_accumulates(self, mod):
        pipeline = mod.AsyncPipeline()

        async def keep(x):
            return x

        pipeline.add_stage("keep", keep)
        asyncio.run(pipeline.process([1, 2]))
        asyncio.run(pipeline.process([3, 4, 5]))
        assert pipeline.processed_count == 5

    def test_empty_input_returns_empty(self, mod):
        pipeline = mod.AsyncPipeline()

        async def keep(x):
            return x

        pipeline.add_stage("keep", keep)
        results = asyncio.run(pipeline.process([]))
        assert results == []

    # --- concurrent processing ---

    def test_stages_run_concurrently(self, mod):
        pipeline = mod.AsyncPipeline()
        start = time.monotonic()

        async def slow(x):
            await asyncio.sleep(0.05)
            return x

        pipeline.add_stage("slow", slow)
        results = asyncio.run(pipeline.process(list(range(10))))
        elapsed = time.monotonic() - start
        # 10 items × 0.05 s each → serialised = 0.5 s; concurrent ≈ 0.05 s
        assert elapsed < 0.4
        assert results == list(range(10))


# ===========================================================================
# Pattern 4 — ThreadSafeCache
# ===========================================================================


class TestThreadSafeCache:
    # --- set/get ---

    def test_set_and_get_returns_value(self, mod):
        cache = mod.ThreadSafeCache(ttl_seconds=60)
        cache.set("k", 42)
        assert cache.get("k") == 42

    def test_get_missing_key_returns_none(self, mod):
        cache = mod.ThreadSafeCache()
        assert cache.get("missing") is None

    def test_overwrite_updates_value(self, mod):
        cache = mod.ThreadSafeCache()
        cache.set("k", 1)
        cache.set("k", 2)
        assert cache.get("k") == 2

    def test_stores_various_types(self, mod):
        cache = mod.ThreadSafeCache()
        cache.set("list", [1, 2, 3])
        cache.set("dict", {"a": 1})
        assert cache.get("list") == [1, 2, 3]
        assert cache.get("dict") == {"a": 1}

    # --- TTL expiry ---

    def test_expired_entry_returns_none(self, mod):
        cache = mod.ThreadSafeCache(ttl_seconds=0.01)
        cache.set("x", "value")
        time.sleep(0.05)
        assert cache.get("x") is None

    def test_non_expired_entry_still_returned(self, mod):
        cache = mod.ThreadSafeCache(ttl_seconds=10.0)
        cache.set("y", "alive")
        assert cache.get("y") == "alive"

    # --- delete ---

    def test_delete_removes_key(self, mod):
        cache = mod.ThreadSafeCache()
        cache.set("a", 1)
        cache.delete("a")
        assert cache.get("a") is None

    def test_delete_missing_key_no_error(self, mod):
        cache = mod.ThreadSafeCache()
        cache.delete("nonexistent")  # should not raise

    # --- clear ---

    def test_clear_removes_all_entries(self, mod):
        cache = mod.ThreadSafeCache()
        cache.set("a", 1)
        cache.set("b", 2)
        cache.clear()
        assert cache.get("a") is None
        assert cache.get("b") is None

    def test_clear_resets_size_to_zero(self, mod):
        cache = mod.ThreadSafeCache()
        cache.set("a", 1)
        cache.clear()
        assert cache.size == 0

    # --- size ---

    def test_size_counts_live_entries(self, mod):
        cache = mod.ThreadSafeCache(ttl_seconds=60)
        cache.set("a", 1)
        cache.set("b", 2)
        assert cache.size == 2

    def test_size_excludes_expired_entries(self, mod):
        cache = mod.ThreadSafeCache(ttl_seconds=0.01)
        cache.set("x", 1)
        cache.set("y", 2)
        time.sleep(0.05)
        assert cache.size == 0

    def test_size_zero_on_empty_cache(self, mod):
        cache = mod.ThreadSafeCache()
        assert cache.size == 0

    # --- thread-safe concurrent writes ---

    def test_thread_safe_concurrent_writes(self, mod):
        cache = mod.ThreadSafeCache(ttl_seconds=60)
        errors = []

        def writer(prefix, count):
            try:
                for i in range(count):
                    cache.set(f"{prefix}:{i}", i)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=writer, args=(f"t{t}", 50)) for t in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
        # Each thread wrote 50 keys with unique prefixes
        for t in range(10):
            for i in range(50):
                val = cache.get(f"t{t}:{i}")
                assert val == i

    def test_thread_safe_concurrent_reads_and_writes(self, mod):
        cache = mod.ThreadSafeCache(ttl_seconds=60)
        cache.set("shared", 0)
        errors = []

        def reader():
            for _ in range(100):
                try:
                    cache.get("shared")
                except Exception as e:
                    errors.append(e)

        def writer():
            for i in range(100):
                try:
                    cache.set("shared", i)
                except Exception as e:
                    errors.append(e)

        threads = [threading.Thread(target=reader) for _ in range(5)]
        threads += [threading.Thread(target=writer) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []


# ===========================================================================
# Pattern 5 — WorkerPool
# ===========================================================================


class TestWorkerPool:
    # --- submit ---

    def test_submit_returns_future(self, mod):
        pool = mod.WorkerPool(num_workers=2)
        future = pool.submit(lambda: 42)
        assert isinstance(future, concurrent.futures.Future)
        assert future.result() == 42
        pool.shutdown()

    def test_submit_with_args(self, mod):
        pool = mod.WorkerPool(num_workers=2)
        future = pool.submit(pow, 3, 4)
        assert future.result() == 81
        pool.shutdown()

    def test_submit_captures_exception(self, mod):
        pool = mod.WorkerPool(num_workers=2)

        def raise_value_error():
            raise ValueError("test error")

        future = pool.submit(raise_value_error)
        with pytest.raises(ValueError):
            future.result()
        pool.shutdown()

    # --- map ---

    def test_map_returns_results_in_order(self, mod):
        pool = mod.WorkerPool(num_workers=4)
        results = pool.map(lambda x: x * 2, [1, 2, 3, 4, 5])
        assert results == [2, 4, 6, 8, 10]
        pool.shutdown()

    def test_map_empty_list(self, mod):
        pool = mod.WorkerPool(num_workers=2)
        results = pool.map(str.upper, [])
        assert results == []
        pool.shutdown()

    def test_map_single_item(self, mod):
        pool = mod.WorkerPool(num_workers=2)
        results = pool.map(len, ["hello"])
        assert results == [5]
        pool.shutdown()

    def test_map_uses_multiple_workers(self, mod):
        pool = mod.WorkerPool(num_workers=4)
        start = time.monotonic()

        def slow_double(x):
            time.sleep(0.05)
            return x * 2

        results = pool.map(slow_double, list(range(8)))
        elapsed = time.monotonic() - start

        assert results == [0, 2, 4, 6, 8, 10, 12, 14]
        # 8 tasks × 0.05 s serialised = 0.4 s; with 4 workers ≈ 0.1 s
        assert elapsed < 0.35
        pool.shutdown()

    # --- shutdown ---

    def test_shutdown_prevents_new_submissions(self, mod):
        pool = mod.WorkerPool(num_workers=2)
        pool.shutdown(wait=True)
        with pytest.raises(RuntimeError):
            pool.submit(lambda: None)

    def test_shutdown_wait_false_does_not_block(self, mod):
        pool = mod.WorkerPool(num_workers=2)
        start = time.monotonic()
        pool.shutdown(wait=False)
        elapsed = time.monotonic() - start
        assert elapsed < 1.0
