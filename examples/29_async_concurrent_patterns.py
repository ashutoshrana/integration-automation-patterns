"""
29_async_concurrent_patterns.py — Async/Concurrent Patterns

Pure-Python async and concurrency primitives for enterprise integration using
only the standard library (``asyncio``, ``threading``, ``concurrent.futures``,
``time``).  No third-party dependencies are required.

    Pattern 1 — AsyncTaskPool (bounded async task execution):
                Submits coroutines for deferred execution, then drains them
                with a configurable concurrency ceiling enforced by
                ``asyncio.Semaphore``.  Results are returned in submission
                order.  A :meth:`run_with_timeout` helper wraps a single
                coroutine in ``asyncio.wait_for`` to enforce a deadline.

    Pattern 2 — AsyncRetry (exponential back-off retry):
                Wraps an async callable (a zero-argument factory that returns
                a coroutine) and re-invokes it on any exception.  Back-off
                follows ``min(base_delay * 2 ** attempt, max_delay)`` with
                optional uniform jitter.  After *max_attempts* failures the
                inner :class:`AsyncRetry.RetryExhausted` exception is raised,
                carrying the total attempt count and the final error.

    Pattern 3 — AsyncPipeline (multi-stage async pipeline):
                An ordered sequence of async stage functions is registered via
                :meth:`add_stage`.  :meth:`process` fans-out all items through
                each stage concurrently (using ``asyncio.gather``).  Any item
                for which a stage returns ``None`` is silently dropped and does
                not proceed to subsequent stages.

    Pattern 4 — ThreadSafeCache (TTL in-memory cache):
                A dictionary-backed cache protected by ``threading.RLock``.
                Each entry records the wall-clock insertion time; reads return
                ``None`` and the entry is evicted lazily when the age exceeds
                *ttl_seconds*.  The :attr:`size` property counts only live
                (non-expired) entries.

    Pattern 5 — WorkerPool (thread-based task pool):
                A thin wrapper around ``concurrent.futures.ThreadPoolExecutor``
                that exposes :meth:`submit` (returns a :class:`Future`) and
                :meth:`map` (returns results in submission order) for CPU-bound
                or blocking tasks that should not run on the async event loop.

No external dependencies required.

Run:
    python examples/29_async_concurrent_patterns.py
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import random
import threading
import time
from typing import Any, Callable, Coroutine, List, Optional


# ===========================================================================
# Pattern 1 — AsyncTaskPool
# ===========================================================================


class AsyncTaskPool:
    """Bounded async task executor.

    Coroutines are queued via :meth:`submit` and executed concurrently up to
    *max_concurrency* at a time when :meth:`run_all` is called.  Results are
    returned in the same order as the original submissions.

    Example::

        pool = AsyncTaskPool(max_concurrency=4)
        for i in range(10):
            pool.submit(some_coro(i))
        results = asyncio.run(pool.run_all())
    """

    def __init__(self, max_concurrency: int) -> None:
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be >= 1")
        self._max_concurrency = max_concurrency
        self._pending: list[Coroutine] = []
        self._completed_count: int = 0

    # ------------------------------------------------------------------
    # Submission
    # ------------------------------------------------------------------

    def submit(self, coro: Coroutine) -> None:
        """Queue *coro* for later execution.

        Args:
            coro: An unawaited coroutine object to schedule.
        """
        self._pending.append(coro)

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    async def run_all(self) -> list:
        """Execute all submitted coroutines respecting *max_concurrency*.

        Coroutines run concurrently; the semaphore prevents more than
        *max_concurrency* from being active at the same time.  Results are
        returned in submission order.  The pending queue is cleared after
        each call.

        Returns:
            List of results aligned with the submission order.
        """
        semaphore = asyncio.Semaphore(self._max_concurrency)
        coros = list(self._pending)
        self._pending.clear()

        async def _guarded(coro: Coroutine) -> Any:
            async with semaphore:
                result = await coro
                self._completed_count += 1
                return result

        tasks = [asyncio.create_task(_guarded(c)) for c in coros]
        return list(await asyncio.gather(*tasks))

    async def run_with_timeout(
        self, coro: Coroutine, timeout_seconds: float
    ) -> Any:
        """Run a single coroutine with a hard deadline.

        Args:
            coro:            The coroutine to run.
            timeout_seconds: Maximum seconds to wait before cancellation.

        Returns:
            The coroutine's return value.

        Raises:
            asyncio.TimeoutError: If the coroutine does not complete in time.
        """
        return await asyncio.wait_for(coro, timeout=timeout_seconds)

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def completed_count(self) -> int:
        """Cumulative count of coroutines that have completed via :meth:`run_all`."""
        return self._completed_count


# ===========================================================================
# Pattern 2 — AsyncRetry
# ===========================================================================


class AsyncRetry:
    """Async retry wrapper with exponential back-off.

    Wraps an async callable factory (a zero-argument callable that *returns* a
    coroutine, so a fresh coroutine is created for each attempt) and retries it
    on any exception.

    Example::

        retry = AsyncRetry(max_attempts=3, base_delay=0.01)

        call_count = 0

        async def flaky():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("not yet")
            return "ok"

        result = asyncio.run(retry.execute(flaky))
        assert result == "ok"
    """

    class RetryExhausted(Exception):
        """Raised when all retry attempts have been exhausted.

        Attributes:
            attempts:   Total number of attempts made.
            last_error: The exception raised on the final attempt.
        """

        def __init__(self, attempts: int, last_error: Exception) -> None:
            super().__init__(
                f"Retry exhausted after {attempts} attempt(s): {last_error}"
            )
            self.attempts = attempts
            self.last_error = last_error

    def __init__(
        self,
        max_attempts: int = 3,
        base_delay: float = 0.01,
        max_delay: float = 1.0,
        jitter: bool = True,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        self._max_attempts = max_attempts
        self._base_delay = base_delay
        self._max_delay = max_delay
        self._jitter = jitter
        self._attempt_count: int = 0

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    async def execute(
        self, coro_fn: Callable[[], Coroutine]
    ) -> Any:
        """Invoke *coro_fn()* up to *max_attempts* times.

        On each failure the coroutine is re-created by calling *coro_fn* again
        after a back-off delay.  The delay follows
        ``min(base_delay * 2 ** attempt, max_delay)`` where *attempt* starts
        at 0.  When *jitter* is True a uniformly random fraction of the
        computed delay is subtracted to spread retried calls.

        Args:
            coro_fn: Zero-argument callable that returns a fresh coroutine.

        Returns:
            The return value of *coro_fn()* on success.

        Raises:
            AsyncRetry.RetryExhausted: After all attempts fail.
        """
        last_error: Optional[Exception] = None
        for attempt in range(self._max_attempts):
            self._attempt_count += 1
            try:
                return await coro_fn()
            except Exception as exc:
                last_error = exc
                if attempt < self._max_attempts - 1:
                    delay = min(
                        self._base_delay * (2 ** attempt), self._max_delay
                    )
                    if self._jitter:
                        delay = delay * (1 - random.random() * 0.5)
                    await asyncio.sleep(delay)
        raise AsyncRetry.RetryExhausted(
            attempts=self._max_attempts, last_error=last_error  # type: ignore[arg-type]
        )

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def attempt_count(self) -> int:
        """Total number of individual attempts made across all :meth:`execute` calls."""
        return self._attempt_count


# ===========================================================================
# Pattern 3 — AsyncPipeline
# ===========================================================================


class AsyncPipeline:
    """Multi-stage async processing pipeline.

    Each stage is an async callable that accepts one item and returns the
    (possibly transformed) item, or ``None`` to remove it from the stream.
    Stages are applied in registration order.  Within each stage all surviving
    items are processed concurrently via ``asyncio.gather``.

    Example::

        pipeline = AsyncPipeline()

        async def double(x):
            return x * 2

        async def drop_large(x):
            return None if x > 10 else x

        pipeline.add_stage("double", double)
        pipeline.add_stage("filter", drop_large)
        results = asyncio.run(pipeline.process([1, 2, 3, 4, 5, 6]))
        # [2, 4, 6, 8, 10]  → 12 dropped
    """

    def __init__(self) -> None:
        self._stages: list[tuple[str, Callable]] = []
        self._processed_count: int = 0

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def add_stage(self, name: str, fn: Callable) -> None:
        """Append *fn* as the next pipeline stage.

        Args:
            name: Human-readable label for the stage.
            fn:   Async callable that accepts one item and returns an item or
                  ``None``.
        """
        self._stages.append((name, fn))

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    async def process(self, items: list) -> list:
        """Run *items* through all pipeline stages.

        Within each stage the surviving items are processed concurrently.
        Items for which a stage returns ``None`` are dropped and do not
        proceed.  :attr:`processed_count` is updated with the final survivor
        count after all stages complete.

        Args:
            items: Input items to process.

        Returns:
            Items that survived every stage, in their original relative order.
        """
        current = list(items)
        for _name, fn in self._stages:
            if not current:
                break
            results = await asyncio.gather(*(fn(item) for item in current))
            current = [r for r in results if r is not None]
        self._processed_count += len(current)
        return current

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def stage_count(self) -> int:
        """Number of stages registered in the pipeline."""
        return len(self._stages)

    @property
    def processed_count(self) -> int:
        """Cumulative count of items that survived all stages across all :meth:`process` calls."""
        return self._processed_count


# ===========================================================================
# Pattern 4 — ThreadSafeCache
# ===========================================================================


class ThreadSafeCache:
    """Thread-safe in-memory cache with time-to-live (TTL) expiry.

    All mutations and reads are serialised with a ``threading.RLock``.
    Expired entries are evicted lazily on access and on :attr:`size` queries.

    Example::

        cache = ThreadSafeCache(ttl_seconds=5.0)
        cache.set("key", 42)
        assert cache.get("key") == 42
    """

    def __init__(self, ttl_seconds: float = 60.0) -> None:
        self._ttl = ttl_seconds
        self._store: dict[str, tuple[Any, float]] = {}  # key → (value, inserted_at)
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # Mutations
    # ------------------------------------------------------------------

    def set(self, key: str, value: Any) -> None:
        """Store *value* under *key* with the current timestamp.

        Args:
            key:   Cache key.
            value: Value to store (any picklable object).
        """
        with self._lock:
            self._store[key] = (value, time.monotonic())

    def delete(self, key: str) -> None:
        """Remove *key* from the cache (no-op if absent).

        Args:
            key: Cache key to remove.
        """
        with self._lock:
            self._store.pop(key, None)

    def clear(self) -> None:
        """Remove all entries from the cache."""
        with self._lock:
            self._store.clear()

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    def get(self, key: str) -> Optional[Any]:
        """Return the cached value for *key*, or ``None`` if absent or expired.

        Expired entries are evicted on access.

        Args:
            key: Cache key to look up.

        Returns:
            The stored value, or ``None``.
        """
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            value, inserted_at = entry
            if time.monotonic() - inserted_at > self._ttl:
                del self._store[key]
                return None
            return value

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def size(self) -> int:
        """Number of non-expired entries currently in the cache."""
        now = time.monotonic()
        with self._lock:
            live = [
                k for k, (_, ts) in self._store.items()
                if now - ts <= self._ttl
            ]
            # Evict expired entries while we have the lock
            expired = [k for k in self._store if k not in live]
            for k in expired:
                del self._store[k]
            return len(live)


# ===========================================================================
# Pattern 5 — WorkerPool
# ===========================================================================


class WorkerPool:
    """Thread-based worker pool for blocking or CPU-bound tasks.

    A thin wrapper around :class:`concurrent.futures.ThreadPoolExecutor` that
    adds convenience methods for single-task submission and bulk mapping.

    Example::

        pool = WorkerPool(num_workers=4)
        future = pool.submit(len, "hello")
        assert future.result() == 5
        results = pool.map(str.upper, ["a", "b", "c"])
        assert results == ["A", "B", "C"]
        pool.shutdown()
    """

    def __init__(self, num_workers: int) -> None:
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=num_workers
        )

    # ------------------------------------------------------------------
    # Submission
    # ------------------------------------------------------------------

    def submit(
        self, fn: Callable, *args: Any, **kwargs: Any
    ) -> concurrent.futures.Future:
        """Submit *fn(*args, **kwargs)* for execution in the pool.

        Args:
            fn:     Callable to execute.
            *args:  Positional arguments forwarded to *fn*.
            **kwargs: Keyword arguments forwarded to *fn*.

        Returns:
            A :class:`concurrent.futures.Future` representing the pending
            result.
        """
        return self._executor.submit(fn, *args, **kwargs)

    def map(self, fn: Callable, items: list) -> list:
        """Apply *fn* to every item in *items* using the pool.

        Results are returned in the same order as *items*; the call blocks
        until all tasks have completed.

        Args:
            fn:    Callable to apply to each item.
            items: Sequence of input items.

        Returns:
            List of results in submission order.
        """
        futures = [self._executor.submit(fn, item) for item in items]
        return [f.result() for f in futures]

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def shutdown(self, wait: bool = True) -> None:
        """Shut down the underlying executor.

        Args:
            wait: If ``True`` (default), block until all running tasks finish.
        """
        self._executor.shutdown(wait=wait)


# ===========================================================================
# Smoke-test helpers (used only in __main__)
# ===========================================================================


def _separator(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def _run_async_task_pool_demo() -> None:
    """Scenario 1: AsyncTaskPool — bounded concurrency and timeout."""
    _separator("Scenario 1 — AsyncTaskPool: bounded concurrency + timeout")

    async def _main() -> None:
        pool = AsyncTaskPool(max_concurrency=3)

        async def work(n: int) -> int:
            await asyncio.sleep(0)
            return n * n

        for i in range(6):
            pool.submit(work(i))

        results = await pool.run_all()
        print(f"Results        : {results}")
        assert results == [0, 1, 4, 9, 16, 25]
        print(f"Completed count: {pool.completed_count}")
        assert pool.completed_count == 6

        # Timeout scenario
        async def slow() -> str:
            await asyncio.sleep(10)
            return "done"

        try:
            await pool.run_with_timeout(slow(), timeout_seconds=0.05)
            assert False, "Should have raised"
        except asyncio.TimeoutError:
            print("Timeout raised : OK")

    asyncio.run(_main())
    print("PASS")


def _run_async_retry_demo() -> None:
    """Scenario 2: AsyncRetry — success, failure, exhaustion."""
    _separator("Scenario 2 — AsyncRetry: success + exhaustion")

    async def _main() -> None:
        retry = AsyncRetry(max_attempts=3, base_delay=0.001, jitter=False)

        call_count = 0

        async def flaky() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("not yet")
            return "ok"

        result = await retry.execute(flaky)
        print(f"Result         : {result}")
        assert result == "ok"
        assert retry.attempt_count == 3

        # Exhaustion
        async def always_fails() -> None:
            raise RuntimeError("boom")

        try:
            await retry.execute(always_fails)
            assert False, "Should have raised"
        except AsyncRetry.RetryExhausted as exc:
            print(f"RetryExhausted : attempts={exc.attempts}")
            assert exc.attempts == 3
            assert isinstance(exc.last_error, RuntimeError)

    asyncio.run(_main())
    print("PASS")


def _run_async_pipeline_demo() -> None:
    """Scenario 3: AsyncPipeline — multi-stage with drop."""
    _separator("Scenario 3 — AsyncPipeline: multi-stage + drop")

    async def _main() -> None:
        pipeline = AsyncPipeline()

        async def double(x: int) -> int:
            return x * 2

        async def drop_large(x: int) -> Optional[int]:
            return None if x > 8 else x

        pipeline.add_stage("double", double)
        pipeline.add_stage("filter", drop_large)

        results = await pipeline.process([1, 2, 3, 4, 5])
        print(f"Results        : {results}")
        # 1→2, 2→4, 3→6, 4→8, 5→10(dropped)
        assert results == [2, 4, 6, 8]
        assert pipeline.stage_count == 2
        assert pipeline.processed_count == 4

    asyncio.run(_main())
    print("PASS")


def _run_thread_safe_cache_demo() -> None:
    """Scenario 4: ThreadSafeCache — set/get/TTL/size."""
    _separator("Scenario 4 — ThreadSafeCache: set/get/expiry/size")

    cache = ThreadSafeCache(ttl_seconds=0.1)
    cache.set("a", 1)
    cache.set("b", 2)
    assert cache.get("a") == 1
    assert cache.size == 2

    time.sleep(0.15)
    assert cache.get("a") is None
    print("TTL expiry     : OK")
    assert cache.size == 0

    cache.set("c", 3)
    cache.delete("c")
    assert cache.get("c") is None
    print("Delete         : OK")

    print("PASS")


def _run_worker_pool_demo() -> None:
    """Scenario 5: WorkerPool — submit/map/shutdown."""
    _separator("Scenario 5 — WorkerPool: submit + map + shutdown")

    pool = WorkerPool(num_workers=4)

    future = pool.submit(pow, 2, 10)
    assert future.result() == 1024
    print(f"submit result  : {future.result()}")

    results = pool.map(str.upper, ["alpha", "beta", "gamma"])
    print(f"map results    : {results}")
    assert results == ["ALPHA", "BETA", "GAMMA"]

    pool.shutdown()
    print("PASS")


# ===========================================================================
# Entry point
# ===========================================================================


if __name__ == "__main__":
    print("Async/Concurrent Patterns — Smoke Test Suite")
    print(
        "Patterns: AsyncTaskPool | AsyncRetry | AsyncPipeline "
        "| ThreadSafeCache | WorkerPool"
    )

    _run_async_task_pool_demo()
    _run_async_retry_demo()
    _run_async_pipeline_demo()
    _run_thread_safe_cache_demo()
    _run_worker_pool_demo()

    print("\n" + "=" * 60)
    print("  All scenarios passed.")
    print("=" * 60)
