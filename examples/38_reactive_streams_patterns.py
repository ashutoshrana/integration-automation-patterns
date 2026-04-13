"""
38_reactive_streams_patterns.py — Reactive Streams and Backpressure Patterns

Pure-Python implementation of five reactive-streams primitives using only the
standard library (``threading``, ``queue``, ``concurrent.futures``,
``collections``, ``time``, ``typing``).  No external dependencies are required.

    Pattern 1 — Observable (push-based observable stream):
                Cold observable that emits items to each subscriber
                independently.  :meth:`subscribe` returns a
                :class:`Subscription` whose :meth:`unsubscribe` detaches the
                observer.  Operator methods ``map``, ``filter``, and ``take``
                return new ``Observable`` instances enabling fluent composition.
                The static factory :meth:`from_iterable` produces an
                ``Observable`` that synchronously emits every item from an
                iterable and then calls ``on_complete``.

    Pattern 2 — Subject (hot multicast observable):
                Extends ``Observable`` as a hot source; :meth:`emit` pushes an
                item to **all** current subscribers, :meth:`error` fans out an
                exception, and :meth:`complete` signals completion.  All
                mutation methods are protected by a ``threading.Lock`` so
                multiple producers and consumers may operate concurrently.

    Pattern 3 — BackpressureBuffer (bounded queue with overflow strategies):
                Wraps ``queue.Queue`` with three configurable overflow
                strategies.  ``"drop_latest"`` (default) silently discards the
                newly arriving item when the buffer is full.  ``"drop_oldest"``
                evicts the head of the queue to make room for the new item.
                ``"block"`` parks the caller until space is available, subject
                to an optional ``timeout_seconds``; :meth:`put` returns
                ``False`` on timeout.  :meth:`get` blocks up to
                ``timeout_seconds`` (or indefinitely when ``None``) and raises
                ``queue.Empty`` on timeout.  The ``size`` property and
                ``is_full`` property expose current occupancy.

    Pattern 4 — ReactiveProcessor (concurrent stream processing):
                Subscribes to an ``Observable``, dispatches each item to a
                caller-supplied *handler* using a
                ``concurrent.futures.ThreadPoolExecutor`` bounded to
                *concurrency* workers, and collects results in emission order.
                :meth:`process_with_retry` wraps the handler in per-item
                exponential-back-off retry logic (up to *max_retries* attempts
                with 0.1 s × 2^attempt base delay).

    Pattern 5 — StreamMerger (multi-stream combinators):
                :meth:`merge` fans-in multiple observables into one, emitting
                items in arrival order using a shared queue drained by a single
                output thread.  :meth:`zip` pairs items by position: the result
                observable emits one tuple per "round" only after every source
                has contributed a new item.  :meth:`concat` subscribes to each
                observable in turn, starting the next only after the previous
                completes.

No external dependencies required.

Run:
    python examples/38_reactive_streams_patterns.py
"""

from __future__ import annotations

import queue
import threading
import time
from collections import deque
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

__all__ = [
    "Subscription",
    "Observable",
    "Subject",
    "BackpressureBuffer",
    "ReactiveProcessor",
    "StreamMerger",
]

# ---------------------------------------------------------------------------
# Sentinel objects
# ---------------------------------------------------------------------------

_COMPLETED = object()  # signals stream completion
_ERROR = object()  # signals stream error (item is the exception)


# ===========================================================================
# Pattern 1 — Observable
# ===========================================================================


class Subscription:
    """Handle returned by :meth:`Observable.subscribe`.

    Calling :meth:`unsubscribe` prevents further delivery of items to the
    attached callbacks.  Subsequent calls to :meth:`unsubscribe` are no-ops.
    """

    def __init__(self) -> None:
        self._active: bool = True
        self._lock: threading.Lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def unsubscribe(self) -> None:
        """Detach this subscription; subsequent items are silently dropped."""
        with self._lock:
            self._active = False

    @property
    def is_active(self) -> bool:
        """``True`` while the subscription has not been cancelled."""
        with self._lock:
            return self._active


class Observable:
    """Push-based cold observable stream.

    Each call to :meth:`subscribe` independently triggers the source producer.
    Items are delivered synchronously to the three observer callbacks.

    Example::

        obs = Observable.from_iterable([1, 2, 3])
        results = []
        obs.subscribe(on_next=results.append)
        assert results == [1, 2, 3]
    """

    def __init__(self, producer: Callable[[Callable, Callable, Callable], None]) -> None:
        """Initialise from a low-level *producer* callable.

        Args:
            producer: A callable that accepts three arguments
                ``(on_next, on_error, on_complete)`` and drives the stream.
                It is called once per :meth:`subscribe` invocation.
        """
        self._producer = producer

    # ------------------------------------------------------------------
    # Subscribe
    # ------------------------------------------------------------------

    def subscribe(
        self,
        on_next: Callable[[Any], None],
        on_error: Callable[[Exception], None] | None = None,
        on_complete: Callable[[], None] | None = None,
    ) -> Subscription:
        """Subscribe to the observable stream.

        Args:
            on_next:     Called once per emitted item.
            on_error:    Called at most once if the stream raises an exception.
            on_complete: Called at most once after the last item is delivered.

        Returns:
            A :class:`Subscription` that may be cancelled via
            :meth:`Subscription.unsubscribe`.
        """
        sub = Subscription()

        def _noop_error(_e: Exception) -> None:
            pass

        def _noop_complete() -> None:
            pass

        _on_error = on_error if on_error is not None else _noop_error
        _on_complete = on_complete if on_complete is not None else _noop_complete

        def _guarded_next(item: Any) -> None:
            if sub.is_active:
                on_next(item)

        def _guarded_error(exc: Exception) -> None:
            if sub.is_active:
                _on_error(exc)

        def _guarded_complete() -> None:
            if sub.is_active:
                _on_complete()

        try:
            self._producer(_guarded_next, _guarded_error, _guarded_complete)
        except Exception as exc:  # noqa: BLE001
            if sub.is_active:
                _on_error(exc)

        return sub

    # ------------------------------------------------------------------
    # Operators
    # ------------------------------------------------------------------

    def map(self, fn: Callable[[Any], Any]) -> Observable:
        """Return a new ``Observable`` that applies *fn* to each item.

        Args:
            fn: Transformation function applied to every emitted value.

        Returns:
            A new :class:`Observable` emitting transformed values.
        """

        def _producer(on_next, on_error, on_complete):
            def _mapped_next(item):
                try:
                    on_next(fn(item))
                except Exception as exc:  # noqa: BLE001
                    on_error(exc)

            self.subscribe(_mapped_next, on_error, on_complete)

        return Observable(_producer)

    def filter(self, predicate: Callable[[Any], bool]) -> Observable:
        """Return a new ``Observable`` emitting only items satisfying *predicate*.

        Args:
            predicate: Boolean callable; items for which it returns ``True``
                       are forwarded; others are silently discarded.

        Returns:
            A new :class:`Observable` emitting only matching values.
        """

        def _producer(on_next, on_error, on_complete):
            def _filtered_next(item):
                try:
                    if predicate(item):
                        on_next(item)
                except Exception as exc:  # noqa: BLE001
                    on_error(exc)

            self.subscribe(_filtered_next, on_error, on_complete)

        return Observable(_producer)

    def take(self, n: int) -> Observable:
        """Return a new ``Observable`` completing after at most *n* items.

        Args:
            n: Maximum number of items to emit (non-negative integer).

        Returns:
            A new :class:`Observable`` that forwards the first *n* items then
            completes.
        """
        if n < 0:
            raise ValueError(f"take(n) requires n >= 0; got {n}")

        def _producer(on_next, on_error, on_complete):
            count = [0]
            done = [False]

            def _taking_next(item):
                if done[0]:
                    return
                if count[0] < n:
                    count[0] += 1
                    on_next(item)
                    if count[0] == n:
                        done[0] = True
                        on_complete()

            def _taking_complete():
                if not done[0]:
                    done[0] = True
                    on_complete()

            self.subscribe(_taking_next, on_error, _taking_complete)

        return Observable(_producer)

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @staticmethod
    def from_iterable(items: Any) -> Observable:
        """Create an ``Observable`` that emits every item from *items* then completes.

        Args:
            items: Any iterable (list, tuple, generator, …).

        Returns:
            A cold :class:`Observable` backed by the iterable.
        """

        def _producer(on_next, on_error, on_complete):
            try:
                for item in items:
                    on_next(item)
                on_complete()
            except Exception as exc:  # noqa: BLE001
                on_error(exc)

        return Observable(_producer)


# ===========================================================================
# Pattern 2 — Subject
# ===========================================================================


class Subject(Observable):
    """Hot multicast observable that is also a producer.

    Multiple subscribers may attach at any time.  :meth:`emit` fans out the
    item to all currently active subscribers.  :meth:`error` and
    :meth:`complete` broadcast terminal signals and prevent further emission.

    Thread safety is provided by an internal ``threading.Lock``.

    Example::

        subject = Subject()
        received = []
        subject.subscribe(on_next=received.append)
        subject.emit(42)
        subject.complete()
        assert received == [42]
    """

    def __init__(self) -> None:
        # Subject is hot; we don't use the cold-producer pathway.
        super().__init__(producer=lambda _n, _e, _c: None)
        self._subscribers: list[tuple[Subscription, Callable, Callable, Callable]] = []
        self._lock: threading.Lock = threading.Lock()
        self._completed: bool = False
        self._error: Exception | None = None

    # ------------------------------------------------------------------
    # Override subscribe for hot behaviour
    # ------------------------------------------------------------------

    def subscribe(
        self,
        on_next: Callable[[Any], None],
        on_error: Callable[[Exception], None] | None = None,
        on_complete: Callable[[], None] | None = None,
    ) -> Subscription:
        """Attach an observer to this hot subject.

        If the subject has already completed or errored, the appropriate
        terminal callback is invoked immediately and the returned
        :class:`Subscription` is already inactive.

        Args:
            on_next:     Called once per emitted item.
            on_error:    Called at most once if the subject errors.
            on_complete: Called at most once after the subject completes.

        Returns:
            A :class:`Subscription` that may be cancelled via
            :meth:`Subscription.unsubscribe`.
        """
        def _noop_error(_e: Exception) -> None:
            pass

        def _noop_complete() -> None:
            pass

        _on_error = on_error if on_error is not None else _noop_error
        _on_complete = on_complete if on_complete is not None else _noop_complete

        sub = Subscription()

        with self._lock:
            if self._error is not None:
                sub.unsubscribe()
                _on_error(self._error)
                return sub
            if self._completed:
                sub.unsubscribe()
                _on_complete()
                return sub
            self._subscribers.append((sub, on_next, _on_error, _on_complete))

        return sub

    # ------------------------------------------------------------------
    # Producer interface
    # ------------------------------------------------------------------

    def emit(self, item: Any) -> None:
        """Push *item* to all active subscribers.

        Args:
            item: The value to broadcast.

        Raises:
            RuntimeError: If the subject has already been completed or errored.
        """
        with self._lock:
            if self._completed or self._error is not None:
                raise RuntimeError("Cannot emit on a completed/errored Subject")
            snapshot = list(self._subscribers)

        for sub, on_next, _on_error, _on_complete in snapshot:
            if sub.is_active:
                try:
                    on_next(item)
                except Exception:  # noqa: BLE001
                    pass  # individual observer errors do not terminate the subject

    def error(self, exc: Exception) -> None:
        """Broadcast an error to all active subscribers and close the subject.

        Args:
            exc: The exception to fan out.

        Raises:
            RuntimeError: If the subject has already been completed or errored.
        """
        with self._lock:
            if self._completed or self._error is not None:
                raise RuntimeError("Subject is already terminated")
            self._error = exc
            snapshot = list(self._subscribers)
            self._subscribers.clear()

        for sub, _on_next, on_error, _on_complete in snapshot:
            if sub.is_active:
                sub.unsubscribe()
                try:
                    on_error(exc)
                except Exception:  # noqa: BLE001
                    pass

    def complete(self) -> None:
        """Signal completion to all active subscribers and close the subject.

        Raises:
            RuntimeError: If the subject has already been completed or errored.
        """
        with self._lock:
            if self._completed or self._error is not None:
                raise RuntimeError("Subject is already terminated")
            self._completed = True
            snapshot = list(self._subscribers)
            self._subscribers.clear()

        for sub, _on_next, _on_error, on_complete in snapshot:
            if sub.is_active:
                sub.unsubscribe()
                try:
                    on_complete()
                except Exception:  # noqa: BLE001
                    pass


# ===========================================================================
# Pattern 3 — BackpressureBuffer
# ===========================================================================


class BackpressureBuffer:
    """Bounded FIFO queue with configurable overflow strategies.

    Three overflow strategies are supported:

    ``"drop_latest"`` (default)
        When the buffer is full, newly arriving items are silently discarded.
        :meth:`put` returns ``False`` for dropped items.

    ``"drop_oldest"``
        When the buffer is full, the oldest (head) item is evicted to make
        room for the new item.  :meth:`put` always returns ``True``.

    ``"block"``
        :meth:`put` parks the calling thread until capacity is available or
        *timeout_seconds* elapses.  Returns ``False`` on timeout.

    Example::

        buf = BackpressureBuffer(capacity=3, strategy="drop_oldest")
        buf.put(1); buf.put(2); buf.put(3); buf.put(4)   # 4 evicts 1
        assert buf.size == 3
        assert buf.get() == 2
    """

    _VALID_STRATEGIES = frozenset({"drop_latest", "drop_oldest", "block"})

    def __init__(
        self,
        capacity: int,
        strategy: str = "drop_latest",
        timeout_seconds: float | None = None,
    ) -> None:
        """Initialise the buffer.

        Args:
            capacity:        Maximum number of items to hold (must be >= 1).
            strategy:        Overflow strategy; one of ``"drop_latest"``,
                             ``"drop_oldest"``, or ``"block"``.
            timeout_seconds: Default timeout for :meth:`put` when
                             ``strategy="block"`` and for :meth:`get`.
                             ``None`` means block indefinitely.
        """
        if capacity < 1:
            raise ValueError(f"capacity must be >= 1; got {capacity}")
        if strategy not in self._VALID_STRATEGIES:
            raise ValueError(
                f"strategy must be one of {sorted(self._VALID_STRATEGIES)}; got {strategy!r}"
            )

        self._capacity = capacity
        self._strategy = strategy
        self._default_timeout = timeout_seconds
        self._lock = threading.Lock()
        self._not_full = threading.Condition(self._lock)
        self._not_empty = threading.Condition(self._lock)
        self._buf: deque = deque()

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def size(self) -> int:
        """Current number of items in the buffer."""
        with self._lock:
            return len(self._buf)

    @property
    def is_full(self) -> bool:
        """``True`` when the buffer has reached its capacity."""
        with self._lock:
            return len(self._buf) >= self._capacity

    # ------------------------------------------------------------------
    # put / get
    # ------------------------------------------------------------------

    def put(self, item: Any, timeout_seconds: float | None = None) -> bool:
        """Attempt to add *item* to the buffer.

        Args:
            item:            The value to enqueue.
            timeout_seconds: Override for the blocking timeout (only relevant
                             when ``strategy="block"``).

        Returns:
            ``True`` if the item was accepted; ``False`` if it was dropped or
            the blocking wait timed out.
        """
        timeout = timeout_seconds if timeout_seconds is not None else self._default_timeout

        if self._strategy == "drop_latest":
            with self._not_empty:
                if len(self._buf) >= self._capacity:
                    return False
                self._buf.append(item)
                self._not_empty.notify_all()
            return True

        if self._strategy == "drop_oldest":
            with self._not_empty:
                if len(self._buf) >= self._capacity:
                    self._buf.popleft()  # evict oldest
                self._buf.append(item)
                self._not_empty.notify_all()
            return True

        # strategy == "block"
        deadline = (time.monotonic() + timeout) if timeout is not None else None
        with self._not_full:
            while len(self._buf) >= self._capacity:
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return False
                    self._not_full.wait(timeout=remaining)
                else:
                    self._not_full.wait()
            self._buf.append(item)
            self._not_empty.notify_all()
        return True

    def get(self, timeout_seconds: float | None = None) -> Any:
        """Remove and return the oldest item from the buffer.

        Args:
            timeout_seconds: How long to wait if the buffer is empty.  ``None``
                             uses the default timeout supplied at construction;
                             if that is also ``None``, blocks indefinitely.

        Returns:
            The dequeued item.

        Raises:
            queue.Empty: If no item becomes available within the timeout.
        """
        timeout = timeout_seconds if timeout_seconds is not None else self._default_timeout
        deadline = (time.monotonic() + timeout) if timeout is not None else None

        with self._not_empty:
            while len(self._buf) == 0:
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise queue.Empty("Buffer is empty (timed out)")
                    self._not_empty.wait(timeout=remaining)
                else:
                    self._not_empty.wait()
            item = self._buf.popleft()
            self._not_full.notify_all()

        return item


# ===========================================================================
# Pattern 4 — ReactiveProcessor
# ===========================================================================


class ReactiveProcessor:
    """Processes items from an ``Observable`` with bounded concurrency.

    Items are dispatched to a ``ThreadPoolExecutor`` capped at *concurrency*
    threads.  Results are collected and returned **in the same order as
    emission** (futures are resolved in emission order, not completion order).

    Example::

        obs = Observable.from_iterable([1, 2, 3])
        proc = ReactiveProcessor(concurrency=2)
        results = proc.process(obs, handler=lambda x: x * 10)
        assert results == [10, 20, 30]
    """

    def __init__(self, concurrency: int = 1) -> None:
        """Initialise the processor.

        Args:
            concurrency: Maximum number of concurrent handler invocations
                         (must be >= 1).
        """
        if concurrency < 1:
            raise ValueError(f"concurrency must be >= 1; got {concurrency}")
        self._concurrency = concurrency

    # ------------------------------------------------------------------
    # process
    # ------------------------------------------------------------------

    def process(
        self,
        observable: Observable,
        handler: Callable[[Any], Any],
    ) -> list[Any]:
        """Subscribe to *observable*, apply *handler* concurrently, return results.

        Args:
            observable: Source stream to consume.
            handler:    Function called with each emitted item.  May block or
                        perform I/O; up to *concurrency* invocations may run in
                        parallel.

        Returns:
            List of handler return values in the order items were emitted.

        Raises:
            Exception: Re-raises the first exception emitted by the observable
                       or raised by *handler* (after all in-flight work settles).
        """
        futures: list = []
        errors: list[Exception] = []

        with ThreadPoolExecutor(max_workers=self._concurrency) as executor:

            def _on_next(item: Any) -> None:
                futures.append(executor.submit(handler, item))

            def _on_error(exc: Exception) -> None:
                errors.append(exc)

            observable.subscribe(_on_next, _on_error)

        if errors:
            raise errors[0]

        results = []
        for fut in futures:
            results.append(fut.result())
        return results

    # ------------------------------------------------------------------
    # process_with_retry
    # ------------------------------------------------------------------

    def process_with_retry(
        self,
        observable: Observable,
        handler: Callable[[Any], Any],
        max_retries: int = 3,
    ) -> list[Any]:
        """Like :meth:`process` but retries failed handler calls with back-off.

        Each item is attempted up to ``max_retries + 1`` times.  Between
        attempts the worker sleeps for ``0.05 * 2 ** attempt`` seconds
        (capped at 2 s) to implement exponential back-off.

        Args:
            observable:  Source stream to consume.
            handler:     Function called with each emitted item.
            max_retries: Number of *additional* attempts after the first
                         failure (must be >= 0).

        Returns:
            List of handler return values in emission order.

        Raises:
            Exception: Re-raises the last exception if all retries are
                       exhausted, or the first observable error.
        """
        if max_retries < 0:
            raise ValueError(f"max_retries must be >= 0; got {max_retries}")

        def _with_retry(item: Any) -> Any:
            last_exc: Exception | None = None
            for attempt in range(max_retries + 1):
                try:
                    return handler(item)
                except Exception as exc:  # noqa: BLE001
                    last_exc = exc
                    if attempt < max_retries:
                        sleep_secs = min(0.05 * (2**attempt), 2.0)
                        time.sleep(sleep_secs)
            raise last_exc  # type: ignore[misc]

        return self.process(observable, _with_retry)


# ===========================================================================
# Pattern 5 — StreamMerger
# ===========================================================================

_MERGE_DONE = object()  # sentinel: one source finished
_MERGE_ERROR = object()  # sentinel: source error


class StreamMerger:
    """Combinators for merging multiple ``Observable`` streams.

    All combinators return a new ``Observable`` and are implemented with
    background threads to avoid blocking the subscription call.

    Example::

        obs1 = Observable.from_iterable([1, 2])
        obs2 = Observable.from_iterable([3, 4])
        results = []
        StreamMerger.merge(obs1, obs2).subscribe(on_next=results.append)
        assert sorted(results) == [1, 2, 3, 4]
    """

    # ------------------------------------------------------------------
    # merge
    # ------------------------------------------------------------------

    @staticmethod
    def merge(*observables: Observable) -> Observable:
        """Fan-in items from all *observables* as they arrive (race/merge).

        Items emitted by any source are immediately forwarded in arrival
        order.  The merged stream completes once **all** sources have
        completed.  An error from any source is forwarded and terminates the
        merged stream.

        Args:
            *observables: Source streams to merge.

        Returns:
            A new :class:`Observable` emitting items from all sources.
        """
        n = len(observables)

        def _producer(on_next, on_error, on_complete):
            if n == 0:
                on_complete()
                return

            shared_q: queue.Queue = queue.Queue()
            completed = [0]
            errored = [False]
            lock = threading.Lock()

            def _make_callbacks(idx: int):
                def _next(item):
                    shared_q.put(("item", item))

                def _err(exc):
                    shared_q.put(("error", exc))

                def _done():
                    shared_q.put(("done", idx))

                return _next, _err, _done

            threads = []
            for i, obs in enumerate(observables):
                _n, _e, _c = _make_callbacks(i)

                def _run(o=obs, n_=_n, e_=_e, c_=_c):
                    o.subscribe(n_, e_, c_)

                t = threading.Thread(target=_run, daemon=True)
                threads.append(t)

            for t in threads:
                t.start()

            while True:
                kind, payload = shared_q.get()
                if kind == "error":
                    with lock:
                        if errored[0]:
                            break
                        errored[0] = True
                    on_error(payload)
                    break
                if kind == "item":
                    on_next(payload)
                elif kind == "done":
                    with lock:
                        completed[0] += 1
                        all_done = completed[0] == n
                    if all_done:
                        on_complete()
                        break

            for t in threads:
                t.join(timeout=5.0)

        return Observable(_producer)

    # ------------------------------------------------------------------
    # zip
    # ------------------------------------------------------------------

    @staticmethod
    def zip(*observables: Observable) -> Observable:
        """Emit tuples only when ALL sources have each contributed a new item.

        Items are paired positionally (round 0: first item from each source,
        round 1: second item from each source, etc.).  The zipped stream
        completes as soon as any source completes or errors.

        Args:
            *observables: Source streams to zip.

        Returns:
            A new :class:`Observable` emitting ``tuple`` values.
        """
        n = len(observables)

        def _producer(on_next, on_error, on_complete):
            if n == 0:
                on_complete()
                return

            queues: list[queue.Queue] = [queue.Queue() for _ in range(n)]
            terminated = threading.Event()

            def _make_callbacks(idx: int):
                def _next(item):
                    queues[idx].put(("item", item))

                def _err(exc):
                    queues[idx].put(("error", exc))
                    terminated.set()

                def _done():
                    queues[idx].put(("done", None))
                    terminated.set()

                return _next, _err, _done

            threads = []
            for i, obs in enumerate(observables):
                _n, _e, _c = _make_callbacks(i)

                def _run(o=obs, n_=_n, e_=_e, c_=_c):
                    o.subscribe(n_, e_, c_)

                t = threading.Thread(target=_run, daemon=True)
                threads.append(t)

            for t in threads:
                t.start()

            try:
                while True:
                    row = []
                    for q in queues:
                        kind, payload = q.get()
                        if kind == "error":
                            on_error(payload)
                            return
                        if kind == "done":
                            on_complete()
                            return
                        row.append(payload)
                    on_next(tuple(row))
            finally:
                terminated.set()
                for t in threads:
                    t.join(timeout=5.0)

        return Observable(_producer)

    # ------------------------------------------------------------------
    # concat
    # ------------------------------------------------------------------

    @staticmethod
    def concat(*observables: Observable) -> Observable:
        """Emit items from each observable sequentially (one at a time).

        The second source is not subscribed until the first completes, and so
        on.  An error in any source terminates the concatenated stream
        immediately.

        Args:
            *observables: Source streams to concatenate.

        Returns:
            A new :class:`Observable` emitting items from each source in order.
        """

        def _producer(on_next, on_error, on_complete):
            for obs in observables:
                done_event = threading.Event()
                err_holder: list[Exception] = []

                def _on_err(exc, _holder=err_holder, _ev=done_event):
                    _holder.append(exc)
                    _ev.set()

                def _on_done(_ev=done_event):
                    _ev.set()

                obs.subscribe(on_next, _on_err, _on_done)
                done_event.wait()

                if err_holder:
                    on_error(err_holder[0])
                    return

            on_complete()

        return Observable(_producer)


# ===========================================================================
# Self-test / demo (run directly)
# ===========================================================================

if __name__ == "__main__":
    print("=== Observable.from_iterable + map + filter ===")
    results: list = []
    obs = Observable.from_iterable(range(1, 8)).filter(lambda x: x % 2 == 0).map(lambda x: x * 10)
    obs.subscribe(on_next=results.append)
    print(f"  even ×10 from 1..7: {results}")  # [20, 40, 60]

    print("\n=== Observable.take ===")
    taken: list = []
    Observable.from_iterable(range(100)).take(5).subscribe(on_next=taken.append)
    print(f"  first 5: {taken}")  # [0, 1, 2, 3, 4]

    print("\n=== Subject ===")
    subject = Subject()
    received_a: list = []
    received_b: list = []
    subject.subscribe(on_next=received_a.append)
    subject.subscribe(on_next=received_b.append)
    subject.emit(1)
    subject.emit(2)
    subject.complete()
    print(f"  subscriber A: {received_a}")  # [1, 2]
    print(f"  subscriber B: {received_b}")  # [1, 2]

    print("\n=== BackpressureBuffer (drop_latest) ===")
    buf = BackpressureBuffer(capacity=3)
    for i in range(5):
        accepted = buf.put(i)
        print(f"  put({i}): accepted={accepted}")
    drained = [buf.get() for _ in range(buf.size)]
    print(f"  drained: {drained}")  # [0, 1, 2]

    print("\n=== BackpressureBuffer (drop_oldest) ===")
    buf2 = BackpressureBuffer(capacity=3, strategy="drop_oldest")
    for i in range(5):
        buf2.put(i)
    drained2 = [buf2.get() for _ in range(buf2.size)]
    print(f"  drained (drop_oldest): {drained2}")  # [2, 3, 4]

    print("\n=== ReactiveProcessor ===")
    proc = ReactiveProcessor(concurrency=3)
    out = proc.process(Observable.from_iterable([1, 2, 3, 4]), handler=lambda x: x ** 2)
    print(f"  squares: {out}")  # [1, 4, 9, 16]

    print("\n=== ReactiveProcessor.process_with_retry ===")
    attempts: dict = {}

    def _flaky(x, _a=attempts):
        _a.setdefault(x, 0)
        _a[x] += 1
        if _a[x] < 2 and x == 2:
            raise ValueError("transient")
        return x * 100

    out_r = ReactiveProcessor(concurrency=2).process_with_retry(
        Observable.from_iterable([1, 2, 3]), _flaky, max_retries=3
    )
    print(f"  retry results: {out_r}")  # [100, 200, 300]

    print("\n=== StreamMerger.merge ===")
    merged: list = []
    StreamMerger.merge(
        Observable.from_iterable([10, 20]),
        Observable.from_iterable([30, 40]),
    ).subscribe(on_next=merged.append)
    print(f"  merged (sorted): {sorted(merged)}")  # [10, 20, 30, 40]

    print("\n=== StreamMerger.zip ===")
    zipped: list = []
    StreamMerger.zip(
        Observable.from_iterable([1, 2, 3]),
        Observable.from_iterable(["a", "b", "c"]),
    ).subscribe(on_next=zipped.append)
    print(f"  zipped: {zipped}")  # [(1, 'a'), (2, 'b'), (3, 'c')]

    print("\n=== StreamMerger.concat ===")
    concatenated: list = []
    StreamMerger.concat(
        Observable.from_iterable([1, 2]),
        Observable.from_iterable([3, 4]),
        Observable.from_iterable([5]),
    ).subscribe(on_next=concatenated.append)
    print(f"  concatenated: {concatenated}")  # [1, 2, 3, 4, 5]

    print("\nAll demos complete.")
