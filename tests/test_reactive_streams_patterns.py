"""
Tests for 38_reactive_streams_patterns.py

Covers all five patterns (Observable, Subject, BackpressureBuffer,
ReactiveProcessor, StreamMerger) with 80 test cases using only the
standard library.  No external dependencies required.
"""

from __future__ import annotations

import importlib.util
import queue
import sys
import threading
import time
import types
from pathlib import Path

import pytest

_MOD_PATH = Path(__file__).parent.parent / "examples" / "38_reactive_streams_patterns.py"


def _load_module() -> types.ModuleType:
    module_name = "reactive_streams_patterns_38"
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
# Observable — basics
# ===========================================================================


class TestObservableFromIterable:
    def test_emits_all_items(self, mod):
        results = []
        mod.Observable.from_iterable([1, 2, 3]).subscribe(on_next=results.append)
        assert results == [1, 2, 3]

    def test_completes_after_items(self, mod):
        completed = [False]
        mod.Observable.from_iterable([1]).subscribe(
            on_next=lambda _: None,
            on_complete=lambda: completed.__setitem__(0, True),
        )
        assert completed[0]

    def test_empty_iterable_completes_immediately(self, mod):
        completed = [False]
        received = []
        mod.Observable.from_iterable([]).subscribe(
            on_next=received.append,
            on_complete=lambda: completed.__setitem__(0, True),
        )
        assert received == []
        assert completed[0]

    def test_generator_iterable(self, mod):
        results = []
        mod.Observable.from_iterable(x * 2 for x in range(4)).subscribe(on_next=results.append)
        assert results == [0, 2, 4, 6]

    def test_returns_subscription(self, mod):
        sub = mod.Observable.from_iterable([1]).subscribe(on_next=lambda _: None)
        assert hasattr(sub, "unsubscribe")

    def test_error_in_iterable_forwarded(self, mod):
        errors = []

        def _bad():
            yield 1
            raise ValueError("oops")

        mod.Observable.from_iterable(_bad()).subscribe(
            on_next=lambda _: None,
            on_error=errors.append,
        )
        assert len(errors) == 1
        assert isinstance(errors[0], ValueError)


# ===========================================================================
# Observable — Subscription
# ===========================================================================


class TestSubscription:
    def test_is_active_after_subscribe(self, mod):
        sub = mod.Observable.from_iterable([]).subscribe(on_next=lambda _: None)
        assert sub.is_active

    def test_unsubscribe_makes_inactive(self, mod):
        sub = mod.Observable.from_iterable([]).subscribe(on_next=lambda _: None)
        sub.unsubscribe()
        assert not sub.is_active

    def test_unsubscribe_idempotent(self, mod):
        sub = mod.Observable.from_iterable([]).subscribe(on_next=lambda _: None)
        sub.unsubscribe()
        sub.unsubscribe()  # must not raise
        assert not sub.is_active


# ===========================================================================
# Observable — map operator
# ===========================================================================


class TestObservableMap:
    def test_map_transforms_values(self, mod):
        results = []
        mod.Observable.from_iterable([1, 2, 3]).map(lambda x: x * 10).subscribe(on_next=results.append)
        assert results == [10, 20, 30]

    def test_map_returns_observable(self, mod):
        obs = mod.Observable.from_iterable([1]).map(lambda x: x)
        assert isinstance(obs, mod.Observable)

    def test_map_chained(self, mod):
        results = []
        mod.Observable.from_iterable([1, 2]).map(lambda x: x + 1).map(lambda x: x * 2).subscribe(on_next=results.append)
        assert results == [4, 6]

    def test_map_error_forwarded(self, mod):
        errors = []

        def _raise(_):
            raise ValueError("map error")

        mod.Observable.from_iterable([1]).map(_raise).subscribe(
            on_next=lambda _: None,
            on_error=errors.append,
        )
        assert len(errors) == 1

    def test_map_empty_stream(self, mod):
        results = []
        mod.Observable.from_iterable([]).map(lambda x: x * 2).subscribe(on_next=results.append)
        assert results == []


# ===========================================================================
# Observable — filter operator
# ===========================================================================


class TestObservableFilter:
    def test_filter_even(self, mod):
        results = []
        mod.Observable.from_iterable(range(6)).filter(lambda x: x % 2 == 0).subscribe(on_next=results.append)
        assert results == [0, 2, 4]

    def test_filter_all_pass(self, mod):
        results = []
        mod.Observable.from_iterable([1, 2, 3]).filter(lambda _: True).subscribe(on_next=results.append)
        assert results == [1, 2, 3]

    def test_filter_none_pass(self, mod):
        results = []
        mod.Observable.from_iterable([1, 2, 3]).filter(lambda _: False).subscribe(on_next=results.append)
        assert results == []

    def test_filter_returns_observable(self, mod):
        obs = mod.Observable.from_iterable([1]).filter(lambda _: True)
        assert isinstance(obs, mod.Observable)

    def test_filter_predicate_error_forwarded(self, mod):
        errors = []

        def _raise(_):
            raise RuntimeError("filter error")

        mod.Observable.from_iterable([1]).filter(_raise).subscribe(
            on_next=lambda _: None,
            on_error=errors.append,
        )
        assert len(errors) == 1


# ===========================================================================
# Observable — take operator
# ===========================================================================


class TestObservableTake:
    def test_take_limits_items(self, mod):
        results = []
        mod.Observable.from_iterable(range(100)).take(3).subscribe(on_next=results.append)
        assert results == [0, 1, 2]

    def test_take_zero_emits_nothing(self, mod):
        results = []
        completed = [False]
        mod.Observable.from_iterable([1, 2, 3]).take(0).subscribe(
            on_next=results.append,
            on_complete=lambda: completed.__setitem__(0, True),
        )
        assert results == []
        assert completed[0]

    def test_take_more_than_available(self, mod):
        results = []
        mod.Observable.from_iterable([1, 2]).take(10).subscribe(on_next=results.append)
        assert results == [1, 2]

    def test_take_calls_complete(self, mod):
        completed = [False]
        mod.Observable.from_iterable(range(10)).take(2).subscribe(
            on_next=lambda _: None,
            on_complete=lambda: completed.__setitem__(0, True),
        )
        assert completed[0]

    def test_take_returns_observable(self, mod):
        obs = mod.Observable.from_iterable([1]).take(1)
        assert isinstance(obs, mod.Observable)

    def test_take_negative_raises(self, mod):
        with pytest.raises(ValueError):
            mod.Observable.from_iterable([1]).take(-1)

    def test_take_exact_count(self, mod):
        results = []
        mod.Observable.from_iterable([10, 20, 30]).take(3).subscribe(on_next=results.append)
        assert results == [10, 20, 30]


# ===========================================================================
# Subject
# ===========================================================================


class TestSubject:
    def test_emit_delivers_to_subscriber(self, mod):
        subject = mod.Subject()
        received = []
        subject.subscribe(on_next=received.append)
        subject.emit(42)
        assert received == [42]

    def test_emit_multiple_items(self, mod):
        subject = mod.Subject()
        received = []
        subject.subscribe(on_next=received.append)
        for i in range(5):
            subject.emit(i)
        assert received == list(range(5))

    def test_complete_calls_on_complete(self, mod):
        subject = mod.Subject()
        completed = [False]
        subject.subscribe(
            on_next=lambda _: None,
            on_complete=lambda: completed.__setitem__(0, True),
        )
        subject.complete()
        assert completed[0]

    def test_error_calls_on_error(self, mod):
        subject = mod.Subject()
        errors = []
        subject.subscribe(on_next=lambda _: None, on_error=errors.append)
        exc = ValueError("subject error")
        subject.error(exc)
        assert errors == [exc]

    def test_multicast_two_subscribers(self, mod):
        subject = mod.Subject()
        a, b = [], []
        subject.subscribe(on_next=a.append)
        subject.subscribe(on_next=b.append)
        subject.emit(1)
        subject.emit(2)
        assert a == [1, 2]
        assert b == [1, 2]

    def test_unsubscribed_observer_not_called(self, mod):
        subject = mod.Subject()
        received = []
        sub = subject.subscribe(on_next=received.append)
        sub.unsubscribe()
        subject.emit(99)
        assert received == []

    def test_emit_after_complete_raises(self, mod):
        subject = mod.Subject()
        subject.complete()
        with pytest.raises(RuntimeError):
            subject.emit(1)

    def test_complete_after_complete_raises(self, mod):
        subject = mod.Subject()
        subject.complete()
        with pytest.raises(RuntimeError):
            subject.complete()

    def test_error_after_complete_raises(self, mod):
        subject = mod.Subject()
        subject.complete()
        with pytest.raises(RuntimeError):
            subject.error(ValueError("late"))

    def test_subscribe_after_complete_fires_on_complete(self, mod):
        subject = mod.Subject()
        subject.complete()
        completed = [False]
        subject.subscribe(
            on_next=lambda _: None,
            on_complete=lambda: completed.__setitem__(0, True),
        )
        assert completed[0]

    def test_subscribe_after_error_fires_on_error(self, mod):
        subject = mod.Subject()
        exc = RuntimeError("already errored")
        subject.error(exc)
        errors = []
        subject.subscribe(on_next=lambda _: None, on_error=errors.append)
        assert errors == [exc]

    def test_subject_is_observable(self, mod):
        assert isinstance(mod.Subject(), mod.Observable)

    def test_thread_safe_emit(self, mod):
        subject = mod.Subject()
        received = []
        lock = threading.Lock()

        def _append(x):
            with lock:
                received.append(x)

        subject.subscribe(on_next=_append)

        threads = [threading.Thread(target=lambda i=i: subject.emit(i)) for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert sorted(received) == list(range(20))


# ===========================================================================
# BackpressureBuffer
# ===========================================================================


class TestBackpressureBufferDropLatest:
    def test_put_accepted_when_not_full(self, mod):
        buf = mod.BackpressureBuffer(capacity=3)
        assert buf.put(1) is True

    def test_put_dropped_when_full(self, mod):
        buf = mod.BackpressureBuffer(capacity=2)
        buf.put(1)
        buf.put(2)
        assert buf.put(3) is False

    def test_size_tracks_occupancy(self, mod):
        buf = mod.BackpressureBuffer(capacity=5)
        buf.put(1)
        buf.put(2)
        assert buf.size == 2

    def test_is_full_false_when_not_full(self, mod):
        buf = mod.BackpressureBuffer(capacity=3)
        buf.put(1)
        assert not buf.is_full

    def test_is_full_true_when_at_capacity(self, mod):
        buf = mod.BackpressureBuffer(capacity=2)
        buf.put(1)
        buf.put(2)
        assert buf.is_full

    def test_get_returns_oldest(self, mod):
        buf = mod.BackpressureBuffer(capacity=3)
        buf.put(10)
        buf.put(20)
        assert buf.get(timeout_seconds=0.5) == 10

    def test_get_empty_raises_queue_empty(self, mod):
        buf = mod.BackpressureBuffer(capacity=3)
        with pytest.raises(queue.Empty):
            buf.get(timeout_seconds=0.05)

    def test_drop_latest_preserves_first_n(self, mod):
        buf = mod.BackpressureBuffer(capacity=3, strategy="drop_latest")
        for i in range(6):
            buf.put(i)
        drained = [buf.get(timeout_seconds=0.1) for _ in range(buf.size)]
        assert drained == [0, 1, 2]


class TestBackpressureBufferDropOldest:
    def test_drop_oldest_always_accepts(self, mod):
        buf = mod.BackpressureBuffer(capacity=2, strategy="drop_oldest")
        for i in range(5):
            assert buf.put(i) is True

    def test_drop_oldest_evicts_head(self, mod):
        buf = mod.BackpressureBuffer(capacity=3, strategy="drop_oldest")
        for i in range(5):
            buf.put(i)
        # After 5 puts into capacity-3: [2, 3, 4]
        results = []
        for _ in range(3):
            results.append(buf.get(timeout_seconds=0.1))
        assert results == [2, 3, 4]

    def test_drop_oldest_size_stays_at_capacity(self, mod):
        buf = mod.BackpressureBuffer(capacity=3, strategy="drop_oldest")
        for i in range(10):
            buf.put(i)
        assert buf.size == 3


class TestBackpressureBufferBlock:
    def test_block_accepts_when_space_available(self, mod):
        buf = mod.BackpressureBuffer(capacity=3, strategy="block")
        assert buf.put(1, timeout_seconds=0.5) is True

    def test_block_returns_false_on_timeout(self, mod):
        buf = mod.BackpressureBuffer(capacity=1, strategy="block")
        buf.put(1)
        start = time.monotonic()
        result = buf.put(99, timeout_seconds=0.1)
        elapsed = time.monotonic() - start
        assert result is False
        assert elapsed >= 0.05

    def test_block_unblocks_after_get(self, mod):
        buf = mod.BackpressureBuffer(capacity=1, strategy="block")
        buf.put(1)
        results = []

        def _producer():
            ok = buf.put(2, timeout_seconds=2.0)
            results.append(ok)

        t = threading.Thread(target=_producer)
        t.start()
        time.sleep(0.05)
        buf.get(timeout_seconds=0.5)  # free space
        t.join(timeout=2.0)
        assert results == [True]


class TestBackpressureBufferValidation:
    def test_invalid_capacity_raises(self, mod):
        with pytest.raises(ValueError):
            mod.BackpressureBuffer(capacity=0)

    def test_invalid_strategy_raises(self, mod):
        with pytest.raises(ValueError):
            mod.BackpressureBuffer(capacity=5, strategy="unknown")


# ===========================================================================
# ReactiveProcessor
# ===========================================================================


class TestReactiveProcessor:
    def test_process_returns_results_in_order(self, mod):
        obs = mod.Observable.from_iterable([1, 2, 3, 4])
        proc = mod.ReactiveProcessor(concurrency=2)
        results = proc.process(obs, handler=lambda x: x * 10)
        assert results == [10, 20, 30, 40]

    def test_process_single_item(self, mod):
        obs = mod.Observable.from_iterable([5])
        proc = mod.ReactiveProcessor(concurrency=1)
        assert proc.process(obs, handler=lambda x: x**2) == [25]

    def test_process_empty_observable(self, mod):
        obs = mod.Observable.from_iterable([])
        proc = mod.ReactiveProcessor(concurrency=1)
        assert proc.process(obs, handler=lambda x: x) == []

    def test_process_concurrency_one(self, mod):
        obs = mod.Observable.from_iterable(range(5))
        proc = mod.ReactiveProcessor(concurrency=1)
        results = proc.process(obs, handler=lambda x: x + 100)
        assert results == [100, 101, 102, 103, 104]

    def test_process_handler_exception_propagates(self, mod):
        obs = mod.Observable.from_iterable([1])

        def _bang(x):
            raise RuntimeError("handler boom")

        proc = mod.ReactiveProcessor(concurrency=1)
        with pytest.raises(RuntimeError, match="handler boom"):
            proc.process(obs, _bang)

    def test_invalid_concurrency_raises(self, mod):
        with pytest.raises(ValueError):
            mod.ReactiveProcessor(concurrency=0)

    def test_process_with_retry_succeeds_on_first_try(self, mod):
        obs = mod.Observable.from_iterable([1, 2, 3])
        proc = mod.ReactiveProcessor(concurrency=1)
        results = proc.process_with_retry(obs, handler=lambda x: x * 5, max_retries=2)
        assert results == [5, 10, 15]

    def test_process_with_retry_recovers_from_transient_error(self, mod):
        call_counts: dict = {}

        def _flaky(x):
            call_counts.setdefault(x, 0)
            call_counts[x] += 1
            if call_counts[x] < 2 and x == 2:
                raise ValueError("transient")
            return x * 100

        obs = mod.Observable.from_iterable([1, 2, 3])
        proc = mod.ReactiveProcessor(concurrency=1)
        results = proc.process_with_retry(obs, _flaky, max_retries=3)
        assert results == [100, 200, 300]
        assert call_counts[2] == 2  # retried once

    def test_process_with_retry_raises_after_exhaustion(self, mod):
        def _always_fail(x):
            raise RuntimeError("always")

        obs = mod.Observable.from_iterable([1])
        proc = mod.ReactiveProcessor(concurrency=1)
        with pytest.raises(RuntimeError, match="always"):
            proc.process_with_retry(obs, _always_fail, max_retries=2)

    def test_process_with_retry_invalid_max_retries_raises(self, mod):
        obs = mod.Observable.from_iterable([1])
        proc = mod.ReactiveProcessor(concurrency=1)
        with pytest.raises(ValueError):
            proc.process_with_retry(obs, lambda x: x, max_retries=-1)

    def test_process_high_concurrency(self, mod):
        obs = mod.Observable.from_iterable(range(10))
        proc = mod.ReactiveProcessor(concurrency=5)
        results = proc.process(obs, handler=lambda x: x)
        assert results == list(range(10))


# ===========================================================================
# StreamMerger — merge
# ===========================================================================


class TestStreamMergerMerge:
    def test_merge_two_streams(self, mod):
        obs1 = mod.Observable.from_iterable([1, 2])
        obs2 = mod.Observable.from_iterable([3, 4])
        results = []
        mod.StreamMerger.merge(obs1, obs2).subscribe(on_next=results.append)
        assert sorted(results) == [1, 2, 3, 4]

    def test_merge_single_stream(self, mod):
        results = []
        mod.StreamMerger.merge(mod.Observable.from_iterable([10, 20])).subscribe(on_next=results.append)
        assert sorted(results) == [10, 20]

    def test_merge_empty_streams(self, mod):
        results = []
        completed = [False]
        mod.StreamMerger.merge(
            mod.Observable.from_iterable([]),
            mod.Observable.from_iterable([]),
        ).subscribe(
            on_next=results.append,
            on_complete=lambda: completed.__setitem__(0, True),
        )
        assert results == []
        assert completed[0]

    def test_merge_zero_streams_completes(self, mod):
        completed = [False]
        mod.StreamMerger.merge().subscribe(
            on_next=lambda _: None,
            on_complete=lambda: completed.__setitem__(0, True),
        )
        assert completed[0]

    def test_merge_completes_after_all_sources_done(self, mod):
        completed = [False]
        mod.StreamMerger.merge(
            mod.Observable.from_iterable([1]),
            mod.Observable.from_iterable([2]),
        ).subscribe(
            on_next=lambda _: None,
            on_complete=lambda: completed.__setitem__(0, True),
        )
        assert completed[0]

    def test_merge_error_forwarded(self, mod):
        errors = []

        def _erroring_producer(on_next, on_error, on_complete):
            on_next(1)
            on_error(ValueError("source error"))

        obs_err = mod.Observable(_erroring_producer)
        obs_ok = mod.Observable.from_iterable([10])
        mod.StreamMerger.merge(obs_ok, obs_err).subscribe(
            on_next=lambda _: None,
            on_error=errors.append,
        )
        assert len(errors) == 1


# ===========================================================================
# StreamMerger — zip
# ===========================================================================


class TestStreamMergerZip:
    def test_zip_pairs_items(self, mod):
        results = []
        mod.StreamMerger.zip(
            mod.Observable.from_iterable([1, 2, 3]),
            mod.Observable.from_iterable(["a", "b", "c"]),
        ).subscribe(on_next=results.append)
        assert results == [(1, "a"), (2, "b"), (3, "c")]

    def test_zip_stops_at_shorter_stream(self, mod):
        results = []
        mod.StreamMerger.zip(
            mod.Observable.from_iterable([1, 2, 3]),
            mod.Observable.from_iterable([10, 20]),
        ).subscribe(on_next=results.append)
        assert results == [(1, 10), (2, 20)]

    def test_zip_three_streams(self, mod):
        results = []
        mod.StreamMerger.zip(
            mod.Observable.from_iterable([1, 2]),
            mod.Observable.from_iterable(["a", "b"]),
            mod.Observable.from_iterable([True, False]),
        ).subscribe(on_next=results.append)
        assert results == [(1, "a", True), (2, "b", False)]

    def test_zip_zero_streams_completes(self, mod):
        completed = [False]
        mod.StreamMerger.zip().subscribe(
            on_next=lambda _: None,
            on_complete=lambda: completed.__setitem__(0, True),
        )
        assert completed[0]

    def test_zip_one_empty_stream_yields_nothing(self, mod):
        results = []
        completed = [False]
        mod.StreamMerger.zip(
            mod.Observable.from_iterable([1, 2]),
            mod.Observable.from_iterable([]),
        ).subscribe(
            on_next=results.append,
            on_complete=lambda: completed.__setitem__(0, True),
        )
        assert results == []
        assert completed[0]

    def test_zip_completes_after_pairing(self, mod):
        completed = [False]
        mod.StreamMerger.zip(
            mod.Observable.from_iterable([1]),
            mod.Observable.from_iterable([2]),
        ).subscribe(
            on_next=lambda _: None,
            on_complete=lambda: completed.__setitem__(0, True),
        )
        assert completed[0]


# ===========================================================================
# StreamMerger — concat
# ===========================================================================


class TestStreamMergerConcat:
    def test_concat_sequential_order(self, mod):
        results = []
        mod.StreamMerger.concat(
            mod.Observable.from_iterable([1, 2]),
            mod.Observable.from_iterable([3, 4]),
        ).subscribe(on_next=results.append)
        assert results == [1, 2, 3, 4]

    def test_concat_three_streams(self, mod):
        results = []
        mod.StreamMerger.concat(
            mod.Observable.from_iterable([1]),
            mod.Observable.from_iterable([2, 3]),
            mod.Observable.from_iterable([4]),
        ).subscribe(on_next=results.append)
        assert results == [1, 2, 3, 4]

    def test_concat_empty_middle(self, mod):
        results = []
        mod.StreamMerger.concat(
            mod.Observable.from_iterable([1]),
            mod.Observable.from_iterable([]),
            mod.Observable.from_iterable([2]),
        ).subscribe(on_next=results.append)
        assert results == [1, 2]

    def test_concat_single_stream(self, mod):
        results = []
        mod.StreamMerger.concat(mod.Observable.from_iterable([10, 20, 30])).subscribe(on_next=results.append)
        assert results == [10, 20, 30]

    def test_concat_no_streams_completes(self, mod):
        completed = [False]
        mod.StreamMerger.concat().subscribe(
            on_next=lambda _: None,
            on_complete=lambda: completed.__setitem__(0, True),
        )
        assert completed[0]

    def test_concat_completes_after_all(self, mod):
        completed = [False]
        mod.StreamMerger.concat(
            mod.Observable.from_iterable([1, 2]),
            mod.Observable.from_iterable([3]),
        ).subscribe(
            on_next=lambda _: None,
            on_complete=lambda: completed.__setitem__(0, True),
        )
        assert completed[0]

    def test_concat_error_stops_processing(self, mod):
        errors = []
        results = []

        def _error_producer(on_next, on_error, on_complete):
            on_next(99)
            on_error(RuntimeError("concat error"))

        obs_err = mod.Observable(_error_producer)
        obs_after = mod.Observable.from_iterable([100])

        mod.StreamMerger.concat(obs_err, obs_after).subscribe(
            on_next=results.append,
            on_error=errors.append,
        )
        assert 99 in results
        assert 100 not in results
        assert len(errors) == 1


# ===========================================================================
# Cross-pattern integration tests
# ===========================================================================


class TestCrossPattern:
    def test_subject_with_map_operator(self, mod):
        """Subject emissions can be chained through operators via wrap."""
        subject = mod.Subject()
        results = []

        # Wrap subject in an Observable for operator use
        def _producer(on_next, on_error, on_complete):
            subject.subscribe(on_next, on_error, on_complete)

        obs = mod.Observable(_producer).map(lambda x: x * 2)
        obs.subscribe(on_next=results.append)

        subject.emit(1)
        subject.emit(2)
        subject.complete()

        assert results == [2, 4]

    def test_reactive_processor_with_backpressure_buffer(self, mod):
        """Items from an Observable can be forwarded into a BackpressureBuffer."""
        buf = mod.BackpressureBuffer(capacity=10)
        obs = mod.Observable.from_iterable([1, 2, 3])

        proc = mod.ReactiveProcessor(concurrency=1)
        proc.process(obs, handler=lambda x: buf.put(x))

        collected = []
        while buf.size > 0:
            collected.append(buf.get(timeout_seconds=0.5))

        assert sorted(collected) == [1, 2, 3]

    def test_merge_then_process(self, mod):
        obs1 = mod.Observable.from_iterable([1, 2])
        obs2 = mod.Observable.from_iterable([3, 4])
        merged = mod.StreamMerger.merge(obs1, obs2)
        proc = mod.ReactiveProcessor(concurrency=2)
        results = proc.process(merged, handler=lambda x: x * 10)
        assert sorted(results) == [10, 20, 30, 40]

    def test_concat_then_take_then_filter(self, mod):
        results = []
        mod.StreamMerger.concat(
            mod.Observable.from_iterable([1, 2, 3]),
            mod.Observable.from_iterable([4, 5, 6]),
        ).take(5).filter(lambda x: x % 2 != 0).subscribe(on_next=results.append)
        # first 5 items: 1,2,3,4,5 — odd ones: 1,3,5
        assert results == [1, 3, 5]
