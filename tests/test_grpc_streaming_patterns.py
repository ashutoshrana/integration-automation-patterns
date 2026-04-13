"""
Tests for 28_grpc_streaming_patterns.py

Covers:
    - UnaryUnary               (12 tests)
    - ServerStreaming           (11 tests)
    - ClientStreaming           (10 tests)
    - BidirectionalStreaming    (11 tests)
    - StreamingInterceptor     (10 tests)

Total: 54 tests
"""

from __future__ import annotations

import importlib.util
import queue
import sys
import threading
import types
from collections.abc import Iterator
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Module loader
# ---------------------------------------------------------------------------

_MOD_PATH = Path(__file__).parent.parent / "examples" / "28_grpc_streaming_patterns.py"


def _load_module():
    module_name = "grpc_streaming_patterns"
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


# ===========================================================================
# UnaryUnary tests
# ===========================================================================


class TestUnaryUnary:
    """12 tests covering basic call, metadata, interceptor chain."""

    def test_basic_call_invokes_handler(self, m):
        """call() passes the request to the handler and returns its response."""
        rpc = m.UnaryUnary(handler=lambda req: {"pong": req["ping"]})
        resp = rpc.call({"ping": "hello"})
        assert resp == {"pong": "hello"}

    def test_call_with_empty_request(self, m):
        """call() works with an empty request dict."""
        rpc = m.UnaryUnary(handler=lambda req: {"empty": True})
        resp = rpc.call({})
        assert resp == {"empty": True}

    def test_call_with_metadata_returns_tuple(self, m):
        """call_with_metadata() returns a (response, metadata) tuple."""
        rpc = m.UnaryUnary(handler=lambda req: {"ok": True})
        result = rpc.call_with_metadata({"x": 1}, {"client": "c1"})
        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_call_with_metadata_response_correct(self, m):
        """The first element of call_with_metadata is the handler's response."""
        rpc = m.UnaryUnary(handler=lambda req: {"value": req["n"] * 2})
        resp, _ = rpc.call_with_metadata({"n": 7}, {})
        assert resp == {"value": 14}

    def test_call_with_metadata_includes_server_time(self, m):
        """Response metadata from call_with_metadata contains 'server_time' as a float."""
        rpc = m.UnaryUnary(handler=lambda req: {})
        _, meta = rpc.call_with_metadata({}, {})
        assert "server_time" in meta
        assert isinstance(meta["server_time"], float)
        assert meta["server_time"] > 0

    def test_call_with_metadata_server_time_is_recent(self, m):
        """server_time is within the last 5 seconds (sanity check)."""
        import time

        rpc = m.UnaryUnary(handler=lambda req: {})
        _, meta = rpc.call_with_metadata({}, {})
        assert abs(meta["server_time"] - time.time()) < 5.0

    def test_single_interceptor_transforms_request(self, m):
        """A single interceptor modifies the request before it reaches the handler."""
        rpc = m.UnaryUnary(handler=lambda req: {"got": req.get("added")})
        rpc.add_interceptor(lambda req: {**req, "added": "yes"})
        resp = rpc.call({"original": True})
        assert resp["got"] == "yes"

    def test_multiple_interceptors_applied_in_order(self, m):
        """Multiple interceptors are applied in the order they were added."""
        rpc = m.UnaryUnary(handler=lambda req: {"steps": req.get("steps", [])})
        rpc.add_interceptor(lambda req: {**req, "steps": req.get("steps", []) + ["first"]})
        rpc.add_interceptor(lambda req: {**req, "steps": req.get("steps", []) + ["second"]})
        resp = rpc.call({})
        assert resp["steps"] == ["first", "second"]

    def test_interceptors_list_initially_empty(self, m):
        """A newly created UnaryUnary has an empty interceptors list."""
        rpc = m.UnaryUnary(handler=lambda req: req)
        assert rpc.interceptors == []

    def test_add_interceptor_appends_to_list(self, m):
        """add_interceptor appends the callable to the interceptors list."""

        def fn1(req):
            return req

        def fn2(req):
            return req

        rpc = m.UnaryUnary(handler=lambda req: req)
        rpc.add_interceptor(fn1)
        rpc.add_interceptor(fn2)
        assert rpc.interceptors == [fn1, fn2]

    def test_interceptor_sees_previous_interceptor_output(self, m):
        """Each interceptor receives the output of the previous one."""
        rpc = m.UnaryUnary(handler=lambda req: {"count": req.get("count")})
        rpc.add_interceptor(lambda req: {**req, "count": 1})
        rpc.add_interceptor(lambda req: {**req, "count": req.get("count", 0) + 1})
        resp = rpc.call({})
        assert resp["count"] == 2

    def test_call_without_interceptors_passes_request_directly(self, m):
        """With no interceptors, the original request is passed to the handler unchanged."""
        captured = []
        rpc = m.UnaryUnary(handler=lambda req: captured.append(req) or {})
        original = {"key": "value"}
        rpc.call(original)
        assert captured[0] is original


# ===========================================================================
# ServerStreaming tests
# ===========================================================================


class TestServerStreaming:
    """11 tests covering call, call_with_limit, item_count."""

    def _make_range_handler(self, count: int):
        """Helper: creates a handler that yields ``count`` dicts."""

        def handler(req: dict) -> Iterator[dict]:
            for i in range(req.get("count", count)):
                yield {"index": i}

        return handler

    def test_call_yields_all_items(self, m):
        """call() yields every item produced by the handler generator."""
        rpc = m.ServerStreaming(handler=self._make_range_handler(4))
        items = list(rpc.call({"count": 4}))
        assert len(items) == 4

    def test_call_yields_correct_values(self, m):
        """Items yielded by call() match the handler's output."""
        rpc = m.ServerStreaming(handler=self._make_range_handler(3))
        items = list(rpc.call({"count": 3}))
        assert items == [{"index": 0}, {"index": 1}, {"index": 2}]

    def test_call_with_empty_stream(self, m):
        """call() returns an empty iterable when the handler yields nothing."""
        rpc = m.ServerStreaming(handler=lambda req: iter([]))
        items = list(rpc.call({}))
        assert items == []

    def test_call_with_limit_stops_at_max(self, m):
        """call_with_limit() returns at most max_items items."""
        rpc = m.ServerStreaming(handler=self._make_range_handler(100))
        items = rpc.call_with_limit({"count": 100}, max_items=5)
        assert len(items) == 5

    def test_call_with_limit_returns_all_when_stream_shorter(self, m):
        """call_with_limit() returns all items when stream has fewer than max_items."""
        rpc = m.ServerStreaming(handler=self._make_range_handler(3))
        items = rpc.call_with_limit({"count": 3}, max_items=100)
        assert len(items) == 3

    def test_call_with_limit_returns_list(self, m):
        """call_with_limit() returns a list, not a generator."""
        rpc = m.ServerStreaming(handler=self._make_range_handler(5))
        result = rpc.call_with_limit({}, max_items=3)
        assert isinstance(result, list)

    def test_item_count_starts_at_zero(self, m):
        """A newly created ServerStreaming has item_count == 0."""
        rpc = m.ServerStreaming(handler=self._make_range_handler(5))
        assert rpc.item_count == 0

    def test_item_count_increments_per_yielded_item(self, m):
        """item_count increases by the number of items each call yields."""
        rpc = m.ServerStreaming(handler=self._make_range_handler(4))
        list(rpc.call({"count": 4}))
        assert rpc.item_count == 4

    def test_item_count_accumulates_across_calls(self, m):
        """item_count accumulates across multiple calls."""
        rpc = m.ServerStreaming(handler=self._make_range_handler(3))
        list(rpc.call({"count": 3}))
        list(rpc.call({"count": 3}))
        assert rpc.item_count == 6

    def test_item_count_includes_call_with_limit(self, m):
        """Items consumed via call_with_limit are counted in item_count."""
        rpc = m.ServerStreaming(handler=self._make_range_handler(10))
        rpc.call_with_limit({"count": 10}, max_items=4)
        assert rpc.item_count == 4

    def test_call_is_generator(self, m):
        """call() returns a generator (lazy evaluation)."""
        rpc = m.ServerStreaming(handler=self._make_range_handler(3))
        result = rpc.call({"count": 3})
        assert hasattr(result, "__iter__") and hasattr(result, "__next__")


# ===========================================================================
# ClientStreaming tests
# ===========================================================================


class TestClientStreaming:
    """10 tests covering call, stream_and_call, aggregation handlers."""

    def test_call_passes_list_to_handler(self, m):
        """call() passes the entire request list to the handler."""
        captured = []
        rpc = m.ClientStreaming(handler=lambda reqs: captured.extend(reqs) or {})
        rpc.call([{"a": 1}, {"a": 2}])
        assert len(captured) == 2

    def test_call_returns_handler_response(self, m):
        """call() returns the single response dict from the handler."""
        rpc = m.ClientStreaming(handler=lambda reqs: {"count": len(reqs)})
        resp = rpc.call([{"x": 1}, {"x": 2}, {"x": 3}])
        assert resp == {"count": 3}

    def test_call_with_empty_list(self, m):
        """call() with an empty list passes an empty list to the handler."""
        rpc = m.ClientStreaming(handler=lambda reqs: {"count": len(reqs)})
        resp = rpc.call([])
        assert resp == {"count": 0}

    def test_call_aggregates_values(self, m):
        """An aggregation handler correctly sums values from all requests."""
        rpc = m.ClientStreaming(handler=lambda reqs: {"total": sum(r.get("value", 0) for r in reqs)})
        resp = rpc.call([{"value": 10}, {"value": 20}, {"value": 30}])
        assert resp == {"total": 60}

    def test_stream_and_call_consumes_generator(self, m):
        """stream_and_call() consumes a generator and passes collected items to handler."""
        rpc = m.ClientStreaming(handler=lambda reqs: {"count": len(reqs)})
        gen = ({"n": i} for i in range(5))
        resp = rpc.stream_and_call(gen)
        assert resp == {"count": 5}

    def test_stream_and_call_preserves_order(self, m):
        """stream_and_call() preserves the generator's item order."""
        order = []

        def capture_handler(reqs):
            order.extend(reqs)
            return {}

        rpc = m.ClientStreaming(handler=capture_handler)
        gen = ({"seq": i} for i in [3, 1, 4, 1, 5])
        rpc.stream_and_call(gen)
        assert [r["seq"] for r in order] == [3, 1, 4, 1, 5]

    def test_stream_and_call_with_empty_generator(self, m):
        """stream_and_call() with an empty generator passes an empty list to handler."""
        rpc = m.ClientStreaming(handler=lambda reqs: {"count": len(reqs)})
        resp = rpc.stream_and_call(iter([]))
        assert resp == {"count": 0}

    def test_stream_and_call_with_list_input(self, m):
        """stream_and_call() also accepts a plain list (any iterable)."""
        rpc = m.ClientStreaming(handler=lambda reqs: {"sum": sum(r["v"] for r in reqs)})
        resp = rpc.stream_and_call([{"v": 1}, {"v": 2}])
        assert resp["sum"] == 3

    def test_handler_receives_all_requests_once(self, m):
        """The handler is called exactly once with all requests."""
        call_count = [0]

        def handler(reqs):
            call_count[0] += 1
            return {"n": len(reqs)}

        rpc = m.ClientStreaming(handler=handler)
        rpc.call([{"x": 1}, {"x": 2}])
        assert call_count[0] == 1

    def test_call_single_request(self, m):
        """call() works correctly with a single-item request list."""
        rpc = m.ClientStreaming(handler=lambda reqs: reqs[0])
        resp = rpc.call([{"only": "one"}])
        assert resp == {"only": "one"}


# ===========================================================================
# BidirectionalStreaming tests
# ===========================================================================


class TestBidirectionalStreaming:
    """11 tests covering call, None skipping, call_with_queue, response_count."""

    def test_call_processes_all_requests(self, m):
        """call() processes every request and returns all non-None responses."""
        rpc = m.BidirectionalStreaming(handler=lambda req: {"echo": req["msg"]})
        responses = rpc.call([{"msg": "a"}, {"msg": "b"}, {"msg": "c"}])
        assert len(responses) == 3

    def test_call_returns_correct_response_values(self, m):
        """Responses correspond correctly to each request."""
        rpc = m.BidirectionalStreaming(handler=lambda req: {"v": req["n"] * 2})
        responses = rpc.call([{"n": 1}, {"n": 2}, {"n": 3}])
        assert responses == [{"v": 2}, {"v": 4}, {"v": 6}]

    def test_none_responses_are_skipped(self, m):
        """Requests for which the handler returns None are excluded from results."""

        def handler(req: dict) -> dict | None:
            return None if req.get("skip") else {"kept": True}

        rpc = m.BidirectionalStreaming(handler=handler)
        responses = rpc.call(
            [
                {"skip": False},
                {"skip": True},
                {"skip": False},
            ]
        )
        assert len(responses) == 2
        assert all(r["kept"] is True for r in responses)

    def test_all_none_returns_empty_list(self, m):
        """When all responses are None, call() returns an empty list."""
        rpc = m.BidirectionalStreaming(handler=lambda req: None)
        responses = rpc.call([{"x": 1}, {"x": 2}])
        assert responses == []

    def test_empty_request_list_returns_empty(self, m):
        """call() with an empty request list returns an empty response list."""
        rpc = m.BidirectionalStreaming(handler=lambda req: {"ok": True})
        responses = rpc.call([])
        assert responses == []

    def test_response_count_starts_at_zero(self, m):
        """A newly created BidirectionalStreaming has response_count == 0."""
        rpc = m.BidirectionalStreaming(handler=lambda req: {"ok": True})
        assert rpc.response_count == 0

    def test_response_count_increments_for_non_none(self, m):
        """response_count counts only non-None responses."""

        def handler(req: dict) -> dict | None:
            return None if req.get("skip") else {}

        rpc = m.BidirectionalStreaming(handler=handler)
        rpc.call([{"skip": False}, {"skip": True}, {"skip": False}])
        assert rpc.response_count == 2

    def test_response_count_accumulates_across_calls(self, m):
        """response_count accumulates across multiple call() invocations."""
        rpc = m.BidirectionalStreaming(handler=lambda req: {"ok": True})
        rpc.call([{"x": 1}, {"x": 2}])
        rpc.call([{"x": 3}])
        assert rpc.response_count == 3

    def test_call_with_queue_basic(self, m):
        """call_with_queue() reads requests from input_queue, writes responses to output_queue."""
        rpc = m.BidirectionalStreaming(handler=lambda req: {"echo": req["msg"]})
        in_q: queue.Queue = queue.Queue()
        out_q: queue.Queue = queue.Queue()
        sentinel = object()
        in_q.put({"msg": "hello"})
        in_q.put({"msg": "world"})
        in_q.put(sentinel)
        rpc.call_with_queue(in_q, out_q, sentinel=sentinel)
        results = []
        while not out_q.empty():
            results.append(out_q.get())
        assert len(results) == 2
        assert results[0]["echo"] == "hello"
        assert results[1]["echo"] == "world"

    def test_call_with_queue_skips_none_responses(self, m):
        """call_with_queue() does not put None responses into the output queue."""

        def handler(req: dict) -> dict | None:
            return None if req.get("skip") else {"kept": True}

        rpc = m.BidirectionalStreaming(handler=handler)
        in_q: queue.Queue = queue.Queue()
        out_q: queue.Queue = queue.Queue()
        sentinel = object()
        in_q.put({"skip": False})
        in_q.put({"skip": True})
        in_q.put({"skip": False})
        in_q.put(sentinel)
        rpc.call_with_queue(in_q, out_q, sentinel=sentinel)
        results = []
        while not out_q.empty():
            results.append(out_q.get())
        assert len(results) == 2

    def test_call_with_queue_thread_safe(self, m):
        """call_with_queue() can be safely called from a separate thread."""
        rpc = m.BidirectionalStreaming(handler=lambda req: {"v": req["n"]})
        in_q: queue.Queue = queue.Queue()
        out_q: queue.Queue = queue.Queue()
        sentinel = object()

        def producer():
            for i in range(5):
                in_q.put({"n": i})
            in_q.put(sentinel)

        producer_thread = threading.Thread(target=producer)
        consumer_thread = threading.Thread(
            target=rpc.call_with_queue,
            args=(in_q, out_q),
            kwargs={"sentinel": sentinel},
        )
        producer_thread.start()
        consumer_thread.start()
        producer_thread.join()
        consumer_thread.join()

        results = []
        while not out_q.empty():
            results.append(out_q.get())
        assert len(results) == 5


# ===========================================================================
# StreamingInterceptor tests
# ===========================================================================


class TestStreamingInterceptor:
    """10 tests covering request/response transforms, wrap methods, transform_count."""

    def test_transform_count_starts_at_zero(self, m):
        """A newly created StreamingInterceptor has transform_count == 0."""
        interceptor = m.StreamingInterceptor()
        assert interceptor.transform_count == 0

    def test_add_request_transform_increments_count(self, m):
        """add_request_transform increases transform_count by 1."""
        interceptor = m.StreamingInterceptor()
        interceptor.add_request_transform("t1", lambda r: r)
        assert interceptor.transform_count == 1

    def test_add_response_transform_increments_count(self, m):
        """add_response_transform increases transform_count by 1."""
        interceptor = m.StreamingInterceptor()
        interceptor.add_response_transform("r1", lambda r: r)
        assert interceptor.transform_count == 1

    def test_transform_count_sums_both_types(self, m):
        """transform_count reflects the total of request and response transforms."""
        interceptor = m.StreamingInterceptor()
        interceptor.add_request_transform("a", lambda r: r)
        interceptor.add_request_transform("b", lambda r: r)
        interceptor.add_response_transform("c", lambda r: r)
        assert interceptor.transform_count == 3

    def test_wrap_server_streaming_applies_request_transform(self, m):
        """wrap_server_streaming applies request transforms before calling handler."""
        interceptor = m.StreamingInterceptor()
        interceptor.add_request_transform("add_flag", lambda r: {**r, "flag": "set"})

        def handler(req: dict) -> Iterator[dict]:
            yield {"flag_received": req.get("flag")}

        wrapped = interceptor.wrap_server_streaming(handler)
        rpc = m.ServerStreaming(handler=wrapped)
        items = list(rpc.call({}))
        assert items[0]["flag_received"] == "set"

    def test_wrap_server_streaming_applies_response_transform(self, m):
        """wrap_server_streaming applies response transforms to each yielded item."""
        interceptor = m.StreamingInterceptor()
        interceptor.add_response_transform("add_ok", lambda r: {**r, "ok": True})

        def handler(req: dict) -> Iterator[dict]:
            yield {"data": "hello"}

        wrapped = interceptor.wrap_server_streaming(handler)
        rpc = m.ServerStreaming(handler=wrapped)
        items = list(rpc.call({}))
        assert items[0]["ok"] is True

    def test_wrap_server_streaming_applies_both_transforms(self, m):
        """wrap_server_streaming applies both request and response transforms."""
        interceptor = m.StreamingInterceptor()
        interceptor.add_request_transform("add_tenant", lambda r: {**r, "tenant": "acme"})
        interceptor.add_response_transform("add_ok", lambda r: {**r, "ok": True})

        def handler(req: dict) -> Iterator[dict]:
            yield {"tenant_echo": req.get("tenant")}

        wrapped = interceptor.wrap_server_streaming(handler)
        rpc = m.ServerStreaming(handler=wrapped)
        items = list(rpc.call({}))
        assert items[0]["tenant_echo"] == "acme"
        assert items[0]["ok"] is True

    def test_wrap_bidirectional_applies_request_transform(self, m):
        """wrap_bidirectional applies request transforms before calling handler."""
        interceptor = m.StreamingInterceptor()
        interceptor.add_request_transform("double", lambda r: {**r, "n": r.get("n", 0) * 2})

        def handler(req: dict) -> dict | None:
            return {"result": req["n"]}

        wrapped = interceptor.wrap_bidirectional(handler)
        rpc = m.BidirectionalStreaming(handler=wrapped)
        responses = rpc.call([{"n": 5}])
        assert responses[0]["result"] == 10

    def test_wrap_bidirectional_applies_response_transform(self, m):
        """wrap_bidirectional applies response transforms to each response."""
        interceptor = m.StreamingInterceptor()
        interceptor.add_response_transform("tag", lambda r: {**r, "tagged": True})

        wrapped = interceptor.wrap_bidirectional(lambda req: {"x": req.get("x")})
        rpc = m.BidirectionalStreaming(handler=wrapped)
        responses = rpc.call([{"x": 42}])
        assert responses[0]["tagged"] is True

    def test_wrap_bidirectional_passes_through_none(self, m):
        """wrap_bidirectional does not apply response transforms when handler returns None."""
        interceptor = m.StreamingInterceptor()
        transform_called = [False]

        def response_transform(r: dict) -> dict:
            transform_called[0] = True
            return r

        interceptor.add_response_transform("check", response_transform)
        wrapped = interceptor.wrap_bidirectional(lambda req: None)
        rpc = m.BidirectionalStreaming(handler=wrapped)
        responses = rpc.call([{"x": 1}])
        assert responses == []
        assert transform_called[0] is False
