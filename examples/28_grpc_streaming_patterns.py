"""
28_grpc_streaming_patterns.py — gRPC Streaming Patterns

Pure-Python simulation of the four gRPC streaming modes plus a streaming
interceptor middleware layer. No gRPC library is required — the protocol
semantics are replicated using generators, lists, ``queue.Queue``, and
Python callables.

    Pattern 1 — UnaryUnary (standard RPC):
                One request dict → one response dict.  An ordered chain of
                interceptor callables transforms the request before it reaches
                the handler.  ``call_with_metadata`` additionally returns a
                response-metadata dict that includes ``"server_time"``
                (fractional seconds since epoch).

    Pattern 2 — ServerStreaming (one request → stream of responses):
                The handler is a generator factory — it receives the request
                and yields zero or more response dicts.  ``call_with_limit``
                materialises at most *max_items* responses into a list without
                exhausting the underlying generator.  A cumulative
                ``item_count`` property tracks all items ever yielded.

    Pattern 3 — ClientStreaming (stream of requests → one response):
                The handler receives the complete list of request dicts and
                returns a single response dict.  ``stream_and_call`` lazily
                consumes a request generator, collecting all items before
                forwarding them to the handler — simulating client-side
                streaming over a synchronous channel.

    Pattern 4 — BidirectionalStreaming (stream ↔ stream):
                The handler processes one request at a time and returns either
                a response dict or ``None`` (to skip that request).
                ``call_with_queue`` provides a thread-safe variant: one thread
                pushes requests into an ``input_queue``; the method drains the
                queue until a sentinel value is seen, writes non-None responses
                into ``output_queue``, and returns.  A cumulative
                ``response_count`` property tracks all non-None responses ever
                produced.

    Pattern 5 — StreamingInterceptor (middleware for streaming calls):
                Maintains an ordered registry of named request-transforms and
                named response-transforms.  ``wrap_server_streaming`` returns a
                new handler callable that applies all request-transforms to the
                incoming request before delegation, then applies all
                response-transforms to each yielded item.
                ``wrap_bidirectional`` does the same for bidirectional handlers
                (one request → one optional response per call).  A
                ``transform_count`` property reports the total number of
                registered transforms across both lists.

No external dependencies required.

Run:
    python examples/28_grpc_streaming_patterns.py
"""

from __future__ import annotations

import queue
import time
import threading
from typing import Any, Callable, Generator, Iterator, Optional


# ===========================================================================
# Pattern 1 — UnaryUnary
# ===========================================================================


class UnaryUnary:
    """Simulates a unary gRPC RPC (one request → one response).

    An ordered chain of interceptor callables transforms the request dict
    before it is forwarded to the underlying handler.  Interceptors are
    applied in the order they were added via :meth:`add_interceptor`.

    Example::

        rpc = UnaryUnary(handler=lambda req: {"echo": req["msg"]})
        rpc.add_interceptor(lambda req: {**req, "trace_id": "abc"})
        resp = rpc.call({"msg": "hello"})
        assert resp == {"echo": "hello"}
    """

    def __init__(self, handler: Callable[[dict], dict]) -> None:
        self._handler = handler
        self.interceptors: list[Callable[[dict], dict]] = []

    # ------------------------------------------------------------------
    # Interceptor registration
    # ------------------------------------------------------------------

    def add_interceptor(self, fn: Callable[[dict], dict]) -> None:
        """Append *fn* to the interceptor chain.

        Each interceptor receives the (possibly already-transformed) request
        dict and must return a (possibly modified) request dict.

        Args:
            fn: Callable that accepts a request dict and returns a request dict.
        """
        self.interceptors.append(fn)

    # ------------------------------------------------------------------
    # Call variants
    # ------------------------------------------------------------------

    def _apply_interceptors(self, request: dict) -> dict:
        """Run the request through every registered interceptor in order.

        Args:
            request: The original request dict.

        Returns:
            The request dict after all interceptors have been applied.
        """
        result = request
        for fn in self.interceptors:
            result = fn(result)
        return result

    def call(self, request: dict) -> dict:
        """Invoke the RPC with *request*.

        The request is first passed through the interceptor chain; the final
        transformed request is forwarded to the handler.

        Args:
            request: Incoming request dict.

        Returns:
            The handler's response dict.
        """
        transformed = self._apply_interceptors(request)
        return self._handler(transformed)

    def call_with_metadata(
        self, request: dict, metadata: dict
    ) -> tuple[dict, dict]:
        """Invoke the RPC and return both the response and server metadata.

        Server metadata always includes ``"server_time"`` (a float: seconds
        since the Unix epoch at the moment the response is produced) plus any
        entries forwarded from the incoming *metadata* dict under the key
        ``"client_metadata"``.

        Args:
            request:  Incoming request dict.
            metadata: Client-supplied metadata dict (passed through to the
                      response metadata for inspection).

        Returns:
            A ``(response, response_metadata)`` tuple where *response_metadata*
            contains at least ``"server_time"`` (float).
        """
        transformed = self._apply_interceptors(request)
        response = self._handler(transformed)
        response_metadata: dict = {
            "server_time": time.time(),
            "client_metadata": metadata,
        }
        return response, response_metadata


# ===========================================================================
# Pattern 2 — ServerStreaming
# ===========================================================================


class ServerStreaming:
    """Simulates a server-streaming gRPC RPC (one request → stream of responses).

    The *handler* is a generator factory: it accepts the request dict and
    yields zero or more response dicts.  :meth:`call` wraps the generator
    transparently while tracking the cumulative item count.

    Example::

        def range_handler(req):
            for i in range(req["count"]):
                yield {"index": i}

        rpc = ServerStreaming(handler=range_handler)
        items = list(rpc.call({"count": 3}))
        assert len(items) == 3
    """

    def __init__(self, handler: Callable[[dict], Iterator[dict]]) -> None:
        self._handler = handler
        self._item_count: int = 0

    # ------------------------------------------------------------------
    # Call variants
    # ------------------------------------------------------------------

    def call(self, request: dict) -> Generator[dict, None, None]:
        """Return a generator that yields all responses produced by the handler.

        Each yielded item increments :attr:`item_count`.

        Args:
            request: Incoming request dict.

        Yields:
            Response dicts from the handler generator.
        """
        for item in self._handler(request):
            self._item_count += 1
            yield item

    def call_with_limit(self, request: dict, max_items: int) -> list[dict]:
        """Materialise up to *max_items* responses from the server stream.

        Stops as soon as *max_items* have been collected or the stream is
        exhausted — whichever comes first.  Items are counted toward
        :attr:`item_count` even when the stream is truncated.

        Args:
            request:   Incoming request dict.
            max_items: Maximum number of response items to collect.

        Returns:
            A list containing at most *max_items* response dicts.
        """
        results: list[dict] = []
        for item in self._handler(request):
            self._item_count += 1
            results.append(item)
            if len(results) >= max_items:
                break
        return results

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def item_count(self) -> int:
        """Cumulative number of items yielded across all calls."""
        return self._item_count


# ===========================================================================
# Pattern 3 — ClientStreaming
# ===========================================================================


class ClientStreaming:
    """Simulates a client-streaming gRPC RPC (stream of requests → one response).

    The *handler* receives the complete list of request dicts and returns a
    single response dict.

    Example::

        def sum_handler(requests):
            total = sum(r["value"] for r in requests)
            return {"total": total}

        rpc = ClientStreaming(handler=sum_handler)
        response = rpc.call([{"value": 1}, {"value": 2}, {"value": 3}])
        assert response == {"total": 6}
    """

    def __init__(self, handler: Callable[[list[dict]], dict]) -> None:
        self._handler = handler

    # ------------------------------------------------------------------
    # Call variants
    # ------------------------------------------------------------------

    def call(self, requests: list[dict]) -> dict:
        """Pass *requests* directly to the handler and return its response.

        Args:
            requests: Complete list of client request dicts.

        Returns:
            Single response dict from the handler.
        """
        return self._handler(requests)

    def stream_and_call(
        self, request_generator: Iterator[dict]
    ) -> dict:
        """Consume *request_generator*, collect all requests, then call handler.

        Simulates a client that builds a stream lazily (e.g. reading from a
        file or network socket) and forwards the complete batch to the server.

        Args:
            request_generator: Any iterable/generator producing request dicts.

        Returns:
            Single response dict from the handler.
        """
        collected: list[dict] = list(request_generator)
        return self._handler(collected)


# ===========================================================================
# Pattern 4 — BidirectionalStreaming
# ===========================================================================


class BidirectionalStreaming:
    """Simulates a bidirectional-streaming gRPC RPC (stream ↔ stream).

    The *handler* processes one request dict at a time and returns either a
    response dict or ``None``.  Returning ``None`` causes that slot to be
    skipped — no item is written to the output stream.

    Example::

        def echo_handler(req):
            if req.get("skip"):
                return None
            return {"echo": req["msg"]}

        rpc = BidirectionalStreaming(handler=echo_handler)
        responses = rpc.call([
            {"msg": "hi"},
            {"msg": "bye", "skip": True},
        ])
        assert responses == [{"echo": "hi"}]
    """

    def __init__(
        self, handler: Callable[[dict], Optional[dict]]
    ) -> None:
        self._handler = handler
        self._response_count: int = 0

    # ------------------------------------------------------------------
    # Call variants
    # ------------------------------------------------------------------

    def call(self, requests: list[dict]) -> list[dict]:
        """Process every request through the handler; collect non-None responses.

        Args:
            requests: Ordered list of request dicts.

        Returns:
            List of response dicts (``None`` responses are excluded).
        """
        responses: list[dict] = []
        for req in requests:
            resp = self._handler(req)
            if resp is not None:
                self._response_count += 1
                responses.append(resp)
        return responses

    def call_with_queue(
        self,
        input_queue: queue.Queue,
        output_queue: queue.Queue,
        sentinel: object = None,
    ) -> None:
        """Thread-safe bidirectional streaming via ``queue.Queue`` objects.

        Reads request dicts from *input_queue* until *sentinel* is dequeued,
        processes each through the handler, and puts non-None responses into
        *output_queue*.

        Designed to be called from one thread while another thread feeds
        *input_queue* and consumes *output_queue*.

        Args:
            input_queue:  Queue supplying request dicts (and eventually the
                          sentinel value to signal end-of-stream).
            output_queue: Queue into which non-None response dicts are placed.
            sentinel:     Value that signals end-of-stream (default ``None``).
        """
        while True:
            item = input_queue.get()
            if item is sentinel:
                break
            resp = self._handler(item)
            if resp is not None:
                self._response_count += 1
                output_queue.put(resp)

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def response_count(self) -> int:
        """Cumulative number of non-None responses produced across all calls."""
        return self._response_count


# ===========================================================================
# Pattern 5 — StreamingInterceptor
# ===========================================================================


class StreamingInterceptor:
    """Middleware layer for streaming gRPC calls.

    Maintains two ordered registries:

    * **request-transforms** — each is applied to every incoming request dict
      before it reaches the underlying handler.
    * **response-transforms** — each is applied to every outgoing response
      dict before it is yielded or returned to the caller.

    :meth:`wrap_server_streaming` returns a *new* handler callable that embeds
    both transform lists.  The wrapped handler conforms to the same signature
    as the original ``ServerStreaming`` handler (accepts ``dict``, yields
    ``dict``).

    :meth:`wrap_bidirectional` returns a new single-request handler (accepts
    ``dict``, returns ``dict | None``) with both transform lists embedded.

    Example::

        interceptor = StreamingInterceptor()
        interceptor.add_request_transform("add_version", lambda r: {**r, "v": 2})
        interceptor.add_response_transform("add_ok", lambda r: {**r, "ok": True})

        def server_handler(req):
            yield {"data": req["payload"]}

        wrapped = interceptor.wrap_server_streaming(server_handler)
        rpc = ServerStreaming(handler=wrapped)
        items = list(rpc.call({"payload": "hello"}))
        assert items[0]["ok"] is True
        assert items[0]["v"] is None   # v was on the request, not the response
    """

    def __init__(self) -> None:
        self._request_transforms: list[
            tuple[str, Callable[[dict], dict]]
        ] = []
        self._response_transforms: list[
            tuple[str, Callable[[dict], dict]]
        ] = []

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def add_request_transform(
        self, name: str, fn: Callable[[dict], dict]
    ) -> None:
        """Register a named request transform.

        Transforms are applied in insertion order to each incoming request.

        Args:
            name: Identifier for this transform (used in introspection).
            fn:   Callable that accepts a request dict and returns one.
        """
        self._request_transforms.append((name, fn))

    def add_response_transform(
        self, name: str, fn: Callable[[dict], dict]
    ) -> None:
        """Register a named response transform.

        Transforms are applied in insertion order to each outgoing response.

        Args:
            name: Identifier for this transform.
            fn:   Callable that accepts a response dict and returns one.
        """
        self._response_transforms.append((name, fn))

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _apply_request_transforms(self, request: dict) -> dict:
        result = request
        for _name, fn in self._request_transforms:
            result = fn(result)
        return result

    def _apply_response_transforms(self, response: dict) -> dict:
        result = response
        for _name, fn in self._response_transforms:
            result = fn(result)
        return result

    # ------------------------------------------------------------------
    # Wrapping
    # ------------------------------------------------------------------

    def wrap_server_streaming(
        self, handler: Callable[[dict], Iterator[dict]]
    ) -> Callable[[dict], Generator[dict, None, None]]:
        """Wrap a server-streaming handler with request and response transforms.

        Args:
            handler: Original handler callable (request dict → generator of
                     response dicts).

        Returns:
            A new handler callable with the same signature that transparently
            applies all registered transforms.
        """
        interceptor = self  # capture for closure

        def wrapped(request: dict) -> Generator[dict, None, None]:
            transformed_request = interceptor._apply_request_transforms(request)
            for response in handler(transformed_request):
                yield interceptor._apply_response_transforms(response)

        return wrapped

    def wrap_bidirectional(
        self, handler: Callable[[dict], Optional[dict]]
    ) -> Callable[[dict], Optional[dict]]:
        """Wrap a bidirectional handler with request and response transforms.

        The wrapped handler applies request-transforms to the incoming request
        and (when the original handler returns a non-None response)
        response-transforms to the outgoing response.

        Args:
            handler: Original per-request handler (request dict → response
                     dict or None).

        Returns:
            A new per-request handler with the same signature.
        """
        interceptor = self

        def wrapped(request: dict) -> Optional[dict]:
            transformed_request = interceptor._apply_request_transforms(request)
            response = handler(transformed_request)
            if response is None:
                return None
            return interceptor._apply_response_transforms(response)

        return wrapped

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def transform_count(self) -> int:
        """Total number of transforms registered (request + response)."""
        return len(self._request_transforms) + len(self._response_transforms)


# ===========================================================================
# Smoke-test helpers (used only in __main__)
# ===========================================================================


def _separator(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def _run_unary_unary_demo() -> None:
    """Scenario 1: UnaryUnary basic call, metadata, interceptor chain."""
    _separator("Scenario 1 — UnaryUnary: basic + metadata + interceptors")

    rpc = UnaryUnary(handler=lambda req: {"pong": req.get("ping"), "trace": req.get("trace_id")})
    rpc.add_interceptor(lambda req: {**req, "trace_id": "abc-123"})

    resp = rpc.call({"ping": "hello"})
    print(f"Response       : {resp}")
    assert resp["pong"] == "hello"
    assert resp["trace"] == "abc-123"

    resp2, meta = rpc.call_with_metadata({"ping": "world"}, {"client_id": "c1"})
    print(f"With metadata  : {resp2}")
    print(f"Server time    : {meta['server_time']}")
    assert isinstance(meta["server_time"], float)
    assert meta["server_time"] > 0
    print("PASS")


def _run_server_streaming_demo() -> None:
    """Scenario 2: ServerStreaming call, limit, item_count."""
    _separator("Scenario 2 — ServerStreaming: yield all + limit + count")

    def range_handler(req: dict) -> Iterator[dict]:
        for i in range(req.get("count", 5)):
            yield {"index": i}

    rpc = ServerStreaming(handler=range_handler)
    items = list(rpc.call({"count": 4}))
    print(f"All items      : {items}")
    assert len(items) == 4

    limited = rpc.call_with_limit({"count": 10}, max_items=3)
    print(f"Limited items  : {limited}")
    assert len(limited) == 3

    print(f"Item count     : {rpc.item_count}")
    assert rpc.item_count == 7  # 4 + 3
    print("PASS")


def _run_client_streaming_demo() -> None:
    """Scenario 3: ClientStreaming call and stream_and_call."""
    _separator("Scenario 3 — ClientStreaming: list + generator")

    def sum_handler(requests: list[dict]) -> dict:
        return {"total": sum(r.get("value", 0) for r in requests)}

    rpc = ClientStreaming(handler=sum_handler)
    resp = rpc.call([{"value": 1}, {"value": 2}, {"value": 3}])
    print(f"Sum via call   : {resp}")
    assert resp == {"total": 6}

    gen = ({"value": i} for i in range(1, 6))
    resp2 = rpc.stream_and_call(gen)
    print(f"Sum via stream : {resp2}")
    assert resp2 == {"total": 15}
    print("PASS")


def _run_bidirectional_streaming_demo() -> None:
    """Scenario 4: BidirectionalStreaming call and call_with_queue."""
    _separator("Scenario 4 — BidirectionalStreaming: basic + queue + count")

    def echo_handler(req: dict) -> Optional[dict]:
        if req.get("skip"):
            return None
        return {"echo": req["msg"]}

    rpc = BidirectionalStreaming(handler=echo_handler)
    responses = rpc.call([
        {"msg": "hi"},
        {"msg": "bye", "skip": True},
        {"msg": "there"},
    ])
    print(f"Responses      : {responses}")
    assert len(responses) == 2
    assert responses[0]["echo"] == "hi"

    in_q: queue.Queue = queue.Queue()
    out_q: queue.Queue = queue.Queue()
    sentinel = object()
    in_q.put({"msg": "a"})
    in_q.put({"msg": "b"})
    in_q.put(sentinel)
    rpc.call_with_queue(in_q, out_q, sentinel=sentinel)
    collected = []
    while not out_q.empty():
        collected.append(out_q.get())
    print(f"Queue results  : {collected}")
    assert len(collected) == 2

    print(f"Response count : {rpc.response_count}")
    assert rpc.response_count == 4  # 2 from call + 2 from call_with_queue
    print("PASS")


def _run_streaming_interceptor_demo() -> None:
    """Scenario 5: StreamingInterceptor wraps server-streaming and bidi handlers."""
    _separator("Scenario 5 — StreamingInterceptor: server-streaming + bidi")

    interceptor = StreamingInterceptor()
    interceptor.add_request_transform("add_tenant", lambda r: {**r, "tenant": "acme"})
    interceptor.add_response_transform("add_ok", lambda r: {**r, "ok": True})

    print(f"Transform count: {interceptor.transform_count}")
    assert interceptor.transform_count == 2

    def server_handler(req: dict) -> Iterator[dict]:
        yield {"data": req.get("payload"), "tenant_echo": req.get("tenant")}

    wrapped_server = interceptor.wrap_server_streaming(server_handler)
    rpc = ServerStreaming(handler=wrapped_server)
    items = list(rpc.call({"payload": "hello"}))
    print(f"Server-stream  : {items}")
    assert items[0]["ok"] is True
    assert items[0]["tenant_echo"] == "acme"

    def bidi_handler(req: dict) -> Optional[dict]:
        return {"result": req.get("x", 0) * 2}

    wrapped_bidi = interceptor.wrap_bidirectional(bidi_handler)
    bidi_rpc = BidirectionalStreaming(handler=wrapped_bidi)
    responses = bidi_rpc.call([{"x": 5}, {"x": 10}])
    print(f"Bidi responses : {responses}")
    assert responses[0]["result"] == 10
    assert responses[0]["ok"] is True
    print("PASS")


# ===========================================================================
# Entry point
# ===========================================================================


if __name__ == "__main__":
    print("gRPC Streaming Patterns — Smoke Test Suite")
    print(
        "Patterns: UnaryUnary | ServerStreaming | ClientStreaming "
        "| BidirectionalStreaming | StreamingInterceptor"
    )

    _run_unary_unary_demo()
    _run_server_streaming_demo()
    _run_client_streaming_demo()
    _run_bidirectional_streaming_demo()
    _run_streaming_interceptor_demo()

    print("\n" + "=" * 60)
    print("  All scenarios passed.")
    print("=" * 60)
