"""
30_message_broker_patterns.py — Message Broker Patterns

Pure-Python simulation of message broker primitives using only the standard
library (``queue``, ``threading``, ``time``).  No external broker libraries
are required.

    Pattern 1 — MessageQueue (priority FIFO queue):
                Thread-safe message queue backed by ``queue.PriorityQueue``.
                Messages are enqueued with an optional priority (higher integer
                = higher priority) and dequeued in strict priority order, with
                FIFO ordering preserved for equal-priority messages.  An
                optional *max_size* parameter caps queue depth; ``enqueue``
                raises ``queue.Full`` when the limit is exceeded with
                timeout=0 (non-blocking).

    Pattern 2 — TopicExchange (AMQP-style topic routing):
                Routes published messages to bound queues whose topic pattern
                matches the routing key.  Patterns follow the AMQP topic
                exchange convention: ``*`` matches exactly one dot-separated
                word; ``#`` matches zero or more dot-separated words.
                Multiple queues may be bound to overlapping patterns; each
                matching queue receives an independent copy of the message.

    Pattern 3 — FanoutExchange (broadcast to all subscribers):
                Broadcasts every published message to all currently bound
                queues.  Subscribers may be added or removed at any time via
                :meth:`bind` / :meth:`unbind`.  Each subscriber receives an
                independent shallow copy of the original message dict.

    Pattern 4 — MessageBatch (size- and time-triggered batch collector):
                Accumulates messages and returns a full batch when
                *batch_size* is reached.  :meth:`flush` may also be called
                explicitly to return a partial batch.  Tracks the total number
                of messages flushed across all flushes via :attr:`total_flushed`.

    Pattern 5 — DeduplicationFilter (idempotency filter):
                Tracks seen message IDs within a sliding time window
                (*window_seconds*).  Duplicate IDs within the window are
                rejected; after the window expires the ID may be processed
                again.  Thread-safe; expired entries are evicted lazily on
                every call.

No external dependencies required.

Run:
    python examples/30_message_broker_patterns.py
"""

from __future__ import annotations

import queue
import threading
import time
from typing import Dict, List, Optional


# ===========================================================================
# Pattern 1 — MessageQueue
# ===========================================================================


class MessageQueue:
    """Thread-safe priority FIFO message queue.

    Messages are enqueued with a *priority* value (higher integer = higher
    priority) and dequeued in descending priority order.  Ties are broken in
    FIFO order.  When *max_size* > 0 the queue is bounded; attempting to
    enqueue into a full queue raises :class:`queue.Full`.

    Example::

        mq = MessageQueue(max_size=10)
        mq.enqueue({"body": "low"}, priority=0)
        mq.enqueue({"body": "high"}, priority=10)
        msg = mq.dequeue()          # → {"body": "high"}
    """

    def __init__(self, max_size: int = 0) -> None:
        # PriorityQueue is a min-heap; negate priority for max-heap behaviour.
        self._pq: queue.PriorityQueue = queue.PriorityQueue(maxsize=max_size)
        self._counter: int = 0          # monotonic tie-breaker for FIFO order
        self._lock: threading.Lock = threading.Lock()

    # ------------------------------------------------------------------
    # Mutations
    # ------------------------------------------------------------------

    def enqueue(self, message: dict, priority: int = 0) -> None:
        """Add *message* to the queue at *priority*.

        Higher *priority* values are dequeued first.  For equal priorities,
        earlier-enqueued messages are dequeued first (FIFO).

        Args:
            message:  The message payload (any dict).
            priority: Integer priority; higher values are served first.

        Raises:
            queue.Full: If *max_size* > 0 and the queue is at capacity.
        """
        with self._lock:
            seq = self._counter
            self._counter += 1
        # Negate priority so Python's min-heap behaves as a max-heap.
        # Use seq as a secondary key so ties are resolved by insertion order.
        self._pq.put_nowait((-priority, seq, message))

    def dequeue(self, timeout: Optional[float] = None) -> Optional[dict]:
        """Remove and return the highest-priority message.

        Blocks up to *timeout* seconds waiting for a message if the queue is
        empty.  Returns ``None`` if the timeout expires with no message.

        Args:
            timeout: Maximum seconds to wait; ``None`` waits indefinitely.

        Returns:
            The highest-priority message dict, or ``None`` on timeout.
        """
        try:
            _neg_priority, _seq, message = self._pq.get(
                block=True, timeout=timeout
            )
            return message
        except queue.Empty:
            return None

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    def peek(self) -> Optional[dict]:
        """Return the next message without removing it, or ``None`` if empty.

        Note: In a concurrent context the peeked message may have been
        consumed before the caller acts on it.

        Returns:
            The highest-priority message dict, or ``None`` if the queue is empty.
        """
        with self._pq.mutex:
            if not self._pq.queue:
                return None
            # heapq stores the minimum element at index 0.
            _neg_priority, _seq, message = self._pq.queue[0]
            return message

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def size(self) -> int:
        """Current number of messages in the queue."""
        return self._pq.qsize()

    @property
    def is_empty(self) -> bool:
        """``True`` when the queue contains no messages."""
        return self._pq.empty()


# ===========================================================================
# Pattern 2 — TopicExchange
# ===========================================================================


class TopicExchange:
    """AMQP-style topic exchange for pattern-based message routing.

    Queues are bound to topic patterns that may contain the wildcards:

    * ``*`` — matches exactly **one** dot-separated word.
    * ``#`` — matches **zero or more** dot-separated words.

    When a message is published with a routing key (topic), it is copied into
    every bound queue whose pattern matches that key.

    Example::

        ex = TopicExchange()
        q1 = MessageQueue()
        q2 = MessageQueue()
        ex.bind("orders.*", q1)          # orders.created, orders.updated …
        ex.bind("orders.#", q2)          # orders, orders.created.eu …
        ex.publish("orders.created", {"id": 1})
        # Both q1 and q2 receive the message.
    """

    def __init__(self) -> None:
        self._bindings: List[tuple[str, MessageQueue]] = []

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def bind(self, topic_pattern: str, q: MessageQueue) -> None:
        """Bind *q* so that it receives messages matching *topic_pattern*.

        The same queue may be bound to multiple patterns; it will receive a
        separate copy for each matching binding.

        Args:
            topic_pattern: AMQP-style pattern string.
            q:             :class:`MessageQueue` to deliver matching messages to.
        """
        self._bindings.append((topic_pattern, q))

    # ------------------------------------------------------------------
    # Publishing
    # ------------------------------------------------------------------

    def publish(self, topic: str, message: dict) -> None:
        """Route a copy of *message* to every queue whose pattern matches *topic*.

        Args:
            topic:   Routing key (dot-separated words, e.g. ``"orders.created"``).
            message: Message payload dict.
        """
        for pattern, q in self._bindings:
            if self._matches(pattern, topic):
                q.enqueue(dict(message))

    # ------------------------------------------------------------------
    # Matching logic
    # ------------------------------------------------------------------

    def _matches(self, pattern: str, topic: str) -> bool:
        """Return ``True`` if *topic* matches AMQP *pattern*.

        Rules:
        - ``*`` matches exactly one word.
        - ``#`` matches zero or more words.
        - Literal words must match exactly.

        Args:
            pattern: Pattern string (may contain ``*`` and ``#``).
            topic:   Routing key to test.

        Returns:
            ``True`` if the topic matches the pattern.
        """
        p_parts = pattern.split(".")
        t_parts = topic.split(".")
        return self._match_parts(p_parts, t_parts)

    @staticmethod
    def _match_parts(p: List[str], t: List[str]) -> bool:
        """Recursive helper for :meth:`_matches`."""
        if not p and not t:
            return True
        if not p:
            return False
        if p[0] == "#":
            # # can consume 0 or more topic words.
            for i in range(len(t) + 1):
                if TopicExchange._match_parts(p[1:], t[i:]):
                    return True
            return False
        if not t:
            return False
        if p[0] == "*" or p[0] == t[0]:
            return TopicExchange._match_parts(p[1:], t[1:])
        return False

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def binding_count(self) -> int:
        """Total number of (pattern, queue) bindings registered."""
        return len(self._bindings)


# ===========================================================================
# Pattern 3 — FanoutExchange
# ===========================================================================


class FanoutExchange:
    """Broadcast exchange that delivers every message to all bound queues.

    All bound :class:`MessageQueue` instances receive an independent shallow
    copy of each published message.  Subscribers may be added or removed
    dynamically.

    Example::

        ex = FanoutExchange()
        q1, q2 = MessageQueue(), MessageQueue()
        ex.bind(q1)
        ex.bind(q2)
        ex.publish({"event": "tick"})
        assert q1.size == 1
        assert q2.size == 1
    """

    def __init__(self) -> None:
        self._subscribers: List[MessageQueue] = []
        self._lock: threading.Lock = threading.Lock()

    # ------------------------------------------------------------------
    # Subscription management
    # ------------------------------------------------------------------

    def bind(self, q: MessageQueue) -> None:
        """Add *q* to the fanout subscriber list.

        Args:
            q: :class:`MessageQueue` to receive all published messages.
        """
        with self._lock:
            if q not in self._subscribers:
                self._subscribers.append(q)

    def unbind(self, q: MessageQueue) -> None:
        """Remove *q* from the fanout subscriber list (no-op if not bound).

        Args:
            q: :class:`MessageQueue` to remove.
        """
        with self._lock:
            try:
                self._subscribers.remove(q)
            except ValueError:
                pass

    # ------------------------------------------------------------------
    # Publishing
    # ------------------------------------------------------------------

    def publish(self, message: dict) -> None:
        """Deliver a copy of *message* to every currently bound queue.

        Args:
            message: Message payload dict; each subscriber receives a
                     shallow copy.
        """
        with self._lock:
            subscribers = list(self._subscribers)
        for q in subscribers:
            q.enqueue(dict(message))

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def subscriber_count(self) -> int:
        """Number of currently bound queues."""
        with self._lock:
            return len(self._subscribers)


# ===========================================================================
# Pattern 4 — MessageBatch
# ===========================================================================


class MessageBatch:
    """Batch collector that flushes when *batch_size* is reached.

    Messages are accumulated via :meth:`add`.  When the number of pending
    messages reaches *batch_size* the batch is automatically flushed and the
    full list is returned.  A partial batch may be retrieved at any time via
    :meth:`flush`.

    Example::

        batch = MessageBatch(batch_size=3)
        assert batch.add({"n": 1}) is None
        assert batch.add({"n": 2}) is None
        result = batch.add({"n": 3})   # triggers auto-flush
        assert len(result) == 3
        assert batch.total_flushed == 3
    """

    def __init__(self, batch_size: int, flush_interval: Optional[float] = None) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        self._batch_size = batch_size
        self._flush_interval = flush_interval  # stored for API completeness
        self._pending: List[dict] = []
        self._total_flushed: int = 0

    # ------------------------------------------------------------------
    # Accumulation
    # ------------------------------------------------------------------

    def add(self, message: dict) -> Optional[List[dict]]:
        """Append *message* to the current batch.

        If adding *message* brings the pending count to *batch_size* the
        batch is automatically flushed and returned.

        Args:
            message: Message payload dict to accumulate.

        Returns:
            The complete batch (list of dicts) when *batch_size* is reached;
            ``None`` otherwise.
        """
        self._pending.append(message)
        if len(self._pending) >= self._batch_size:
            return self.flush()
        return None

    # ------------------------------------------------------------------
    # Flush
    # ------------------------------------------------------------------

    def flush(self) -> List[dict]:
        """Return the current pending batch and reset the accumulator.

        Returns:
            List of all pending messages (may be empty if nothing was added
            since the last flush).
        """
        batch = list(self._pending)
        self._total_flushed += len(batch)
        self._pending.clear()
        return batch

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def pending_count(self) -> int:
        """Number of messages accumulated since the last flush."""
        return len(self._pending)

    @property
    def total_flushed(self) -> int:
        """Cumulative number of messages returned across all flushes."""
        return self._total_flushed


# ===========================================================================
# Pattern 5 — DeduplicationFilter
# ===========================================================================


class DeduplicationFilter:
    """Idempotency filter for message streams.

    Maintains a time-windowed set of seen message IDs.  Messages whose ID was
    already seen within *window_seconds* are rejected as duplicates; once the
    window expires the ID may be accepted again.  Thread-safe via an internal
    :class:`threading.Lock`; expired entries are evicted lazily on every call.

    Example::

        flt = DeduplicationFilter(window_seconds=60.0)
        assert flt.process({"message_id": "abc", "data": 1}) is True
        assert flt.process({"message_id": "abc", "data": 2}) is False  # dup
        assert flt.seen_count == 1
        assert flt.duplicate_count == 1
    """

    def __init__(self, window_seconds: float = 60.0) -> None:
        self._window = window_seconds
        # Maps message_id → first-seen monotonic timestamp
        self._seen: Dict[str, float] = {}
        self._lock: threading.Lock = threading.Lock()
        self._seen_count: int = 0
        self._duplicate_count: int = 0

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _evict_expired(self, now: float) -> None:
        """Remove entries older than *window_seconds* (call with lock held)."""
        expired = [
            mid for mid, ts in self._seen.items()
            if now - ts > self._window
        ]
        for mid in expired:
            del self._seen[mid]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def is_duplicate(self, message_id: str) -> bool:
        """Check whether *message_id* is a duplicate within the current window.

        If *message_id* is new it is recorded and ``False`` is returned.
        If it was seen within *window_seconds* ``True`` is returned without
        updating the timestamp.

        Args:
            message_id: Unique identifier of the message to check.

        Returns:
            ``True`` if the ID is a duplicate; ``False`` if it is new.
        """
        now = time.monotonic()
        with self._lock:
            self._evict_expired(now)
            if message_id in self._seen:
                self._duplicate_count += 1
                return True
            self._seen[message_id] = now
            self._seen_count += 1
            return False

    def process(self, message: dict, id_field: str = "message_id") -> bool:
        """Determine whether *message* should be processed.

        Extracts the ID from *message[id_field]* and delegates to
        :meth:`is_duplicate`.

        Args:
            message:  Message payload dict containing the ID field.
            id_field: Key within *message* that holds the unique message ID.

        Returns:
            ``True`` if the message is new and should be processed;
            ``False`` if it is a duplicate and should be discarded.
        """
        message_id = message[id_field]
        return not self.is_duplicate(message_id)

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def seen_count(self) -> int:
        """Total number of unique message IDs seen (not duplicates)."""
        with self._lock:
            return self._seen_count

    @property
    def duplicate_count(self) -> int:
        """Total number of duplicate messages rejected."""
        with self._lock:
            return self._duplicate_count


# ===========================================================================
# Smoke-test helpers (used only in __main__)
# ===========================================================================


def _separator(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def _run_message_queue_demo() -> None:
    """Scenario 1: MessageQueue — priority ordering and blocking dequeue."""
    _separator("Scenario 1 — MessageQueue: priority + FIFO + peek")

    mq = MessageQueue()
    mq.enqueue({"body": "normal"}, priority=0)
    mq.enqueue({"body": "high"}, priority=10)
    mq.enqueue({"body": "low"}, priority=-5)

    first = mq.dequeue()
    assert first is not None and first["body"] == "high", f"Expected 'high', got {first}"
    print(f"First dequeued : {first['body']} (expected: high)")

    peeked = mq.peek()
    assert peeked is not None and peeked["body"] == "normal"
    print(f"Peeked         : {peeked['body']} (expected: normal)")
    assert mq.size == 2

    # Timeout on empty queue
    empty_q = MessageQueue()
    result = empty_q.dequeue(timeout=0.05)
    assert result is None
    print("Timeout        : OK (None returned)")

    print("PASS")


def _run_topic_exchange_demo() -> None:
    """Scenario 2: TopicExchange — wildcard routing."""
    _separator("Scenario 2 — TopicExchange: * and # wildcards")

    ex = TopicExchange()
    q_star = MessageQueue()
    q_hash = MessageQueue()
    q_exact = MessageQueue()

    ex.bind("orders.*", q_star)
    ex.bind("orders.#", q_hash)
    ex.bind("orders.created", q_exact)

    ex.publish("orders.created", {"id": 1})
    assert q_star.size == 1
    assert q_hash.size == 1
    assert q_exact.size == 1
    print(f"orders.created : q_star={q_star.size}, q_hash={q_hash.size}, q_exact={q_exact.size}")

    ex.publish("orders.created.eu", {"id": 2})
    # * does NOT match two words; # matches zero-or-more
    assert q_star.size == 1   # still 1 (no new match)
    assert q_hash.size == 2
    print(f"orders.created.eu: q_star unchanged={q_star.size}, q_hash={q_hash.size}")

    print(f"Binding count  : {ex.binding_count}")
    assert ex.binding_count == 3
    print("PASS")


def _run_fanout_exchange_demo() -> None:
    """Scenario 3: FanoutExchange — broadcast + unbind."""
    _separator("Scenario 3 — FanoutExchange: broadcast + unbind")

    ex = FanoutExchange()
    q1, q2, q3 = MessageQueue(), MessageQueue(), MessageQueue()
    ex.bind(q1)
    ex.bind(q2)
    ex.bind(q3)

    ex.publish({"event": "tick"})
    assert q1.size == q2.size == q3.size == 1
    print(f"All 3 queues received: sizes={q1.size},{q2.size},{q3.size}")

    ex.unbind(q2)
    ex.publish({"event": "tock"})
    assert q1.size == 2
    assert q2.size == 1   # did not receive second message
    assert q3.size == 2
    print(f"After unbind q2: q1={q1.size}, q2={q2.size}, q3={q3.size}")
    assert ex.subscriber_count == 2
    print("PASS")


def _run_message_batch_demo() -> None:
    """Scenario 4: MessageBatch — auto-flush and partial flush."""
    _separator("Scenario 4 — MessageBatch: auto-flush + partial flush")

    batch = MessageBatch(batch_size=3)
    assert batch.add({"n": 1}) is None
    assert batch.add({"n": 2}) is None
    print("After 2 adds   : no flush yet")
    result = batch.add({"n": 3})
    assert result is not None and len(result) == 3
    print(f"Auto-flush     : {len(result)} messages returned")
    assert batch.total_flushed == 3
    assert batch.pending_count == 0

    batch.add({"n": 4})
    partial = batch.flush()
    assert len(partial) == 1
    print(f"Partial flush  : {len(partial)} message returned")
    assert batch.total_flushed == 4
    print("PASS")


def _run_deduplication_filter_demo() -> None:
    """Scenario 5: DeduplicationFilter — new IDs pass, duplicates rejected."""
    _separator("Scenario 5 — DeduplicationFilter: dedup + window expiry")

    flt = DeduplicationFilter(window_seconds=60.0)
    assert flt.process({"message_id": "msg-1", "data": "x"}) is True
    assert flt.process({"message_id": "msg-2", "data": "y"}) is True
    assert flt.process({"message_id": "msg-1", "data": "x"}) is False  # dup

    print(f"seen_count     : {flt.seen_count} (expected 2)")
    print(f"duplicate_count: {flt.duplicate_count} (expected 1)")
    assert flt.seen_count == 2
    assert flt.duplicate_count == 1

    # Tiny window — entry should expire quickly
    flt2 = DeduplicationFilter(window_seconds=0.05)
    flt2.process({"message_id": "x"})
    time.sleep(0.1)
    assert flt2.process({"message_id": "x"}) is True  # expired, treated as new
    print("Window expiry  : OK")
    print("PASS")


# ===========================================================================
# Entry point
# ===========================================================================


if __name__ == "__main__":
    print("Message Broker Patterns — Smoke Test Suite")
    print(
        "Patterns: MessageQueue | TopicExchange | FanoutExchange "
        "| MessageBatch | DeduplicationFilter"
    )

    _run_message_queue_demo()
    _run_topic_exchange_demo()
    _run_fanout_exchange_demo()
    _run_message_batch_demo()
    _run_deduplication_filter_demo()

    print("\n" + "=" * 60)
    print("  All scenarios passed.")
    print("=" * 60)
