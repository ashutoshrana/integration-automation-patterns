"""
18_message_routing_patterns.py — Classic message routing patterns from
Enterprise Integration Patterns (Hohpe & Woolf, 2003).

Demonstrates four complementary patterns that together implement a complete
message routing pipeline for enterprise integration scenarios:

    Pattern 1 — Content-Based Router:
                Routes each incoming message to exactly one named channel
                based on the first matching predicate rule. Rules are
                evaluated in registration order (first match wins). Messages
                that satisfy no rule are sent to a configurable default
                "unrouted" channel. Supports batch routing for efficiency.

    Pattern 2 — Message Splitter:
                Decomposes a single message with a composite payload into a
                sequence of smaller messages, each carrying one element of
                the original payload. All split messages share the same
                correlation_id (set to the parent's message_id), carry the
                parent_id for traceability, and are numbered with sequence /
                total_parts so a downstream aggregator can detect completion.

    Pattern 3 — Message Aggregator:
                Collects related messages that share a correlation_id and
                emits one aggregated message when a caller-supplied completion
                predicate returns True. The aggregation strategy function
                determines how the accumulated payloads are combined into the
                final result. Supports timeout detection via
                receive_with_timeout() to handle partial groups caused by
                lost messages or upstream failures.

    Pattern 4 — Message Filter:
                Passes messages that satisfy a predicate unchanged; discards
                (or dead-letters) messages that do not. Tracks accepted and
                rejected counts for operational visibility. When a
                dead_letter_channel is configured, filtered messages are
                re-channeled rather than silently dropped, enabling downstream
                auditing or manual reprocessing.

No external dependencies required.

Run:
    python examples/18_message_routing_patterns.py
"""

from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Shared Message dataclass
# ---------------------------------------------------------------------------


@dataclass
class Message:
    """
    A self-describing unit of information moving through an integration channel.

    Attributes
    ----------
    payload : Any
        The message body — can be any Python object (dict, list, str, etc.).
    message_id : str
        Unique identifier for this message instance. Auto-generated if not set.
    correlation_id : Optional[str]
        Groups related messages (e.g. all parts of a split). Set by the
        Splitter on child messages; used by the Aggregator as the grouping key.
    channel : str
        Logical destination channel name. Updated by the Router.
    headers : Dict[str, str]
        Arbitrary string key/value metadata (content-type, priority, etc.).
    sequence : Optional[int]
        Zero-based position of this message within a split group.
    total_parts : Optional[int]
        Total number of split messages expected in this group.
    parent_id : Optional[str]
        message_id of the original message before splitting.
    timestamp : float
        Unix epoch time at message creation.
    """

    payload: Any
    message_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    correlation_id: Optional[str] = None
    channel: str = "default"
    headers: Dict[str, str] = field(default_factory=dict)
    sequence: Optional[int] = None
    total_parts: Optional[int] = None
    parent_id: Optional[str] = None
    timestamp: float = field(default_factory=time.time)


# ---------------------------------------------------------------------------
# Pattern 1 — Content-Based Router
# ---------------------------------------------------------------------------


class ContentBasedRouter:
    """
    Routes messages to named channels based on message content.

    Rules are evaluated in the order they were added; the first matching
    predicate determines the destination channel. If no rule matches, the
    message is routed to *default_channel* (default: ``"unrouted"``).

    This implements the **Content-Based Router** EIP (Hohpe & Woolf, p. 230).

    Example
    -------
    >>> router = ContentBasedRouter()
    >>> router.add_route("orders", lambda m: m.headers.get("type") == "order")
    >>> router.add_route("alerts", lambda m: m.payload.get("severity") == "HIGH")
    >>> channel = router.route(msg)
    """

    def __init__(self, default_channel: str = "unrouted") -> None:
        self._routes: List[Tuple[str, Callable[[Message], bool]]] = []
        self._default_channel = default_channel

    def add_route(
        self, channel: str, predicate: Callable[[Message], bool]
    ) -> "ContentBasedRouter":
        """
        Register a routing rule.

        Parameters
        ----------
        channel : str
            Name of the destination channel when *predicate* returns True.
        predicate : Callable[[Message], bool]
            A function that inspects a Message and returns True for a match.

        Returns
        -------
        ContentBasedRouter
            Returns *self* to allow method chaining.
        """
        self._routes.append((channel, predicate))
        return self

    def route(self, message: Message) -> str:
        """
        Determine the destination channel for a single message.

        Evaluates rules in registration order. The first matching predicate
        wins. Returns *default_channel* when no rule matches.

        Parameters
        ----------
        message : Message
            The message to route.

        Returns
        -------
        str
            Name of the destination channel.
        """
        for channel, predicate in self._routes:
            if predicate(message):
                return channel
        return self._default_channel

    def route_all(self, messages: List[Message]) -> Dict[str, List[Message]]:
        """
        Route a batch of messages and group them by destination channel.

        Parameters
        ----------
        messages : List[Message]
            Messages to route.

        Returns
        -------
        Dict[str, List[Message]]
            Mapping of channel name → list of messages assigned to that channel.
            Only channels that received at least one message are present.
        """
        result: Dict[str, List[Message]] = {}
        for msg in messages:
            channel = self.route(msg)
            result.setdefault(channel, []).append(msg)
        return result


# ---------------------------------------------------------------------------
# Pattern 2 — Message Splitter
# ---------------------------------------------------------------------------


class MessageSplitter:
    """
    Decomposes a single composite message into multiple smaller messages.

    The *split_fn* callable extracts a list of payload items from the
    original message. Each item becomes an independent child message that
    inherits the parent's channel and headers, and is stamped with:

    * ``correlation_id`` — set to the parent's ``correlation_id`` if already
      set, otherwise set to the parent's ``message_id``.
    * ``parent_id`` — the original ``message_id``.
    * ``sequence`` / ``total_parts`` — zero-based index and total count,
      enabling a downstream Aggregator to detect when all parts have arrived.

    This implements the **Splitter** EIP (Hohpe & Woolf, p. 259).

    Example
    -------
    >>> splitter = MessageSplitter(lambda m: m.payload["line_items"])
    >>> parts = splitter.split(batch_order_message)
    """

    def __init__(self, split_fn: Callable[[Message], List[Any]]) -> None:
        """
        Parameters
        ----------
        split_fn : Callable[[Message], List[Any]]
            Receives a Message and returns a list of payload items, one per
            child message. An empty list is valid (produces no output).
        """
        self._split_fn = split_fn

    def split(self, message: Message) -> List[Message]:
        """
        Split one message into many.

        Parameters
        ----------
        message : Message
            The composite message to split.

        Returns
        -------
        List[Message]
            Ordered list of child messages. Empty if *split_fn* returns [].
        """
        items = self._split_fn(message)
        total = len(items)
        # Propagate an existing correlation_id if present; otherwise use the
        # parent's message_id so the group is traceable back to the origin.
        correlation_id = message.correlation_id or message.message_id

        result: List[Message] = []
        for seq, item in enumerate(items):
            child = Message(
                payload=item,
                correlation_id=correlation_id,
                channel=message.channel,
                headers=dict(message.headers),  # shallow copy
                sequence=seq,
                total_parts=total,
                parent_id=message.message_id,
            )
            result.append(child)
        return result


# ---------------------------------------------------------------------------
# Pattern 3 — Message Aggregator
# ---------------------------------------------------------------------------


class AggregationTimeout(Exception):
    """
    Raised by :meth:`MessageAggregator.receive_with_timeout` when a
    correlation group has been waiting longer than *max_wait_seconds* without
    meeting the completion predicate.
    """


class MessageAggregator:
    """
    Collects related messages by ``correlation_id`` and emits one aggregated
    message when a completion condition is satisfied.

    The *completion_predicate* is evaluated after every new message is added
    to a group. When it returns True the *aggregation_strategy* is called to
    merge the accumulated payloads into a single result payload, and the
    aggregated Message is returned to the caller.

    Thread safety: each correlation group has its own :class:`threading.Lock`,
    so multiple groups can be populated concurrently without contention.

    This implements the **Aggregator** EIP (Hohpe & Woolf, p. 268).

    Parameters
    ----------
    completion_predicate : Callable[[List[Message]], bool]
        Returns True when the collected messages for a group are complete
        and ready to aggregate (e.g. all parts received).
    aggregation_strategy : Callable[[List[Message]], Any]
        Combines the collected messages into a single payload value.
    max_wait_seconds : float
        Maximum time (seconds) to wait for a group to complete before
        :meth:`receive_with_timeout` raises :exc:`AggregationTimeout`.
        Defaults to 30.0.

    Example
    -------
    >>> def all_parts(msgs):
    ...     return msgs and msgs[0].total_parts == len(msgs)
    >>> agg = MessageAggregator(all_parts, lambda msgs: [m.payload for m in msgs])
    >>> result = agg.receive(part_message)  # None until complete
    """

    def __init__(
        self,
        completion_predicate: Callable[[List[Message]], bool],
        aggregation_strategy: Callable[[List[Message]], Any],
        max_wait_seconds: float = 30.0,
    ) -> None:
        self._completion_predicate = completion_predicate
        self._aggregation_strategy = aggregation_strategy
        self._max_wait_seconds = max_wait_seconds

        self._groups: Dict[str, List[Message]] = {}
        self._locks: Dict[str, threading.Lock] = {}
        self._timestamps: Dict[str, float] = {}  # time of first message in group
        self._global_lock = threading.Lock()  # guards _groups/_locks/_timestamps dicts

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_or_create_lock(self, correlation_id: str) -> threading.Lock:
        """Return (creating if necessary) the per-group lock."""
        with self._global_lock:
            if correlation_id not in self._locks:
                self._locks[correlation_id] = threading.Lock()
            return self._locks[correlation_id]

    def _try_aggregate(self, correlation_id: str) -> Optional[Message]:
        """
        Check completion predicate and, if met, build and return the
        aggregated Message, then remove the group from internal state.
        Caller must hold the per-group lock.
        """
        group = self._groups[correlation_id]
        if self._completion_predicate(group):
            aggregated_payload = self._aggregation_strategy(group)
            # Derive metadata from the first message in the group
            first = group[0]
            result = Message(
                payload=aggregated_payload,
                correlation_id=correlation_id,
                channel=first.channel,
                headers=dict(first.headers),
                parent_id=first.parent_id,
            )
            # Clean up group state
            with self._global_lock:
                self._groups.pop(correlation_id, None)
                self._locks.pop(correlation_id, None)
                self._timestamps.pop(correlation_id, None)
            return result
        return None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def receive(self, message: Message) -> Optional[Message]:
        """
        Add *message* to its correlation group and check for completion.

        Parameters
        ----------
        message : Message
            Must have ``correlation_id`` set. Raises :exc:`ValueError` if not.

        Returns
        -------
        Optional[Message]
            The aggregated Message if the completion predicate is satisfied
            after adding this message; ``None`` otherwise.

        Raises
        ------
        ValueError
            If ``message.correlation_id`` is None.
        """
        if message.correlation_id is None:
            raise ValueError(
                "MessageAggregator requires message.correlation_id to be set."
            )

        cid = message.correlation_id
        lock = self._get_or_create_lock(cid)

        with lock:
            with self._global_lock:
                if cid not in self._groups:
                    self._groups[cid] = []
                    self._timestamps[cid] = time.time()
            self._groups[cid].append(message)
            return self._try_aggregate(cid)

    def receive_with_timeout(self, message: Message) -> Optional[Message]:
        """
        Like :meth:`receive`, but raises :exc:`AggregationTimeout` if the
        group has been open longer than *max_wait_seconds*.

        This check is performed *after* adding the new message, so the
        message is always recorded before a timeout is signalled.

        Parameters
        ----------
        message : Message
            Must have ``correlation_id`` set.

        Returns
        -------
        Optional[Message]
            Aggregated Message on completion, ``None`` if group is not yet
            complete and has not timed out.

        Raises
        ------
        AggregationTimeout
            When the group has exceeded *max_wait_seconds* without completing.
        ValueError
            If ``message.correlation_id`` is None.
        """
        if message.correlation_id is None:
            raise ValueError(
                "MessageAggregator requires message.correlation_id to be set."
            )

        cid = message.correlation_id
        lock = self._get_or_create_lock(cid)

        with lock:
            with self._global_lock:
                if cid not in self._groups:
                    self._groups[cid] = []
                    self._timestamps[cid] = time.time()
            self._groups[cid].append(message)

            # Check completion first; a timed-out group that just completed is
            # still emitted rather than raising an error.
            aggregated = self._try_aggregate(cid)
            if aggregated is not None:
                return aggregated

            elapsed = time.time() - self._timestamps.get(cid, time.time())
            if elapsed > self._max_wait_seconds:
                # Preserve messages in group for flush_group(); only raise.
                raise AggregationTimeout(
                    f"Correlation group '{cid}' timed out after "
                    f"{elapsed:.2f}s (max {self._max_wait_seconds}s). "
                    f"Collected {len(self._groups.get(cid, []))} of "
                    f"{self._groups[cid][0].total_parts} expected parts."
                )

            return None

    def pending_groups(self) -> List[str]:
        """
        Return the correlation_ids of all groups that have not yet completed.

        Returns
        -------
        List[str]
            Snapshot of pending group identifiers.
        """
        with self._global_lock:
            return list(self._groups.keys())

    def flush_group(self, correlation_id: str) -> Optional[List[Message]]:
        """
        Force-complete a group, returning its accumulated messages regardless
        of whether the completion predicate has been met.

        This is useful for draining timed-out or stalled groups during
        shutdown or after an :exc:`AggregationTimeout`.

        Parameters
        ----------
        correlation_id : str
            The group to flush.

        Returns
        -------
        Optional[List[Message]]
            The accumulated messages, or ``None`` if the group is not found.
        """
        lock = self._get_or_create_lock(correlation_id)
        with lock:
            with self._global_lock:
                messages = self._groups.pop(correlation_id, None)
                self._locks.pop(correlation_id, None)
                self._timestamps.pop(correlation_id, None)
            return messages


# ---------------------------------------------------------------------------
# Pattern 4 — Message Filter
# ---------------------------------------------------------------------------


class MessageFilter:
    """
    Passes messages that satisfy a predicate; discards or dead-letters others.

    When *dead_letter_channel* is supplied, rejected messages are not silently
    dropped — instead their ``channel`` attribute is set to *dead_letter_channel*
    and they are returned in the second list from :meth:`filter_all` (and as
    the return value of :meth:`filter`). This enables downstream auditing,
    alerting, or manual reprocessing without data loss.

    Counters (:attr:`accepted_count`, :attr:`rejected_count`) provide
    operational visibility and can be reset via :meth:`reset_stats`.

    This implements the **Message Filter** EIP (Hohpe & Woolf, p. 237).

    Example
    -------
    >>> f = MessageFilter(
    ...     lambda m: m.headers.get("priority") in ("HIGH", "MEDIUM"),
    ...     dead_letter_channel="low-priority-dlq",
    ... )
    >>> accepted, dead = f.filter_all(incoming_messages)
    """

    def __init__(
        self,
        predicate: Callable[[Message], bool],
        dead_letter_channel: Optional[str] = None,
    ) -> None:
        """
        Parameters
        ----------
        predicate : Callable[[Message], bool]
            Returns True for messages that should pass through.
        dead_letter_channel : Optional[str]
            When set, rejected messages have their ``channel`` updated to this
            value and are returned rather than dropped.
        """
        self._predicate = predicate
        self._dead_letter_channel = dead_letter_channel
        self._accepted: int = 0
        self._rejected: int = 0

    def filter(self, message: Message) -> Optional[Message]:
        """
        Evaluate a single message against the predicate.

        Parameters
        ----------
        message : Message
            The message to evaluate.

        Returns
        -------
        Optional[Message]
            * The original *message* (unchanged) if it passes.
            * The *message* with its ``channel`` set to *dead_letter_channel*
              if the predicate fails and a dead-letter channel is configured.
            * ``None`` if the predicate fails and no dead-letter channel is set.
        """
        if self._predicate(message):
            self._accepted += 1
            return message
        else:
            self._rejected += 1
            if self._dead_letter_channel is not None:
                message.channel = self._dead_letter_channel
                return message
            return None

    def filter_all(
        self, messages: List[Message]
    ) -> Tuple[List[Message], List[Message]]:
        """
        Filter a batch of messages.

        Parameters
        ----------
        messages : List[Message]
            Messages to evaluate.

        Returns
        -------
        Tuple[List[Message], List[Message]]
            A 2-tuple of ``(accepted, rejected_or_dead_lettered)``.

            * *accepted* — messages that passed the predicate.
            * *rejected_or_dead_lettered* — messages that failed. When a
              dead-letter channel is configured these messages will have their
              ``channel`` set accordingly; when not configured this list
              contains the original rejected Message objects for caller
              inspection (stats are still incremented).
        """
        accepted: List[Message] = []
        rejected: List[Message] = []
        for msg in messages:
            result = self.filter(msg)
            if result is not None and result.channel != self._dead_letter_channel:
                # Passed the predicate
                accepted.append(result)
            elif result is not None:
                # Dead-lettered
                rejected.append(result)
            else:
                # Silently dropped — still track it for caller inspection
                rejected.append(msg)
        return accepted, rejected

    @property
    def accepted_count(self) -> int:
        """Total number of messages that have passed the predicate."""
        return self._accepted

    @property
    def rejected_count(self) -> int:
        """Total number of messages that have been rejected (or dead-lettered)."""
        return self._rejected

    def reset_stats(self) -> None:
        """Reset accepted and rejected counters to zero."""
        self._accepted = 0
        self._rejected = 0


# ---------------------------------------------------------------------------
# Demo scenarios
# ---------------------------------------------------------------------------


def scenario_a_content_based_router() -> None:
    """
    Scenario A — ContentBasedRouter

    Three order messages are routed to separate channels based on their
    ``order_type`` header. A fourth message with an unknown type falls
    through to the default "unrouted" channel.
    """
    print("\n=== Scenario A: Content-Based Router ===")

    router = (
        ContentBasedRouter(default_channel="unrouted")
        .add_route(
            "orders",
            lambda m: m.headers.get("order_type") == "PURCHASE",
        )
        .add_route(
            "payments",
            lambda m: m.headers.get("order_type") == "PAYMENT",
        )
        .add_route(
            "shipping",
            lambda m: m.headers.get("order_type") == "SHIPMENT",
        )
    )

    messages = [
        Message(
            payload={"item": "Widget A", "qty": 3},
            headers={"order_type": "PURCHASE"},
        ),
        Message(
            payload={"amount": 59.99, "currency": "USD"},
            headers={"order_type": "PAYMENT"},
        ),
        Message(
            payload={"tracking": "1Z999AA1", "carrier": "UPS"},
            headers={"order_type": "SHIPMENT"},
        ),
        Message(
            payload={"code": "RETURN-001"},
            headers={"order_type": "RETURN"},
        ),
    ]

    routed = router.route_all(messages)

    for channel, msgs in sorted(routed.items()):
        payloads = [m.payload for m in msgs]
        print(f"  Channel '{channel}': {payloads}")

    # Verify: orders/payments/shipping get one each; unrouted gets the return
    assert "orders" in routed and len(routed["orders"]) == 1
    assert "payments" in routed and len(routed["payments"]) == 1
    assert "shipping" in routed and len(routed["shipping"]) == 1
    assert "unrouted" in routed and len(routed["unrouted"]) == 1
    print("  [PASS] All 4 messages routed correctly.")


def scenario_b_message_splitter() -> None:
    """
    Scenario B — MessageSplitter

    A batch order with three line items is split into three individual
    line-item messages. Each child message carries the parent's correlation_id,
    a sequence number, and the expected total_parts count.
    """
    print("\n=== Scenario B: Message Splitter ===")

    splitter = MessageSplitter(
        split_fn=lambda m: m.payload.get("line_items", [])
    )

    batch_order = Message(
        payload={
            "order_id": "ORD-9001",
            "customer": "Acme Corp",
            "line_items": [
                {"sku": "SKU-100", "qty": 2, "price": 19.99},
                {"sku": "SKU-200", "qty": 1, "price": 49.99},
                {"sku": "SKU-300", "qty": 5, "price": 9.99},
            ],
        },
        channel="orders",
        headers={"source": "web-portal"},
    )

    parts = splitter.split(batch_order)

    print(f"  Original message_id : {batch_order.message_id}")
    print(f"  Split into {len(parts)} part(s):")
    for part in parts:
        print(
            f"    seq={part.sequence}/{part.total_parts - 1}  "
            f"correlation_id={part.correlation_id}  "
            f"payload={part.payload}"
        )

    # Verify structural properties
    assert len(parts) == 3
    for i, part in enumerate(parts):
        assert part.sequence == i
        assert part.total_parts == 3
        assert part.correlation_id == batch_order.message_id
        assert part.parent_id == batch_order.message_id
        assert part.channel == "orders"
        assert part.headers["source"] == "web-portal"

    print("  [PASS] Split messages have correct metadata.")


def scenario_c_message_aggregator() -> None:
    """
    Scenario C — MessageAggregator

    The three split line-item messages from Scenario B are fed into an
    Aggregator. The completion predicate fires when all expected parts have
    arrived (detected via sequence count vs. total_parts). The aggregation
    strategy reassembles the payloads into a sorted list.
    """
    print("\n=== Scenario C: Message Aggregator ===")

    def all_parts_received(messages: List[Message]) -> bool:
        """Complete when we have received all expected parts."""
        if not messages:
            return False
        first = messages[0]
        if first.total_parts is None:
            return False
        return len(messages) == first.total_parts

    def merge_line_items(messages: List[Message]) -> List[Any]:
        """Sort by sequence and collect payloads in order."""
        ordered = sorted(messages, key=lambda m: m.sequence or 0)
        return [m.payload for m in ordered]

    aggregator = MessageAggregator(
        completion_predicate=all_parts_received,
        aggregation_strategy=merge_line_items,
        max_wait_seconds=5.0,
    )

    # Reconstruct the parts from Scenario B without actually calling B
    parent_id = str(uuid.uuid4())
    correlation_id = parent_id
    raw_items = [
        {"sku": "SKU-100", "qty": 2, "price": 19.99},
        {"sku": "SKU-200", "qty": 1, "price": 49.99},
        {"sku": "SKU-300", "qty": 5, "price": 9.99},
    ]
    parts = [
        Message(
            payload=item,
            correlation_id=correlation_id,
            parent_id=parent_id,
            channel="orders",
            sequence=i,
            total_parts=3,
            headers={"source": "web-portal"},
        )
        for i, item in enumerate(raw_items)
    ]

    result = None
    for part in parts:
        result = aggregator.receive(part)
        if result is not None:
            print(f"  Aggregation complete on part {part.sequence}.")
            break
        else:
            print(f"  Part {part.sequence} received — group not yet complete.")

    assert result is not None, "Aggregated message should have been produced."
    print(f"  Aggregated payload ({len(result.payload)} items): {result.payload}")
    assert result.correlation_id == correlation_id
    assert len(result.payload) == 3
    assert result.payload[0]["sku"] == "SKU-100"
    assert aggregator.pending_groups() == [], "Group should be cleared after completion."
    print("  [PASS] Aggregation produced correct merged message.")


def scenario_d_message_filter() -> None:
    """
    Scenario D — MessageFilter

    A batch of five messages with varying priority headers is passed through
    a filter that accepts only HIGH and MEDIUM priority. LOW priority messages
    are redirected to a dead-letter channel for manual review.
    """
    print("\n=== Scenario D: Message Filter (dead-letter routing) ===")

    msg_filter = MessageFilter(
        predicate=lambda m: m.headers.get("priority") in ("HIGH", "MEDIUM"),
        dead_letter_channel="low-priority-dlq",
    )

    priorities = ["HIGH", "MEDIUM", "LOW", "HIGH", "LOW"]
    messages = [
        Message(
            payload={"event": f"EVT-{i:03d}"},
            headers={"priority": prio},
        )
        for i, prio in enumerate(priorities)
    ]

    accepted, dead_lettered = msg_filter.filter_all(messages)

    print(f"  Total messages  : {len(messages)}")
    print(f"  Accepted        : {len(accepted)}  (HIGH/MEDIUM)")
    print(f"  Dead-lettered   : {len(dead_lettered)}  (LOW → low-priority-dlq)")
    for msg in dead_lettered:
        print(
            f"    dead-letter payload={msg.payload}  channel={msg.channel}"
        )

    assert len(accepted) == 3
    assert len(dead_lettered) == 2
    assert msg_filter.accepted_count == 3
    assert msg_filter.rejected_count == 2
    for msg in dead_lettered:
        assert msg.channel == "low-priority-dlq"

    # Verify reset_stats clears counters
    msg_filter.reset_stats()
    assert msg_filter.accepted_count == 0
    assert msg_filter.rejected_count == 0

    print("  [PASS] Filter correctly separated HIGH/MEDIUM from LOW messages.")
    print("  [PASS] Counters reset cleanly.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


if __name__ == "__main__":
    print("Enterprise Integration Patterns — Message Routing")
    print("=" * 52)

    scenario_a_content_based_router()
    scenario_b_message_splitter()
    scenario_c_message_aggregator()
    scenario_d_message_filter()

    print("\nAll scenarios passed.")
