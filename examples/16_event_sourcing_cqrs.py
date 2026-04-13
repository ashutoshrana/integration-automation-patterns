"""
16_event_sourcing_cqrs.py — Event Sourcing and CQRS patterns for microservices.

Demonstrates four complementary patterns that together provide reliable,
auditable, and eventually-consistent distributed data management:

    Pattern 1 — Event Sourcing (EventStore + DomainEvent):
                Instead of persisting current entity state, all changes are
                recorded as an immutable sequence of domain events. The current
                state can always be reconstructed by replaying events from the
                beginning (or from a snapshot). The EventStore provides
                append-only writes with optimistic concurrency control via
                expected-version checks.

    Pattern 2 — Aggregate (command + event apply):
                A domain Aggregate encapsulates business invariants. It
                processes commands by validating business rules and emitting
                domain events. The aggregate's state is rebuilt by applying
                events in order. Uncommitted events are buffered until saved
                to the EventStore.

    Pattern 3 — Command/Query Responsibility Segregation (CQRS):
                The write side (CommandHandler) validates and executes
                commands, saving new events to the EventStore. The read side
                (Projection + ReadModel) builds materialized views from the
                event stream for efficient querying. Write and read models
                evolve independently.

    Pattern 4 — Snapshot:
                To avoid replaying thousands of events on every load, a
                SnapshotStore periodically captures the aggregate's state at
                a given version. On load, the EventStore first checks for a
                snapshot and replays only events after the snapshot version.

No external dependencies required.

Run:
    python examples/16_event_sourcing_cqrs.py
"""

from __future__ import annotations

import threading
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Type


# ---------------------------------------------------------------------------
# Pattern 1 — Event Sourcing
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DomainEvent:
    """
    An immutable record of something that happened in the domain.

    Parameters
    ----------
    event_id : str
        Globally unique event identifier (UUID).
    aggregate_id : str
        Identifier of the aggregate that produced this event.
    aggregate_type : str
        Type name of the aggregate (e.g., ``"Order"``).
    event_type : str
        Dot-separated event type (e.g., ``"order.placed"``).
    payload : dict
        Event-specific data. Deserialization is the caller's responsibility.
    sequence : int
        Position of this event in the aggregate's event stream (1-based).
    timestamp : float
        Unix timestamp of event creation.
    """

    event_id: str
    aggregate_id: str
    aggregate_type: str
    event_type: str
    payload: Dict[str, Any]
    sequence: int
    timestamp: float = field(default_factory=time.time)


class ConcurrencyError(Exception):
    """
    Raised when the EventStore detects an optimistic concurrency conflict.

    This means another writer has already appended events to the stream
    since the aggregate was loaded. The caller should reload and retry.
    """


class ExpectedVersion:
    """
    Sentinel values for optimistic concurrency control in EventStore.append.

    ANY     — Skip version check; always append.
    NO_STREAM — Expect the stream to not yet exist (first write).
    Use an integer N to expect the stream to have exactly N events already.
    """

    ANY: int = -1
    NO_STREAM: int = 0


class EventStore:
    """
    Append-only event store with per-aggregate streams and optimistic
    concurrency control.

    Thread-safe for concurrent reads and writes to different aggregates.
    Concurrent writes to the same aggregate are serialized via per-stream locks.

    Example
    -------
    >>> store = EventStore()
    >>> event = DomainEvent(
    ...     event_id=str(uuid.uuid4()),
    ...     aggregate_id="ORD-001",
    ...     aggregate_type="Order",
    ...     event_type="order.placed",
    ...     payload={"amount": 99.0},
    ...     sequence=1,
    ... )
    >>> store.append("ORD-001", [event], expected_version=ExpectedVersion.NO_STREAM)
    >>> stream = store.load_stream("ORD-001")
    >>> len(stream)
    1
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._streams: Dict[str, List[DomainEvent]] = defaultdict(list)
        self._global: List[DomainEvent] = []
        self._stream_locks: Dict[str, threading.Lock] = defaultdict(threading.Lock)

    def append(
        self,
        aggregate_id: str,
        events: List[DomainEvent],
        expected_version: int = ExpectedVersion.ANY,
    ) -> None:
        """
        Append events to an aggregate's stream.

        Parameters
        ----------
        aggregate_id : str
            The aggregate stream to append to.
        events : List[DomainEvent]
            Ordered list of events to append.
        expected_version : int
            Optimistic concurrency guard. Use ExpectedVersion.ANY to skip
            the check; ExpectedVersion.NO_STREAM to assert the stream is
            new; or an integer N to assert the stream currently has N events.

        Raises
        ------
        ConcurrencyError
            If the actual stream length does not match expected_version.
        """
        with self._stream_locks[aggregate_id]:
            current = len(self._streams[aggregate_id])
            if expected_version not in (ExpectedVersion.ANY,) and expected_version != current:
                raise ConcurrencyError(
                    f"Concurrency conflict on aggregate {aggregate_id}: "
                    f"expected version {expected_version}, actual {current}"
                )
            with self._lock:
                self._streams[aggregate_id].extend(events)
                self._global.extend(events)

    def load_stream(
        self,
        aggregate_id: str,
        from_sequence: int = 1,
    ) -> List[DomainEvent]:
        """
        Load all events for an aggregate, optionally starting from a sequence.
        """
        with self._lock:
            stream = list(self._streams.get(aggregate_id, []))
        return [e for e in stream if e.sequence >= from_sequence]

    def global_stream(
        self,
        after_position: int = 0,
    ) -> List[DomainEvent]:
        """
        Return all events in global insertion order, for projection replay.
        """
        with self._lock:
            return list(self._global[after_position:])

    def stream_version(self, aggregate_id: str) -> int:
        """Return the current number of events in an aggregate's stream."""
        with self._lock:
            return len(self._streams.get(aggregate_id, []))


# ---------------------------------------------------------------------------
# Pattern 2 — Aggregate
# ---------------------------------------------------------------------------


class Aggregate:
    """
    Base class for event-sourced aggregates.

    Subclasses define:
    - ``apply_<EventType>`` methods that update ``self._state`` in response
      to domain events (called for both new events and replay)
    - Command methods that validate invariants, then call ``_raise_event``

    The aggregate does NOT call the EventStore directly. It buffers
    uncommitted events in ``_uncommitted_events``. The CommandHandler
    is responsible for saving them to the EventStore.

    Example subclass
    ----------------
    class OrderAggregate(Aggregate):

        def place_order(self, amount: float) -> None:
            if self._state.get("placed"):
                raise ValueError("Order already placed")
            self._raise_event("order.placed", {"amount": amount})

        def apply_order_placed(self, payload: dict) -> None:
            self._state["placed"] = True
            self._state["amount"] = payload["amount"]
    """

    aggregate_type: str = "Aggregate"

    def __init__(self, aggregate_id: str) -> None:
        self.aggregate_id = aggregate_id
        self._version: int = 0
        self._state: Dict[str, Any] = {}
        self._uncommitted_events: List[DomainEvent] = []

    @property
    def version(self) -> int:
        return self._version

    @property
    def uncommitted_events(self) -> List[DomainEvent]:
        return list(self._uncommitted_events)

    def mark_committed(self) -> None:
        """Clear the uncommitted events buffer after successful EventStore save."""
        self._uncommitted_events.clear()

    def _raise_event(self, event_type: str, payload: Dict[str, Any]) -> None:
        """Create a new domain event and apply it to self."""
        self._version += 1
        event = DomainEvent(
            event_id=str(uuid.uuid4()),
            aggregate_id=self.aggregate_id,
            aggregate_type=self.aggregate_type,
            event_type=event_type,
            payload=payload,
            sequence=self._version,
        )
        self._apply_event(event)
        self._uncommitted_events.append(event)

    def _apply_event(self, event: DomainEvent) -> None:
        """Dispatch to apply_<event_type_snake_case> handler."""
        method_name = "apply_" + event.event_type.replace(".", "_")
        handler = getattr(self, method_name, None)
        if handler:
            handler(event.payload)

    def load_from_history(self, events: List[DomainEvent]) -> None:
        """
        Reconstruct aggregate state by replaying historical events.
        Does not populate uncommitted_events.
        """
        for event in events:
            self._apply_event(event)
            self._version = event.sequence


# ---------------------------------------------------------------------------
# Sample aggregate — Order
# ---------------------------------------------------------------------------


class OrderStatus(str, Enum):
    PENDING = "PENDING"
    PLACED = "PLACED"
    PAID = "PAID"
    SHIPPED = "SHIPPED"
    CANCELLED = "CANCELLED"


class OrderAggregate(Aggregate):
    """
    Order domain aggregate demonstrating event sourcing.

    Commands: place_order, pay_order, ship_order, cancel_order
    Events: order.placed, order.paid, order.shipped, order.cancelled
    """

    aggregate_type = "Order"

    def __init__(self, aggregate_id: str) -> None:
        super().__init__(aggregate_id)
        self._state = {"status": None, "amount": 0.0, "customer_id": None}

    @property
    def status(self) -> Optional[OrderStatus]:
        return self._state.get("status")

    @property
    def amount(self) -> float:
        return self._state.get("amount", 0.0)

    # --- Commands ---

    def place_order(self, customer_id: str, amount: float) -> None:
        if self._state.get("status") is not None:
            raise ValueError("Order has already been placed")
        if amount <= 0:
            raise ValueError("Order amount must be positive")
        self._raise_event("order.placed", {"customer_id": customer_id, "amount": amount})

    def pay_order(self, payment_reference: str) -> None:
        if self._state.get("status") != OrderStatus.PLACED:
            raise ValueError(f"Cannot pay order in status {self._state.get('status')}")
        self._raise_event("order.paid", {"payment_reference": payment_reference})

    def ship_order(self, tracking_number: str) -> None:
        if self._state.get("status") != OrderStatus.PAID:
            raise ValueError(f"Cannot ship order in status {self._state.get('status')}")
        self._raise_event("order.shipped", {"tracking_number": tracking_number})

    def cancel_order(self, reason: str) -> None:
        if self._state.get("status") in (OrderStatus.SHIPPED, OrderStatus.CANCELLED):
            raise ValueError(f"Cannot cancel order in status {self._state.get('status')}")
        self._raise_event("order.cancelled", {"reason": reason})

    # --- Event apply handlers ---

    def apply_order_placed(self, payload: dict) -> None:
        self._state["status"] = OrderStatus.PLACED
        self._state["customer_id"] = payload["customer_id"]
        self._state["amount"] = payload["amount"]

    def apply_order_paid(self, payload: dict) -> None:
        self._state["status"] = OrderStatus.PAID
        self._state["payment_reference"] = payload["payment_reference"]

    def apply_order_shipped(self, payload: dict) -> None:
        self._state["status"] = OrderStatus.SHIPPED
        self._state["tracking_number"] = payload["tracking_number"]

    def apply_order_cancelled(self, payload: dict) -> None:
        self._state["status"] = OrderStatus.CANCELLED
        self._state["cancellation_reason"] = payload["reason"]


# ---------------------------------------------------------------------------
# Pattern 3 — CQRS: CommandHandler
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CommandResult:
    """Result of a command execution."""

    success: bool
    aggregate_id: str
    events_saved: int = 0
    error: Optional[str] = None


class CommandHandler:
    """
    CQRS write-side: validates and executes commands against aggregates,
    saving new events to the EventStore.

    Parameters
    ----------
    event_store : EventStore
        The EventStore to load and save events from/to.
    aggregate_class : Type[Aggregate]
        The Aggregate subclass to instantiate for this handler.
    """

    def __init__(
        self,
        event_store: EventStore,
        aggregate_class: Type[Aggregate],
    ) -> None:
        self._store = event_store
        self._aggregate_class = aggregate_class

    def _load_aggregate(self, aggregate_id: str) -> Aggregate:
        events = self._store.load_stream(aggregate_id)
        agg = self._aggregate_class(aggregate_id)
        agg.load_from_history(events)
        return agg

    def execute(
        self,
        aggregate_id: str,
        command: Callable[[Aggregate], None],
    ) -> CommandResult:
        """
        Load the aggregate, execute the command function against it, and
        save the resulting events to the EventStore.

        Parameters
        ----------
        aggregate_id : str
            The aggregate to operate on.
        command : Callable[[Aggregate], None]
            A function that calls command methods on the aggregate.
            Should raise on business rule violations.
        """
        try:
            agg = self._load_aggregate(aggregate_id)
            expected = agg.version
            command(agg)
            new_events = agg.uncommitted_events
            if new_events:
                self._store.append(aggregate_id, new_events, expected_version=expected)
                agg.mark_committed()
            return CommandResult(
                success=True,
                aggregate_id=aggregate_id,
                events_saved=len(new_events),
            )
        except (ValueError, ConcurrencyError) as exc:
            return CommandResult(
                success=False,
                aggregate_id=aggregate_id,
                error=str(exc),
            )


# ---------------------------------------------------------------------------
# Pattern 3 — CQRS: Projection + ReadModel
# ---------------------------------------------------------------------------


@dataclass
class OrderReadModel:
    """
    Denormalized read model for an Order, built from domain events.
    """

    order_id: str
    customer_id: Optional[str] = None
    amount: float = 0.0
    status: Optional[str] = None
    payment_reference: Optional[str] = None
    tracking_number: Optional[str] = None
    cancellation_reason: Optional[str] = None
    event_count: int = 0


class OrderProjection:
    """
    CQRS read-side projection that builds OrderReadModel from domain events.

    Subscribes to the EventStore's global stream and updates read models
    incrementally. Supports both initial rebuild (full replay) and
    incremental updates.

    Example
    -------
    >>> store = EventStore()
    >>> projection = OrderProjection(store)
    >>> # ... commands executed ...
    >>> projection.rebuild()
    >>> model = projection.get("ORD-001")
    """

    def __init__(self, event_store: EventStore) -> None:
        self._store = event_store
        self._models: Dict[str, OrderReadModel] = {}
        self._processed_position: int = 0

    def rebuild(self) -> None:
        """Replay all events in the global stream and rebuild all read models."""
        self._models.clear()
        self._processed_position = 0
        self._process_new_events()

    def update_incremental(self) -> int:
        """Process new events since last update. Returns number of events processed."""
        before = self._processed_position
        self._process_new_events()
        return self._processed_position - before

    def _process_new_events(self) -> None:
        events = self._store.global_stream(after_position=self._processed_position)
        for event in events:
            self._apply(event)
            self._processed_position += 1

    def _apply(self, event: DomainEvent) -> None:
        if event.aggregate_type != "Order":
            return

        model = self._models.setdefault(event.aggregate_id, OrderReadModel(event.aggregate_id))
        model.event_count += 1

        if event.event_type == "order.placed":
            model.customer_id = event.payload["customer_id"]
            model.amount = event.payload["amount"]
            model.status = OrderStatus.PLACED.value
        elif event.event_type == "order.paid":
            model.status = OrderStatus.PAID.value
            model.payment_reference = event.payload["payment_reference"]
        elif event.event_type == "order.shipped":
            model.status = OrderStatus.SHIPPED.value
            model.tracking_number = event.payload["tracking_number"]
        elif event.event_type == "order.cancelled":
            model.status = OrderStatus.CANCELLED.value
            model.cancellation_reason = event.payload["reason"]

    def get(self, order_id: str) -> Optional[OrderReadModel]:
        return self._models.get(order_id)

    def list_by_status(self, status: str) -> List[OrderReadModel]:
        return [m for m in self._models.values() if m.status == status]


# ---------------------------------------------------------------------------
# Pattern 4 — Snapshot
# ---------------------------------------------------------------------------


@dataclass
class Snapshot:
    """
    A point-in-time capture of an aggregate's state for replay optimization.

    Parameters
    ----------
    aggregate_id : str
        The aggregate this snapshot belongs to.
    aggregate_type : str
        The aggregate type name.
    version : int
        The aggregate version (number of events applied) at snapshot time.
    state : dict
        A copy of the aggregate's internal state dict at the snapshot version.
    taken_at : float
        Unix timestamp when the snapshot was taken.
    """

    aggregate_id: str
    aggregate_type: str
    version: int
    state: Dict[str, Any]
    taken_at: float = field(default_factory=time.time)


class SnapshotStore:
    """
    In-memory snapshot store (replace with a database table in production).

    The SnapshotStore stores the most recent snapshot per aggregate.
    Loading an aggregate from snapshot + tail events is O(events since snapshot),
    rather than O(all events), which matters for long-lived aggregates.

    Example
    -------
    >>> snap_store = SnapshotStore()
    >>> agg = OrderAggregate("ORD-001")
    >>> # ... replay events ...
    >>> snap_store.save(Snapshot(
    ...     aggregate_id="ORD-001",
    ...     aggregate_type="Order",
    ...     version=agg.version,
    ...     state=dict(agg._state),
    ... ))
    >>> snapshot = snap_store.load("ORD-001")
    >>> snapshot.version
    3
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._snapshots: Dict[str, Snapshot] = {}

    def save(self, snapshot: Snapshot) -> None:
        """Save or replace the snapshot for an aggregate."""
        with self._lock:
            self._snapshots[snapshot.aggregate_id] = snapshot

    def load(self, aggregate_id: str) -> Optional[Snapshot]:
        """Return the most recent snapshot for an aggregate, or None."""
        with self._lock:
            return self._snapshots.get(aggregate_id)


class SnapshotCommandHandler(CommandHandler):
    """
    CommandHandler extended with snapshot-aware aggregate loading.

    On load, checks for a snapshot and replays only events after the
    snapshot version, then re-applies the snapshot state to the aggregate.
    """

    def __init__(
        self,
        event_store: EventStore,
        snapshot_store: SnapshotStore,
        aggregate_class: Type[Aggregate],
        snapshot_every: int = 10,
    ) -> None:
        super().__init__(event_store, aggregate_class)
        self._snap_store = snapshot_store
        self._snapshot_every = snapshot_every

    def _load_aggregate(self, aggregate_id: str) -> Aggregate:
        agg = self._aggregate_class(aggregate_id)
        snapshot = self._snap_store.load(aggregate_id)

        if snapshot:
            # Restore state from snapshot
            agg._state = dict(snapshot.state)
            agg._version = snapshot.version
            # Replay only events after the snapshot
            tail_events = self._store.load_stream(
                aggregate_id, from_sequence=snapshot.version + 1
            )
            agg.load_from_history(tail_events)
        else:
            events = self._store.load_stream(aggregate_id)
            agg.load_from_history(events)

        return agg

    def execute(
        self,
        aggregate_id: str,
        command: Callable[[Aggregate], None],
    ) -> CommandResult:
        result = super().execute(aggregate_id, command)
        if result.success:
            current_version = self._store.stream_version(aggregate_id)
            if current_version % self._snapshot_every == 0:
                agg = self._aggregate_class(aggregate_id)
                events = self._store.load_stream(aggregate_id)
                agg.load_from_history(events)
                self._snap_store.save(Snapshot(
                    aggregate_id=aggregate_id,
                    aggregate_type=self._aggregate_class.aggregate_type,
                    version=agg.version,
                    state=dict(agg._state),
                ))
        return result


# ---------------------------------------------------------------------------
# Scenario demonstrations
# ---------------------------------------------------------------------------


def scenario_event_sourcing_replay() -> None:
    """Events appended, aggregate rebuilt from history."""
    print("\n--- Event Sourcing: Place → Pay → Ship and Replay ---")
    store = EventStore()
    handler = CommandHandler(store, OrderAggregate)

    oid = "ORD-ES-001"
    handler.execute(oid, lambda a: a.place_order("CUST-1", 199.99))
    handler.execute(oid, lambda a: a.pay_order("PAY-REF-001"))
    handler.execute(oid, lambda a: a.ship_order("TRACK-9999"))

    # Rebuild from events
    agg = OrderAggregate(oid)
    agg.load_from_history(store.load_stream(oid))
    print(f"  Status: {agg.status.value}")
    print(f"  Amount: {agg.amount}")
    print(f"  Events in stream: {store.stream_version(oid)}")


def scenario_optimistic_concurrency() -> None:
    """Two concurrent writes to the same aggregate — second fails."""
    print("\n--- CQRS: Optimistic Concurrency Conflict ---")
    store = EventStore()
    oid = "ORD-CC-001"

    agg1 = OrderAggregate(oid)
    agg1.place_order("CUST-A", 50.0)
    store.append(oid, agg1.uncommitted_events, expected_version=ExpectedVersion.NO_STREAM)

    # Second writer loads the same version=0 state — will conflict
    agg2 = OrderAggregate(oid)
    agg2.place_order("CUST-B", 75.0)
    try:
        store.append(oid, agg2.uncommitted_events, expected_version=ExpectedVersion.NO_STREAM)
        print("  ERROR: Should have raised ConcurrencyError")
    except Exception as e:
        print(f"  ConcurrencyError caught (expected): {str(e)[:70]}...")


def scenario_cqrs_projection() -> None:
    """Write-side events reflected in read-side projection."""
    print("\n--- CQRS: Write Side → Read Side Projection ---")
    store = EventStore()
    handler = CommandHandler(store, OrderAggregate)
    projection = OrderProjection(store)

    for i in range(3):
        oid = f"ORD-PROJ-{i:03d}"
        handler.execute(oid, lambda a, c=f"CUST-{i}": a.place_order(c, float(i * 100 + 50)))
        if i < 2:
            handler.execute(oid, lambda a: a.pay_order(f"PAY-{i}"))

    projection.rebuild()
    placed = projection.list_by_status(OrderStatus.PLACED.value)
    paid = projection.list_by_status(OrderStatus.PAID.value)
    print(f"  Orders in PLACED: {len(placed)}")
    print(f"  Orders in PAID: {len(paid)}")
    model = projection.get("ORD-PROJ-000")
    print(f"  ORD-PROJ-000 status: {model.status if model else 'not found'}")


def scenario_snapshot_optimization() -> None:
    """Snapshot taken every N events; load uses snapshot + tail."""
    print("\n--- Snapshot: Load from Snapshot + Tail Events ---")
    store = EventStore()
    snap_store = SnapshotStore()
    handler = SnapshotCommandHandler(store, snap_store, OrderAggregate, snapshot_every=2)

    oid = "ORD-SNAP-001"
    handler.execute(oid, lambda a: a.place_order("CUST-SNAP", 299.0))
    handler.execute(oid, lambda a: a.pay_order("PAY-SNAP"))
    # After 2 events, snapshot should be taken (snapshot_every=2)

    snap = snap_store.load(oid)
    print(f"  Snapshot at version: {snap.version if snap else 'none'}")

    # Ship without snapshot — load will use snapshot + 1 tail event
    handler.execute(oid, lambda a: a.ship_order("TRACK-SNAP"))
    agg = OrderAggregate(oid)
    tail = store.load_stream(oid, from_sequence=(snap.version + 1) if snap else 1)
    if snap:
        agg._state = dict(snap.state)
        agg._version = snap.version
    agg.load_from_history(tail)
    print(f"  Final status from snapshot+tail: {agg.status.value}")
    print(f"  Tail events replayed: {len(tail)}")


if __name__ == "__main__":
    scenario_event_sourcing_replay()
    scenario_optimistic_concurrency()
    scenario_cqrs_projection()
    scenario_snapshot_optimization()
    print("\nAll scenarios complete.")
