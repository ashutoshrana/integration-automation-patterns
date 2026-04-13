"""
35_event_sourcing_advanced.py — Advanced Event Sourcing Patterns

Pure-Python implementation of five advanced event sourcing primitives using
only the standard library (``dataclasses``, ``collections``, ``typing``,
``uuid``, ``time``).  No external dependencies are required.

    Pattern 1 — EventProjector:
                Projects a stream of domain events into a read model by
                dispatching each event to a registered handler keyed on
                ``event_type``.  Handlers receive the current accumulated
                state dict and the event dict and return an updated state.
                Unknown event types are silently skipped.  :meth:`reset`
                wipes the accumulated state so the projector can be reused
                for multiple projections.

    Pattern 2 — EventUpcaster:
                Migrates events produced by older schema versions to the
                current schema version.  Each migration is a function that
                transforms an event dict from ``from_version`` to
                ``to_version``.  :meth:`upcast` chains migrations in version
                order until the event reaches :attr:`current_version`.  If
                no migration is registered for the event's current version
                the event is returned as-is.

    Pattern 3 — EventReplay:
                Provides filtered views over an in-memory event log.  Events
                are plain dicts with at minimum a ``sequence_id`` (int),
                ``aggregate_id`` (str), and ``event_type`` (str).  Provides
                four query methods: :meth:`since`, :meth:`by_aggregate`,
                :meth:`by_type`, and :meth:`between`.

    Pattern 4 — CausationChain:
                Tracks causal relationships between events using
                ``causation_id`` and ``correlation_id`` metadata fields.
                :meth:`chain` walks the parent graph from a given event back
                to the root, returning the full causal path.
                :meth:`correlation_group` returns all events sharing a
                correlation ID, useful for grouping saga-scoped events.

    Pattern 5 — TemporalQuery:
                Enables point-in-time queries over an event log.  Events
                must carry a ``timestamp`` field (float UNIX epoch).
                :meth:`state_at` replays only events on or before the given
                timestamp through an :class:`EventProjector` to produce a
                snapshot of state at that instant.  :meth:`events_in_window`
                and :meth:`latest_before` complete the temporal API.

No external dependencies required.

Run:
    python examples/35_event_sourcing_advanced.py
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

# ===========================================================================
# Pattern 1 — EventProjector
# ===========================================================================


class EventProjector:
    """Projects a stream of domain events into a read-model state dict.

    Each event type is associated with a *handler* function that receives
    the current accumulated state and the event dict and returns the updated
    state.  Events whose type has no registered handler are silently skipped.

    Example::

        proj = EventProjector()

        def on_created(state, event):
            state["name"] = event["name"]
            return state

        def on_renamed(state, event):
            state["name"] = event["new_name"]
            return state

        proj.register("UserCreated", on_created)
        proj.register("UserRenamed", on_renamed)

        events = [
            {"event_type": "UserCreated", "name": "Alice"},
            {"event_type": "UserRenamed", "new_name": "Alicia"},
        ]
        state = proj.project(events)
        print(state)   # {"name": "Alicia"}
    """

    def __init__(self) -> None:
        self._handlers: dict[str, Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]]] = {}
        self._state: dict[str, Any] = {}

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register(
        self,
        event_type: str,
        handler: Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]],
    ) -> None:
        """Register a projection handler for *event_type*.

        Args:
            event_type: String key matched against each event's
                        ``event_type`` field.
            handler:    Callable ``(state, event) -> state`` that mutates
                        or returns a new state dict.
        """
        self._handlers[event_type] = handler

    # ------------------------------------------------------------------
    # Projection
    # ------------------------------------------------------------------

    def project(self, events: list[dict[str, Any]]) -> dict[str, Any]:
        """Apply all registered handlers in order to produce accumulated state.

        Args:
            events: Ordered list of event dicts, each with an
                    ``event_type`` field.

        Returns:
            The accumulated state dict after processing all events.
        """
        for event in events:
            etype = event.get("event_type", "")
            handler = self._handlers.get(etype)
            if handler is not None:
                self._state = handler(self._state, event)
        return self._state

    def reset(self) -> None:
        """Clear accumulated state so the projector can be reused."""
        self._state = {}


# ===========================================================================
# Pattern 2 — EventUpcaster
# ===========================================================================


@dataclass
class _MigrationKey:
    """Key for a registered migration step.

    Attributes:
        from_version: Source schema version.
        to_version:   Target schema version.
    """

    from_version: int
    to_version: int


class EventUpcaster:
    """Migrates events from older schema versions to the current schema.

    Migrations are registered as ``(from_version, to_version)`` pairs.
    :meth:`upcast` chains them in ascending order until the event's
    ``schema_version`` equals :attr:`current_version`.

    Example::

        uc = EventUpcaster()

        def v1_to_v2(event):
            event["schema_version"] = 2
            event["full_name"] = event.pop("name", "")
            return event

        def v2_to_v3(event):
            event["schema_version"] = 3
            event["tags"] = []
            return event

        uc.register_migration(1, 2, v1_to_v2)
        uc.register_migration(2, 3, v2_to_v3)

        old_event = {"event_type": "UserCreated", "schema_version": 1, "name": "Alice"}
        current = uc.upcast(old_event)
        print(current["schema_version"])  # 3
        print(current["full_name"])       # "Alice"
    """

    def __init__(self) -> None:
        self._migrations: dict[int, tuple[int, Callable[[dict[str, Any]], dict[str, Any]]]] = {}

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register_migration(
        self,
        from_version: int,
        to_version: int,
        migrate_fn: Callable[[dict[str, Any]], dict[str, Any]],
    ) -> None:
        """Register a migration function from *from_version* to *to_version*.

        Args:
            from_version: Schema version the migration accepts.
            to_version:   Schema version the migration produces.
            migrate_fn:   Callable ``event -> event`` that transforms the
                          event dict in-place or returns a new dict.
        """
        self._migrations[from_version] = (to_version, migrate_fn)

    # ------------------------------------------------------------------
    # Upcasting
    # ------------------------------------------------------------------

    def upcast(self, event: dict[str, Any]) -> dict[str, Any]:
        """Apply chained migrations until the event reaches :attr:`current_version`.

        Args:
            event: Event dict with a ``schema_version`` integer field.

        Returns:
            The event dict at the highest registered target version.
        """
        result = dict(event)
        while True:
            version = result.get("schema_version", 0)
            if version not in self._migrations:
                break
            _to, migrate_fn = self._migrations[version]
            result = migrate_fn(result)
        return result

    @property
    def current_version(self) -> int:
        """Return the highest registered target schema version.

        Returns:
            The maximum ``to_version`` across all registered migrations,
            or 0 if no migrations have been registered.
        """
        if not self._migrations:
            return 0
        return max(to for to, _ in self._migrations.values())


# ===========================================================================
# Pattern 3 — EventReplay
# ===========================================================================


class EventReplay:
    """Replays a filtered subset of events from an in-memory event log.

    Each event dict is expected to carry at minimum:

    * ``sequence_id`` (int) — monotonically increasing sequence number.
    * ``aggregate_id`` (str) — identifies the aggregate the event belongs to.
    * ``event_type``   (str) — discriminator for the kind of event.

    Example::

        log = [
            {"sequence_id": 1, "aggregate_id": "order-1", "event_type": "OrderPlaced"},
            {"sequence_id": 2, "aggregate_id": "order-2", "event_type": "OrderPlaced"},
            {"sequence_id": 3, "aggregate_id": "order-1", "event_type": "OrderShipped"},
        ]
        replay = EventReplay(log)
        print(replay.since(2))           # events 2 and 3
        print(replay.by_aggregate("order-1"))  # events 1 and 3
    """

    def __init__(self, events: list[dict[str, Any]]) -> None:
        """Store the event log.

        Args:
            events: List of event dicts sorted (or unsorted) by sequence_id.
        """
        self._events: list[dict[str, Any]] = list(events)

    # ------------------------------------------------------------------
    # Filtering API
    # ------------------------------------------------------------------

    def since(self, sequence_id: int) -> list[dict[str, Any]]:
        """Return events with ``sequence_id`` >= *sequence_id*.

        Args:
            sequence_id: Lower bound (inclusive) of the sequence range.

        Returns:
            Filtered list of events in original order.
        """
        return [e for e in self._events if e.get("sequence_id", 0) >= sequence_id]

    def by_aggregate(self, aggregate_id: str) -> list[dict[str, Any]]:
        """Return events belonging to the given aggregate.

        Args:
            aggregate_id: The aggregate identifier to filter on.

        Returns:
            Filtered list of events in original order.
        """
        return [e for e in self._events if e.get("aggregate_id") == aggregate_id]

    def by_type(self, event_type: str) -> list[dict[str, Any]]:
        """Return events of the given type.

        Args:
            event_type: The event type discriminator to filter on.

        Returns:
            Filtered list of events in original order.
        """
        return [e for e in self._events if e.get("event_type") == event_type]

    def between(self, start_id: int, end_id: int) -> list[dict[str, Any]]:
        """Return events with ``sequence_id`` in [*start_id*, *end_id*] inclusive.

        Args:
            start_id: Lower bound (inclusive).
            end_id:   Upper bound (inclusive).

        Returns:
            Filtered list of events in original order.
        """
        return [e for e in self._events if start_id <= e.get("sequence_id", 0) <= end_id]


# ===========================================================================
# Pattern 4 — CausationChain
# ===========================================================================


@dataclass
class _EventRecord:
    """Internal record for a single event in the causal graph.

    Attributes:
        event_id:       Unique event identifier.
        causation_id:   ID of the event that directly caused this event,
                        or ``None`` for root events.
        correlation_id: Saga/workflow-scoped correlation identifier,
                        or ``None`` if not provided.
    """

    event_id: str
    causation_id: str | None = None
    correlation_id: str | None = None


class CausationChain:
    """Tracks causal and correlational relationships between events.

    Events are recorded with optional ``causation_id`` (the event that
    directly triggered this event) and ``correlation_id`` (a shared
    identifier grouping all events in the same saga or workflow).

    Example::

        chain = CausationChain()
        chain.record("evt-1")
        chain.record("evt-2", causation_id="evt-1", correlation_id="saga-A")
        chain.record("evt-3", causation_id="evt-2", correlation_id="saga-A")

        print(chain.chain("evt-3"))            # ["evt-1", "evt-2", "evt-3"]
        print(chain.correlation_group("saga-A"))  # ["evt-2", "evt-3"]
    """

    def __init__(self) -> None:
        self._records: dict[str, _EventRecord] = {}
        self._correlation_index: dict[str, list[str]] = defaultdict(list)

    # ------------------------------------------------------------------
    # Recording
    # ------------------------------------------------------------------

    def record(
        self,
        event_id: str,
        causation_id: str | None = None,
        correlation_id: str | None = None,
    ) -> None:
        """Record an event with optional causal and correlation metadata.

        Args:
            event_id:       Unique identifier for this event.
            causation_id:   ID of the event that caused this one, or
                            ``None`` for root events.
            correlation_id: Shared saga/workflow correlation identifier,
                            or ``None`` if not applicable.
        """
        rec = _EventRecord(
            event_id=event_id,
            causation_id=causation_id,
            correlation_id=correlation_id,
        )
        self._records[event_id] = rec
        if correlation_id is not None:
            self._correlation_index[correlation_id].append(event_id)

    # ------------------------------------------------------------------
    # Querying
    # ------------------------------------------------------------------

    def caused_by(self, event_id: str) -> str | None:
        """Return the ``causation_id`` for the given event.

        Args:
            event_id: Event whose causation to look up.

        Returns:
            The ``causation_id`` string, or ``None`` if the event is a
            root event or is unknown.
        """
        rec = self._records.get(event_id)
        return rec.causation_id if rec is not None else None

    def chain(self, event_id: str) -> list[str]:
        """Return the full causal chain from the root event to *event_id*.

        The chain is built by following ``causation_id`` links backwards
        until a root event (no causation) is reached, then the path is
        reversed so it runs root → leaf.

        Args:
            event_id: Terminal event whose ancestry to trace.

        Returns:
            Ordered list of event IDs from the root to *event_id*.
            Returns ``[event_id]`` if the event has no recorded causation.
            Returns ``[]`` if *event_id* is not recorded at all.
        """
        if event_id not in self._records:
            return []
        path: list[str] = []
        current: str | None = event_id
        visited: set[str] = set()
        while current is not None:
            if current in visited:
                # Cycle guard — stop to avoid infinite loop.
                break
            visited.add(current)
            path.append(current)
            rec = self._records.get(current)
            current = rec.causation_id if rec is not None else None
        path.reverse()
        return path

    def correlation_group(self, correlation_id: str) -> list[str]:
        """Return all event IDs sharing the given *correlation_id*.

        Args:
            correlation_id: Correlation identifier to look up.

        Returns:
            List of event IDs in recording order.  Returns ``[]`` if no
            events have been recorded with this correlation ID.
        """
        return list(self._correlation_index.get(correlation_id, []))


# ===========================================================================
# Pattern 5 — TemporalQuery
# ===========================================================================


class TemporalQuery:
    """Point-in-time queries over a timestamped event log.

    Events must carry a ``timestamp`` field (float UNIX epoch seconds).
    :meth:`state_at` replays all events up to a given point in time through
    a caller-supplied :class:`EventProjector` to produce a historical
    snapshot of aggregate state.

    Example::

        events = [
            {"event_type": "Created",  "timestamp": 1000.0, "name": "Alice"},
            {"event_type": "Renamed",  "timestamp": 2000.0, "new_name": "Alicia"},
            {"event_type": "Archived", "timestamp": 3000.0},
        ]
        tq = TemporalQuery(events)

        proj = EventProjector()
        proj.register("Created",  lambda s, e: {**s, "name": e["name"]})
        proj.register("Renamed",  lambda s, e: {**s, "name": e["new_name"]})
        proj.register("Archived", lambda s, e: {**s, "archived": True})

        print(tq.state_at(1500.0, proj))   # {"name": "Alice"}
        print(tq.latest_before(2500.0))    # {"event_type": "Renamed", ...}
    """

    def __init__(self, events: list[dict[str, Any]]) -> None:
        """Store the timestamped event log.

        Args:
            events: List of event dicts each with a ``timestamp`` float field.
        """
        self._events: list[dict[str, Any]] = list(events)

    # ------------------------------------------------------------------
    # Temporal API
    # ------------------------------------------------------------------

    def state_at(self, timestamp: float, projector: EventProjector) -> dict[str, Any]:
        """Return the projected state at *timestamp* using *projector*.

        Only events with ``timestamp <= timestamp`` are replayed.  The
        projector is reset before replaying so prior calls do not
        contaminate the result.

        Args:
            timestamp: Point-in-time boundary (UNIX epoch float, inclusive).
            projector: :class:`EventProjector` instance with handlers
                       registered for the relevant event types.

        Returns:
            Accumulated state dict produced by replaying qualifying events.
        """
        projector.reset()
        qualifying = [e for e in self._events if e.get("timestamp", 0.0) <= timestamp]
        return projector.project(qualifying)

    def events_in_window(self, start: float, end: float) -> list[dict[str, Any]]:
        """Return events whose ``timestamp`` falls in [*start*, *end*] inclusive.

        Args:
            start: Window start boundary (UNIX epoch float, inclusive).
            end:   Window end boundary (UNIX epoch float, inclusive).

        Returns:
            Filtered list of events in original order.
        """
        return [e for e in self._events if start <= e.get("timestamp", 0.0) <= end]

    def latest_before(self, timestamp: float) -> dict[str, Any] | None:
        """Return the most recent event strictly before *timestamp*.

        Args:
            timestamp: Exclusive upper bound (UNIX epoch float).

        Returns:
            The event dict with the largest ``timestamp`` that is still
            strictly less than *timestamp*, or ``None`` if no such event
            exists.
        """
        candidates = [e for e in self._events if e.get("timestamp", 0.0) < timestamp]
        if not candidates:
            return None
        return max(candidates, key=lambda e: e.get("timestamp", 0.0))


# ===========================================================================
# Demo helpers
# ===========================================================================


def _separator(title: str) -> None:
    width = 60
    print("\n" + "=" * width)
    print(f"  {title}")
    print("=" * width)


# ===========================================================================
# Demo scenarios
# ===========================================================================


def _run_event_projector_demo() -> None:
    """Scenario 1: EventProjector — aggregate state rebuild."""
    _separator("Scenario 1 — EventProjector: aggregate state rebuild")

    proj = EventProjector()

    proj.register("AccountOpened", lambda s, e: {**s, "balance": e.get("initial_balance", 0), "open": True})
    proj.register("MoneyDeposited", lambda s, e: {**s, "balance": s.get("balance", 0) + e["amount"]})
    proj.register("MoneyWithdrawn", lambda s, e: {**s, "balance": s.get("balance", 0) - e["amount"]})
    proj.register("AccountClosed", lambda s, e: {**s, "open": False})

    events = [
        {"event_type": "AccountOpened", "initial_balance": 100},
        {"event_type": "MoneyDeposited", "amount": 50},
        {"event_type": "MoneyWithdrawn", "amount": 30},
        {"event_type": "UnknownEvent", "data": "ignored"},
        {"event_type": "AccountClosed"},
    ]

    state = proj.project(events)
    print(f"Final state: {state}")
    assert state["balance"] == 120, f"Expected 120 got {state['balance']}"
    assert state["open"] is False

    proj.reset()
    assert proj._state == {}
    print("Reset OK — state cleared")
    print("PASS")


def _run_event_upcaster_demo() -> None:
    """Scenario 2: EventUpcaster — multi-version schema migration."""
    _separator("Scenario 2 — EventUpcaster: multi-version schema migration")

    uc = EventUpcaster()

    def v1_to_v2(event: dict) -> dict:
        event["schema_version"] = 2
        event["full_name"] = event.pop("name", "")
        return event

    def v2_to_v3(event: dict) -> dict:
        event["schema_version"] = 3
        event["tags"] = event.get("tags", [])
        event["active"] = True
        return event

    uc.register_migration(1, 2, v1_to_v2)
    uc.register_migration(2, 3, v2_to_v3)

    print(f"current_version = {uc.current_version}")
    assert uc.current_version == 3

    old_event = {"event_type": "UserCreated", "schema_version": 1, "name": "Alice"}
    upcasted = uc.upcast(old_event)
    print(f"Upcasted: {upcasted}")
    assert upcasted["schema_version"] == 3
    assert upcasted["full_name"] == "Alice"
    assert upcasted["active"] is True

    current_event = {"event_type": "UserCreated", "schema_version": 3, "full_name": "Bob"}
    unchanged = uc.upcast(current_event)
    assert unchanged["schema_version"] == 3
    print("PASS")


def _run_event_replay_demo() -> None:
    """Scenario 3: EventReplay — filtered log queries."""
    _separator("Scenario 3 — EventReplay: filtered log queries")

    log = [
        {"sequence_id": 1, "aggregate_id": "order-1", "event_type": "OrderPlaced"},
        {"sequence_id": 2, "aggregate_id": "order-2", "event_type": "OrderPlaced"},
        {"sequence_id": 3, "aggregate_id": "order-1", "event_type": "OrderShipped"},
        {"sequence_id": 4, "aggregate_id": "order-3", "event_type": "OrderPlaced"},
        {"sequence_id": 5, "aggregate_id": "order-2", "event_type": "OrderShipped"},
    ]
    replay = EventReplay(log)

    since_3 = replay.since(3)
    print(f"since(3) => {[e['sequence_id'] for e in since_3]}")
    assert [e["sequence_id"] for e in since_3] == [3, 4, 5]

    order1 = replay.by_aggregate("order-1")
    assert len(order1) == 2

    placed = replay.by_type("OrderPlaced")
    assert len(placed) == 3

    between = replay.between(2, 4)
    assert [e["sequence_id"] for e in between] == [2, 3, 4]

    print("PASS")


def _run_causation_chain_demo() -> None:
    """Scenario 4: CausationChain — causal graph traversal."""
    _separator("Scenario 4 — CausationChain: causal graph traversal")

    chain = CausationChain()
    chain.record("cmd-1")
    chain.record("evt-1", causation_id="cmd-1", correlation_id="saga-A")
    chain.record("evt-2", causation_id="evt-1", correlation_id="saga-A")
    chain.record("evt-3", causation_id="evt-2", correlation_id="saga-A")
    chain.record("side-effect", causation_id="evt-2", correlation_id="saga-B")

    causal_path = chain.chain("evt-3")
    print(f"chain('evt-3') = {causal_path}")
    assert causal_path == ["cmd-1", "evt-1", "evt-2", "evt-3"]

    group_a = chain.correlation_group("saga-A")
    print(f"correlation_group('saga-A') = {group_a}")
    assert set(group_a) == {"evt-1", "evt-2", "evt-3"}

    assert chain.caused_by("evt-3") == "evt-2"
    assert chain.caused_by("cmd-1") is None
    print("PASS")


def _run_temporal_query_demo() -> None:
    """Scenario 5: TemporalQuery — point-in-time state snapshots."""
    _separator("Scenario 5 — TemporalQuery: point-in-time state snapshots")

    events = [
        {"event_type": "AccountOpened", "timestamp": 1000.0, "balance": 100},
        {"event_type": "Deposited", "timestamp": 2000.0, "amount": 50},
        {"event_type": "Withdrawn", "timestamp": 3000.0, "amount": 20},
    ]
    tq = TemporalQuery(events)

    proj = EventProjector()
    proj.register("AccountOpened", lambda s, e: {**s, "balance": e["balance"]})
    proj.register("Deposited", lambda s, e: {**s, "balance": s.get("balance", 0) + e["amount"]})
    proj.register("Withdrawn", lambda s, e: {**s, "balance": s.get("balance", 0) - e["amount"]})

    state_at_1500 = tq.state_at(1500.0, proj)
    print(f"state_at(1500) = {state_at_1500}")
    assert state_at_1500 == {"balance": 100}

    state_at_2500 = tq.state_at(2500.0, proj)
    assert state_at_2500 == {"balance": 150}

    window = tq.events_in_window(1500.0, 2500.0)
    assert len(window) == 1
    assert window[0]["event_type"] == "Deposited"

    latest = tq.latest_before(2500.0)
    assert latest is not None
    assert latest["event_type"] == "Deposited"
    print("PASS")


# ===========================================================================
# Entry point
# ===========================================================================


if __name__ == "__main__":
    print("Advanced Event Sourcing Patterns — Smoke Test Suite")
    print(
        "Patterns: EventProjector | EventUpcaster | EventReplay"
        " | CausationChain | TemporalQuery"
    )

    _run_event_projector_demo()
    _run_event_upcaster_demo()
    _run_event_replay_demo()
    _run_causation_chain_demo()
    _run_temporal_query_demo()

    print("\n" + "=" * 60)
    print("  All scenarios passed.")
    print("=" * 60)
