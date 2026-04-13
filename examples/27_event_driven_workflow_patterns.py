"""
27_event_driven_workflow_patterns.py — Event-driven workflow patterns using
choreography, event sourcing, saga compensation, dead-letter queues, and
stream filtering/transformation.

Demonstrates five complementary patterns that together implement a production-
grade event-driven workflow system suitable for loosely-coupled, asynchronous
business processes:

    Pattern 1 — EventBus (Publish/Subscribe):
                Thread-safe publish-subscribe bus where producers publish
                events by type and consumers subscribe handler callables.
                Supports wildcard ("*") handlers that receive every event
                regardless of type. Handlers are invoked outside the internal
                lock to prevent deadlocks under re-entrant publish calls.
                Mirrors the semantics of Apache Kafka topics, AWS EventBridge
                event buses, and MuleSoft's event-driven connectors.

    Pattern 2 — EventSourcingWorkflow (Append-Only Event Log):
                Stores workflow state as an ordered, append-only log of domain
                events rather than mutable records. State is reconstructed by
                replaying events from the beginning (or up to a sequence
                number). Supports partial replay for point-in-time queries and
                state snapshots. This is the core pattern behind CQRS/ES
                architectures, Kafka event logs, and AWS Step Functions
                execution history.

    Pattern 3 — ChoreographySaga (Decentralised Coordination):
                Implements a saga with no central orchestrator. Each
                participant reacts to domain events by subscribing to the
                shared EventBus. Compensating actions run in reverse order
                from the point of failure, guaranteeing eventual consistency
                without distributed locking. Contrasts with the orchestration
                saga in example 19 — here coordination emerges from event
                subscriptions rather than a central engine.

    Pattern 4 — DeadLetterQueue (Reliable Failure Handling):
                Wraps event handlers with a retry envelope. On repeated
                failure the event is parked in a dead-letter queue (DLQ)
                for later inspection and re-processing. Retrying DLQ items
                clears recovered events, leaving only persistently-failing
                messages. Mirrors AWS SQS DLQ, Azure Service Bus dead-letter
                sub-queues, and Kafka consumer error topics.

    Pattern 5 — EventFilter (Stream Filtering and Transformation):
                Composable pipeline of named predicates and named
                transformers applied sequentially to each event. Any
                predicate returning False short-circuits the pipeline and
                discards the event (returns None). Transformers modify
                the event dict in place through the chain. Mirrors
                MuleSoft DataWeave filters, Apache Camel route predicates,
                and Kafka Streams filter/map operations.

No external dependencies required.

Run:
    python examples/27_event_driven_workflow_patterns.py
"""

from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Any, Callable, Optional


# ===========================================================================
# Pattern 1 — EventBus
# ===========================================================================


class EventBus:
    """Thread-safe publish-subscribe event bus.

    Consumers register callable handlers for a specific event type or for
    "*" (wildcard) to receive every event.  Publishers call :meth:`publish`
    with a dict that must contain at minimum a ``"type"`` key.

    Handlers are invoked **outside** the internal lock so that a handler
    that calls :meth:`publish` re-entrantly does not deadlock.

    Example::

        bus = EventBus()

        def on_order(event):
            print("order received:", event)

        bus.subscribe("order.created", on_order)
        bus.publish({"type": "order.created", "order_id": "ORD-1"})
    """

    def __init__(self) -> None:
        self._handlers: dict[str, list[Callable[[dict], Any]]] = {}
        self._lock = threading.Lock()
        self._event_count: int = 0

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def subscribe(self, event_type: str, handler: Callable[[dict], Any]) -> None:
        """Register *handler* to be called whenever *event_type* is published.

        Use ``"*"`` as *event_type* to receive every event published on
        this bus.

        Args:
            event_type: The event type string (or ``"*"`` for all events).
            handler: Callable that accepts a single ``dict`` argument.
        """
        with self._lock:
            self._handlers.setdefault(event_type, []).append(handler)

    def unsubscribe(self, event_type: str, handler: Callable[[dict], Any]) -> None:
        """Remove *handler* from the subscriber list for *event_type*.

        Silently does nothing if the handler is not currently registered.

        Args:
            event_type: The event type string (or ``"*"``).
            handler: The callable previously passed to :meth:`subscribe`.
        """
        with self._lock:
            handlers = self._handlers.get(event_type, [])
            if handler in handlers:
                handlers.remove(handler)

    # ------------------------------------------------------------------
    # Publishing
    # ------------------------------------------------------------------

    def publish(self, event: dict) -> None:
        """Invoke all handlers registered for ``event["type"]`` and all
        wildcard (``"*"``) handlers.

        Handlers are collected while holding the lock but invoked after
        releasing it to avoid re-entrant deadlocks.

        Args:
            event: A dict with at least a ``"type"`` key.
        """
        event_type = event.get("type", "")
        with self._lock:
            specific = list(self._handlers.get(event_type, []))
            wildcards = list(self._handlers.get("*", []))
            self._event_count += 1

        for handler in specific + wildcards:
            handler(event)

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def event_count(self) -> int:
        """Total number of events that have been published on this bus."""
        return self._event_count


# ===========================================================================
# Pattern 2 — EventSourcingWorkflow
# ===========================================================================


class EventSourcingWorkflow:
    """Append-only event log with state reconstruction by replay.

    State is never mutated in-place.  Instead every state change is
    represented as an immutable event appended to :attr:`_log`.  Calling
    :attr:`current_state` replays the entire log to reconstruct the latest
    state, which makes point-in-time queries trivial via :meth:`replay`.

    Supported event types and their state transitions:

    * ``"workflow.started"``      → ``{"status": "RUNNING",      ...payload}``
    * ``"workflow.step_completed"`` → previous state merged with payload, status stays ``"RUNNING"``
    * ``"workflow.completed"``    → ``{"status": "COMPLETED",    ...payload}``
    * ``"workflow.failed"``       → ``{"status": "FAILED",  "error": payload.get("error")}``
    * ``"workflow.compensated"``  → ``{"status": "COMPENSATED",  ...payload}``

    Example::

        wf = EventSourcingWorkflow("wf-1")
        wf.record_event("workflow.started", {"order_id": "ORD-1"})
        wf.record_event("workflow.step_completed", {"step": "validate"})
        wf.record_event("workflow.completed", {})
        assert wf.current_state["status"] == "COMPLETED"
    """

    def __init__(self, workflow_id: str) -> None:
        self._workflow_id = workflow_id
        self._log: list[dict] = []

    # ------------------------------------------------------------------
    # Mutation
    # ------------------------------------------------------------------

    def record_event(self, event_type: str, payload: dict) -> None:
        """Append a new event to the log.

        Args:
            event_type: Logical event name (e.g. ``"workflow.started"``).
            payload: Arbitrary data associated with the event.
        """
        seq = len(self._log)
        self._log.append(
            {
                "seq": seq,
                "type": event_type,
                "payload": payload,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        )

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def replay(self, up_to_seq: Optional[int] = None) -> list[dict]:
        """Return a list of events up to (and including) *up_to_seq*.

        Args:
            up_to_seq: Inclusive upper bound on the sequence number.
                       Pass ``None`` (the default) to return all events.

        Returns:
            A new list containing the matching event dicts.
        """
        if up_to_seq is None:
            return list(self._log)
        return [e for e in self._log if e["seq"] <= up_to_seq]

    @property
    def current_state(self) -> dict:
        """Reconstruct the workflow state by folding over the entire event log.

        Returns:
            A dict representing the current state, always containing at
            minimum a ``"status"`` key.  Returns ``{}`` if no events have
            been recorded yet.
        """
        state: dict = {}
        for event in self._log:
            event_type = event["type"]
            payload = event["payload"]
            if event_type == "workflow.started":
                state = {"status": "RUNNING", **payload}
            elif event_type == "workflow.step_completed":
                state = {**state, **payload, "status": "RUNNING"}
            elif event_type == "workflow.completed":
                state = {**state, **payload, "status": "COMPLETED"}
            elif event_type == "workflow.failed":
                state = {**state, "status": "FAILED", "error": payload.get("error")}
            elif event_type == "workflow.compensated":
                state = {**state, **payload, "status": "COMPENSATED"}
        return state

    @property
    def event_count(self) -> int:
        """Total number of events recorded in this workflow's log."""
        return len(self._log)

    @property
    def workflow_id(self) -> str:
        """The identifier supplied at construction time."""
        return self._workflow_id


# ===========================================================================
# Pattern 3 — ChoreographySaga
# ===========================================================================


class ChoreographySaga:
    """Saga implemented via event choreography (no central orchestrator).

    Each saga step subscribes to an event type on a shared :class:`EventBus`.
    When the triggering event is published the step's action callable runs
    automatically.  There is no saga manager that explicitly calls each step —
    coordination emerges from the event subscriptions.

    If a step fails (raises an exception) the saga transitions to
    ``"COMPENSATING"`` and the compensating callables of all previously
    completed steps are invoked in reverse order.

    Example::

        bus = EventBus()
        saga = ChoreographySaga("saga-1", bus)

        compensators = []

        saga.register_step(
            "reserve_inventory",
            on_event="order.created",
            action=lambda e: compensators.append("reserved"),
            compensate=lambda e: compensators.append("released"),
        )

        saga.start({"type": "order.created", "order_id": "ORD-1"})
        assert saga.status == "COMPLETED"
    """

    def __init__(self, saga_id: str, event_bus: EventBus) -> None:
        self._saga_id = saga_id
        self._event_bus = event_bus
        self._steps: list[dict] = []          # ordered registration list
        self._completed_steps: list[str] = []
        self._status: str = "RUNNING"

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register_step(
        self,
        step_name: str,
        on_event: str,
        action: Callable[[dict], Any],
        compensate: Callable[[dict], Any],
    ) -> None:
        """Register a saga step that is triggered by *on_event*.

        Args:
            step_name: Human-readable step identifier.
            on_event:  The event type that triggers this step.
            action:    Callable executed when *on_event* is received.
                       If it raises, compensation begins.
            compensate: Callable executed during rollback.
        """
        step = {
            "name": step_name,
            "on_event": on_event,
            "action": action,
            "compensate": compensate,
        }
        self._steps.append(step)

        # Closure captures the step dict so the handler can reference it.
        def _handler(event: dict, _step: dict = step) -> None:
            if self._status not in ("RUNNING",):
                return
            try:
                _step["action"](event)
                self._completed_steps.append(_step["name"])
            except Exception:  # noqa: BLE001
                self._status = "COMPENSATING"
                self.compensate_from(_step["name"])

        self._event_bus.subscribe(on_event, _handler)

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def start(self, initial_event: dict) -> None:
        """Publish *initial_event* to the bus to kick off the saga.

        Steps whose ``on_event`` matches ``initial_event["type"]`` will be
        invoked synchronously (since :class:`EventBus` invokes handlers
        inline on the calling thread).

        After all triggered steps complete without raising, the saga
        transitions to ``"COMPLETED"``.

        Args:
            initial_event: Dict with at least a ``"type"`` key.
        """
        self._event_bus.publish(initial_event)
        if self._status == "RUNNING":
            self._status = "COMPLETED"

    def compensate_from(self, failed_step: str) -> None:
        """Run compensating actions in reverse order from *failed_step* backward.

        Only steps that appear in :attr:`completed_steps` are compensated.
        Compensation runs in reverse registration order up to and not
        including *failed_step* itself (which never succeeded).

        Args:
            failed_step: Name of the step that triggered the rollback.
        """
        self._status = "COMPENSATING"
        # Collect steps completed before the failing step, in reverse order.
        steps_to_compensate = [
            s for s in reversed(self._steps)
            if s["name"] in self._completed_steps and s["name"] != failed_step
        ]
        for step in steps_to_compensate:
            try:
                step["compensate"]({"saga_id": self._saga_id})
            except Exception:  # noqa: BLE001
                pass  # Best-effort compensation
        self._status = "COMPENSATED"

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def completed_steps(self) -> list[str]:
        """Names of steps whose action callable ran without raising."""
        return list(self._completed_steps)

    @property
    def status(self) -> str:
        """Current saga status: ``"RUNNING"``, ``"COMPLETED"``, ``"COMPENSATING"``, or ``"COMPENSATED"``."""
        return self._status

    @property
    def saga_id(self) -> str:
        """The identifier supplied at construction time."""
        return self._saga_id


# ===========================================================================
# Pattern 4 — DeadLetterQueue
# ===========================================================================


class DeadLetterQueue:
    """Dead-letter queue for failed event processing with configurable retries.

    :meth:`process` wraps a handler callable with a retry envelope.  On each
    call failure the attempt count increments.  Once the attempt count
    reaches *max_retries* the event is moved to the DLQ.

    Parked DLQ items can be retried in bulk via :meth:`retry_dead_letters`,
    which removes successfully-recovered items.

    Example::

        dlq = DeadLetterQueue(max_retries=2)
        calls = []

        def flaky(event):
            calls.append(1)
            if len(calls) < 3:
                raise ValueError("transient")

        event = {"type": "payment.process", "amount": 100}
        dlq.process(event, flaky)   # fails, attempt 1
        dlq.process(event, flaky)   # fails, attempt 2 — moves to DLQ
        recovered = dlq.retry_dead_letters(flaky)   # succeeds on 3rd call
        assert recovered == 1
        assert dlq.dlq_size == 0
    """

    def __init__(self, max_retries: int = 3) -> None:
        self._max_retries = max_retries
        self._dead_letters: list[dict] = []
        # Tracks in-flight retry counts keyed by event identity.
        # Key: id(event) is not stable; use a tuple of sorted items or
        # fall back to a monotonic counter.
        self._retry_counts: dict[int, int] = {}  # id(event) → attempt count

    # ------------------------------------------------------------------
    # Processing
    # ------------------------------------------------------------------

    def process(self, event: dict, handler: Callable[[dict], Any]) -> bool:
        """Attempt to process *event* with *handler*.

        Increments the retry counter for the event on failure.  When the
        counter reaches *max_retries*, the event is moved to the DLQ.

        Args:
            event:   The event dict to process.
            handler: Callable that accepts *event* and raises on failure.

        Returns:
            ``True`` if the handler succeeded; ``False`` otherwise.
        """
        key = id(event)
        try:
            handler(event)
            # Success — clean up retry tracking if present.
            self._retry_counts.pop(key, None)
            return True
        except Exception as exc:  # noqa: BLE001
            attempts = self._retry_counts.get(key, 0) + 1
            self._retry_counts[key] = attempts
            if attempts >= self._max_retries:
                self._dead_letters.append(
                    {
                        "event": event,
                        "error": str(exc),
                        "attempts": attempts,
                    }
                )
                self._retry_counts.pop(key, None)
            return False

    # ------------------------------------------------------------------
    # DLQ management
    # ------------------------------------------------------------------

    def retry_dead_letters(self, handler: Callable[[dict], Any]) -> int:
        """Retry every item currently in the DLQ with *handler*.

        Items that succeed are removed from the DLQ.  Items that fail
        remain in the DLQ.

        Args:
            handler: Callable that accepts an event dict.

        Returns:
            The number of items successfully recovered (and removed from
            the DLQ).
        """
        recovered_count = 0
        still_dead: list[dict] = []
        for dlq_item in self._dead_letters:
            try:
                handler(dlq_item["event"])
                recovered_count += 1
            except Exception:  # noqa: BLE001
                still_dead.append(dlq_item)
        self._dead_letters = still_dead
        return recovered_count

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def dead_letters(self) -> list[dict]:
        """A copy of all items currently parked in the dead-letter queue."""
        return list(self._dead_letters)

    @property
    def dlq_size(self) -> int:
        """Number of events currently parked in the dead-letter queue."""
        return len(self._dead_letters)


# ===========================================================================
# Pattern 5 — EventFilter
# ===========================================================================


class EventFilter:
    """Composable event stream filter and transformer pipeline.

    Filters (predicates) and transformers are stored in insertion order.
    :meth:`process` applies all filters first: if any predicate returns
    ``False`` the event is discarded and ``None`` is returned.  Then all
    transformers are applied in order, each receiving the output of the
    previous one.

    Filters and transformers are identified by name for introspection and
    potential removal in derived classes.

    Example::

        ef = EventFilter()

        # Only pass high-priority events
        ef.add_filter("high_priority", lambda e: e.get("priority") == "HIGH")

        # Enrich events with a processing timestamp
        ef.add_transformer("add_ts", lambda e: {**e, "processed_at": "now"})

        result = ef.process({"type": "alert", "priority": "HIGH"})
        assert result is not None
        assert "processed_at" in result

        dropped = ef.process({"type": "debug", "priority": "LOW"})
        assert dropped is None
    """

    def __init__(self) -> None:
        self._filters: list[tuple[str, Callable[[dict], bool]]] = []
        self._transformers: list[tuple[str, Callable[[dict], dict]]] = []

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def add_filter(self, name: str, predicate: Callable[[dict], bool]) -> None:
        """Add a named filter (predicate) to the pipeline.

        If *predicate* returns ``False`` for an event, :meth:`process`
        returns ``None`` for that event without applying further steps.

        Args:
            name:      Identifier for this filter (used in introspection).
            predicate: Callable that accepts an event dict and returns bool.
        """
        self._filters.append((name, predicate))

    def add_transformer(self, name: str, transform: Callable[[dict], dict]) -> None:
        """Add a named transformer to the pipeline.

        Transformers are applied in insertion order after all filters pass.

        Args:
            name:      Identifier for this transformer.
            transform: Callable that accepts an event dict and returns a
                       (possibly modified) event dict.
        """
        self._transformers.append((name, transform))

    # ------------------------------------------------------------------
    # Processing
    # ------------------------------------------------------------------

    def process(self, event: dict) -> Optional[dict]:
        """Apply all filters then all transformers to *event*.

        Args:
            event: The event dict to process.

        Returns:
            The transformed event dict if all filters pass, or ``None``
            if any filter rejects the event.
        """
        for _name, predicate in self._filters:
            if not predicate(event):
                return None

        result = event
        for _name, transform in self._transformers:
            result = transform(result)
        return result

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def filter_count(self) -> int:
        """Number of filters currently registered."""
        return len(self._filters)

    @property
    def transformer_count(self) -> int:
        """Number of transformers currently registered."""
        return len(self._transformers)


# ===========================================================================
# Smoke-test helpers (used only in __main__)
# ===========================================================================


def _separator(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def _run_event_bus_demo() -> None:
    """Scenario 1: EventBus publish/subscribe with wildcard handler."""
    _separator("Scenario 1 — EventBus: pub/sub + wildcard")

    bus = EventBus()
    received: list[dict] = []
    all_events: list[dict] = []

    bus.subscribe("order.created", lambda e: received.append(e))
    bus.subscribe("*", lambda e: all_events.append(e))

    bus.publish({"type": "order.created", "order_id": "ORD-1"})
    bus.publish({"type": "order.shipped", "order_id": "ORD-1"})

    print(f"Events for 'order.created' : {len(received)}")
    print(f"Events for '*'             : {len(all_events)}")
    print(f"Total published            : {bus.event_count}")

    assert len(received) == 1
    assert len(all_events) == 2
    assert bus.event_count == 2
    print("PASS")


def _run_event_sourcing_demo() -> None:
    """Scenario 2: EventSourcingWorkflow state reconstruction via replay."""
    _separator("Scenario 2 — EventSourcingWorkflow: state from events")

    wf = EventSourcingWorkflow("wf-demo")
    wf.record_event("workflow.started", {"order_id": "ORD-2"})
    wf.record_event("workflow.step_completed", {"step": "validate", "result": "ok"})
    wf.record_event("workflow.step_completed", {"step": "charge", "result": "charged"})
    wf.record_event("workflow.completed", {})

    state = wf.current_state
    print(f"Final state  : {state}")
    print(f"Event count  : {wf.event_count}")
    print(f"Replay[0..1] : {wf.replay(up_to_seq=1)}")

    assert state["status"] == "COMPLETED"
    assert wf.event_count == 4
    assert len(wf.replay(up_to_seq=1)) == 2
    print("PASS")


def _run_choreography_saga_demo() -> None:
    """Scenario 3: ChoreographySaga — steps triggered by events."""
    _separator("Scenario 3 — ChoreographySaga: decentralised coordination")

    bus = EventBus()
    saga = ChoreographySaga("saga-demo", bus)
    log: list[str] = []

    saga.register_step(
        "reserve_inventory",
        on_event="order.created",
        action=lambda e: log.append("reserve"),
        compensate=lambda e: log.append("release"),
    )

    saga.start({"type": "order.created", "order_id": "ORD-3"})

    print(f"Completed steps : {saga.completed_steps}")
    print(f"Status          : {saga.status}")
    print(f"Action log      : {log}")

    assert saga.status == "COMPLETED"
    assert "reserve_inventory" in saga.completed_steps
    print("PASS")


def _run_dlq_demo() -> None:
    """Scenario 4: DeadLetterQueue — exhausted retries → DLQ → recovery."""
    _separator("Scenario 4 — DeadLetterQueue: retry → DLQ → recovery")

    dlq = DeadLetterQueue(max_retries=2)
    calls: list[int] = []

    def flaky_handler(event: dict) -> None:
        calls.append(1)
        if len(calls) < 3:
            raise RuntimeError("transient")

    event = {"type": "payment.process", "amount": 99}
    dlq.process(event, flaky_handler)   # attempt 1 — fails
    dlq.process(event, flaky_handler)   # attempt 2 — fails → DLQ

    print(f"DLQ size after exhaustion  : {dlq.dlq_size}")

    recovered = dlq.retry_dead_letters(flaky_handler)  # attempt 3 — succeeds
    print(f"Recovered from DLQ         : {recovered}")
    print(f"DLQ size after recovery    : {dlq.dlq_size}")

    assert dlq.dlq_size == 0
    assert recovered == 1
    print("PASS")


def _run_event_filter_demo() -> None:
    """Scenario 5: EventFilter — filter + transformer pipeline."""
    _separator("Scenario 5 — EventFilter: filter + transform pipeline")

    ef = EventFilter()
    ef.add_filter("high_priority", lambda e: e.get("priority") == "HIGH")
    ef.add_transformer("add_source", lambda e: {**e, "source": "bus"})
    ef.add_transformer("uppercase_type", lambda e: {**e, "type": e["type"].upper()})

    result = ef.process({"type": "alert", "priority": "HIGH"})
    dropped = ef.process({"type": "debug", "priority": "LOW"})

    print(f"Processed high-priority : {result}")
    print(f"Dropped low-priority    : {dropped}")
    print(f"Filter count  : {ef.filter_count}")
    print(f"Transformer count : {ef.transformer_count}")

    assert result is not None
    assert result["source"] == "bus"
    assert result["type"] == "ALERT"
    assert dropped is None
    print("PASS")


# ===========================================================================
# Entry point
# ===========================================================================


if __name__ == "__main__":
    print("Event-Driven Workflow Patterns — Smoke Test Suite")
    print(
        "Patterns: EventBus | EventSourcingWorkflow | ChoreographySaga "
        "| DeadLetterQueue | EventFilter"
    )

    _run_event_bus_demo()
    _run_event_sourcing_demo()
    _run_choreography_saga_demo()
    _run_dlq_demo()
    _run_event_filter_demo()

    print("\n" + "=" * 60)
    print("  All scenarios passed.")
    print("=" * 60)
