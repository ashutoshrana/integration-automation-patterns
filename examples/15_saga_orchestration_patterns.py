"""
15_saga_orchestration_patterns.py — Distributed saga patterns for
microservices: orchestration-based, choreography-based, transactional
outbox, and dead-letter queue.

Demonstrates four complementary patterns that together provide reliable
distributed transaction semantics without two-phase commit:

    Pattern 1 — Orchestration-Based Saga:
                A central coordinator (SagaOrchestrator) executes a sequence
                of local transactions (SagaSteps). If any step fails, the
                orchestrator triggers compensating transactions in reverse
                order to undo the completed work. Each step is explicit,
                auditable, and independent.

    Pattern 2 — Choreography-Based Saga:
                Services react to domain events emitted by other services
                without a central coordinator. Each service subscribes to
                the events it cares about and emits events when its local
                transaction completes (or fails). The ChoreographyEventBus
                provides pub/sub with thread-safe in-memory delivery.

    Pattern 3 — Transactional Outbox:
                Instead of sending messages directly after a database write
                (which risks inconsistency if the message broker is
                unavailable), the service writes the message to an outbox
                table in the same local transaction. The OutboxPoller reads
                PENDING messages and publishes them to the message broker,
                marking each as PUBLISHED on success.

    Pattern 4 — Dead Letter Queue:
                Messages that fail processing after max_retries are moved to
                a dead-letter queue (DLQ) for inspection, manual replay, or
                discard. The DeadLetterQueue tracks failure reasons, retry
                attempts, and provides safe replay and inspection APIs.

Usage context
-------------
These four patterns are typically used together in practice:

- Use orchestration sagas for complex multi-service workflows where you need
  centralized visibility and rollback control (e.g., order fulfillment
  spanning inventory, payment, and shipping services).

- Use choreography sagas for loosely-coupled event flows where services
  should not know about each other (e.g., analytics pipelines that react
  to domain events without being part of the saga).

- Use transactional outbox to guarantee at-least-once delivery of domain
  events from any service that also writes to a local database.

- Use DLQ to handle poison messages and enable operator replay without
  losing data.

No external dependencies required.

Run:
    python examples/15_saga_orchestration_patterns.py
"""

from __future__ import annotations

import threading
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional


# ---------------------------------------------------------------------------
# Pattern 1 — Orchestration-Based Saga
# ---------------------------------------------------------------------------


class SagaStatus(str, Enum):
    """Lifecycle state of a saga execution."""

    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    COMPENSATING = "COMPENSATING"
    COMPENSATED = "COMPENSATED"
    FAILED = "FAILED"              # Compensation also failed


class SagaStepStatus(str, Enum):
    """Outcome of a single saga step."""

    PENDING = "PENDING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    COMPENSATED = "COMPENSATED"
    COMPENSATION_FAILED = "COMPENSATION_FAILED"


@dataclass
class SagaStep:
    """
    A single step in an orchestration-based saga.

    Each step has an ``action`` callable that performs the local transaction
    and a ``compensate`` callable that undoes it. Both receive the shared
    saga context dict, which is mutated in place to pass outputs between steps.

    Parameters
    ----------
    name : str
        Human-readable step name for logging and audit.
    action : Callable[[dict], None]
        Performs the local transaction. Should raise on failure.
    compensate : Callable[[dict], None]
        Undoes the local transaction. Called only if a later step fails.
        Should be idempotent — it may be called more than once.
    """

    name: str
    action: Callable[[dict], None]
    compensate: Callable[[dict], None]
    status: SagaStepStatus = SagaStepStatus.PENDING
    error: Optional[str] = None


@dataclass
class SagaExecution:
    """
    Record of a saga execution, including per-step outcomes and the saga context.
    """

    saga_id: str
    steps: List[SagaStep]
    status: SagaStatus = SagaStatus.RUNNING
    context: Dict[str, Any] = field(default_factory=dict)
    completed_steps: List[str] = field(default_factory=list)
    failure_step: Optional[str] = None
    failure_reason: Optional[str] = None


class SagaOrchestrator:
    """
    Orchestration-based saga coordinator.

    Executes each SagaStep in order. On any failure:
      1. Records the failing step and reason.
      2. Sets status to COMPENSATING.
      3. Calls ``compensate()`` on completed steps in reverse order.
      4. Sets status to COMPENSATED if all compensations succeed,
         or FAILED if any compensation also fails.

    Example
    -------
    >>> def reserve_inventory(ctx):
    ...     ctx["reservation_id"] = "RES-001"
    ...
    >>> def release_inventory(ctx):
    ...     del ctx["reservation_id"]
    ...
    >>> def charge_payment(ctx):
    ...     raise ValueError("Card declined")
    ...
    >>> def refund_payment(ctx):
    ...     pass
    ...
    >>> orchestrator = SagaOrchestrator()
    >>> execution = orchestrator.execute(
    ...     steps=[
    ...         SagaStep("reserve", reserve_inventory, release_inventory),
    ...         SagaStep("payment", charge_payment, refund_payment),
    ...     ],
    ...     initial_context={"order_id": "ORD-42"},
    ... )
    >>> execution.status
    <SagaStatus.COMPENSATED: 'COMPENSATED'>
    """

    def execute(
        self,
        steps: List[SagaStep],
        initial_context: Optional[dict] = None,
    ) -> SagaExecution:
        execution = SagaExecution(
            saga_id=str(uuid.uuid4()),
            steps=steps,
            context=dict(initial_context or {}),
        )

        completed_indices: list[int] = []

        for i, step in enumerate(steps):
            try:
                step.action(execution.context)
                step.status = SagaStepStatus.COMPLETED
                execution.completed_steps.append(step.name)
                completed_indices.append(i)
            except Exception as exc:
                step.status = SagaStepStatus.FAILED
                step.error = str(exc)
                execution.failure_step = step.name
                execution.failure_reason = str(exc)
                execution.status = SagaStatus.COMPENSATING

                # Compensate in reverse order
                compensation_failed = False
                for j in reversed(completed_indices):
                    comp_step = steps[j]
                    try:
                        comp_step.compensate(execution.context)
                        comp_step.status = SagaStepStatus.COMPENSATED
                    except Exception as comp_exc:
                        comp_step.status = SagaStepStatus.COMPENSATION_FAILED
                        comp_step.error = str(comp_exc)
                        compensation_failed = True

                execution.status = (
                    SagaStatus.FAILED if compensation_failed else SagaStatus.COMPENSATED
                )
                return execution

        execution.status = SagaStatus.COMPLETED
        return execution


# ---------------------------------------------------------------------------
# Pattern 2 — Choreography-Based Saga
# ---------------------------------------------------------------------------


@dataclass
class ChoreographyEvent:
    """
    A domain event emitted by a service in a choreography-based saga.

    Parameters
    ----------
    event_id : str
        Unique event identifier (UUID recommended for deduplication).
    event_type : str
        Dot-separated event type (e.g., ``order.created``, ``payment.failed``).
    aggregate_id : str
        Identifier of the aggregate that produced the event.
    payload : dict
        Event-specific data. Subscribers receive this dict.
    timestamp : float
        Unix timestamp of event creation.
    """

    event_id: str
    event_type: str
    aggregate_id: str
    payload: Dict[str, Any]
    timestamp: float = field(default_factory=time.time)


class ChoreographyEventBus:
    """
    In-process pub/sub event bus for choreography-based sagas.

    Thread-safe. Handlers are called synchronously in the publishing thread.
    Exceptions in handlers are caught and do not affect other handlers or
    the publishing service.

    In production, replace this with a durable message broker (Kafka, SNS,
    RabbitMQ). The interface — emit, subscribe, unsubscribe — remains the same.

    Example
    -------
    >>> bus = ChoreographyEventBus()
    >>> received = []
    >>> bus.subscribe("order.created", lambda e: received.append(e.aggregate_id))
    >>> bus.emit(ChoreographyEvent(
    ...     event_id=str(uuid.uuid4()),
    ...     event_type="order.created",
    ...     aggregate_id="ORD-99",
    ...     payload={"total": 250.0},
    ... ))
    >>> received
    ['ORD-99']
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._handlers: Dict[str, List[Callable[[ChoreographyEvent], None]]] = (
            defaultdict(list)
        )
        self._published: List[ChoreographyEvent] = []

    def subscribe(
        self,
        event_type: str,
        handler: Callable[[ChoreographyEvent], None],
    ) -> None:
        """Register ``handler`` for events of type ``event_type``."""
        with self._lock:
            self._handlers[event_type].append(handler)

    def unsubscribe(
        self,
        event_type: str,
        handler: Callable[[ChoreographyEvent], None],
    ) -> None:
        """Remove ``handler`` from ``event_type`` subscriptions."""
        with self._lock:
            try:
                self._handlers[event_type].remove(handler)
            except ValueError:
                pass

    def emit(self, event: ChoreographyEvent) -> None:
        """
        Publish ``event`` to all registered handlers for its event_type.

        Exceptions raised by individual handlers are caught and do not
        prevent other handlers from being called.
        """
        with self._lock:
            handlers = list(self._handlers.get(event.event_type, []))
            self._published.append(event)

        for handler in handlers:
            try:
                handler(event)
            except Exception:
                pass

    @property
    def published_events(self) -> List[ChoreographyEvent]:
        """Return all events published via this bus (for testing)."""
        with self._lock:
            return list(self._published)


# ---------------------------------------------------------------------------
# Pattern 3 — Transactional Outbox
# ---------------------------------------------------------------------------


class OutboxMessageStatus(str, Enum):
    """Lifecycle status of a transactional outbox message."""

    PENDING = "PENDING"
    PUBLISHED = "PUBLISHED"
    FAILED = "FAILED"


@dataclass
class OutboxMessage:
    """
    A message stored in the transactional outbox.

    The message is written in the same local database transaction as the
    domain model update, guaranteeing that the message exists if and only
    if the domain update committed.

    Parameters
    ----------
    message_id : str
        Unique message identifier.
    aggregate_type : str
        Type of the aggregate that produced this message (e.g., ``Order``).
    aggregate_id : str
        Identifier of the aggregate instance.
    event_type : str
        Dot-separated event type (e.g., ``order.created``).
    payload : dict
        Event payload for downstream consumers.
    created_at : float
        Unix timestamp of creation.
    """

    message_id: str
    aggregate_type: str
    aggregate_id: str
    event_type: str
    payload: Dict[str, Any]
    status: OutboxMessageStatus = OutboxMessageStatus.PENDING
    created_at: float = field(default_factory=time.time)
    published_at: Optional[float] = None
    retry_count: int = 0


class OutboxStore:
    """
    In-memory transactional outbox store (replace with a database table).

    The ``save`` method simulates writing the outbox message in the same
    transaction as the domain model. In production, both writes use the
    same database connection / transaction.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._messages: Dict[str, OutboxMessage] = {}

    def save(self, message: OutboxMessage) -> None:
        """Persist an outbox message (called inside the domain transaction)."""
        with self._lock:
            self._messages[message.message_id] = message

    def get_pending(self, batch_size: int = 10) -> List[OutboxMessage]:
        """Return up to ``batch_size`` PENDING messages ordered by creation time."""
        with self._lock:
            pending = [
                m for m in self._messages.values()
                if m.status == OutboxMessageStatus.PENDING
            ]
            pending.sort(key=lambda m: m.created_at)
            return pending[:batch_size]

    def mark_published(self, message_id: str) -> None:
        with self._lock:
            msg = self._messages.get(message_id)
            if msg:
                msg.status = OutboxMessageStatus.PUBLISHED
                msg.published_at = time.time()

    def mark_failed(self, message_id: str) -> None:
        with self._lock:
            msg = self._messages.get(message_id)
            if msg:
                msg.status = OutboxMessageStatus.FAILED
                msg.retry_count += 1

    def all_messages(self) -> List[OutboxMessage]:
        with self._lock:
            return list(self._messages.values())


class OutboxPoller:
    """
    Background poller that reads PENDING outbox messages and publishes them.

    In production this is a separate process or scheduled job. The poller
    implements the at-least-once delivery guarantee: a message may be
    published more than once if the poller crashes after publishing but
    before marking it as PUBLISHED. Downstream consumers must be idempotent.

    Parameters
    ----------
    store : OutboxStore
        The transactional outbox store to poll.
    publisher : Callable[[OutboxMessage], None]
        The broker publish function. Should raise on failure.
    batch_size : int
        Number of messages to process per poll cycle.
    max_retries : int
        Number of publish attempts before marking a message as FAILED.
    """

    def __init__(
        self,
        store: OutboxStore,
        publisher: Callable[[OutboxMessage], None],
        batch_size: int = 10,
        max_retries: int = 3,
    ) -> None:
        self._store = store
        self._publisher = publisher
        self._batch_size = batch_size
        self._max_retries = max_retries

    def poll_once(self) -> int:
        """
        Process one batch of PENDING messages.

        Returns the number of successfully published messages.
        """
        messages = self._store.get_pending(self._batch_size)
        published_count = 0

        for msg in messages:
            if msg.retry_count >= self._max_retries:
                self._store.mark_failed(msg.message_id)
                continue
            try:
                self._publisher(msg)
                self._store.mark_published(msg.message_id)
                published_count += 1
            except Exception:
                self._store.mark_failed(msg.message_id)

        return published_count


# ---------------------------------------------------------------------------
# Pattern 4 — Dead Letter Queue
# ---------------------------------------------------------------------------


class DLQMessageStatus(str, Enum):
    """Lifecycle status of a dead-letter queue message."""

    DEAD = "DEAD"
    REPLAYED = "REPLAYED"
    DISCARDED = "DISCARDED"


@dataclass
class DLQMessage:
    """
    A message that has been moved to the dead-letter queue after exhausting
    retry attempts.

    Parameters
    ----------
    dlq_id : str
        Unique DLQ entry identifier.
    original_message_id : str
        Identifier of the original message that failed processing.
    queue_name : str
        Name of the source queue (for routing replayed messages).
    payload : dict
        Original message payload.
    failure_reason : str
        Most recent failure exception message.
    failed_at : float
        Unix timestamp when the message was moved to the DLQ.
    attempt_count : int
        Total number of processing attempts before DLQ placement.
    """

    dlq_id: str
    original_message_id: str
    queue_name: str
    payload: Dict[str, Any]
    failure_reason: str
    failed_at: float = field(default_factory=time.time)
    attempt_count: int = 0
    status: DLQMessageStatus = DLQMessageStatus.DEAD
    replayed_at: Optional[float] = None


class DeadLetterQueue:
    """
    Dead-letter queue for messages that have exhausted retry attempts.

    Supports:
    - ``enqueue`` — move a failed message into the DLQ
    - ``replay`` — re-process a specific DLQ message through the provided handler
    - ``discard`` — mark a message as discarded (will not be replayed)
    - ``list_messages`` — inspect all messages (or filter by status/queue)
    - ``size`` — number of DEAD (unresolved) messages

    Example
    -------
    >>> dlq = DeadLetterQueue()
    >>> dlq.enqueue(DLQMessage(
    ...     dlq_id=str(uuid.uuid4()),
    ...     original_message_id="MSG-001",
    ...     queue_name="payments",
    ...     payload={"amount": 100.0},
    ...     failure_reason="Connection timeout",
    ...     attempt_count=3,
    ... ))
    >>> dlq.size()
    1
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._messages: Dict[str, DLQMessage] = {}

    def enqueue(self, message: DLQMessage) -> None:
        """Add a failed message to the DLQ."""
        with self._lock:
            self._messages[message.dlq_id] = message

    def replay(
        self,
        dlq_id: str,
        handler: Callable[[DLQMessage], None],
    ) -> bool:
        """
        Re-process a DEAD message through ``handler``.

        Returns True if the replay succeeded and marks the message as REPLAYED.
        Returns False if the message is not found, not DEAD, or the handler raises.
        """
        with self._lock:
            msg = self._messages.get(dlq_id)
            if msg is None or msg.status != DLQMessageStatus.DEAD:
                return False

        try:
            handler(msg)
        except Exception:
            return False

        with self._lock:
            msg = self._messages.get(dlq_id)
            if msg and msg.status == DLQMessageStatus.DEAD:
                msg.status = DLQMessageStatus.REPLAYED
                msg.replayed_at = time.time()
        return True

    def discard(self, dlq_id: str) -> bool:
        """
        Mark a DEAD message as discarded (operator decision — no retry).

        Returns True if the message was found and marked discarded.
        """
        with self._lock:
            msg = self._messages.get(dlq_id)
            if msg and msg.status == DLQMessageStatus.DEAD:
                msg.status = DLQMessageStatus.DISCARDED
                return True
            return False

    def list_messages(
        self,
        status: Optional[DLQMessageStatus] = None,
        queue_name: Optional[str] = None,
    ) -> List[DLQMessage]:
        """Return DLQ messages, optionally filtered by status and/or queue."""
        with self._lock:
            msgs = list(self._messages.values())
        if status is not None:
            msgs = [m for m in msgs if m.status == status]
        if queue_name is not None:
            msgs = [m for m in msgs if m.queue_name == queue_name]
        return msgs

    def size(self, status: Optional[DLQMessageStatus] = None) -> int:
        """Return the count of messages, optionally filtered by status."""
        return len(self.list_messages(status=status))


# ---------------------------------------------------------------------------
# Scenario demonstrations
# ---------------------------------------------------------------------------


def scenario_orchestration_success() -> None:
    """All saga steps succeed — saga reaches COMPLETED."""
    print("\n--- Orchestration Saga: All Steps Succeed ---")
    log: list[str] = []

    orchestrator = SagaOrchestrator()
    steps = [
        SagaStep(
            name="reserve_inventory",
            action=lambda ctx: log.append("reserved") or ctx.update({"reserved": True}),
            compensate=lambda ctx: log.append("released_inventory"),
        ),
        SagaStep(
            name="charge_payment",
            action=lambda ctx: log.append("charged") or ctx.update({"charged": True}),
            compensate=lambda ctx: log.append("refunded"),
        ),
        SagaStep(
            name="create_shipment",
            action=lambda ctx: log.append("shipped") or ctx.update({"shipped": True}),
            compensate=lambda ctx: log.append("cancelled_shipment"),
        ),
    ]
    result = orchestrator.execute(steps, initial_context={"order_id": "ORD-001"})
    print(f"  Status: {result.status.value}")
    print(f"  Log: {log}")


def scenario_orchestration_compensation() -> None:
    """Payment step fails — inventory reservation is compensated."""
    print("\n--- Orchestration Saga: Payment Fails → Inventory Compensated ---")
    log: list[str] = []

    orchestrator = SagaOrchestrator()
    steps = [
        SagaStep(
            name="reserve_inventory",
            action=lambda ctx: log.append("reserved") or ctx.update({"reserved": True}),
            compensate=lambda ctx: log.append("released_inventory"),
        ),
        SagaStep(
            name="charge_payment",
            action=lambda ctx: (_ for _ in ()).throw(ValueError("Card declined")),
            compensate=lambda ctx: log.append("refunded"),
        ),
    ]
    result = orchestrator.execute(steps, initial_context={"order_id": "ORD-002"})
    print(f"  Status: {result.status.value}")
    print(f"  Failure: {result.failure_reason}")
    print(f"  Log: {log}")


def scenario_choreography() -> None:
    """Event bus delivers events to correct subscribers."""
    print("\n--- Choreography Saga: Event Bus Pub/Sub ---")
    received_by_inventory: list[str] = []
    received_by_analytics: list[str] = []

    bus = ChoreographyEventBus()
    bus.subscribe("order.created", lambda e: received_by_inventory.append(e.aggregate_id))
    bus.subscribe("order.created", lambda e: received_by_analytics.append(e.aggregate_id))
    bus.subscribe("payment.failed", lambda e: received_by_inventory.append(f"cancel:{e.aggregate_id}"))

    bus.emit(ChoreographyEvent(
        event_id=str(uuid.uuid4()),
        event_type="order.created",
        aggregate_id="ORD-003",
        payload={"total": 150.0},
    ))
    bus.emit(ChoreographyEvent(
        event_id=str(uuid.uuid4()),
        event_type="payment.failed",
        aggregate_id="ORD-003",
        payload={"reason": "NSF"},
    ))

    print(f"  Inventory received: {received_by_inventory}")
    print(f"  Analytics received: {received_by_analytics}")
    print(f"  Total events published: {len(bus.published_events)}")


def scenario_transactional_outbox() -> None:
    """Outbox poller publishes messages and marks them PUBLISHED."""
    print("\n--- Transactional Outbox: Poll and Publish ---")
    published_ids: list[str] = []
    store = OutboxStore()

    for i in range(3):
        msg = OutboxMessage(
            message_id=f"MSG-{i:03d}",
            aggregate_type="Order",
            aggregate_id=f"ORD-{i:03d}",
            event_type="order.created",
            payload={"index": i},
        )
        store.save(msg)

    def publisher(msg: OutboxMessage) -> None:
        published_ids.append(msg.message_id)

    poller = OutboxPoller(store, publisher, batch_size=10)
    count = poller.poll_once()
    print(f"  Published: {count} messages")
    print(f"  IDs: {published_ids}")
    published = [m for m in store.all_messages() if m.status == OutboxMessageStatus.PUBLISHED]
    print(f"  Marked PUBLISHED in store: {len(published)}")


def scenario_dead_letter_queue() -> None:
    """DLQ: enqueue, inspect, replay, discard."""
    print("\n--- Dead Letter Queue: Enqueue, Replay, Discard ---")
    replayed: list[str] = []
    dlq = DeadLetterQueue()

    for i in range(3):
        dlq.enqueue(DLQMessage(
            dlq_id=f"DLQ-{i:03d}",
            original_message_id=f"MSG-{i:03d}",
            queue_name="payments",
            payload={"attempt": i},
            failure_reason="Timeout",
            attempt_count=3,
        ))

    print(f"  DLQ size (DEAD): {dlq.size(DLQMessageStatus.DEAD)}")

    # Replay DLQ-000
    dlq.replay("DLQ-000", lambda m: replayed.append(m.dlq_id))
    print(f"  Replayed: {replayed}")

    # Discard DLQ-001
    dlq.discard("DLQ-001")

    print(f"  DEAD remaining: {dlq.size(DLQMessageStatus.DEAD)}")
    print(f"  REPLAYED: {dlq.size(DLQMessageStatus.REPLAYED)}")
    print(f"  DISCARDED: {dlq.size(DLQMessageStatus.DISCARDED)}")


if __name__ == "__main__":
    scenario_orchestration_success()
    scenario_orchestration_compensation()
    scenario_choreography()
    scenario_transactional_outbox()
    scenario_dead_letter_queue()
    print("\nAll scenarios complete.")
