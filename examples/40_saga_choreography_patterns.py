"""
40_saga_choreography_patterns.py — Event-Driven Saga Choreography Patterns

Pure-Python implementation of five saga choreography and compensation
primitives using only the standard library (``dataclasses``, ``uuid``,
``time``, ``typing``).  No external dependencies are required — no Kafka,
no RabbitMQ, no event-store libraries.

    Pattern 1 — SagaEvent:
                Immutable, correlated saga event carrier.  A ``SagaEvent``
                is a ``@dataclass(frozen=True)`` that bundles an
                ``event_id`` (UUID-generated), a ``saga_id`` (correlation
                key shared by every event in the same distributed
                transaction), an ``event_type`` label, an arbitrary
                ``payload`` dict, a wall-clock ``timestamp``, and an
                integer ``sequence`` that records the ordering of steps
                within the saga.  :meth:`create` is a class-method factory
                that auto-generates the ``event_id`` so callers never
                manipulate raw UUIDs directly.

    Pattern 2 — SagaParticipant:
                Models a single service taking part in a choreographed
                saga.  Each participant owns a *local transaction*
                (:meth:`execute`) and a *compensating transaction*
                (:meth:`compensate`).  Both methods record the triggering
                event internally and return a new ``SagaEvent`` describing
                the outcome — ``"{name}_completed"`` or
                ``"{name}_compensated"`` respectively.
                :meth:`executed_count` and :meth:`compensated_count` expose
                internal counters so monitors and tests can verify
                progress without inspecting private state.

    Pattern 3 — SagaChoreographer:
                Central event bus and saga-lifecycle manager for a
                choreography-based saga.  Participants register callables
                (handlers) keyed by ``event_type`` via
                :meth:`register_handler`.  :meth:`start_saga` creates the
                saga record with ``status="running"`` and records the
                initial event.  :meth:`publish_event` dispatches the event
                to every registered handler for its type, collects response
                events, appends them to the saga's event history, and
                returns them.  :meth:`complete_saga` / :meth:`fail_saga`
                transition the status to ``"completed"`` / ``"failed"``.
                :meth:`get_saga_status` returns the current status string
                (or ``"not_found"`` when the saga is unknown).
                :meth:`get_saga_events` returns the full ordered event
                history for a saga.

    Pattern 4 — CompensationEngine:
                Runs compensating transactions in the reverse order of
                the original executions — the canonical saga rollback
                algorithm.  Participants register via
                :meth:`register_participant`; each forward step is
                recorded with :meth:`record_execution` (storing a
                ``(participant_name, event)`` tuple).
                :meth:`compensate_all` iterates the execution log in
                reverse, locates the participant by name, invokes
                :meth:`SagaParticipant.compensate`, and accumulates the
                returned compensation events.  :meth:`compensation_count`
                reports how many compensation events have been produced
                across all :meth:`compensate_all` calls.

    Pattern 5 — SagaMonitor:
                Health-tracking and timeout-detection sidecar for a saga
                choreographer.  :meth:`record_saga_start` stamps the
                ``time.time()`` of the first event; :meth:`record_saga_end`
                stamps completion and records the final status string.
                :meth:`is_timed_out` returns ``True`` when a saga has
                started but not ended and the elapsed time exceeds
                ``timeout_seconds``.  :meth:`get_stats` returns a summary
                dict with ``total``, ``completed``, ``failed``, ``running``,
                and ``timed_out`` counts.  :meth:`get_timed_out_sagas`
                returns the list of saga-ids that have exceeded the timeout.

Usage example
-------------
::

    # Build participants
    order_svc = SagaParticipant("order_service")
    payment_svc = SagaParticipant("payment_service")
    inventory_svc = SagaParticipant("inventory_service")

    # Wire choreographer
    choreographer = SagaChoreographer()
    choreographer.register_handler("order_placed", order_svc.execute)
    choreographer.register_handler("order_placed", payment_svc.execute)

    # Start saga
    saga_id = "saga-001"
    initial = SagaEvent.create(saga_id, "order_placed", {"order_id": "O-42"})
    choreographer.start_saga(saga_id, initial)
    responses = choreographer.publish_event(initial)

    # Monitor
    monitor = SagaMonitor(timeout_seconds=60.0)
    monitor.record_saga_start(saga_id)

    if all_responses_ok(responses):
        choreographer.complete_saga(saga_id)
        monitor.record_saga_end(saga_id, "completed")
    else:
        engine = CompensationEngine()
        engine.register_participant(order_svc)
        engine.register_participant(payment_svc)
        engine.record_execution("order_service", initial)
        comp_events = engine.compensate_all()
        choreographer.fail_saga(saga_id)
        monitor.record_saga_end(saga_id, "failed")
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Any

# ---------------------------------------------------------------------------
# Pattern 1 — SagaEvent
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SagaEvent:
    """Immutable, correlated saga event carrier.

    Parameters
    ----------
    event_id:
        Unique identifier for this specific event instance (UUID-format string).
    saga_id:
        Correlation key shared by every event that belongs to the same
        distributed saga transaction.
    event_type:
        A label describing the business event, e.g. ``"order_placed"`` or
        ``"payment_completed"``.
    payload:
        Arbitrary key-value data carried by the event.  Must be serialisable
        to a plain dict; the field is *not* deepcopied — callers should pass
        immutable or freshly-created dicts.
    timestamp:
        Wall-clock creation time (``time.time()``).  Defaults to the current
        time at construction.
    sequence:
        Zero-based ordinal position of this event within the saga.  Useful
        for ordering and replay.
    """

    event_id: str
    saga_id: str
    event_type: str
    payload: dict[str, Any]
    timestamp: float = field(default_factory=time.time)
    sequence: int = 0

    @classmethod
    def create(
        cls,
        saga_id: str,
        event_type: str,
        payload: dict[str, Any],
        sequence: int = 0,
    ) -> SagaEvent:
        """Factory method that auto-generates a UUID ``event_id``.

        Parameters
        ----------
        saga_id:
            Correlation identifier for the owning saga.
        event_type:
            Business event label.
        payload:
            Arbitrary event data.
        sequence:
            Optional step ordinal (default ``0``).

        Returns
        -------
        SagaEvent
            A new, fully-populated, immutable event.
        """
        return cls(
            event_id=str(uuid.uuid4()),
            saga_id=saga_id,
            event_type=event_type,
            payload=payload,
            sequence=sequence,
        )


# ---------------------------------------------------------------------------
# Pattern 2 — SagaParticipant
# ---------------------------------------------------------------------------


class SagaParticipant:
    """Saga participant with a local transaction and a compensating transaction.

    Parameters
    ----------
    name:
        Human-readable service name, used as a prefix in outcome event types
        (e.g. ``"payment_service"`` → ``"payment_service_completed"``).
    """

    def __init__(self, name: str) -> None:
        self.name: str = name
        self._executed: list[SagaEvent] = []
        self._compensated: list[SagaEvent] = []

    # ------------------------------------------------------------------
    # Forward transaction
    # ------------------------------------------------------------------

    def execute(self, event: SagaEvent) -> SagaEvent:
        """Run the local transaction for *event*.

        Records *event* in the internal execution log and returns a new
        ``SagaEvent`` with type ``"{name}_completed"`` and a payload that
        references the original ``event_id``.

        Parameters
        ----------
        event:
            The triggering saga event.

        Returns
        -------
        SagaEvent
            Outcome event signalling successful local transaction completion.
        """
        self._executed.append(event)
        return SagaEvent.create(
            saga_id=event.saga_id,
            event_type=f"{self.name}_completed",
            payload={"original": event.event_id},
            sequence=event.sequence,
        )

    # ------------------------------------------------------------------
    # Compensating transaction
    # ------------------------------------------------------------------

    def compensate(self, event: SagaEvent) -> SagaEvent:
        """Run the compensating transaction for *event*.

        Records *event* in the internal compensation log and returns a new
        ``SagaEvent`` with type ``"{name}_compensated"`` and a payload that
        references the original ``event_id``.

        Parameters
        ----------
        event:
            The event whose effects must be undone.

        Returns
        -------
        SagaEvent
            Outcome event signalling successful compensation.
        """
        self._compensated.append(event)
        return SagaEvent.create(
            saga_id=event.saga_id,
            event_type=f"{self.name}_compensated",
            payload={"original": event.event_id},
            sequence=event.sequence,
        )

    # ------------------------------------------------------------------
    # Counters
    # ------------------------------------------------------------------

    def executed_count(self) -> int:
        """Return the number of events processed by :meth:`execute`."""
        return len(self._executed)

    def compensated_count(self) -> int:
        """Return the number of events processed by :meth:`compensate`."""
        return len(self._compensated)


# ---------------------------------------------------------------------------
# Pattern 3 — SagaChoreographer
# ---------------------------------------------------------------------------


class SagaChoreographer:
    """Central event bus and saga-lifecycle manager.

    Maintains a registry of event-type → handler callables and an in-memory
    store of saga records.  All saga records are plain dicts with keys:
    ``"status"``, ``"events"`` (ordered list of :class:`SagaEvent`), and
    ``"participants"`` (reserved for future extension).
    """

    def __init__(self) -> None:
        self._sagas: dict[str, dict[str, Any]] = {}
        self._handlers: dict[str, list[Any]] = {}

    # ------------------------------------------------------------------
    # Handler registration
    # ------------------------------------------------------------------

    def register_handler(self, event_type: str, handler: Any) -> None:
        """Register a callable *handler* to be invoked for *event_type*.

        Multiple handlers may be registered for the same event type; they
        are called in registration order.

        Parameters
        ----------
        event_type:
            The business event label that triggers the handler.
        handler:
            A callable that accepts a single :class:`SagaEvent` argument and
            returns a :class:`SagaEvent` response.
        """
        self._handlers.setdefault(event_type, []).append(handler)

    # ------------------------------------------------------------------
    # Saga lifecycle
    # ------------------------------------------------------------------

    def start_saga(self, saga_id: str, initial_event: SagaEvent) -> None:
        """Create a new saga record seeded with *initial_event*.

        Parameters
        ----------
        saga_id:
            Unique correlation key for the saga.
        initial_event:
            The first event that triggered the distributed transaction.
        """
        self._sagas[saga_id] = {
            "status": "running",
            "events": [initial_event],
            "participants": [],
        }

    def publish_event(self, event: SagaEvent) -> list[SagaEvent]:
        """Dispatch *event* to all registered handlers and collect responses.

        For each handler registered for ``event.event_type``, the handler
        is called with *event* and the returned :class:`SagaEvent` is
        collected.  All response events are appended to the saga's event
        history.

        Parameters
        ----------
        event:
            The event to dispatch.

        Returns
        -------
        list[SagaEvent]
            All response events returned by the registered handlers.
        """
        responses: list[SagaEvent] = []
        handlers = self._handlers.get(event.event_type, [])
        for handler in handlers:
            response = handler(event)
            responses.append(response)

        if event.saga_id in self._sagas:
            self._sagas[event.saga_id]["events"].extend(responses)

        return responses

    def complete_saga(self, saga_id: str) -> None:
        """Transition *saga_id* to ``"completed"`` status.

        Parameters
        ----------
        saga_id:
            The saga to finalise successfully.
        """
        if saga_id in self._sagas:
            self._sagas[saga_id]["status"] = "completed"

    def fail_saga(self, saga_id: str) -> None:
        """Transition *saga_id* to ``"failed"`` status.

        Parameters
        ----------
        saga_id:
            The saga to mark as failed.
        """
        if saga_id in self._sagas:
            self._sagas[saga_id]["status"] = "failed"

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def get_saga_status(self, saga_id: str) -> str:
        """Return the current status string for *saga_id*.

        Returns
        -------
        str
            One of ``"running"``, ``"completed"``, ``"failed"``, or
            ``"not_found"`` when no saga with *saga_id* exists.
        """
        saga = self._sagas.get(saga_id)
        if saga is None:
            return "not_found"
        return saga["status"]  # type: ignore[return-value]

    def get_saga_events(self, saga_id: str) -> list[SagaEvent]:
        """Return the ordered event history for *saga_id*.

        Parameters
        ----------
        saga_id:
            The saga whose history to retrieve.

        Returns
        -------
        list[SagaEvent]
            Chronological list of all events recorded for this saga, including
            the initial event and all handler-response events.  Returns an empty
            list when the saga is not found.
        """
        saga = self._sagas.get(saga_id)
        if saga is None:
            return []
        return list(saga["events"])


# ---------------------------------------------------------------------------
# Pattern 4 — CompensationEngine
# ---------------------------------------------------------------------------


class CompensationEngine:
    """Runs compensating transactions in reverse execution order.

    The canonical saga rollback algorithm requires that each step's
    compensation runs in the *reverse* order of the original forward
    steps.  :class:`CompensationEngine` maintains two lists — a registry
    of known :class:`SagaParticipant` instances and an ordered log of
    ``(participant_name, event)`` tuples — and implements this reversal
    in :meth:`compensate_all`.
    """

    def __init__(self) -> None:
        self._participants: list[SagaParticipant] = []
        self._executed_events: list[tuple[str, SagaEvent]] = []
        self._total_compensations: int = 0

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register_participant(self, participant: SagaParticipant) -> None:
        """Register *participant* so it can be looked up by name during rollback.

        Parameters
        ----------
        participant:
            The :class:`SagaParticipant` to register.
        """
        self._participants.append(participant)

    def record_execution(self, participant_name: str, event: SagaEvent) -> None:
        """Record that *participant_name* successfully executed *event*.

        This entry will be replayed (in reverse) by :meth:`compensate_all`.

        Parameters
        ----------
        participant_name:
            The :attr:`SagaParticipant.name` that ran the forward step.
        event:
            The event that was passed to :meth:`SagaParticipant.execute`.
        """
        self._executed_events.append((participant_name, event))

    # ------------------------------------------------------------------
    # Compensation
    # ------------------------------------------------------------------

    def compensate_all(self) -> list[SagaEvent]:
        """Run all compensating transactions in reverse-execution order.

        Iterates ``_executed_events`` from last to first, looks up the
        participant by name, calls :meth:`SagaParticipant.compensate`, and
        accumulates the returned :class:`SagaEvent` objects.

        Returns
        -------
        list[SagaEvent]
            All compensation events produced, in compensation order (i.e.
            reverse of the original forward order).
        """
        compensation_events: list[SagaEvent] = []
        for participant_name, event in reversed(self._executed_events):
            participant = self._find_participant(participant_name)
            if participant is not None:
                comp_event = participant.compensate(event)
                compensation_events.append(comp_event)
                self._total_compensations += 1
        return compensation_events

    def compensation_count(self) -> int:
        """Return the total number of compensation events produced so far.

        The counter accumulates across multiple :meth:`compensate_all`
        calls.
        """
        return self._total_compensations

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _find_participant(self, name: str) -> SagaParticipant | None:
        """Return the registered participant with *name*, or ``None``."""
        for p in self._participants:
            if p.name == name:
                return p
        return None


# ---------------------------------------------------------------------------
# Pattern 5 — SagaMonitor
# ---------------------------------------------------------------------------


class SagaMonitor:
    """Saga health tracking and timeout detection.

    Tracks the start and end times of sagas and classifies them as
    ``running``, ``completed``, ``failed``, or ``timed_out``.

    Parameters
    ----------
    timeout_seconds:
        Maximum allowed duration for a saga before it is considered
        timed out.  Defaults to ``30.0`` seconds.
    """

    def __init__(self, timeout_seconds: float = 30.0) -> None:
        self.timeout_seconds: float = timeout_seconds
        # saga_id → {"start": float, "end": float | None, "status": str | None}
        self._records: dict[str, dict[str, Any]] = {}

    # ------------------------------------------------------------------
    # Lifecycle recording
    # ------------------------------------------------------------------

    def record_saga_start(self, saga_id: str) -> None:
        """Record the wall-clock start time for *saga_id*.

        Parameters
        ----------
        saga_id:
            The saga that has just begun execution.
        """
        self._records[saga_id] = {
            "start": time.time(),
            "end": None,
            "status": None,
        }

    def record_saga_end(self, saga_id: str, status: str) -> None:
        """Record the wall-clock end time and final *status* for *saga_id*.

        Parameters
        ----------
        saga_id:
            The saga that has finished.
        status:
            Final outcome — typically ``"completed"`` or ``"failed"``.
        """
        if saga_id in self._records:
            self._records[saga_id]["end"] = time.time()
            self._records[saga_id]["status"] = status
        else:
            # Record end even if start was not explicitly recorded
            self._records[saga_id] = {
                "start": time.time(),
                "end": time.time(),
                "status": status,
            }

    # ------------------------------------------------------------------
    # Timeout detection
    # ------------------------------------------------------------------

    def is_timed_out(self, saga_id: str) -> bool:
        """Return ``True`` when *saga_id* has exceeded ``timeout_seconds``.

        A saga is considered timed out when:
        * It has a recorded start time.
        * It has **not** ended (``end`` is ``None``).
        * The elapsed time since ``start`` exceeds ``timeout_seconds``.

        Parameters
        ----------
        saga_id:
            The saga to inspect.
        """
        record = self._records.get(saga_id)
        if record is None:
            return False
        if record["end"] is not None:
            return False
        elapsed = time.time() - record["start"]
        return elapsed > self.timeout_seconds

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------

    def get_stats(self) -> dict[str, int]:
        """Return a summary dict of saga states.

        Returns
        -------
        dict
            Keys: ``"total"``, ``"completed"``, ``"failed"``, ``"running"``,
            ``"timed_out"``.  ``"running"`` counts sagas that are active but
            not yet timed out; ``"timed_out"`` counts sagas that are running
            *and* have exceeded the timeout threshold.
        """
        total = len(self._records)
        completed = 0
        failed = 0
        running = 0
        timed_out = 0

        for saga_id, record in self._records.items():
            if record["end"] is not None:
                if record["status"] == "completed":
                    completed += 1
                else:
                    failed += 1
            else:
                if self.is_timed_out(saga_id):
                    timed_out += 1
                else:
                    running += 1

        return {
            "total": total,
            "completed": completed,
            "failed": failed,
            "running": running,
            "timed_out": timed_out,
        }

    def get_timed_out_sagas(self) -> list[str]:
        """Return a list of saga-ids that have timed out.

        A saga is included when :meth:`is_timed_out` returns ``True`` for it.

        Returns
        -------
        list[str]
            Saga-ids that have exceeded ``timeout_seconds`` without ending.
        """
        return [saga_id for saga_id in self._records if self.is_timed_out(saga_id)]
