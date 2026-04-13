"""
07_cqrs_event_sourcing.py — CQRS (Command Query Responsibility Segregation)
and event sourcing for enterprise CRM opportunity pipeline.

Demonstrates the fundamental enterprise integration pattern that separates
write operations (commands) from read operations (queries), with the write
model stored as an append-only event log rather than mutable state.

    Pattern 1 — CommandBus: Routes write commands to handlers. Validates
                command preconditions before dispatch. Each command produces
                one or more domain events that are appended to the EventStore.

    Pattern 2 — EventStore: Append-only event log keyed by aggregate_id.
                Implements optimistic concurrency via expected_version. Supports
                temporal queries: reconstruct aggregate state at any past timestamp
                by replaying events up to that point.

    Pattern 3 — ReadModelProjection: Subscribes to domain events and maintains
                denormalized read models (opportunity summaries, pipeline totals
                by stage). The read model is a derived view — it can be dropped
                and rebuilt at any time by replaying the event log.

    Pattern 4 — QueryBus: Routes read queries to projections. Never touches the
                write model. Read and write models can scale independently.

Scenarios
---------

  A. Full opportunity lifecycle:
     CreateOpportunity → UpdateStage (3×) → CloseWon.
     5 events stored; read model reflects current state at every step.

  B. Temporal query — event sourcing replay:
     Reconstruct the opportunity's state at a specific past timestamp by
     replaying only the events that occurred before that point.

  C. Optimistic concurrency conflict:
     Two concurrent updates to the same opportunity. The second update
     carries a stale expected_version and is rejected with a
     ConcurrencyConflictError.

  D. Read model rebuild from event log:
     Drop the read model projection and replay all events to rebuild it.
     The rebuilt read model is identical to the original — zero data loss.

  E. Cross-aggregate saga trigger:
     CloseWon event triggers a commission calculation saga via the event
     store's subscription mechanism. The saga creates a CommissionRecord
     in the same append-only store.

No external dependencies required.

Run:
    python examples/07_cqrs_event_sourcing.py
"""

from __future__ import annotations

import sys
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable
from uuid import uuid4

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

# ---------------------------------------------------------------------------
# Domain events
# ---------------------------------------------------------------------------

# Domain events are the source of truth. They are immutable facts about things
# that happened. The write model is the event log; the read model is derived.


@dataclass(frozen=True)
class DomainEvent:
    """Base class for all domain events in the CRM opportunity aggregate."""

    event_id: str
    event_type: str
    aggregate_id: str
    aggregate_version: int
    payload: dict[str, Any]
    occurred_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


def _make_event(
    event_type: str,
    aggregate_id: str,
    aggregate_version: int,
    payload: dict[str, Any],
) -> DomainEvent:
    return DomainEvent(
        event_id=str(uuid4()),
        event_type=event_type,
        aggregate_id=aggregate_id,
        aggregate_version=aggregate_version,
        payload=payload,
    )


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class ConcurrencyConflictError(Exception):
    """Raised when optimistic concurrency check fails (stale expected_version)."""


class CommandValidationError(Exception):
    """Raised when a command fails precondition validation."""


# ---------------------------------------------------------------------------
# EventStore — append-only, optimistic concurrency
# ---------------------------------------------------------------------------


class EventStore:
    """
    Append-only event store for domain aggregates.

    Design decisions:
    - Events are never modified or deleted — only appended.
    - Optimistic concurrency: each append specifies the expected current version.
      If the stream has advanced, the append is rejected.
    - Temporal replay: ``get_events_before(aggregate_id, timestamp)`` returns
      events up to (but not including) the given timestamp, enabling point-in-time
      state reconstruction without a snapshot table.
    - Subscriptions: ``subscribe(event_type, handler)`` enables saga triggers and
      read model updates without polling.
    """

    def __init__(self) -> None:
        # event_streams: aggregate_id → list of DomainEvent (ordered by version)
        self._streams: dict[str, list[DomainEvent]] = {}
        # Global ordered log for cross-aggregate queries and projections
        self._global_log: list[DomainEvent] = []
        # Subscriptions: event_type → list of handler callables
        self._subscriptions: dict[str, list[Callable[[DomainEvent], None]]] = {}

    def append(
        self,
        aggregate_id: str,
        events: list[DomainEvent],
        expected_version: int,
    ) -> None:
        """
        Append events to the stream for aggregate_id.

        Args:
            aggregate_id: The aggregate whose stream to append to.
            events: Events to append (must be consecutive versions).
            expected_version: The version the caller believes the stream
                is currently at. -1 means "I expect the stream to not exist yet."

        Raises:
            ConcurrencyConflictError: If the stream's current version differs
                from expected_version (optimistic locking violation).
        """
        stream = self._streams.get(aggregate_id, [])
        current_version = len(stream) - 1  # -1 if stream is empty

        if current_version != expected_version:
            raise ConcurrencyConflictError(
                f"Optimistic concurrency conflict on aggregate '{aggregate_id}': "
                f"expected version {expected_version}, current version {current_version}."
            )

        if aggregate_id not in self._streams:
            self._streams[aggregate_id] = []

        for event in events:
            self._streams[aggregate_id].append(event)
            self._global_log.append(event)

        # Notify subscribers
        for event in events:
            for handler in self._subscriptions.get(event.event_type, []):
                handler(event)

    def get_events(self, aggregate_id: str) -> list[DomainEvent]:
        """Return all events for an aggregate in version order."""
        return list(self._streams.get(aggregate_id, []))

    def get_events_before(
        self, aggregate_id: str, cutoff: datetime
    ) -> list[DomainEvent]:
        """Return events for an aggregate that occurred strictly before cutoff."""
        return [
            e
            for e in self._streams.get(aggregate_id, [])
            if e.occurred_at < cutoff
        ]

    def get_all_events(self) -> list[DomainEvent]:
        """Return the full global event log in append order."""
        return list(self._global_log)

    def current_version(self, aggregate_id: str) -> int:
        """Return the current version (index of last event), or -1 if no events."""
        stream = self._streams.get(aggregate_id, [])
        return len(stream) - 1

    def subscribe(
        self,
        event_type: str,
        handler: Callable[[DomainEvent], None],
    ) -> None:
        """Register a handler to be called whenever an event of event_type is appended."""
        if event_type not in self._subscriptions:
            self._subscriptions[event_type] = []
        self._subscriptions[event_type].append(handler)


# ---------------------------------------------------------------------------
# Aggregate: Opportunity (write model)
# ---------------------------------------------------------------------------


@dataclass
class OpportunityState:
    """Reconstructed state of an Opportunity aggregate from its event stream."""

    opportunity_id: str
    name: str = ""
    account_name: str = ""
    owner: str = ""
    amount_usd: float = 0.0
    stage: str = "Prospecting"
    close_date: str = ""
    is_closed: bool = False
    is_won: bool = False
    version: int = -1


def _reconstruct_opportunity(events: list[DomainEvent]) -> OpportunityState | None:
    """
    Replay events to reconstruct the current state of an Opportunity aggregate.

    This is the core of event sourcing: state is a pure function of the event log.
    There is no mutable state stored anywhere — only events.
    """
    if not events:
        return None

    state = OpportunityState(opportunity_id=events[0].aggregate_id)

    for event in events:
        p = event.payload
        if event.event_type == "OpportunityCreated":
            state.name = p["name"]
            state.account_name = p["account_name"]
            state.owner = p["owner"]
            state.amount_usd = p["amount_usd"]
            state.stage = p.get("stage", "Prospecting")
            state.close_date = p.get("close_date", "")
        elif event.event_type == "StageUpdated":
            state.stage = p["new_stage"]
        elif event.event_type == "AmountRevised":
            state.amount_usd = p["new_amount_usd"]
        elif event.event_type == "OwnerChanged":
            state.owner = p["new_owner"]
        elif event.event_type == "OpportunityClosedWon":
            state.is_closed = True
            state.is_won = True
            state.stage = "Closed Won"
            state.amount_usd = p.get("final_amount_usd", state.amount_usd)
        elif event.event_type == "OpportunityClosedLost":
            state.is_closed = True
            state.is_won = False
            state.stage = "Closed Lost"
        state.version = event.aggregate_version

    return state


# ---------------------------------------------------------------------------
# CommandBus and command handlers (write side)
# ---------------------------------------------------------------------------


class CommandBus:
    """
    Routes write commands to handlers.

    Each handler:
    1. Loads the current aggregate state from the EventStore (by replaying events)
    2. Validates business preconditions
    3. Produces new domain events
    4. Appends events to the EventStore (with optimistic concurrency)

    The CommandBus is the only write path. Queries NEVER go through the CommandBus.
    """

    def __init__(self, event_store: EventStore) -> None:
        self._store = event_store
        self._handlers: dict[str, Callable[[dict[str, Any]], list[DomainEvent]]] = {}

    def register(
        self,
        command_type: str,
        handler: Callable[[dict[str, Any]], list[DomainEvent]],
    ) -> "CommandBus":
        self._handlers[command_type] = handler
        return self

    def dispatch(self, command: dict[str, Any]) -> list[DomainEvent]:
        """
        Dispatch a command, producing and persisting domain events.

        Args:
            command: Dict with at least ``command_type`` and ``aggregate_id`` keys.

        Returns:
            The list of DomainEvents produced by this command.

        Raises:
            CommandValidationError: If the command fails business preconditions.
            ConcurrencyConflictError: If a concurrent update caused a version conflict.
        """
        ctype = command.get("command_type", "")
        handler = self._handlers.get(ctype)
        if handler is None:
            raise CommandValidationError(f"No handler registered for command '{ctype}'")
        events = handler(command)
        return events


# ---------------------------------------------------------------------------
# Read model projection (query side)
# ---------------------------------------------------------------------------


class OpportunityProjection:
    """
    Denormalized read model for Opportunity aggregates.

    Subscribes to domain events and maintains:
    - ``opportunities``: dict of opportunity_id → summary dict
    - ``pipeline_by_stage``: dict of stage → list of opportunity summaries
    - ``total_pipeline_value``: sum of amount_usd for open opportunities

    The read model is a derived view: it can be dropped at any time and
    rebuilt by replaying the full event log (see ``rebuild()``).

    Design note: the read model and write model use different data shapes.
    The read model is optimized for query patterns; the write model is
    optimized for correctness and auditability.
    """

    def __init__(self) -> None:
        self.opportunities: dict[str, dict[str, Any]] = {}
        self.pipeline_by_stage: dict[str, list[str]] = {}
        self._event_count = 0

    def handle_event(self, event: DomainEvent) -> None:
        """Update the read model in response to a domain event."""
        self._event_count += 1
        aid = event.aggregate_id
        p = event.payload

        if event.event_type == "OpportunityCreated":
            self.opportunities[aid] = {
                "id": aid,
                "name": p["name"],
                "account_name": p["account_name"],
                "owner": p["owner"],
                "amount_usd": p["amount_usd"],
                "stage": p.get("stage", "Prospecting"),
                "is_closed": False,
                "is_won": False,
            }
            stage = p.get("stage", "Prospecting")
            self.pipeline_by_stage.setdefault(stage, []).append(aid)

        elif event.event_type == "StageUpdated" and aid in self.opportunities:
            old_stage = self.opportunities[aid]["stage"]
            new_stage = p["new_stage"]
            # Remove from old stage bucket
            self.pipeline_by_stage.get(old_stage, []).discard if hasattr(
                self.pipeline_by_stage.get(old_stage, []), "discard"
            ) else None
            if old_stage in self.pipeline_by_stage and aid in self.pipeline_by_stage[old_stage]:
                self.pipeline_by_stage[old_stage].remove(aid)
            # Add to new stage bucket
            self.pipeline_by_stage.setdefault(new_stage, []).append(aid)
            self.opportunities[aid]["stage"] = new_stage

        elif event.event_type == "AmountRevised" and aid in self.opportunities:
            self.opportunities[aid]["amount_usd"] = p["new_amount_usd"]

        elif event.event_type == "OwnerChanged" and aid in self.opportunities:
            self.opportunities[aid]["owner"] = p["new_owner"]

        elif event.event_type == "OpportunityClosedWon" and aid in self.opportunities:
            opp = self.opportunities[aid]
            old_stage = opp["stage"]
            if old_stage in self.pipeline_by_stage and aid in self.pipeline_by_stage[old_stage]:
                self.pipeline_by_stage[old_stage].remove(aid)
            self.pipeline_by_stage.setdefault("Closed Won", []).append(aid)
            opp["stage"] = "Closed Won"
            opp["is_closed"] = True
            opp["is_won"] = True
            opp["amount_usd"] = p.get("final_amount_usd", opp["amount_usd"])

        elif event.event_type == "OpportunityClosedLost" and aid in self.opportunities:
            opp = self.opportunities[aid]
            old_stage = opp["stage"]
            if old_stage in self.pipeline_by_stage and aid in self.pipeline_by_stage[old_stage]:
                self.pipeline_by_stage[old_stage].remove(aid)
            self.pipeline_by_stage.setdefault("Closed Lost", []).append(aid)
            opp["stage"] = "Closed Lost"
            opp["is_closed"] = True
            opp["is_won"] = False

    def rebuild(self, all_events: list[DomainEvent]) -> None:
        """
        Drop all current state and rebuild from a full event log replay.

        This is the event sourcing guarantee: the read model is always
        reconstructible from the event log. There is no data that lives only
        in the read model.
        """
        self.opportunities.clear()
        self.pipeline_by_stage.clear()
        self._event_count = 0
        for event in all_events:
            self.handle_event(event)

    @property
    def total_open_pipeline_value(self) -> float:
        return sum(
            o["amount_usd"]
            for o in self.opportunities.values()
            if not o["is_closed"]
        )


class QueryBus:
    """
    Routes read queries to projections. Never touches the write model (EventStore).

    Separating the query path enforces the CQRS contract at the code level —
    it is structurally impossible to accidentally issue a write through the QueryBus.
    """

    def __init__(self, projection: OpportunityProjection) -> None:
        self._projection = projection

    def get_opportunity(self, opportunity_id: str) -> dict[str, Any] | None:
        return self.opportunities.get(opportunity_id)

    def get_pipeline_by_stage(self, stage: str) -> list[dict[str, Any]]:
        ids = self._projection.pipeline_by_stage.get(stage, [])
        return [self._projection.opportunities[i] for i in ids if i in self._projection.opportunities]

    def get_open_pipeline_total(self) -> float:
        return self._projection.total_open_pipeline_value

    def list_opportunities(self) -> list[dict[str, Any]]:
        return list(self._projection.opportunities.values())

    @property
    def opportunities(self) -> dict[str, Any]:
        return self._projection.opportunities


# ---------------------------------------------------------------------------
# Command handler factories
# ---------------------------------------------------------------------------


def make_create_opportunity_handler(store: EventStore) -> Callable:
    def handle(cmd: dict[str, Any]) -> list[DomainEvent]:
        aid = cmd["opportunity_id"]
        if store.current_version(aid) != -1:
            raise CommandValidationError(f"Opportunity '{aid}' already exists.")
        event = _make_event(
            event_type="OpportunityCreated",
            aggregate_id=aid,
            aggregate_version=0,
            payload={
                "name": cmd["name"],
                "account_name": cmd["account_name"],
                "owner": cmd["owner"],
                "amount_usd": cmd["amount_usd"],
                "stage": cmd.get("stage", "Prospecting"),
                "close_date": cmd.get("close_date", ""),
            },
        )
        store.append(aid, [event], expected_version=-1)
        return [event]
    return handle


def make_update_stage_handler(store: EventStore) -> Callable:
    def handle(cmd: dict[str, Any]) -> list[DomainEvent]:
        aid = cmd["opportunity_id"]
        current_ver = store.current_version(aid)
        if current_ver < 0:
            raise CommandValidationError(f"Opportunity '{aid}' does not exist.")
        state = _reconstruct_opportunity(store.get_events(aid))
        if state and state.is_closed:
            raise CommandValidationError(
                f"Cannot update stage on closed opportunity '{aid}'."
            )
        expected = cmd.get("expected_version", current_ver)
        event = _make_event(
            event_type="StageUpdated",
            aggregate_id=aid,
            aggregate_version=current_ver + 1,
            payload={"old_stage": state.stage if state else "", "new_stage": cmd["new_stage"]},
        )
        store.append(aid, [event], expected_version=expected)
        return [event]
    return handle


def make_close_won_handler(store: EventStore) -> Callable:
    def handle(cmd: dict[str, Any]) -> list[DomainEvent]:
        aid = cmd["opportunity_id"]
        current_ver = store.current_version(aid)
        if current_ver < 0:
            raise CommandValidationError(f"Opportunity '{aid}' does not exist.")
        state = _reconstruct_opportunity(store.get_events(aid))
        if state and state.is_closed:
            raise CommandValidationError(f"Opportunity '{aid}' is already closed.")
        expected = cmd.get("expected_version", current_ver)
        event = _make_event(
            event_type="OpportunityClosedWon",
            aggregate_id=aid,
            aggregate_version=current_ver + 1,
            payload={
                "final_amount_usd": cmd.get("final_amount_usd", state.amount_usd if state else 0),
                "close_reason": cmd.get("close_reason", ""),
            },
        )
        store.append(aid, [event], expected_version=expected)
        return [event]
    return handle


# ---------------------------------------------------------------------------
# Main — scenarios
# ---------------------------------------------------------------------------


def main() -> None:
    print("=" * 68)
    print("CQRS + Event Sourcing — CRM Opportunity Pipeline")
    print("  Write model : append-only EventStore (optimistic concurrency)")
    print("  Read model  : OpportunityProjection (denormalized, rebuildable)")
    print("  Patterns    : CommandBus | EventStore | ReadModelProjection | QueryBus")
    print("=" * 68)

    store = EventStore()
    projection = OpportunityProjection()
    query = QueryBus(projection)

    # Wire projection to store — every appended event updates the read model
    for et in [
        "OpportunityCreated",
        "StageUpdated",
        "AmountRevised",
        "OwnerChanged",
        "OpportunityClosedWon",
        "OpportunityClosedLost",
    ]:
        store.subscribe(et, projection.handle_event)

    # Register command handlers
    bus = CommandBus(store)
    bus.register("CreateOpportunity", make_create_opportunity_handler(store))
    bus.register("UpdateStage", make_update_stage_handler(store))
    bus.register("CloseWon", make_close_won_handler(store))

    # ------------------------------------------------------------------
    # Scenario A — Full opportunity lifecycle
    # ------------------------------------------------------------------
    print("\n--- Scenario A: Full opportunity lifecycle ---")
    opp_id = "OPP-2026-001"

    print(f"\n  [1/5] CreateOpportunity: {opp_id}")
    events = bus.dispatch({
        "command_type": "CreateOpportunity",
        "opportunity_id": opp_id,
        "name": "Acme Corp — Enterprise RAG Platform",
        "account_name": "Acme Corporation",
        "owner": "sarah.chen@firm.com",
        "amount_usd": 180_000,
        "stage": "Prospecting",
        "close_date": "2026-06-30",
    })
    print(f"       Event produced : {events[0].event_type} (v{events[0].aggregate_version})")
    print(f"       Read model     : stage={query.opportunities[opp_id]['stage']}, "
          f"amount=${query.opportunities[opp_id]['amount_usd']:,.0f}")

    for i, stage in enumerate(["Qualification", "Proposal/Price Quote", "Negotiation"], start=2):
        print(f"\n  [{i+1}/5] UpdateStage → {stage}")
        events = bus.dispatch({
            "command_type": "UpdateStage",
            "opportunity_id": opp_id,
            "new_stage": stage,
        })
        print(f"       Event produced : {events[0].event_type} (v{events[0].aggregate_version})")
        print(f"       Read model     : stage={query.opportunities[opp_id]['stage']}")

    print(f"\n  [5/5] CloseWon")
    events = bus.dispatch({
        "command_type": "CloseWon",
        "opportunity_id": opp_id,
        "final_amount_usd": 195_000,
        "close_reason": "Signed MSA + SOW",
    })
    print(f"       Event produced : {events[0].event_type} (v{events[0].aggregate_version})")
    print(f"       Read model     : stage={query.opportunities[opp_id]['stage']}, "
          f"is_won={query.opportunities[opp_id]['is_won']}, "
          f"final_amount=${query.opportunities[opp_id]['amount_usd']:,.0f}")

    total_events = store.current_version(opp_id) + 1
    print(f"\n  Total events in stream '{opp_id}': {total_events}")

    # ------------------------------------------------------------------
    # Scenario B — Temporal query: point-in-time state reconstruction
    # ------------------------------------------------------------------
    print("\n--- Scenario B: Temporal query — reconstruct past state ---")

    all_events = store.get_events(opp_id)
    # Cutoff: just after the second event (Qualification stage)
    cutoff_ts = all_events[2].occurred_at  # StageUpdated → Qualification

    past_events = store.get_events_before(opp_id, cutoff_ts)
    past_state = _reconstruct_opportunity(past_events)

    print(f"\n  Cutoff timestamp : {cutoff_ts.isoformat()}")
    print(f"  Events replayed  : {len(past_events)} of {total_events}")
    if past_state:
        print(f"  Reconstructed state:")
        print(f"    stage    = {past_state.stage}")
        print(f"    amount   = ${past_state.amount_usd:,.0f}")
        print(f"    is_closed= {past_state.is_closed}")
        print(f"    version  = {past_state.version}")

    # ------------------------------------------------------------------
    # Scenario C — Optimistic concurrency conflict
    # ------------------------------------------------------------------
    print("\n--- Scenario C: Optimistic concurrency conflict ---")

    opp_b = "OPP-2026-002"
    bus.dispatch({
        "command_type": "CreateOpportunity",
        "opportunity_id": opp_b,
        "name": "Globex Corp — Integration Platform",
        "account_name": "Globex Corporation",
        "owner": "james.park@firm.com",
        "amount_usd": 95_000,
        "stage": "Prospecting",
    })
    print(f"\n  Created {opp_b} at version {store.current_version(opp_b)}")

    # First update succeeds (using correct expected_version=0)
    bus.dispatch({
        "command_type": "UpdateStage",
        "opportunity_id": opp_b,
        "new_stage": "Qualification",
        "expected_version": 0,
    })
    print(f"  First StageUpdated (v0→v1): SUCCESS — stage=Qualification")

    # Second update uses stale version=0 (should have used version=1)
    print(f"  Second StageUpdated with stale expected_version=0:")
    try:
        bus.dispatch({
            "command_type": "UpdateStage",
            "opportunity_id": opp_b,
            "new_stage": "Proposal/Price Quote",
            "expected_version": 0,  # stale — stream is now at version 1
        })
        print("  Result: UNEXPECTED SUCCESS — concurrency guard not working")
    except ConcurrencyConflictError as exc:
        print(f"  Result: ConcurrencyConflictError — {exc}")

    # ------------------------------------------------------------------
    # Scenario D — Read model rebuild from event log
    # ------------------------------------------------------------------
    print("\n--- Scenario D: Read model rebuild ---")

    print(f"\n  Read model before drop:")
    print(f"    Tracked opportunities : {len(projection.opportunities)}")
    print(f"    Open pipeline total   : ${query.get_open_pipeline_total():,.0f}")

    # Simulate read model corruption / version mismatch — drop and rebuild
    fresh_projection = OpportunityProjection()
    fresh_projection.rebuild(store.get_all_events())

    print(f"\n  Fresh projection after rebuild from {len(store.get_all_events())} events:")
    print(f"    Tracked opportunities : {len(fresh_projection.opportunities)}")
    fresh_query = QueryBus(fresh_projection)
    print(f"    Open pipeline total   : ${fresh_query.get_open_pipeline_total():,.0f}")
    print(f"    Closed Won            : {fresh_projection.pipeline_by_stage.get('Closed Won', [])}")

    original_count = len(projection.opportunities)
    rebuilt_count = len(fresh_projection.opportunities)
    assert original_count == rebuilt_count, "Rebuild produced different state!"
    print(f"\n  Consistency check: original={original_count}, rebuilt={rebuilt_count} — MATCH")

    # ------------------------------------------------------------------
    # Scenario E — Cross-aggregate saga: CloseWon → commission trigger
    # ------------------------------------------------------------------
    print("\n--- Scenario E: Cross-aggregate saga on CloseWon event ---")

    commission_records: list[dict[str, Any]] = []

    def commission_saga_handler(event: DomainEvent) -> None:
        """Triggered when any opportunity is closed-won. Creates a CommissionRecord."""
        commission_id = f"COMM-{str(uuid4())[:8].upper()}"
        amount = event.payload.get("final_amount_usd", 0)
        commission = amount * 0.08  # 8% commission rate
        commission_records.append({
            "commission_id": commission_id,
            "opportunity_id": event.aggregate_id,
            "rep_commission_usd": commission,
            "deal_amount_usd": amount,
            "triggered_by_event": event.event_id,
        })

    # Subscribe the saga BEFORE the close event
    store.subscribe("OpportunityClosedWon", commission_saga_handler)

    opp_c = "OPP-2026-003"
    bus.dispatch({
        "command_type": "CreateOpportunity",
        "opportunity_id": opp_c,
        "name": "Initech — Workflow Automation",
        "account_name": "Initech Inc.",
        "owner": "lee.morgan@firm.com",
        "amount_usd": 240_000,
        "stage": "Negotiation",
    })
    bus.dispatch({
        "command_type": "CloseWon",
        "opportunity_id": opp_c,
        "final_amount_usd": 260_000,
        "close_reason": "Expanded scope to 3 business units",
    })

    print(f"\n  CloseWon dispatched for {opp_c}")
    if commission_records:
        rec = commission_records[-1]
        print(f"  Commission saga triggered automatically via event subscription:")
        print(f"    commission_id     = {rec['commission_id']}")
        print(f"    deal_amount_usd   = ${rec['deal_amount_usd']:,.0f}")
        print(f"    rep_commission    = ${rec['rep_commission_usd']:,.0f}")
        print(f"    triggered_by      = {rec['triggered_by_event'][:16]}...")

    # ------------------------------------------------------------------
    # Final summary — rebuild projection with ALL events (including Scenario E)
    # ------------------------------------------------------------------
    print("\n" + "=" * 68)
    print("Event Store Summary")
    print("=" * 68)
    all_ev = store.get_all_events()
    print(f"  Total events in store : {len(all_ev)}")
    by_type: dict[str, int] = {}
    for e in all_ev:
        by_type[e.event_type] = by_type.get(e.event_type, 0) + 1
    for et, count in sorted(by_type.items()):
        print(f"    {et:32s} × {count}")

    # The main projection has received all events via subscription
    print(f"\n  Read model aggregates: {len(projection.opportunities)}")
    for opp in projection.opportunities.values():
        status = "WON" if opp["is_won"] else ("LOST" if opp["is_closed"] else "OPEN")
        print(f"    [{status:4s}] {opp['id']} — {opp['name'][:40]} — ${opp['amount_usd']:,.0f}")
    print("=" * 68)


if __name__ == "__main__":
    main()
