"""
22_event_sourcing_cqrs_patterns.py — Advanced Event Sourcing & CQRS patterns
for audit-complete state management, optimistic concurrency control, snapshot-
based performance optimisation, and eventually-consistent read-model projections.

Demonstrates seven complementary patterns that together implement a production-
grade event-sourced domain model as found in financial systems, healthcare
platforms, and enterprise SaaS applications:

    Pattern 1 — DomainEvent (immutable fact):
                A frozen dataclass that records a single state-changing fact
                about an aggregate — what happened, to which aggregate, at
                which logical version, and at what wall-clock time.  Immutability
                is guaranteed by frozen=True so that events stored in an
                EventStore can never be silently mutated after the fact.  This
                is the foundational type in every event-sourced system: Greg
                Young's "events as first-class citizens" principle and the CQRS
                Journey (Microsoft) both treat the domain event as the canonical
                system-of-record rather than current-state rows.

    Pattern 2 — AggregateRoot (event-collecting base class):
                Encapsulates the three responsibilities of an event-sourced
                aggregate: raising new domain events (raise_event), applying
                events to local state (apply / on_<EventType> dispatch), and
                surfacing uncommitted events for persistence (uncommitted_events /
                mark_committed).  Rebuilding an aggregate from history requires
                only load_from_history; no special query path is needed.  This
                mirrors the Aggregate pattern in Domain-Driven Design (Evans 2004)
                and the implementation described in "Implementing Domain-Driven
                Design" (Vernon 2013).

    Pattern 3 — EventStore (append-only log with optimistic concurrency):
                Stores event streams keyed by aggregate_id.  Optimistic
                concurrency is enforced by comparing the caller's expected_version
                (the stream length at the time the aggregate was loaded) against
                the current stream length; a mismatch raises ConcurrencyConflict,
                preventing lost-update anomalies without database-level locking.
                load() supports reading from a specific version offset so that
                SnapshotStore-backed loads only retrieve the delta events rather
                than the full stream.  This pattern underlies EventStoreDB,
                Axon Server, and Marten (PostgreSQL append-only event log).

    Pattern 4 — SnapshotStore (performance cut-off for long streams):
                Persists a lightweight serialised snapshot of aggregate state
                at a configured event-count threshold (default: every 10 events).
                On next load, the repository restores the snapshot and then
                replays only the events emitted after the snapshot version,
                reducing replay time from O(n total events) to O(n mod threshold).
                This is the snapshot pattern described in the CQRS Journey and
                implemented natively in Axon Framework and EventStoreDB's
                $all stream with catch-up subscriptions.

    Pattern 5 — BankAccount (concrete event-sourced aggregate):
                Implements an account lifecycle (open → deposit / withdraw →
                close) using four domain events: AccountOpened, MoneyDeposited,
                MoneyWithdrawn, AccountClosed.  All business invariants
                (non-negative deposit, sufficient funds, closed-account guard)
                are checked in command methods before any event is raised, so
                the event stream always represents a valid sequence of facts.
                The same pattern is used in production financial systems built
                on Axon Framework (Java) and Eventuate (Node/JVM).

    Pattern 6 — AccountBalanceProjection (CQRS read model):
                Subscribes to the domain event stream and maintains a denormalised
                in-memory read model: current balance, owner name, and open-
                account set.  The projection can be rebuilt at any time by
                calling reset() + applying all events, enabling blue/green
                read-model migrations without touching the write side.  This is
                the Query side of CQRS as described by Martin Fowler (2011) and
                corresponds to Axon Framework's @EventHandler projection beans,
                EventStoreDB projections, and AWS DynamoDB Streams + Lambda
                materialised-view pattern.

    Pattern 7 — EventSourcedRepository (repository tying write + snapshot path):
                Combines EventStore and SnapshotStore into the standard
                repository interface (save / load) familiar from DDD.  save()
                extracts uncommitted events, performs the optimistic-concurrency
                append, marks the aggregate committed, and auto-snapshots at the
                configured threshold.  load() checks for a snapshot first,
                restores state from it, then replays the delta events — shielding
                callers from the storage details of either store.  This matches
                the generic EventSourcingRepository in Axon Framework and the
                Repository<T> pattern in Marten's document store.

Relationship to commercial event-sourcing / CQRS platforms:

    - EventStoreDB           : EventStore ≈ EventStoreDB stream (append +
                               read from position); SnapshotStore ≈ EventStoreDB
                               snapshot stream convention ($[stream]-snapshots);
                               AccountBalanceProjection ≈ EventStoreDB
                               persistent projection (JS / C# DSL).
    - Axon Framework (Java)  : AggregateRoot ≈ @Aggregate + @EventSourcingHandler;
                               EventSourcedRepository ≈ EventSourcingRepository<T>;
                               AccountBalanceProjection ≈ @EventHandler bean in
                               a @ProcessingGroup.
    - Marten (PostgreSQL)    : EventStore ≈ IDocumentSession.Events.Append();
                               SnapshotStore ≈ Marten snapshot store with
                               SnapshotLifecycle.Inline; EventSourcedRepository
                               ≈ IEventStore.AggregateStream<T>().
    - AWS (DynamoDB Streams) : EventStore ≈ DynamoDB Streams + conditional
                               writes (version attribute); AccountBalanceProjection
                               ≈ Lambda consumer of DynamoDB Stream building a
                               separate read table.
    - Eventuate Platform     : AggregateRoot ≈ ReflectiveMutableCommandProcessingAggregate;
                               EventStore ≈ EventuateJdbcEventStore; projections
                               ≈ EventuateAggregateSubscriptions + read DB.
    - Azure Event Sourcing   : EventStore ≈ Azure Cosmos DB change feed (append-
                               only partition per aggregate); SnapshotStore ≈
                               separate Cosmos container for snapshots;
                               AccountBalanceProjection ≈ Azure Functions
                               triggered by change feed.

No external dependencies required.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class ConcurrencyConflict(Exception):
    """Raised when an EventStore append detects a version mismatch.

    This indicates that another writer committed events to the same stream
    between when the aggregate was loaded and when the caller attempted to
    save.  The caller should reload the aggregate and retry the command.

    Attributes:
        aggregate_id: The stream whose version did not match.
        expected_version: The version the caller assumed was current.
        actual_version: The version actually found in the store.
    """

    def __init__(self, aggregate_id: str, expected: int, actual: int) -> None:
        self.aggregate_id = aggregate_id
        self.expected_version = expected
        self.actual_version = actual
        super().__init__(
            f"Concurrency conflict on {aggregate_id}: "
            f"expected v{expected}, found v{actual}"
        )


class InsufficientFunds(Exception):
    """Raised when a withdrawal amount exceeds the current account balance."""


class AccountNotFound(Exception):
    """Raised when loading an aggregate that has no events and no snapshot."""


class AccountAlreadyClosed(Exception):
    """Raised when a command targets an account that has been closed."""


# ---------------------------------------------------------------------------
# Pattern 1 — DomainEvent (immutable fact)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DomainEvent:
    """An immutable record of a single state-changing fact about an aggregate.

    Events are the system-of-record in an event-sourced architecture.  Once
    appended to the EventStore they must never be mutated; frozen=True enforces
    this at the Python dataclass level.

    Attributes:
        aggregate_id: The identity of the aggregate that raised this event.
        event_type: Discriminator string, e.g. "AccountOpened".  Must match
                    the on_<event_type> handler name on the AggregateRoot
                    subclass.
        payload: Arbitrary key/value data describing the event.  All numeric
                 values are stored as strings to avoid floating-point drift
                 across serialisation boundaries (Decimal-safe round-trip).
        version: Monotonically increasing integer within the aggregate's
                 event stream.  Version 1 is the first event ever raised.
        occurred_at: Unix epoch float (time.time()) recorded when the event
                     was created inside raise_event().

    Example::

        event = DomainEvent(
            aggregate_id="acct-001",
            event_type="MoneyDeposited",
            payload={"amount": "200.00"},
            version=3,
            occurred_at=time.time(),
        )
        assert event.payload["amount"] == "200.00"
    """

    aggregate_id: str
    event_type: str
    payload: dict  # frozen dataclass; dict is mutable but reference is fixed
    version: int
    occurred_at: float


# ---------------------------------------------------------------------------
# Pattern 2 — AggregateRoot (event-collecting base class)
# ---------------------------------------------------------------------------


class AggregateRoot:
    """Base class for all event-sourced domain aggregates.

    Subclasses implement command methods that call raise_event() and event
    handler methods named on_<EventType> that mutate local state.

    The three core responsibilities are:

    1. **Raising events** — raise_event() creates a DomainEvent, appends it
       to _uncommitted_events, and immediately applies it to local state so
       that subsequent commands within the same unit-of-work see the updated
       state.

    2. **Applying events** — apply() dispatches to the appropriate on_*
       handler and advances self.version.  Both raise_event() (for new events)
       and load_from_history() (for replay) use apply() so there is a single
       state-transition code path.

    3. **Exposing the event stream** — uncommitted_events returns a snapshot
       of events pending persistence; mark_committed() clears the buffer after
       the EventStore has durably appended them.

    Example::

        class Counter(AggregateRoot):
            def __init__(self, cid: str):
                super().__init__(cid)
                self.count = 0

            def increment(self) -> None:
                self.raise_event("Incremented", {})

            def on_Incremented(self, event: DomainEvent) -> None:
                self.count += 1

        c = Counter("c-1")
        c.increment()
        assert c.count == 1
        assert c.version == 1
        assert len(c.uncommitted_events) == 1
    """

    def __init__(self, aggregate_id: str) -> None:
        self.aggregate_id = aggregate_id
        self.version: int = 0
        self._uncommitted_events: list[DomainEvent] = []

    # ------------------------------------------------------------------
    # Core mechanics
    # ------------------------------------------------------------------

    def apply(self, event: DomainEvent) -> None:
        """Apply *event* to aggregate state and advance version.

        Dispatches to the on_<event_type> method if one exists.  Missing
        handlers are silently ignored so that older aggregate code can safely
        replay newer event streams that contain event types it does not yet
        understand (forward-compatibility).
        """
        handler_name = f"on_{event.event_type}"
        handler = getattr(self, handler_name, None)
        if handler is not None:
            handler(event)
        self.version = event.version

    def raise_event(self, event_type: str, payload: dict) -> DomainEvent:
        """Create, register, and immediately apply a new domain event.

        The event version is set to ``self.version + len(pending) + 1`` so
        that multiple events raised within a single command each receive a
        unique, monotonically increasing version number even before the stream
        has been persisted.

        Args:
            event_type: Discriminator string matching an on_<EventType> handler.
            payload: Arbitrary event data.  Callers are responsible for
                     serialising numeric values as strings for Decimal safety.

        Returns:
            The newly created DomainEvent (already applied to local state).
        """
        event = DomainEvent(
            aggregate_id=self.aggregate_id,
            event_type=event_type,
            payload=payload,
            version=self.version + 1,
            occurred_at=time.time(),
        )
        self._uncommitted_events.append(event)
        self.apply(event)
        return event

    @property
    def uncommitted_events(self) -> list[DomainEvent]:
        """Return a copy of all events raised since the last mark_committed()."""
        return list(self._uncommitted_events)

    def mark_committed(self) -> None:
        """Clear the uncommitted event buffer after successful persistence."""
        self._uncommitted_events.clear()

    def load_from_history(self, events: list[DomainEvent]) -> None:
        """Rebuild aggregate state by replaying a historical event sequence.

        After replay, the uncommitted buffer is cleared so that historical
        events are not re-persisted on the next save.

        Args:
            events: Ordered list of DomainEvents from the EventStore, starting
                    from the version immediately after any applied snapshot.
        """
        for event in events:
            self.apply(event)
        self._uncommitted_events.clear()


# ---------------------------------------------------------------------------
# Pattern 5 — BankAccount (concrete event-sourced aggregate)
# ---------------------------------------------------------------------------


class BankAccount(AggregateRoot):
    """Event-sourced aggregate representing a bank account lifecycle.

    Supports four commands (open, deposit, withdraw, close) that emit the
    corresponding domain events.  All business invariants are validated before
    any event is raised so that the event stream is always a valid sequence
    of facts.

    State attributes (populated via event handlers):
        account_id: Matches aggregate_id; provided for readable access.
        owner_name: Full name of the account holder.
        balance:    Current balance as a Decimal for exact arithmetic.
        is_open:    True between AccountOpened and AccountClosed.

    Example::

        acct = BankAccount("acct-001")
        acct.open("Alice", Decimal("500.00"))
        acct.deposit(Decimal("200.00"))
        acct.withdraw(Decimal("100.00"))
        assert acct.balance == Decimal("600.00")
        assert acct.version == 3
    """

    def __init__(self, account_id: str) -> None:
        super().__init__(account_id)
        self.account_id = account_id
        self.owner_name: str = ""
        self.balance: Decimal = Decimal("0.00")
        self.is_open: bool = False

    # ------------------------------------------------------------------
    # Commands
    # ------------------------------------------------------------------

    def open(self, owner_name: str, initial_deposit: Decimal) -> None:
        """Open the account with an owner name and an initial deposit.

        Args:
            owner_name: Full name of the account holder.
            initial_deposit: Starting balance; must be non-negative.

        Raises:
            ValueError: If initial_deposit is negative.
        """
        if initial_deposit < Decimal("0"):
            raise ValueError("Initial deposit cannot be negative")
        self.raise_event(
            "AccountOpened",
            {
                "owner_name": owner_name,
                "initial_deposit": str(initial_deposit),
            },
        )

    def deposit(self, amount: Decimal) -> None:
        """Credit *amount* to the account balance.

        Args:
            amount: Positive amount to deposit.

        Raises:
            AccountAlreadyClosed: If the account has been closed.
            ValueError: If amount is not strictly positive.
        """
        if not self.is_open:
            raise AccountAlreadyClosed(self.account_id)
        if amount <= Decimal("0"):
            raise ValueError("Deposit amount must be positive")
        self.raise_event("MoneyDeposited", {"amount": str(amount)})

    def withdraw(self, amount: Decimal) -> None:
        """Debit *amount* from the account balance.

        Args:
            amount: Positive amount to withdraw.

        Raises:
            AccountAlreadyClosed: If the account has been closed.
            ValueError: If amount is not strictly positive.
            InsufficientFunds: If the current balance is less than amount.
        """
        if not self.is_open:
            raise AccountAlreadyClosed(self.account_id)
        if amount <= Decimal("0"):
            raise ValueError("Withdrawal amount must be positive")
        if self.balance < amount:
            raise InsufficientFunds(
                f"Balance {self.balance} < withdrawal {amount}"
            )
        self.raise_event("MoneyWithdrawn", {"amount": str(amount)})

    def close(self) -> None:
        """Close the account, recording the final balance in the event payload.

        Raises:
            AccountAlreadyClosed: If the account is already closed.
        """
        if not self.is_open:
            raise AccountAlreadyClosed(self.account_id)
        self.raise_event("AccountClosed", {"final_balance": str(self.balance)})

    # ------------------------------------------------------------------
    # Event handlers (invoked by AggregateRoot.apply)
    # ------------------------------------------------------------------

    def on_AccountOpened(self, event: DomainEvent) -> None:
        """Apply AccountOpened: set owner, initial balance, and mark open."""
        self.owner_name = event.payload["owner_name"]
        self.balance = Decimal(event.payload["initial_deposit"])
        self.is_open = True

    def on_MoneyDeposited(self, event: DomainEvent) -> None:
        """Apply MoneyDeposited: credit the deposit amount."""
        self.balance += Decimal(event.payload["amount"])

    def on_MoneyWithdrawn(self, event: DomainEvent) -> None:
        """Apply MoneyWithdrawn: debit the withdrawal amount."""
        self.balance -= Decimal(event.payload["amount"])

    def on_AccountClosed(self, event: DomainEvent) -> None:
        """Apply AccountClosed: mark the account as no longer open."""
        self.is_open = False


# ---------------------------------------------------------------------------
# Pattern 3 — EventStore (append-only log with optimistic concurrency)
# ---------------------------------------------------------------------------


class EventStore:
    """In-memory append-only event log with per-stream optimistic concurrency.

    Each aggregate's events are stored in a named stream (keyed by
    aggregate_id).  Optimistic concurrency prevents the lost-update anomaly:
    the caller must supply the stream length it observed when it loaded the
    aggregate; if another writer has appended events in the interim, the
    actual length will differ and ConcurrencyConflict is raised.

    In production systems this store would be backed by a relational database
    (Marten, custom JSONB table), EventStoreDB, or a Kafka topic partition.
    The in-memory implementation here mirrors the interface exactly so that
    application code is storage-agnostic.

    Example::

        store = EventStore()
        events = [DomainEvent("a1", "AccountOpened", {}, 1, time.time())]
        store.append("a1", events, expected_version=0)
        loaded = store.load("a1")
        assert len(loaded) == 1
    """

    def __init__(self) -> None:
        # aggregate_id → ordered list of all committed DomainEvents
        self._streams: dict[str, list[DomainEvent]] = {}

    def append(
        self,
        aggregate_id: str,
        events: list[DomainEvent],
        expected_version: int,
    ) -> None:
        """Append *events* to the stream, enforcing optimistic concurrency.

        Args:
            aggregate_id: Target stream identifier.
            events: Non-empty list of DomainEvents to append.  May be empty
                    (no-op) when an aggregate has no new events.
            expected_version: The stream length the caller observed when it
                              last loaded the aggregate.  Must equal the current
                              stream length or ConcurrencyConflict is raised.

        Raises:
            ConcurrencyConflict: If expected_version does not match the current
                                 stream length.
        """
        current = self._streams.get(aggregate_id, [])
        actual_version = len(current)
        if actual_version != expected_version:
            raise ConcurrencyConflict(aggregate_id, expected_version, actual_version)
        if events:
            self._streams.setdefault(aggregate_id, []).extend(events)

    def load(
        self, aggregate_id: str, from_version: int = 0
    ) -> list[DomainEvent]:
        """Return events for *aggregate_id* with version > *from_version*.

        When loading with a snapshot, pass snapshot.version as from_version
        to retrieve only the delta events that occurred after the snapshot.

        Args:
            aggregate_id: Stream to read.
            from_version: Exclusive lower bound on event version (default 0
                          returns all events).

        Returns:
            Ordered list of DomainEvents; empty list if none match.
        """
        return [
            e
            for e in self._streams.get(aggregate_id, [])
            if e.version > from_version
        ]

    def get_version(self, aggregate_id: str) -> int:
        """Return the current event count (stream length) for *aggregate_id*."""
        return len(self._streams.get(aggregate_id, []))

    def stream_ids(self) -> list[str]:
        """Return all aggregate IDs that have at least one event."""
        return list(self._streams.keys())


# ---------------------------------------------------------------------------
# Pattern 4 — SnapshotStore (performance cut-off for long streams)
# ---------------------------------------------------------------------------


@dataclass
class Snapshot:
    """A point-in-time serialisation of an aggregate's state.

    Attributes:
        aggregate_id: Identifies the aggregate this snapshot belongs to.
        version: The aggregate version at snapshot time.  EventStore.load()
                 should be called with from_version=snapshot.version so that
                 only delta events are replayed.
        state: Serialised aggregate state as a plain dict (string values for
               Decimal fields to preserve precision).
        taken_at: Unix epoch float when the snapshot was created.
    """

    aggregate_id: str
    version: int
    state: dict
    taken_at: float


class SnapshotStore:
    """In-memory store for the latest snapshot per aggregate.

    Only the most recent snapshot per aggregate_id is retained; older
    snapshots are overwritten on each save().  This matches the "latest
    snapshot only" strategy used by Marten's inline snapshot lifecycle and
    Axon Framework's SnapshotTriggerDefinition.

    Example::

        store = SnapshotStore()
        snap = Snapshot("acct-1", 10, {"balance": "500.00", ...}, time.time())
        store.save(snap)
        loaded = store.load("acct-1")
        assert loaded.version == 10
    """

    def __init__(self) -> None:
        self._snapshots: dict[str, Snapshot] = {}

    def save(self, snapshot: Snapshot) -> None:
        """Persist *snapshot*, replacing any earlier snapshot for the same aggregate."""
        self._snapshots[snapshot.aggregate_id] = snapshot

    def load(self, aggregate_id: str) -> Optional[Snapshot]:
        """Return the latest snapshot for *aggregate_id*, or None if none exists."""
        return self._snapshots.get(aggregate_id)


# ---------------------------------------------------------------------------
# Pattern 6 — AccountBalanceProjection (CQRS read model)
# ---------------------------------------------------------------------------


class AccountBalanceProjection:
    """Eventually-consistent read model tracking balances for all accounts.

    The projection subscribes to the full event stream and maintains three
    denormalised data structures:
        - _balances:       account_id → current Decimal balance
        - _owners:         account_id → owner name string
        - _open_accounts:  set of account_ids that are currently open

    These structures are cheap O(1) lookups compared to replaying the full
    event stream on every query, which is the core motivation for the CQRS
    read/write separation.

    The projection can be rebuilt at any point by calling reset() followed
    by apply() over all events.  rebuild() is a convenience that does both
    in one call, enabling blue/green read-model migrations without downtime
    on the write side.

    Example::

        proj = AccountBalanceProjection()
        for event in store.load("acct-001"):
            proj.apply(event)
        balance = proj.get_balance("acct-001")
    """

    def __init__(self) -> None:
        self._balances: dict[str, Decimal] = {}
        self._owners: dict[str, str] = {}
        self._open_accounts: set[str] = set()

    def apply(self, event: DomainEvent) -> None:
        """Process a single DomainEvent and update the read model accordingly.

        Unknown event types are silently ignored so the projection remains
        forward-compatible with new event types added to the write side.

        Args:
            event: DomainEvent to fold into the projection state.
        """
        aid = event.aggregate_id
        if event.event_type == "AccountOpened":
            self._balances[aid] = Decimal(event.payload["initial_deposit"])
            self._owners[aid] = event.payload["owner_name"]
            self._open_accounts.add(aid)
        elif event.event_type == "MoneyDeposited":
            self._balances[aid] = self._balances.get(aid, Decimal("0")) + Decimal(
                event.payload["amount"]
            )
        elif event.event_type == "MoneyWithdrawn":
            self._balances[aid] = self._balances.get(aid, Decimal("0")) - Decimal(
                event.payload["amount"]
            )
        elif event.event_type == "AccountClosed":
            self._open_accounts.discard(aid)

    def get_balance(self, account_id: str) -> Optional[Decimal]:
        """Return the current balance for *account_id*, or None if unknown."""
        return self._balances.get(account_id)

    def get_owner(self, account_id: str) -> Optional[str]:
        """Return the owner name for *account_id*, or None if unknown."""
        return self._owners.get(account_id)

    def get_open_accounts(self) -> list[str]:
        """Return a list of all account IDs currently in the open state."""
        return list(self._open_accounts)

    def reset(self) -> None:
        """Clear all read-model state (used before a full rebuild)."""
        self._balances.clear()
        self._owners.clear()
        self._open_accounts.clear()

    def rebuild(self, events: list[DomainEvent]) -> None:
        """Rebuild the projection from scratch by replaying *events* in order.

        Args:
            events: Complete ordered event list for all aggregates to include
                    in this projection.
        """
        self.reset()
        for event in events:
            self.apply(event)


# ---------------------------------------------------------------------------
# Pattern 7 — EventSourcedRepository (aggregate persistence facade)
# ---------------------------------------------------------------------------


class EventSourcedRepository:
    """Repository that ties EventStore and SnapshotStore into a clean save/load API.

    save() extracts uncommitted events from the aggregate, enforces optimistic
    concurrency via the EventStore, clears the buffer on success, and
    auto-snapshots when the aggregate's version crosses a multiple of the
    configured snapshot_threshold.

    load() checks the SnapshotStore first.  If a snapshot is found, aggregate
    state is restored from it and only the delta events (version > snapshot.version)
    are replayed, keeping replay time bounded regardless of total stream length.
    If no snapshot and no events exist, AccountNotFound is raised.

    The snapshot_threshold default of 10 is intentionally low for testing
    purposes; production systems typically snapshot every 50–200 events.

    Example::

        event_store = EventStore()
        snap_store = SnapshotStore()
        repo = EventSourcedRepository(event_store, snap_store)

        acct = BankAccount("acct-001")
        acct.open("Alice", Decimal("1000.00"))
        repo.save(acct)

        loaded = repo.load("acct-001")
        assert loaded.balance == Decimal("1000.00")
    """

    def __init__(
        self,
        event_store: EventStore,
        snapshot_store: SnapshotStore,
        snapshot_threshold: int = 10,
    ) -> None:
        self._event_store = event_store
        self._snapshot_store = snapshot_store
        self._snapshot_threshold = snapshot_threshold

    def save(self, aggregate: BankAccount) -> None:
        """Persist uncommitted events and conditionally take a snapshot.

        Calculates the expected_version as ``aggregate.version - len(pending)``
        so that the EventStore can detect concurrent writes.

        Args:
            aggregate: The BankAccount instance to persist.  A no-op if there
                       are no uncommitted events.
        """
        events = aggregate.uncommitted_events
        if not events:
            return
        expected_version = aggregate.version - len(events)
        self._event_store.append(aggregate.aggregate_id, events, expected_version)
        aggregate.mark_committed()

        # Auto-snapshot at version multiples of snapshot_threshold
        if aggregate.version % self._snapshot_threshold == 0:
            self._snapshot_store.save(
                Snapshot(
                    aggregate_id=aggregate.aggregate_id,
                    version=aggregate.version,
                    state={
                        "balance": str(aggregate.balance),
                        "owner_name": aggregate.owner_name,
                        "is_open": aggregate.is_open,
                    },
                    taken_at=time.time(),
                )
            )

    def load(self, account_id: str) -> BankAccount:
        """Load a BankAccount aggregate, using a snapshot as a shortcut when available.

        The load sequence is:
        1. Check SnapshotStore; if found, restore state fields and set version.
        2. Load only events with version > snapshot.version from EventStore.
        3. Replay delta events via load_from_history().
        4. Raise AccountNotFound if no snapshot and no events are present.

        Args:
            account_id: The aggregate identity to load.

        Returns:
            A fully reconstituted BankAccount with version and state current
            as of the latest committed event.

        Raises:
            AccountNotFound: If the aggregate has never been persisted.
        """
        account = BankAccount(account_id)
        snapshot = self._snapshot_store.load(account_id)
        from_version = 0

        if snapshot is not None:
            account.balance = Decimal(snapshot.state["balance"])
            account.owner_name = snapshot.state["owner_name"]
            account.is_open = snapshot.state["is_open"]
            account.version = snapshot.version
            from_version = snapshot.version

        events = self._event_store.load(account_id, from_version=from_version)

        if snapshot is None and not events:
            raise AccountNotFound(account_id)

        account.load_from_history(events)
        return account
