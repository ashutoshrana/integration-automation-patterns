"""
34_distributed_transaction_patterns.py — Distributed Transaction Patterns

Pure-Python implementation of core distributed transaction primitives using
only the standard library (``dataclasses``, ``threading``, ``time``,
``uuid``, ``typing``).  No external dependencies are required.

    Pattern 1 — TwoPhaseCommit (2PC coordinator):
                Implements the classic two-phase commit protocol.  Registers
                named participants, each with prepare / commit / rollback
                callbacks.  Phase 1 polls every participant's prepare_fn; if
                any vote is False the coordinator rolls back all participants
                and returns False.  If all vote True the coordinator commits
                all participants and returns True.  Participant state is
                tracked throughout.

    Pattern 2 — CompensatingTransaction (Saga-style):
                Manages a sequence of named forward steps each paired with a
                compensating callback.  On failure the manager runs already-
                completed steps' compensate_fn in reverse order (LIFO),
                implementing the Saga pattern.  Supports method-chaining via
                :meth:`add_step` and :meth:`reset`.

    Pattern 3 — DistributedLock (token-based TTL lock):
                Token-based distributed lock with configurable time-to-live
                and reentrant support.  An owner may re-acquire its own lock
                (incrementing a depth counter) without blocking.  Expired
                locks are transparently taken over.  Supports TTL extension
                and blocking acquire with timeout.

    Pattern 4 — TransactionCoordinator (multi-resource coordinator):
                Coordinates multiple distributed operations identified by
                UUIDs.  Resources enlist their rollback callbacks; the
                coordinator executes them in LIFO order on rollback and clears
                the registry on commit.

No external dependencies required.

Run:
    python examples/34_distributed_transaction_patterns.py
"""

from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

# ===========================================================================
# Pattern 1 — TwoPhaseCommit
# ===========================================================================


@dataclass
class _TwoPhaseParticipant:
    """Internal state for a single 2PC participant.

    Attributes:
        name:         Unique participant name.
        prepared:     ``True`` after a successful prepare vote.
        committed:    ``True`` after the commit callback ran.
        rolled_back:  ``True`` after the rollback callback ran.
    """

    name: str
    prepared: bool = False
    committed: bool = False
    rolled_back: bool = False


class TwoPhaseCommit:
    """Two-phase commit (2PC) coordinator for distributed transactions.

    Registers named participants each with three lifecycle callbacks
    (prepare / commit / rollback) and executes the 2PC protocol atomically.

    Phase 1 — Prepare:
        Every participant's ``prepare_fn`` is called.  If all return ``True``
        the coordinator proceeds to Phase 2 commit.  If any returns ``False``
        (or raises) the coordinator proceeds to Phase 2 abort.

    Phase 2a — Commit (all prepared):
        Every participant's ``commit_fn`` is called; participant state is
        set to ``committed=True``.

    Phase 2b — Abort (any refused or exception):
        Every participant's ``rollback_fn`` is called; participant state is
        set to ``rolled_back=True``.

    Example::

        tpc = TwoPhaseCommit("tx-001")
        tpc.register("db", lambda: True, lambda: None, lambda: None)
        tpc.register("cache", lambda: True, lambda: None, lambda: None)
        success = tpc.execute()
        print(success)          # True
        print(tpc.status())
    """

    # Expose the participant dataclass under a predictable name.
    Participant = _TwoPhaseParticipant

    def __init__(self, transaction_id: str) -> None:
        """Initialise a :class:`TwoPhaseCommit` coordinator.

        Args:
            transaction_id: Unique identifier for this distributed transaction.
        """
        self.transaction_id = transaction_id
        self._participants: list[_TwoPhaseParticipant] = []
        self._prepare_fns: dict[str, Callable[[], bool]] = {}
        self._commit_fns: dict[str, Callable[[], None]] = {}
        self._rollback_fns: dict[str, Callable[[], None]] = {}
        self._outcome: str = "pending"

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register(
        self,
        name: str,
        prepare_fn: Callable[[], bool],
        commit_fn: Callable[[], None],
        rollback_fn: Callable[[], None],
    ) -> None:
        """Register a named participant with its lifecycle callbacks.

        Args:
            name:        Unique participant name.
            prepare_fn:  Called during Phase 1; returns ``True`` to vote
                         commit, ``False`` to vote abort.
            commit_fn:   Called during Phase 2 commit.
            rollback_fn: Called during Phase 2 abort.

        Raises:
            ValueError: If *name* is already registered.
        """
        if name in self._prepare_fns:
            raise ValueError(f"Participant {name!r} is already registered")
        participant = _TwoPhaseParticipant(name=name)
        self._participants.append(participant)
        self._prepare_fns[name] = prepare_fn
        self._commit_fns[name] = commit_fn
        self._rollback_fns[name] = rollback_fn

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def execute(self) -> bool:
        """Execute the two-phase commit protocol.

        Phase 1: call every ``prepare_fn``; collect votes.
        Phase 2a (all True): call every ``commit_fn``.
        Phase 2b (any False): call every ``rollback_fn``.

        Returns:
            ``True`` if the transaction committed, ``False`` if aborted.
        """
        # Phase 1 — Prepare
        all_prepared = True
        for participant in self._participants:
            try:
                vote = self._prepare_fns[participant.name]()
            except Exception:  # noqa: BLE001
                vote = False
            if vote:
                participant.prepared = True
            else:
                all_prepared = False
                break

        # Phase 2
        if all_prepared:
            for participant in self._participants:
                self._commit_fns[participant.name]()
                participant.committed = True
            self._outcome = "committed"
            return True
        else:
            for participant in self._participants:
                self._rollback_fns[participant.name]()
                participant.rolled_back = True
            self._outcome = "aborted"
            return False

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------

    def status(self) -> dict:
        """Return current transaction status.

        Returns:
            Dict with keys:

            * ``"transaction_id"`` — the transaction ID string.
            * ``"participants"``   — list of per-participant state dicts,
              each with ``name``, ``prepared``, ``committed``,
              ``rolled_back``.
            * ``"outcome"``        — ``"committed"``, ``"aborted"``, or
              ``"pending"``.
        """
        return {
            "transaction_id": self.transaction_id,
            "participants": [
                {
                    "name": p.name,
                    "prepared": p.prepared,
                    "committed": p.committed,
                    "rolled_back": p.rolled_back,
                }
                for p in self._participants
            ],
            "outcome": self._outcome,
        }


# ===========================================================================
# Pattern 2 — CompensatingTransaction
# ===========================================================================


@dataclass
class _CompensatingStep:
    """A single step in a compensating (Saga) transaction.

    Attributes:
        name:          Human-readable step name.
        forward_fn:    Callable that performs the step; may return any value.
        compensate_fn: Callable that undoes the step; return value ignored.
        result:        Stores the return value of ``forward_fn`` after
                       successful execution.
        completed:     ``True`` after ``forward_fn`` ran successfully.
    """

    name: str
    forward_fn: Callable[[], Any]
    compensate_fn: Callable[[], None]
    result: Any = None
    completed: bool = False


class CompensatingTransaction:
    """Saga-style compensating transaction manager.

    Steps are added with :meth:`add_step` and executed in order with
    :meth:`execute`.  If any step raises, all already-completed steps'
    compensating functions are run in reverse (LIFO) order.

    Example::

        saga = (
            CompensatingTransaction()
            .add_step("reserve", lambda: "reserved", lambda: print("unreserve"))
            .add_step("charge",  lambda: "charged",  lambda: print("refund"))
        )
        ok, errors = saga.execute()
        print(ok)      # True
        print(errors)  # []
    """

    # Expose the step dataclass under a predictable name.
    Step = _CompensatingStep

    def __init__(self) -> None:
        self._steps: list[_CompensatingStep] = []

    # ------------------------------------------------------------------
    # Builder
    # ------------------------------------------------------------------

    def add_step(
        self,
        name: str,
        forward_fn: Callable[[], Any],
        compensate_fn: Callable[[], None],
    ) -> CompensatingTransaction:
        """Append a step to the transaction.

        Args:
            name:          Human-readable step identifier.
            forward_fn:    Callable executed in the forward pass.
            compensate_fn: Callable executed if a later step fails.

        Returns:
            *self* for method chaining.
        """
        self._steps.append(
            _CompensatingStep(
                name=name,
                forward_fn=forward_fn,
                compensate_fn=compensate_fn,
            )
        )
        return self

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def execute(self) -> tuple[bool, list[str]]:
        """Execute all steps in order; compensate on failure.

        If a step's ``forward_fn`` raises, all previously completed steps'
        ``compensate_fn`` are called in reverse (LIFO) order.

        Returns:
            Tuple ``(success, errors)`` where *success* is ``True`` if all
            steps completed and *errors* is an empty list.  On failure
            *success* is ``False`` and *errors* contains the error message(s)
            from the failed step.
        """
        completed_so_far: list[_CompensatingStep] = []
        for step in self._steps:
            try:
                step.result = step.forward_fn()
                step.completed = True
                completed_so_far.append(step)
            except Exception as exc:  # noqa: BLE001
                # Run compensation in reverse order for completed steps only.
                for done in reversed(completed_so_far):
                    done.compensate_fn()
                    done.completed = False
                return False, [str(exc)]
        return True, []

    # ------------------------------------------------------------------
    # Accessors
    # ------------------------------------------------------------------

    def executed_steps(self) -> list[str]:
        """Return names of steps that completed successfully (not compensated).

        Returns:
            List of step names whose ``completed`` flag is ``True``.
        """
        return [s.name for s in self._steps if s.completed]

    def reset(self) -> CompensatingTransaction:
        """Clear all step results and re-arm for re-execution.

        Returns:
            *self* for method chaining.
        """
        for step in self._steps:
            step.result = None
            step.completed = False
        return self


# ===========================================================================
# Pattern 3 — DistributedLock
# ===========================================================================


class DistributedLock:
    """Token-based distributed lock with TTL and reentrant support.

    Uses an internal ``threading.Lock`` to protect state mutations.  A lock
    that has passed its expiry time is treated as free (any caller may take
    it over).  The same owner may re-acquire the lock without blocking;
    depth is tracked and the lock is only released when depth reaches zero.

    Example::

        lock = DistributedLock("inventory-lock", ttl_seconds=10.0)
        assert lock.acquire("svc-A")
        assert lock.is_held()
        assert lock.holder() == "svc-A"
        assert lock.release("svc-A")
        assert not lock.is_held()
    """

    def __init__(self, lock_id: str, ttl_seconds: float = 30.0) -> None:
        """Initialise a :class:`DistributedLock`.

        Args:
            lock_id:     Unique identifier for this lock resource.
            ttl_seconds: Time-to-live in seconds after which the lock
                         expires automatically.  Must be > 0.
        """
        self.lock_id = lock_id
        self.ttl_seconds = ttl_seconds

        self._mutex = threading.Lock()
        self._owner: str | None = None
        self._expiry: float = 0.0  # epoch timestamp
        self._depth: int = 0  # reentrant depth counter

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _is_expired(self) -> bool:
        """Return True if the lock has passed its TTL (caller holds _mutex)."""
        return self._owner is not None and time.monotonic() > self._expiry

    # ------------------------------------------------------------------
    # Acquire / Release
    # ------------------------------------------------------------------

    def acquire(self, owner: str, timeout_seconds: float = 5.0) -> bool:
        """Acquire the lock on behalf of *owner*.

        Behaviour:
        * If the lock is free or expired: take it and return ``True``.
        * If held by *owner* (reentrant): increment depth, return ``True``.
        * If held by another owner and not expired: retry until
          *timeout_seconds* elapses; return ``False`` on timeout.

        Args:
            owner:           Caller identity token.
            timeout_seconds: How long to wait before giving up.

        Returns:
            ``True`` if the lock was acquired, ``False`` on timeout.
        """
        deadline = time.monotonic() + timeout_seconds
        while True:
            with self._mutex:
                # Free or expired
                if self._owner is None or self._is_expired():
                    self._owner = owner
                    self._expiry = time.monotonic() + self.ttl_seconds
                    self._depth = 1
                    return True
                # Reentrant — same owner
                if self._owner == owner:
                    self._depth += 1
                    return True
                # Held by another — check timeout
                if time.monotonic() >= deadline:
                    return False
            time.sleep(0.005)

    def release(self, owner: str) -> bool:
        """Release the lock held by *owner*.

        For reentrant acquisitions this decrements the depth counter;
        the lock is only freed when depth reaches zero.

        Args:
            owner: Must match the current lock owner.

        Returns:
            ``True`` if the lock was released (depth reached zero).
            ``False`` if *owner* does not hold the lock (already expired,
            or a different owner).

        Raises:
            ValueError: If the lock is not currently held at all.
        """
        with self._mutex:
            if self._owner is None or self._is_expired():
                raise ValueError(f"Lock {self.lock_id!r} is not currently held")
            if self._owner != owner:
                return False
            self._depth -= 1
            if self._depth <= 0:
                self._owner = None
                self._expiry = 0.0
                self._depth = 0
                return True
            return True

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def is_held(self) -> bool:
        """Return ``True`` if the lock is currently held and not expired.

        Returns:
            ``True`` when locked and within TTL, ``False`` otherwise.
        """
        with self._mutex:
            return self._owner is not None and not self._is_expired()

    def holder(self) -> str | None:
        """Return the current owner, or ``None`` if free / expired.

        Returns:
            Owner token string, or ``None``.
        """
        with self._mutex:
            if self._owner is None or self._is_expired():
                return None
            return self._owner

    def extend(self, owner: str, extra_seconds: float) -> bool:
        """Extend the TTL by *extra_seconds* if *owner* holds the lock.

        Args:
            owner:         Must match the current lock owner.
            extra_seconds: Additional seconds to add to the current expiry.

        Returns:
            ``True`` if the TTL was extended, ``False`` if *owner* does not
            hold the lock or the lock has expired.
        """
        with self._mutex:
            if self._owner != owner or self._is_expired():
                return False
            self._expiry += extra_seconds
            return True


# ===========================================================================
# Pattern 4 — TransactionCoordinator
# ===========================================================================


class TransactionCoordinator:
    """Coordinates multiple distributed operations with rollback tracking.

    Each transaction is identified by a UUID.  Resources enlist their
    rollback callbacks via :meth:`enlist`; on :meth:`rollback` the
    coordinator calls them in LIFO (last-enlisted-first) order.

    Example::

        coord = TransactionCoordinator("svc-coord")
        tx = coord.begin()
        coord.enlist(tx, "db", lambda: print("rollback db"))
        coord.enlist(tx, "queue", lambda: print("rollback queue"))
        rolled = coord.rollback(tx)
        print(rolled)   # ['queue', 'db']
    """

    def __init__(self, coordinator_id: str) -> None:
        """Initialise a :class:`TransactionCoordinator`.

        Args:
            coordinator_id: Unique identifier for this coordinator instance.
        """
        self.coordinator_id = coordinator_id
        # transaction_id → list of (resource_name, rollback_fn) in enlist order
        self._registry: dict[str, list[tuple[str, Callable[[], None]]]] = {}
        self._committed: set[str] = set()
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Transaction lifecycle
    # ------------------------------------------------------------------

    def begin(self) -> str:
        """Start a new transaction and return its unique ID.

        Returns:
            UUID4 string identifying the new transaction.
        """
        tx_id = str(uuid.uuid4())
        with self._lock:
            self._registry[tx_id] = []
        return tx_id

    def enlist(
        self,
        transaction_id: str,
        resource_name: str,
        rollback_fn: Callable[[], None],
    ) -> None:
        """Register a resource's rollback callback for a transaction.

        Args:
            transaction_id: Must be an active (not yet committed) transaction.
            resource_name:  Human-readable resource identifier.
            rollback_fn:    Called if the transaction is rolled back.

        Raises:
            KeyError: If *transaction_id* is unknown or already committed.
        """
        with self._lock:
            if transaction_id not in self._registry:
                raise KeyError(f"Unknown transaction {transaction_id!r}")
            self._registry[transaction_id].append((resource_name, rollback_fn))

    def commit(self, transaction_id: str) -> None:
        """Mark a transaction as committed and clear its rollback registry.

        Args:
            transaction_id: Must be an active transaction.

        Raises:
            KeyError: If *transaction_id* is unknown.
        """
        with self._lock:
            if transaction_id not in self._registry:
                raise KeyError(f"Unknown transaction {transaction_id!r}")
            del self._registry[transaction_id]
            self._committed.add(transaction_id)

    def rollback(self, transaction_id: str) -> list[str]:
        """Execute all enlisted rollback callbacks in LIFO order.

        Args:
            transaction_id: Must be an active transaction.

        Returns:
            List of resource names whose rollback functions were called,
            in the order they were executed (LIFO of enlistment order).

        Raises:
            KeyError: If *transaction_id* is unknown.
        """
        with self._lock:
            if transaction_id not in self._registry:
                raise KeyError(f"Unknown transaction {transaction_id!r}")
            entries = list(self._registry[transaction_id])
            del self._registry[transaction_id]

        rolled_back: list[str] = []
        for resource_name, rollback_fn in reversed(entries):
            rollback_fn()
            rolled_back.append(resource_name)
        return rolled_back

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def active_transactions(self) -> list[str]:
        """Return a list of transaction IDs that have not yet been committed.

        Returns:
            List of active transaction ID strings.
        """
        with self._lock:
            return list(self._registry.keys())

    def transaction_resources(self, transaction_id: str) -> list[str]:
        """Return the resource names enlisted for a transaction.

        Args:
            transaction_id: Target transaction ID.

        Returns:
            List of resource name strings in enlistment order.

        Raises:
            KeyError: If *transaction_id* is unknown.
        """
        with self._lock:
            if transaction_id not in self._registry:
                raise KeyError(f"Unknown transaction {transaction_id!r}")
            return [name for name, _ in self._registry[transaction_id]]


# ===========================================================================
# Smoke-test helpers (used only in __main__)
# ===========================================================================


def _separator(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def _run_two_phase_commit_demo() -> None:
    """Scenario 1: 2PC with success and failure paths."""
    _separator("Scenario 1 — TwoPhaseCommit: success + failure")

    # --- Success path: all 3 participants vote True ---
    committed_log: list[str] = []
    rolled_log: list[str] = []

    tpc = TwoPhaseCommit("tx-success-001")
    for name in ("db", "cache", "queue"):
        _n = name  # capture

        def _prepare(n=_n):
            return True

        def _commit(n=_n):
            committed_log.append(n)

        def _rollback(n=_n):
            rolled_log.append(n)

        tpc.register(name, _prepare, _commit, _rollback)

    result = tpc.execute()
    print(f"All-prepare-True → execute()={result}")
    assert result is True
    assert sorted(committed_log) == ["cache", "db", "queue"]
    assert rolled_log == []

    st = tpc.status()
    print(f"status outcome: {st['outcome']}")
    assert st["outcome"] == "committed"
    assert all(p["committed"] for p in st["participants"])

    # --- Failure path: second participant refuses ---
    committed_log2: list[str] = []
    rolled_log2: list[str] = []

    tpc2 = TwoPhaseCommit("tx-abort-002")

    def _prepare_ok():
        return True

    def _prepare_no():
        return False

    def _noop_commit():
        committed_log2.append("committed")

    def _noop_rb(n):
        rolled_log2.append(n)

    tpc2.register("svc-A", _prepare_ok, _noop_commit, lambda: _noop_rb("svc-A"))
    tpc2.register("svc-B", _prepare_no, _noop_commit, lambda: _noop_rb("svc-B"))
    tpc2.register("svc-C", _prepare_ok, _noop_commit, lambda: _noop_rb("svc-C"))

    result2 = tpc2.execute()
    print(f"One-refuses → execute()={result2}")
    assert result2 is False
    assert committed_log2 == []

    st2 = tpc2.status()
    assert st2["outcome"] == "aborted"
    assert all(p["rolled_back"] for p in st2["participants"])
    print("PASS")


def _run_compensating_transaction_demo() -> None:
    """Scenario 2: Saga with full success and mid-flight failure."""
    _separator("Scenario 2 — CompensatingTransaction: saga pattern")

    # --- Full success ---
    log: list[str] = []

    saga = (
        CompensatingTransaction()
        .add_step("reserve-inventory", lambda: log.append("reserved") or "ok", lambda: log.append("unreserve"))
        .add_step("charge-card", lambda: log.append("charged") or "ok", lambda: log.append("refund"))
        .add_step("send-confirmation", lambda: log.append("sent") or "ok", lambda: log.append("cancel-email"))
    )
    ok, errors = saga.execute()
    print(f"Full success: ok={ok}, errors={errors}, log={log}")
    assert ok is True
    assert errors == []
    assert saga.executed_steps() == ["reserve-inventory", "charge-card", "send-confirmation"]

    # --- Step 2 fails → compensate step 1 only ---
    log2: list[str] = []

    saga2 = (
        CompensatingTransaction()
        .add_step("step-1", lambda: log2.append("step1-done") or "ok", lambda: log2.append("comp-1"))
        .add_step(
            "step-2",
            lambda: (_ for _ in ()).throw(RuntimeError("payment declined")),
            lambda: log2.append("comp-2"),
        )
        .add_step("step-3", lambda: log2.append("step3-done") or "ok", lambda: log2.append("comp-3"))
    )
    ok2, errors2 = saga2.execute()
    print(f"Step-2 failure: ok={ok2}, log={log2}")
    assert ok2 is False
    assert len(errors2) == 1
    # step-1 compensated; step-2 never completed; step-3 never ran
    assert "step1-done" in log2
    assert "comp-1" in log2
    assert "comp-2" not in log2
    assert "step3-done" not in log2

    # --- Reset and re-run ---
    saga.reset()
    ok3, _ = saga.execute()
    assert ok3 is True
    print("PASS")


def _run_distributed_lock_demo() -> None:
    """Scenario 3: DistributedLock with reentrant acquire."""
    _separator("Scenario 3 — DistributedLock: acquire / reentrant / TTL")

    lock = DistributedLock("resource-X", ttl_seconds=60.0)

    # Basic acquire / release
    assert lock.acquire("owner-A")
    assert lock.is_held()
    assert lock.holder() == "owner-A"
    print(f"Acquired by owner-A: holder={lock.holder()}")

    # Reentrant: same owner acquires again
    assert lock.acquire("owner-A")
    assert lock.release("owner-A")  # depth back to 1 — still held
    assert lock.is_held()

    # Different owner blocked (very short timeout)
    blocked = lock.acquire("owner-B", timeout_seconds=0.02)
    print(f"owner-B blocked: {blocked}")
    assert blocked is False

    # Release by owner-A frees the lock
    assert lock.release("owner-A")
    assert not lock.is_held()
    assert lock.holder() is None

    # Now owner-B can acquire
    assert lock.acquire("owner-B")
    assert lock.holder() == "owner-B"

    # Extend TTL
    extended = lock.extend("owner-B", 30.0)
    assert extended is True
    lock.release("owner-B")
    print("PASS")


def _run_transaction_coordinator_demo() -> None:
    """Scenario 4: TransactionCoordinator commit + rollback."""
    _separator("Scenario 4 — TransactionCoordinator: commit + rollback")

    coord = TransactionCoordinator("coord-main")

    # --- Commit path ---
    tx1 = coord.begin()
    rollback_log: list[str] = []
    coord.enlist(tx1, "db", lambda: rollback_log.append("db-rollback"))
    coord.enlist(tx1, "search-index", lambda: rollback_log.append("index-rollback"))
    coord.commit(tx1)
    assert tx1 not in coord.active_transactions()
    print(f"Committed {tx1[:8]}…; active={len(coord.active_transactions())}")

    # --- Rollback path (LIFO order) ---
    tx2 = coord.begin()
    coord.enlist(tx2, "step-1", lambda: rollback_log.append("rb-step-1"))
    coord.enlist(tx2, "step-2", lambda: rollback_log.append("rb-step-2"))
    coord.enlist(tx2, "step-3", lambda: rollback_log.append("rb-step-3"))
    rolled = coord.rollback(tx2)
    print(f"Rollback LIFO order: {rolled}")
    assert rolled == ["step-3", "step-2", "step-1"]
    assert rollback_log == ["rb-step-3", "rb-step-2", "rb-step-1"]
    assert tx2 not in coord.active_transactions()

    # --- Multiple concurrent transactions ---
    ta = coord.begin()
    tb = coord.begin()
    assert len(coord.active_transactions()) == 2
    coord.commit(ta)
    assert len(coord.active_transactions()) == 1
    coord.commit(tb)
    assert len(coord.active_transactions()) == 0
    print("PASS")


# ===========================================================================
# Entry point
# ===========================================================================


if __name__ == "__main__":
    print("Distributed Transaction Patterns — Smoke Test Suite")
    print(
        "Patterns: TwoPhaseCommit | CompensatingTransaction"
        " | DistributedLock | TransactionCoordinator"
    )

    _run_two_phase_commit_demo()
    _run_compensating_transaction_demo()
    _run_distributed_lock_demo()
    _run_transaction_coordinator_demo()

    print("\n" + "=" * 60)
    print("  All scenarios passed.")
    print("=" * 60)
