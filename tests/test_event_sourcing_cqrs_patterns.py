"""
Tests for 22_event_sourcing_cqrs_patterns.py — event sourcing, CQRS,
optimistic concurrency, snapshot store, read-model projections, and
the event-sourced repository pattern.
"""

from __future__ import annotations

import importlib.util
import sys
import time
import types
from decimal import Decimal
from pathlib import Path

import pytest

_MOD_PATH = (
    Path(__file__).parent.parent / "examples" / "22_event_sourcing_cqrs_patterns.py"
)


def _load_module():
    module_name = "es_cqrs_patterns_22"
    if module_name in sys.modules:
        return sys.modules[module_name]
    spec = importlib.util.spec_from_file_location(module_name, _MOD_PATH)
    mod = types.ModuleType(module_name)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def m():
    return _load_module()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _open_account(m, account_id="acct-001", owner="Alice",
                  initial=Decimal("500.00")):
    """Return a freshly opened BankAccount aggregate."""
    acct = m.BankAccount(account_id)
    acct.open(owner, initial)
    return acct


def _make_repo(m, snapshot_threshold=10):
    """Return a fresh (EventStore, SnapshotStore, EventSourcedRepository) triple."""
    es = m.EventStore()
    ss = m.SnapshotStore()
    repo = m.EventSourcedRepository(es, ss, snapshot_threshold=snapshot_threshold)
    return es, ss, repo


# ===========================================================================
# TestBankAccountAggregate
# ===========================================================================


class TestBankAccountAggregate:
    """Tests for the BankAccount event-sourced aggregate."""

    def test_open_sets_owner_name(self, m):
        """open() should set the owner_name attribute on the aggregate."""
        acct = _open_account(m, owner="Alice")
        assert acct.owner_name == "Alice"

    def test_open_sets_balance(self, m):
        """open() should set the balance to the initial_deposit amount."""
        acct = _open_account(m, initial=Decimal("300.00"))
        assert acct.balance == Decimal("300.00")

    def test_open_sets_is_open_true(self, m):
        """open() should set is_open to True."""
        acct = _open_account(m)
        assert acct.is_open is True

    def test_open_produces_one_uncommitted_event(self, m):
        """open() should produce exactly one uncommitted event."""
        acct = _open_account(m)
        assert len(acct.uncommitted_events) == 1

    def test_open_event_type_is_account_opened(self, m):
        """The event raised by open() should have event_type 'AccountOpened'."""
        acct = _open_account(m)
        assert acct.uncommitted_events[0].event_type == "AccountOpened"

    def test_deposit_increases_balance(self, m):
        """deposit() should add the deposit amount to the current balance."""
        acct = _open_account(m, initial=Decimal("100.00"))
        acct.deposit(Decimal("50.00"))
        assert acct.balance == Decimal("150.00")

    def test_deposit_produces_money_deposited_event(self, m):
        """deposit() should raise a MoneyDeposited event."""
        acct = _open_account(m)
        acct.deposit(Decimal("25.00"))
        event_types = [e.event_type for e in acct.uncommitted_events]
        assert "MoneyDeposited" in event_types

    def test_withdraw_decreases_balance(self, m):
        """withdraw() should subtract the amount from the current balance."""
        acct = _open_account(m, initial=Decimal("200.00"))
        acct.withdraw(Decimal("75.00"))
        assert acct.balance == Decimal("125.00")

    def test_withdraw_raises_insufficient_funds_when_balance_too_low(self, m):
        """withdraw() should raise InsufficientFunds when balance < amount."""
        acct = _open_account(m, initial=Decimal("50.00"))
        with pytest.raises(m.InsufficientFunds):
            acct.withdraw(Decimal("100.00"))

    def test_close_sets_is_open_false(self, m):
        """close() should set is_open to False."""
        acct = _open_account(m)
        acct.close()
        assert acct.is_open is False

    def test_close_produces_account_closed_event(self, m):
        """close() should raise an AccountClosed event."""
        acct = _open_account(m)
        acct.close()
        event_types = [e.event_type for e in acct.uncommitted_events]
        assert "AccountClosed" in event_types

    def test_deposit_on_closed_account_raises_account_already_closed(self, m):
        """deposit() on a closed account should raise AccountAlreadyClosed."""
        acct = _open_account(m)
        acct.close()
        with pytest.raises(m.AccountAlreadyClosed):
            acct.deposit(Decimal("10.00"))

    def test_withdraw_on_closed_account_raises_account_already_closed(self, m):
        """withdraw() on a closed account should raise AccountAlreadyClosed."""
        acct = _open_account(m, initial=Decimal("500.00"))
        acct.close()
        with pytest.raises(m.AccountAlreadyClosed):
            acct.withdraw(Decimal("10.00"))

    def test_mark_committed_clears_uncommitted_events(self, m):
        """mark_committed() should empty the uncommitted_events list."""
        acct = _open_account(m)
        acct.deposit(Decimal("20.00"))
        assert len(acct.uncommitted_events) > 0
        acct.mark_committed()
        assert acct.uncommitted_events == []


# ===========================================================================
# TestEventStore
# ===========================================================================


class TestEventStore:
    """Tests for the EventStore append-only log with optimistic concurrency."""

    def test_append_with_correct_expected_version_succeeds(self, m):
        """append() with expected_version=0 on a new stream should not raise."""
        store = m.EventStore()
        acct = _open_account(m, account_id="es-001")
        store.append("es-001", acct.uncommitted_events, expected_version=0)

    def test_load_returns_appended_events(self, m):
        """load() should return events that were previously appended."""
        store = m.EventStore()
        acct = _open_account(m, account_id="es-002")
        events = acct.uncommitted_events
        store.append("es-002", events, expected_version=0)
        loaded = store.load("es-002")
        assert len(loaded) == 1
        assert loaded[0].event_type == "AccountOpened"

    def test_get_version_returns_zero_for_unknown_aggregate(self, m):
        """get_version() on an unseen aggregate_id should return 0."""
        store = m.EventStore()
        assert store.get_version("no-such-id") == 0

    def test_get_version_returns_event_count_after_appends(self, m):
        """get_version() should return the total number of committed events."""
        store = m.EventStore()
        acct = _open_account(m, account_id="es-003")
        acct.deposit(Decimal("10.00"))
        # 2 uncommitted events: AccountOpened + MoneyDeposited
        store.append("es-003", acct.uncommitted_events, expected_version=0)
        assert store.get_version("es-003") == 2

    def test_append_with_wrong_expected_version_raises_concurrency_conflict(self, m):
        """append() with a stale expected_version should raise ConcurrencyConflict."""
        store = m.EventStore()
        acct = _open_account(m, account_id="es-004")
        store.append("es-004", acct.uncommitted_events, expected_version=0)
        # Second append with expected_version=0 again — stale
        acct2 = _open_account(m, account_id="es-004")
        with pytest.raises(m.ConcurrencyConflict):
            store.append("es-004", acct2.uncommitted_events, expected_version=0)

    def test_concurrency_conflict_has_expected_and_actual_version_attributes(self, m):
        """ConcurrencyConflict should expose expected_version and actual_version."""
        store = m.EventStore()
        acct = _open_account(m, account_id="es-005")
        store.append("es-005", acct.uncommitted_events, expected_version=0)
        acct2 = _open_account(m, account_id="es-005")
        with pytest.raises(m.ConcurrencyConflict) as exc_info:
            store.append("es-005", acct2.uncommitted_events, expected_version=0)
        err = exc_info.value
        assert hasattr(err, "expected_version")
        assert hasattr(err, "actual_version")
        assert err.expected_version == 0
        assert err.actual_version == 1

    def test_load_from_version_returns_only_events_after_that_version(self, m):
        """load(from_version=N) should exclude events with version <= N."""
        store = m.EventStore()
        acct = _open_account(m, account_id="es-006", initial=Decimal("100.00"))
        acct.deposit(Decimal("50.00"))
        acct.deposit(Decimal("25.00"))
        # 3 events total: versions 1, 2, 3
        store.append("es-006", acct.uncommitted_events, expected_version=0)
        delta = store.load("es-006", from_version=1)
        # Should return only events with version > 1
        assert all(e.version > 1 for e in delta)
        assert len(delta) == 2

    def test_stream_ids_lists_all_aggregate_ids(self, m):
        """stream_ids() should include every aggregate_id that has been appended."""
        store = m.EventStore()
        for aid in ("es-x", "es-y", "es-z"):
            acct = _open_account(m, account_id=aid)
            store.append(aid, acct.uncommitted_events, expected_version=0)
        ids = store.stream_ids()
        assert "es-x" in ids
        assert "es-y" in ids
        assert "es-z" in ids


# ===========================================================================
# TestSnapshotStore
# ===========================================================================


class TestSnapshotStore:
    """Tests for SnapshotStore: point-in-time aggregate state persistence."""

    def test_save_and_load_returns_snapshot(self, m):
        """save() followed by load() should return the saved Snapshot."""
        store = m.SnapshotStore()
        snap = m.Snapshot(
            aggregate_id="snap-001",
            version=10,
            state={"balance": "500.00", "owner_name": "Bob", "is_open": True},
            taken_at=time.time(),
        )
        store.save(snap)
        loaded = store.load("snap-001")
        assert loaded is snap

    def test_load_returns_none_for_unknown_aggregate(self, m):
        """load() for an unseen aggregate_id should return None."""
        store = m.SnapshotStore()
        assert store.load("never-saved") is None

    def test_save_overwrites_previous_snapshot(self, m):
        """A second save() for the same aggregate_id should replace the first."""
        store = m.SnapshotStore()
        snap1 = m.Snapshot("snap-002", 10, {"balance": "100.00", "owner_name": "A",
                                             "is_open": True}, time.time())
        snap2 = m.Snapshot("snap-002", 20, {"balance": "200.00", "owner_name": "A",
                                             "is_open": True}, time.time())
        store.save(snap1)
        store.save(snap2)
        loaded = store.load("snap-002")
        assert loaded.version == 20

    def test_snapshot_has_required_fields(self, m):
        """Snapshot should expose aggregate_id, version, state, and taken_at."""
        now = time.time()
        snap = m.Snapshot(
            aggregate_id="snap-003",
            version=5,
            state={"balance": "50.00", "owner_name": "Carol", "is_open": False},
            taken_at=now,
        )
        assert snap.aggregate_id == "snap-003"
        assert snap.version == 5
        assert isinstance(snap.state, dict)
        assert snap.taken_at == now


# ===========================================================================
# TestAccountBalanceProjection
# ===========================================================================


class TestAccountBalanceProjection:
    """Tests for the CQRS read-model projection over account events."""

    def test_apply_account_opened_sets_balance(self, m):
        """Applying AccountOpened should record the initial balance."""
        proj = m.AccountBalanceProjection()
        acct = _open_account(m, account_id="proj-001", initial=Decimal("300.00"))
        for event in acct.uncommitted_events:
            proj.apply(event)
        assert proj.get_balance("proj-001") == Decimal("300.00")

    def test_apply_account_opened_sets_owner(self, m):
        """Applying AccountOpened should record the owner name."""
        proj = m.AccountBalanceProjection()
        acct = _open_account(m, account_id="proj-002", owner="Dave")
        for event in acct.uncommitted_events:
            proj.apply(event)
        assert proj.get_owner("proj-002") == "Dave"

    def test_apply_money_deposited_increases_balance(self, m):
        """Applying MoneyDeposited should increase the projected balance."""
        proj = m.AccountBalanceProjection()
        acct = _open_account(m, account_id="proj-003", initial=Decimal("100.00"))
        acct.deposit(Decimal("40.00"))
        for event in acct.uncommitted_events:
            proj.apply(event)
        assert proj.get_balance("proj-003") == Decimal("140.00")

    def test_apply_money_withdrawn_decreases_balance(self, m):
        """Applying MoneyWithdrawn should decrease the projected balance."""
        proj = m.AccountBalanceProjection()
        acct = _open_account(m, account_id="proj-004", initial=Decimal("200.00"))
        acct.withdraw(Decimal("60.00"))
        for event in acct.uncommitted_events:
            proj.apply(event)
        assert proj.get_balance("proj-004") == Decimal("140.00")

    def test_apply_account_closed_removes_from_open_accounts(self, m):
        """Applying AccountClosed should remove the account from open_accounts."""
        proj = m.AccountBalanceProjection()
        acct = _open_account(m, account_id="proj-005")
        acct.close()
        for event in acct.uncommitted_events:
            proj.apply(event)
        assert "proj-005" not in proj.get_open_accounts()

    def test_rebuild_from_event_list_produces_correct_final_balance(self, m):
        """rebuild() over a full event list should yield the correct net balance."""
        proj = m.AccountBalanceProjection()
        acct = _open_account(m, account_id="proj-006", initial=Decimal("1000.00"))
        acct.deposit(Decimal("200.00"))
        acct.withdraw(Decimal("150.00"))
        # Expected: 1000 + 200 - 150 = 1050
        all_events = acct.uncommitted_events
        proj.rebuild(all_events)
        assert proj.get_balance("proj-006") == Decimal("1050.00")

    def test_reset_clears_all_state_and_get_balance_returns_none(self, m):
        """reset() should clear all projection state; get_balance returns None."""
        proj = m.AccountBalanceProjection()
        acct = _open_account(m, account_id="proj-007", initial=Decimal("500.00"))
        for event in acct.uncommitted_events:
            proj.apply(event)
        assert proj.get_balance("proj-007") is not None
        proj.reset()
        assert proj.get_balance("proj-007") is None
        assert proj.get_open_accounts() == []


# ===========================================================================
# TestEventSourcedRepository
# ===========================================================================


class TestEventSourcedRepository:
    """Tests for EventSourcedRepository: combined save/load via EventStore + SnapshotStore."""

    def test_save_persists_aggregate_events_to_event_store(self, m):
        """save() should append the aggregate's uncommitted events to the EventStore."""
        es, ss, repo = _make_repo(m)
        acct = _open_account(m, account_id="repo-001")
        repo.save(acct)
        assert es.get_version("repo-001") == 1

    def test_load_reconstructs_aggregate_with_correct_balance(self, m):
        """load() should return a BankAccount with state matching the saved events."""
        _, _, repo = _make_repo(m)
        acct = _open_account(m, account_id="repo-002", initial=Decimal("400.00"))
        acct.deposit(Decimal("100.00"))
        repo.save(acct)
        loaded = repo.load("repo-002")
        assert loaded.balance == Decimal("500.00")
        assert loaded.owner_name == "Alice"

    def test_load_raises_account_not_found_for_unknown_id(self, m):
        """load() on an aggregate that was never saved should raise AccountNotFound."""
        _, _, repo = _make_repo(m)
        with pytest.raises(m.AccountNotFound):
            repo.load("never-persisted")

    def test_save_then_load_is_identity_for_bank_account_state(self, m):
        """Round-tripping save → load should preserve all relevant state fields."""
        _, _, repo = _make_repo(m)
        acct = _open_account(m, account_id="repo-004",
                              owner="Eve", initial=Decimal("750.00"))
        acct.deposit(Decimal("250.00"))
        acct.withdraw(Decimal("100.00"))
        repo.save(acct)
        loaded = repo.load("repo-004")
        assert loaded.balance == Decimal("900.00")
        assert loaded.owner_name == "Eve"
        assert loaded.is_open is True
        assert loaded.version == acct.version
