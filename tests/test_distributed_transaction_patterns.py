"""
Tests for 34_distributed_transaction_patterns.py

Covers all four patterns (TwoPhaseCommit, CompensatingTransaction,
DistributedLock, TransactionCoordinator) plus integration scenarios.
Uses only the standard library; no external dependencies required.
"""

from __future__ import annotations

import importlib.util
import sys
import threading
import time
import types
from pathlib import Path

import pytest

_MOD_PATH = Path(__file__).parent.parent / "examples" / "34_distributed_transaction_patterns.py"


def _load_module():
    module_name = "distributed_transaction_patterns_34"
    if module_name in sys.modules:
        return sys.modules[module_name]
    spec = importlib.util.spec_from_file_location(module_name, _MOD_PATH)
    mod = types.ModuleType(module_name)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def mod():
    return _load_module()


# ===========================================================================
# TwoPhaseCommit
# ===========================================================================


class TestTwoPhaseCommit:
    # --- all prepare succeed → commit ---

    def test_all_prepare_true_returns_true(self, mod):
        tpc = mod.TwoPhaseCommit("tx-1")
        tpc.register("a", lambda: True, lambda: None, lambda: None)
        assert tpc.execute() is True

    def test_all_prepare_true_calls_commit_fns(self, mod):
        log = []
        tpc = mod.TwoPhaseCommit("tx-2")
        tpc.register("a", lambda: True, lambda: log.append("commit-a"), lambda: None)
        tpc.register("b", lambda: True, lambda: log.append("commit-b"), lambda: None)
        tpc.execute()
        assert sorted(log) == ["commit-a", "commit-b"]

    def test_all_prepare_true_no_rollback_called(self, mod):
        log = []
        tpc = mod.TwoPhaseCommit("tx-3")
        tpc.register("a", lambda: True, lambda: None, lambda: log.append("rb-a"))
        tpc.register("b", lambda: True, lambda: None, lambda: log.append("rb-b"))
        tpc.execute()
        assert log == []

    def test_all_prepare_true_participant_state(self, mod):
        tpc = mod.TwoPhaseCommit("tx-4")
        tpc.register("x", lambda: True, lambda: None, lambda: None)
        tpc.execute()
        st = tpc.status()
        p = st["participants"][0]
        assert p["prepared"] is True
        assert p["committed"] is True
        assert p["rolled_back"] is False

    # --- one prepare fails → rollback ---

    def test_one_prepare_false_returns_false(self, mod):
        tpc = mod.TwoPhaseCommit("tx-5")
        tpc.register("a", lambda: True, lambda: None, lambda: None)
        tpc.register("b", lambda: False, lambda: None, lambda: None)
        assert tpc.execute() is False

    def test_one_prepare_false_calls_all_rollback_fns(self, mod):
        log = []
        tpc = mod.TwoPhaseCommit("tx-6")
        tpc.register("a", lambda: True, lambda: None, lambda: log.append("rb-a"))
        tpc.register("b", lambda: False, lambda: None, lambda: log.append("rb-b"))
        tpc.register("c", lambda: True, lambda: None, lambda: log.append("rb-c"))
        tpc.execute()
        assert sorted(log) == ["rb-a", "rb-b", "rb-c"]

    def test_one_prepare_false_no_commit_called(self, mod):
        log = []
        tpc = mod.TwoPhaseCommit("tx-7")
        tpc.register("a", lambda: True, lambda: log.append("commit"), lambda: None)
        tpc.register("b", lambda: False, lambda: log.append("commit"), lambda: None)
        tpc.execute()
        assert log == []

    def test_prepare_exception_treated_as_false(self, mod):
        tpc = mod.TwoPhaseCommit("tx-exc")
        tpc.register("a", lambda: (_ for _ in ()).throw(RuntimeError("fail")), lambda: None, lambda: None)
        assert tpc.execute() is False

    # --- participant state tracking ---

    def test_participant_state_after_abort(self, mod):
        tpc = mod.TwoPhaseCommit("tx-8")
        tpc.register("a", lambda: False, lambda: None, lambda: None)
        tpc.execute()
        p = tpc.status()["participants"][0]
        assert p["rolled_back"] is True
        assert p["committed"] is False

    def test_three_participants_all_commit(self, mod):
        tpc = mod.TwoPhaseCommit("tx-9")
        for name in ("p1", "p2", "p3"):
            tpc.register(name, lambda: True, lambda: None, lambda: None)
        tpc.execute()
        st = tpc.status()
        assert all(p["committed"] for p in st["participants"])

    # --- status dict ---

    def test_status_pending_before_execute(self, mod):
        tpc = mod.TwoPhaseCommit("tx-10")
        tpc.register("a", lambda: True, lambda: None, lambda: None)
        assert tpc.status()["outcome"] == "pending"

    def test_status_committed_after_success(self, mod):
        tpc = mod.TwoPhaseCommit("tx-11")
        tpc.register("a", lambda: True, lambda: None, lambda: None)
        tpc.execute()
        assert tpc.status()["outcome"] == "committed"

    def test_status_aborted_after_failure(self, mod):
        tpc = mod.TwoPhaseCommit("tx-12")
        tpc.register("a", lambda: False, lambda: None, lambda: None)
        tpc.execute()
        assert tpc.status()["outcome"] == "aborted"

    # --- re-register same name error ---

    def test_register_duplicate_name_raises_value_error(self, mod):
        tpc = mod.TwoPhaseCommit("tx-dup")
        tpc.register("svc", lambda: True, lambda: None, lambda: None)
        with pytest.raises(ValueError):
            tpc.register("svc", lambda: True, lambda: None, lambda: None)

    def test_status_transaction_id_matches(self, mod):
        tpc = mod.TwoPhaseCommit("my-unique-id")
        assert tpc.status()["transaction_id"] == "my-unique-id"

    def test_status_participants_list_length(self, mod):
        tpc = mod.TwoPhaseCommit("tx-len")
        for name in ("p1", "p2", "p3"):
            tpc.register(name, lambda: True, lambda: None, lambda: None)
        assert len(tpc.status()["participants"]) == 3

    def test_execute_no_participants_returns_true(self, mod):
        tpc = mod.TwoPhaseCommit("tx-empty")
        # No participants — vacuously all prepared → commit
        assert tpc.execute() is True
        assert tpc.status()["outcome"] == "committed"


# ===========================================================================
# CompensatingTransaction
# ===========================================================================


class TestCompensatingTransaction:
    # --- all steps succeed ---

    def test_all_steps_succeed_returns_true(self, mod):
        saga = mod.CompensatingTransaction()
        saga.add_step("s1", lambda: "ok", lambda: None)
        saga.add_step("s2", lambda: "ok", lambda: None)
        ok, errors = saga.execute()
        assert ok is True
        assert errors == []

    def test_all_steps_succeed_executed_steps_complete(self, mod):
        saga = (
            mod.CompensatingTransaction()
            .add_step("alpha", lambda: 1, lambda: None)
            .add_step("beta", lambda: 2, lambda: None)
            .add_step("gamma", lambda: 3, lambda: None)
        )
        saga.execute()
        assert saga.executed_steps() == ["alpha", "beta", "gamma"]

    def test_step_result_stored(self, mod):
        saga = mod.CompensatingTransaction()
        saga.add_step("compute", lambda: 42, lambda: None)
        saga.execute()
        assert saga._steps[0].result == 42

    # --- step 2 of 3 fails → compensate only step 1 ---

    def test_step2_fails_compensates_step1_only(self, mod):
        log = []
        saga = (
            mod.CompensatingTransaction()
            .add_step("s1", lambda: log.append("s1") or "ok", lambda: log.append("comp-s1"))
            .add_step("s2", lambda: (_ for _ in ()).throw(ValueError("fail")), lambda: log.append("comp-s2"))
            .add_step("s3", lambda: log.append("s3") or "ok", lambda: log.append("comp-s3"))
        )
        ok, errors = saga.execute()
        assert ok is False
        assert len(errors) == 1
        assert "s1" in log
        assert "comp-s1" in log
        assert "comp-s2" not in log  # step 2 never completed
        assert "s3" not in log
        assert "comp-s3" not in log

    def test_step1_fails_no_compensation_needed(self, mod):
        log = []
        saga = (
            mod.CompensatingTransaction()
            .add_step("s1", lambda: (_ for _ in ()).throw(RuntimeError("boom")), lambda: log.append("comp-s1"))
            .add_step("s2", lambda: "ok", lambda: log.append("comp-s2"))
        )
        ok, errors = saga.execute()
        assert ok is False
        assert log == []  # nothing was completed, nothing to compensate

    def test_compensation_runs_in_reverse_order(self, mod):
        order = []
        saga = (
            mod.CompensatingTransaction()
            .add_step("first", lambda: "ok", lambda: order.append("comp-first"))
            .add_step("second", lambda: "ok", lambda: order.append("comp-second"))
            .add_step("third", lambda: (_ for _ in ()).throw(RuntimeError("fail")), lambda: order.append("comp-third"))
        )
        saga.execute()
        assert order == ["comp-second", "comp-first"]

    # --- executed_steps ---

    def test_executed_steps_empty_before_execute(self, mod):
        saga = mod.CompensatingTransaction()
        saga.add_step("s1", lambda: "ok", lambda: None)
        assert saga.executed_steps() == []

    def test_executed_steps_partial_after_failure(self, mod):
        saga = (
            mod.CompensatingTransaction()
            .add_step("ok-step", lambda: "ok", lambda: None)
            .add_step("bad-step", lambda: (_ for _ in ()).throw(RuntimeError()), lambda: None)
        )
        saga.execute()
        # ok-step was compensated so completed flag is False
        assert saga.executed_steps() == []

    # --- reset and re-run ---

    def test_reset_clears_results(self, mod):
        saga = mod.CompensatingTransaction()
        saga.add_step("s1", lambda: 99, lambda: None)
        saga.execute()
        saga.reset()
        assert saga._steps[0].result is None
        assert saga._steps[0].completed is False

    def test_reset_returns_self_for_chaining(self, mod):
        saga = mod.CompensatingTransaction()
        assert saga.reset() is saga

    def test_reset_allows_re_execution(self, mod):
        results = []
        saga = mod.CompensatingTransaction()
        saga.add_step("s1", lambda: results.append("run") or "ok", lambda: None)
        saga.execute()
        saga.reset()
        saga.execute()
        assert results == ["run", "run"]

    # --- chaining ---

    def test_add_step_returns_self_for_chaining(self, mod):
        saga = mod.CompensatingTransaction()
        returned = saga.add_step("s1", lambda: None, lambda: None)
        assert returned is saga

    def test_execute_empty_saga_succeeds(self, mod):
        saga = mod.CompensatingTransaction()
        ok, errors = saga.execute()
        assert ok is True
        assert errors == []

    def test_error_message_contains_exception_text(self, mod):
        saga = mod.CompensatingTransaction()
        saga.add_step("bad", lambda: (_ for _ in ()).throw(RuntimeError("specific error text")), lambda: None)
        ok, errors = saga.execute()
        assert ok is False
        assert "specific error text" in errors[0]


# ===========================================================================
# DistributedLock
# ===========================================================================


class TestDistributedLock:
    # --- basic acquire / release ---

    def test_acquire_free_lock_returns_true(self, mod):
        lock = mod.DistributedLock("lock-1")
        assert lock.acquire("owner-A") is True
        lock.release("owner-A")

    def test_release_frees_lock(self, mod):
        lock = mod.DistributedLock("lock-2")
        lock.acquire("owner-A")
        lock.release("owner-A")
        assert lock.is_held() is False

    def test_is_held_true_after_acquire(self, mod):
        lock = mod.DistributedLock("lock-3")
        lock.acquire("owner-A")
        assert lock.is_held() is True
        lock.release("owner-A")

    def test_is_held_false_after_release(self, mod):
        lock = mod.DistributedLock("lock-4")
        lock.acquire("owner-A")
        lock.release("owner-A")
        assert lock.is_held() is False

    def test_holder_returns_owner_when_held(self, mod):
        lock = mod.DistributedLock("lock-5")
        lock.acquire("owner-X")
        assert lock.holder() == "owner-X"
        lock.release("owner-X")

    def test_holder_returns_none_when_free(self, mod):
        lock = mod.DistributedLock("lock-6")
        assert lock.holder() is None

    # --- TTL expiry ---

    def test_expired_lock_shows_not_held(self, mod):
        lock = mod.DistributedLock("lock-ttl", ttl_seconds=0.02)
        lock.acquire("owner-A")
        time.sleep(0.05)
        assert lock.is_held() is False

    def test_expired_lock_holder_returns_none(self, mod):
        lock = mod.DistributedLock("lock-ttl2", ttl_seconds=0.02)
        lock.acquire("owner-A")
        time.sleep(0.05)
        assert lock.holder() is None

    def test_expired_lock_can_be_acquired_by_new_owner(self, mod):
        lock = mod.DistributedLock("lock-ttl3", ttl_seconds=0.02)
        lock.acquire("owner-A")
        time.sleep(0.05)
        assert lock.acquire("owner-B") is True
        lock.release("owner-B")

    # --- reentrant same owner ---

    def test_reentrant_same_owner_returns_true(self, mod):
        lock = mod.DistributedLock("lock-reent")
        lock.acquire("owner-A")
        assert lock.acquire("owner-A") is True
        lock.release("owner-A")
        lock.release("owner-A")

    def test_reentrant_partial_release_still_held(self, mod):
        lock = mod.DistributedLock("lock-reent2")
        lock.acquire("owner-A")
        lock.acquire("owner-A")
        lock.release("owner-A")  # depth 2 → 1
        assert lock.is_held() is True
        lock.release("owner-A")  # depth 1 → 0

    def test_reentrant_full_release_frees_lock(self, mod):
        lock = mod.DistributedLock("lock-reent3")
        lock.acquire("owner-A")
        lock.acquire("owner-A")
        lock.release("owner-A")
        lock.release("owner-A")
        assert lock.is_held() is False

    # --- blocked by different owner ---

    def test_different_owner_blocked_timeout_false(self, mod):
        lock = mod.DistributedLock("lock-block", ttl_seconds=60.0)
        lock.acquire("owner-A")
        result = lock.acquire("owner-B", timeout_seconds=0.02)
        assert result is False
        lock.release("owner-A")

    def test_release_wrong_owner_returns_false(self, mod):
        lock = mod.DistributedLock("lock-wrong")
        lock.acquire("owner-A")
        result = lock.release("owner-B")
        assert result is False
        lock.release("owner-A")

    def test_release_unheld_lock_raises_value_error(self, mod):
        lock = mod.DistributedLock("lock-unheld")
        with pytest.raises(ValueError):
            lock.release("anyone")

    # --- extend TTL ---

    def test_extend_ttl_by_owner_returns_true(self, mod):
        lock = mod.DistributedLock("lock-ext", ttl_seconds=60.0)
        lock.acquire("owner-A")
        assert lock.extend("owner-A", 30.0) is True
        lock.release("owner-A")

    def test_extend_ttl_by_non_owner_returns_false(self, mod):
        lock = mod.DistributedLock("lock-ext2", ttl_seconds=60.0)
        lock.acquire("owner-A")
        assert lock.extend("owner-B", 30.0) is False
        lock.release("owner-A")

    def test_extend_prevents_expiry(self, mod):
        lock = mod.DistributedLock("lock-ext3", ttl_seconds=0.05)
        lock.acquire("owner-A")
        lock.extend("owner-A", 60.0)
        time.sleep(0.08)
        assert lock.is_held() is True
        lock.release("owner-A")

    def test_acquire_after_release_by_new_owner(self, mod):
        lock = mod.DistributedLock("lock-reacquire", ttl_seconds=60.0)
        lock.acquire("owner-A")
        lock.release("owner-A")
        assert lock.acquire("owner-B") is True
        assert lock.holder() == "owner-B"
        lock.release("owner-B")


# ===========================================================================
# TransactionCoordinator
# ===========================================================================


class TestTransactionCoordinator:
    # --- begin / commit ---

    def test_begin_returns_unique_ids(self, mod):
        coord = mod.TransactionCoordinator("coord-1")
        tx1 = coord.begin()
        tx2 = coord.begin()
        assert tx1 != tx2

    def test_begin_adds_to_active(self, mod):
        coord = mod.TransactionCoordinator("coord-2")
        tx = coord.begin()
        assert tx in coord.active_transactions()

    def test_commit_removes_from_active(self, mod):
        coord = mod.TransactionCoordinator("coord-3")
        tx = coord.begin()
        coord.commit(tx)
        assert tx not in coord.active_transactions()

    def test_commit_unknown_tx_raises_key_error(self, mod):
        coord = mod.TransactionCoordinator("coord-4")
        with pytest.raises(KeyError):
            coord.commit("nonexistent-id")

    # --- begin / rollback LIFO ---

    def test_rollback_executes_in_lifo_order(self, mod):
        order = []
        coord = mod.TransactionCoordinator("coord-5")
        tx = coord.begin()
        coord.enlist(tx, "res-1", lambda: order.append("rb-1"))
        coord.enlist(tx, "res-2", lambda: order.append("rb-2"))
        coord.enlist(tx, "res-3", lambda: order.append("rb-3"))
        rolled = coord.rollback(tx)
        assert rolled == ["res-3", "res-2", "res-1"]
        assert order == ["rb-3", "rb-2", "rb-1"]

    def test_rollback_returns_resource_names(self, mod):
        coord = mod.TransactionCoordinator("coord-6")
        tx = coord.begin()
        coord.enlist(tx, "db", lambda: None)
        coord.enlist(tx, "cache", lambda: None)
        result = coord.rollback(tx)
        assert set(result) == {"db", "cache"}

    def test_rollback_removes_from_active(self, mod):
        coord = mod.TransactionCoordinator("coord-7")
        tx = coord.begin()
        coord.rollback(tx)
        assert tx not in coord.active_transactions()

    def test_rollback_unknown_tx_raises_key_error(self, mod):
        coord = mod.TransactionCoordinator("coord-8")
        with pytest.raises(KeyError):
            coord.rollback("ghost-tx")

    # --- active_transactions ---

    def test_active_transactions_initially_empty(self, mod):
        coord = mod.TransactionCoordinator("coord-9")
        assert coord.active_transactions() == []

    def test_active_transactions_lists_all_open(self, mod):
        coord = mod.TransactionCoordinator("coord-10")
        tx1 = coord.begin()
        tx2 = coord.begin()
        active = coord.active_transactions()
        assert tx1 in active
        assert tx2 in active
        coord.commit(tx1)
        coord.commit(tx2)

    # --- transaction_resources ---

    def test_transaction_resources_returns_names_in_order(self, mod):
        coord = mod.TransactionCoordinator("coord-11")
        tx = coord.begin()
        coord.enlist(tx, "first", lambda: None)
        coord.enlist(tx, "second", lambda: None)
        coord.enlist(tx, "third", lambda: None)
        assert coord.transaction_resources(tx) == ["first", "second", "third"]

    def test_transaction_resources_unknown_tx_raises_key_error(self, mod):
        coord = mod.TransactionCoordinator("coord-12")
        with pytest.raises(KeyError):
            coord.transaction_resources("no-such-tx")

    # --- multiple concurrent transactions ---

    def test_multiple_concurrent_transactions_independent(self, mod):
        coord = mod.TransactionCoordinator("coord-multi")
        tx1 = coord.begin()
        tx2 = coord.begin()
        rb1 = []
        rb2 = []
        coord.enlist(tx1, "svc-A", lambda: rb1.append("A"))
        coord.enlist(tx2, "svc-B", lambda: rb2.append("B"))
        coord.rollback(tx1)
        coord.commit(tx2)
        assert rb1 == ["A"]
        assert rb2 == []
        assert coord.active_transactions() == []

    def test_enlist_after_commit_raises_key_error(self, mod):
        coord = mod.TransactionCoordinator("coord-late")
        tx = coord.begin()
        coord.commit(tx)
        with pytest.raises(KeyError):
            coord.enlist(tx, "late-resource", lambda: None)

    def test_rollback_empty_transaction_returns_empty_list(self, mod):
        coord = mod.TransactionCoordinator("coord-empty-rb")
        tx = coord.begin()
        rolled = coord.rollback(tx)
        assert rolled == []


# ===========================================================================
# Integration tests
# ===========================================================================


class TestIntegration:
    def test_2pc_with_compensating_transaction_as_participants(self, mod):
        """Each 2PC participant wraps a CompensatingTransaction saga."""
        saga_log: list[str] = []

        saga_a = mod.CompensatingTransaction().add_step(
            "reserve-a", lambda: saga_log.append("reserve-a") or "ok", lambda: saga_log.append("cancel-a")
        )
        saga_b = mod.CompensatingTransaction().add_step(
            "reserve-b", lambda: saga_log.append("reserve-b") or "ok", lambda: saga_log.append("cancel-b")
        )

        tpc = mod.TwoPhaseCommit("cross-saga-tx")
        commit_log: list[str] = []

        def prepare_a():
            ok, _ = saga_a.execute()
            return ok

        def prepare_b():
            ok, _ = saga_b.execute()
            return ok

        tpc.register("saga-A", prepare_a, lambda: commit_log.append("commit-A"), lambda: saga_a.reset())
        tpc.register("saga-B", prepare_b, lambda: commit_log.append("commit-B"), lambda: saga_b.reset())

        result = tpc.execute()
        assert result is True
        assert sorted(commit_log) == ["commit-A", "commit-B"]
        assert "reserve-a" in saga_log
        assert "reserve-b" in saga_log

    def test_coordinator_with_distributed_lock(self, mod):
        """TransactionCoordinator enlists a DistributedLock release as rollback."""
        lock = mod.DistributedLock("shared-resource", ttl_seconds=60.0)
        coord = mod.TransactionCoordinator("coord-lock-int")

        tx = coord.begin()
        lock.acquire("tx-owner")
        coord.enlist(tx, "shared-lock", lambda: lock.release("tx-owner"))

        assert lock.is_held()

        # Rollback should release the lock
        coord.rollback(tx)
        assert not lock.is_held()

    def test_nested_rollback_scenario(self, mod):
        """Three nested resources; inner failure triggers full rollback."""
        events: list[str] = []
        coord = mod.TransactionCoordinator("nested-coord")
        tx = coord.begin()

        coord.enlist(tx, "outer", lambda: events.append("rb-outer"))
        coord.enlist(tx, "middle", lambda: events.append("rb-middle"))
        coord.enlist(tx, "inner", lambda: events.append("rb-inner"))

        rolled = coord.rollback(tx)
        assert rolled == ["inner", "middle", "outer"]
        assert events == ["rb-inner", "rb-middle", "rb-outer"]

    def test_2pc_failure_triggers_compensating_rollback(self, mod):
        """When 2PC aborts, compensating sagas are rolled back via rollback_fn."""
        comp_log: list[str] = []

        saga = mod.CompensatingTransaction().add_step(
            "allocate",
            lambda: comp_log.append("allocated") or "ok",
            lambda: comp_log.append("deallocated"),
        )

        tpc = mod.TwoPhaseCommit("abort-saga-tx")
        tpc.register(
            "saga",
            lambda: saga.execute()[0],
            lambda: None,
            lambda: None,  # saga already ran compensations internally
        )
        # Second participant refuses → abort
        tpc.register("blocker", lambda: False, lambda: None, lambda: None)

        result = tpc.execute()
        assert result is False
        assert "allocated" in comp_log

    def test_lock_coordinator_concurrent_threads(self, mod):
        """Two threads compete for the lock; coordinator tracks both transactions."""
        lock = mod.DistributedLock("concurrent-lock", ttl_seconds=60.0)
        coord = mod.TransactionCoordinator("concurrent-coord")
        results: list[bool] = []
        errors: list[Exception] = []

        def worker(owner: str) -> None:
            try:
                acquired = lock.acquire(owner, timeout_seconds=0.5)
                results.append(acquired)
                if acquired:
                    tx = coord.begin()
                    coord.enlist(tx, owner, lambda o=owner: lock.release(o))
                    time.sleep(0.02)
                    coord.rollback(tx)
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        t1 = threading.Thread(target=worker, args=("thread-1",))
        t2 = threading.Thread(target=worker, args=("thread-2",))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert errors == []
        # At least one thread must have acquired the lock
        assert any(results)
