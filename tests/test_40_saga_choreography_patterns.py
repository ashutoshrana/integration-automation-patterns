"""
Tests for 40_saga_choreography_patterns.py

Covers all five patterns:
  - SagaEvent (frozen dataclass, create factory, fields)
  - SagaParticipant (execute, compensate, counters)
  - SagaChoreographer (register_handler, start/complete/fail, publish_event,
                       get_saga_status, get_saga_events)
  - CompensationEngine (register_participant, record_execution,
                        compensate_all reverse order, compensation_count)
  - SagaMonitor (record_start/end, is_timed_out, get_stats,
                 get_timed_out_sagas)

Uses only the standard library.  No external dependencies required.
"""

from __future__ import annotations

import importlib.util
import sys
import time
import types
import uuid
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Module loading
# ---------------------------------------------------------------------------

_MOD_PATH = Path(__file__).parent.parent / "examples" / "40_saga_choreography_patterns.py"


def _load_module() -> types.ModuleType:
    module_name = "saga_choreography_patterns_40"
    if module_name in sys.modules:
        return sys.modules[module_name]
    spec = importlib.util.spec_from_file_location(module_name, _MOD_PATH)
    mod = types.ModuleType(module_name)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


@pytest.fixture(scope="module")
def mod() -> types.ModuleType:
    return _load_module()


# ===========================================================================
# SagaEvent — basic construction
# ===========================================================================


class TestSagaEventInit:
    def test_all_fields_stored(self, mod):
        eid = str(uuid.uuid4())
        sid = "saga-001"
        ev = mod.SagaEvent(
            event_id=eid,
            saga_id=sid,
            event_type="order_placed",
            payload={"order": 1},
            timestamp=1_000_000.0,
            sequence=2,
        )
        assert ev.event_id == eid
        assert ev.saga_id == sid
        assert ev.event_type == "order_placed"
        assert ev.payload == {"order": 1}
        assert ev.timestamp == 1_000_000.0
        assert ev.sequence == 2

    def test_default_timestamp_is_set(self, mod):
        before = time.time()
        ev = mod.SagaEvent(
            event_id="x",
            saga_id="s",
            event_type="t",
            payload={},
        )
        after = time.time()
        assert before <= ev.timestamp <= after

    def test_default_sequence_is_zero(self, mod):
        ev = mod.SagaEvent(event_id="x", saga_id="s", event_type="t", payload={})
        assert ev.sequence == 0

    def test_frozen_immutable_event_id(self, mod):
        ev = mod.SagaEvent(event_id="x", saga_id="s", event_type="t", payload={})
        with pytest.raises((AttributeError, TypeError)):
            ev.event_id = "y"  # type: ignore[misc]

    def test_frozen_immutable_event_type(self, mod):
        ev = mod.SagaEvent(event_id="x", saga_id="s", event_type="t", payload={})
        with pytest.raises((AttributeError, TypeError)):
            ev.event_type = "z"  # type: ignore[misc]

    def test_frozen_immutable_saga_id(self, mod):
        ev = mod.SagaEvent(event_id="x", saga_id="s", event_type="t", payload={})
        with pytest.raises((AttributeError, TypeError)):
            ev.saga_id = "new"  # type: ignore[misc]

    def test_two_events_are_equal_with_same_fields(self, mod):
        ev1 = mod.SagaEvent(event_id="a", saga_id="s", event_type="t", payload={}, timestamp=1.0, sequence=0)
        ev2 = mod.SagaEvent(event_id="a", saga_id="s", event_type="t", payload={}, timestamp=1.0, sequence=0)
        assert ev1 == ev2

    def test_different_event_ids_not_equal(self, mod):
        ev1 = mod.SagaEvent(event_id="a", saga_id="s", event_type="t", payload={}, timestamp=1.0, sequence=0)
        ev2 = mod.SagaEvent(event_id="b", saga_id="s", event_type="t", payload={}, timestamp=1.0, sequence=0)
        assert ev1 != ev2


# ===========================================================================
# SagaEvent — create factory
# ===========================================================================


class TestSagaEventCreate:
    def test_creates_instance(self, mod):
        ev = mod.SagaEvent.create("saga-1", "order_placed", {"item": "book"})
        assert isinstance(ev, mod.SagaEvent)

    def test_event_id_is_valid_uuid(self, mod):
        ev = mod.SagaEvent.create("saga-1", "order_placed", {})
        # Should parse as a valid UUID without raising
        parsed = uuid.UUID(ev.event_id)
        assert str(parsed) == ev.event_id

    def test_saga_id_preserved(self, mod):
        ev = mod.SagaEvent.create("my-saga-99", "payment_initiated", {})
        assert ev.saga_id == "my-saga-99"

    def test_event_type_preserved(self, mod):
        ev = mod.SagaEvent.create("s", "payment_completed", {"amount": 100})
        assert ev.event_type == "payment_completed"

    def test_payload_preserved(self, mod):
        payload = {"order_id": "O-42", "amount": 99.99}
        ev = mod.SagaEvent.create("s", "t", payload)
        assert ev.payload == payload

    def test_default_sequence_zero(self, mod):
        ev = mod.SagaEvent.create("s", "t", {})
        assert ev.sequence == 0

    def test_custom_sequence_preserved(self, mod):
        ev = mod.SagaEvent.create("s", "t", {}, sequence=5)
        assert ev.sequence == 5

    def test_two_creates_have_different_event_ids(self, mod):
        ev1 = mod.SagaEvent.create("s", "t", {})
        ev2 = mod.SagaEvent.create("s", "t", {})
        assert ev1.event_id != ev2.event_id

    def test_timestamp_is_recent(self, mod):
        before = time.time()
        ev = mod.SagaEvent.create("s", "t", {})
        after = time.time()
        assert before <= ev.timestamp <= after


# ===========================================================================
# SagaParticipant — execute
# ===========================================================================


class TestSagaParticipantExecute:
    def test_execute_returns_saga_event(self, mod):
        p = mod.SagaParticipant("order_service")
        ev = mod.SagaEvent.create("saga-1", "order_placed", {})
        result = p.execute(ev)
        assert isinstance(result, mod.SagaEvent)

    def test_execute_event_type_includes_name(self, mod):
        p = mod.SagaParticipant("payment_service")
        ev = mod.SagaEvent.create("saga-1", "order_placed", {})
        result = p.execute(ev)
        assert result.event_type == "payment_service_completed"

    def test_execute_preserves_saga_id(self, mod):
        p = mod.SagaParticipant("inventory_service")
        ev = mod.SagaEvent.create("saga-xyz", "order_placed", {})
        result = p.execute(ev)
        assert result.saga_id == "saga-xyz"

    def test_execute_payload_references_original(self, mod):
        p = mod.SagaParticipant("order_service")
        ev = mod.SagaEvent.create("saga-1", "order_placed", {})
        result = p.execute(ev)
        assert result.payload["original"] == ev.event_id

    def test_execute_increments_executed_count(self, mod):
        p = mod.SagaParticipant("svc")
        ev = mod.SagaEvent.create("s", "t", {})
        assert p.executed_count() == 0
        p.execute(ev)
        assert p.executed_count() == 1

    def test_execute_multiple_increments(self, mod):
        p = mod.SagaParticipant("svc")
        for i in range(5):
            p.execute(mod.SagaEvent.create("s", "t", {}))
        assert p.executed_count() == 5

    def test_execute_does_not_affect_compensated_count(self, mod):
        p = mod.SagaParticipant("svc")
        p.execute(mod.SagaEvent.create("s", "t", {}))
        assert p.compensated_count() == 0


# ===========================================================================
# SagaParticipant — compensate
# ===========================================================================


class TestSagaParticipantCompensate:
    def test_compensate_returns_saga_event(self, mod):
        p = mod.SagaParticipant("order_service")
        ev = mod.SagaEvent.create("saga-1", "order_placed", {})
        result = p.compensate(ev)
        assert isinstance(result, mod.SagaEvent)

    def test_compensate_event_type_includes_name(self, mod):
        p = mod.SagaParticipant("payment_service")
        ev = mod.SagaEvent.create("saga-1", "order_placed", {})
        result = p.compensate(ev)
        assert result.event_type == "payment_service_compensated"

    def test_compensate_preserves_saga_id(self, mod):
        p = mod.SagaParticipant("inventory_service")
        ev = mod.SagaEvent.create("saga-abc", "order_placed", {})
        result = p.compensate(ev)
        assert result.saga_id == "saga-abc"

    def test_compensate_payload_references_original(self, mod):
        p = mod.SagaParticipant("order_service")
        ev = mod.SagaEvent.create("saga-1", "order_placed", {})
        result = p.compensate(ev)
        assert result.payload["original"] == ev.event_id

    def test_compensate_increments_compensated_count(self, mod):
        p = mod.SagaParticipant("svc")
        ev = mod.SagaEvent.create("s", "t", {})
        assert p.compensated_count() == 0
        p.compensate(ev)
        assert p.compensated_count() == 1

    def test_compensate_does_not_affect_executed_count(self, mod):
        p = mod.SagaParticipant("svc")
        p.compensate(mod.SagaEvent.create("s", "t", {}))
        assert p.executed_count() == 0

    def test_both_counters_independent(self, mod):
        p = mod.SagaParticipant("svc")
        p.execute(mod.SagaEvent.create("s", "e", {}))
        p.execute(mod.SagaEvent.create("s", "e", {}))
        p.compensate(mod.SagaEvent.create("s", "c", {}))
        assert p.executed_count() == 2
        assert p.compensated_count() == 1


# ===========================================================================
# SagaChoreographer — lifecycle
# ===========================================================================


class TestSagaChoreographerLifecycle:
    def test_unknown_saga_returns_not_found(self, mod):
        ch = mod.SagaChoreographer()
        assert ch.get_saga_status("no-such-saga") == "not_found"

    def test_start_saga_sets_running(self, mod):
        ch = mod.SagaChoreographer()
        ev = mod.SagaEvent.create("s1", "order_placed", {})
        ch.start_saga("s1", ev)
        assert ch.get_saga_status("s1") == "running"

    def test_complete_saga_sets_completed(self, mod):
        ch = mod.SagaChoreographer()
        ev = mod.SagaEvent.create("s2", "order_placed", {})
        ch.start_saga("s2", ev)
        ch.complete_saga("s2")
        assert ch.get_saga_status("s2") == "completed"

    def test_fail_saga_sets_failed(self, mod):
        ch = mod.SagaChoreographer()
        ev = mod.SagaEvent.create("s3", "order_placed", {})
        ch.start_saga("s3", ev)
        ch.fail_saga("s3")
        assert ch.get_saga_status("s3") == "failed"

    def test_complete_unknown_saga_is_noop(self, mod):
        ch = mod.SagaChoreographer()
        ch.complete_saga("nonexistent")  # should not raise
        assert ch.get_saga_status("nonexistent") == "not_found"

    def test_fail_unknown_saga_is_noop(self, mod):
        ch = mod.SagaChoreographer()
        ch.fail_saga("nonexistent")  # should not raise

    def test_start_saga_records_initial_event(self, mod):
        ch = mod.SagaChoreographer()
        ev = mod.SagaEvent.create("s4", "order_placed", {"x": 1})
        ch.start_saga("s4", ev)
        events = ch.get_saga_events("s4")
        assert len(events) == 1
        assert events[0].event_id == ev.event_id

    def test_get_saga_events_unknown_returns_empty(self, mod):
        ch = mod.SagaChoreographer()
        assert ch.get_saga_events("nope") == []

    def test_multiple_sagas_isolated(self, mod):
        ch = mod.SagaChoreographer()
        e1 = mod.SagaEvent.create("saga-A", "t", {})
        e2 = mod.SagaEvent.create("saga-B", "t", {})
        ch.start_saga("saga-A", e1)
        ch.start_saga("saga-B", e2)
        ch.complete_saga("saga-A")
        assert ch.get_saga_status("saga-A") == "completed"
        assert ch.get_saga_status("saga-B") == "running"


# ===========================================================================
# SagaChoreographer — publish_event
# ===========================================================================


class TestSagaChoreographerPublish:
    def test_publish_with_no_handlers_returns_empty(self, mod):
        ch = mod.SagaChoreographer()
        ev = mod.SagaEvent.create("s", "unknown_event", {})
        ch.start_saga("s", ev)
        result = ch.publish_event(ev)
        assert result == []

    def test_single_handler_called(self, mod):
        ch = mod.SagaChoreographer()
        p = mod.SagaParticipant("order_service")
        ch.register_handler("order_placed", p.execute)
        ev = mod.SagaEvent.create("s", "order_placed", {})
        ch.start_saga("s", ev)
        responses = ch.publish_event(ev)
        assert len(responses) == 1
        assert responses[0].event_type == "order_service_completed"

    def test_two_handlers_both_called(self, mod):
        ch = mod.SagaChoreographer()
        p1 = mod.SagaParticipant("order_service")
        p2 = mod.SagaParticipant("payment_service")
        ch.register_handler("order_placed", p1.execute)
        ch.register_handler("order_placed", p2.execute)
        ev = mod.SagaEvent.create("s", "order_placed", {})
        ch.start_saga("s", ev)
        responses = ch.publish_event(ev)
        assert len(responses) == 2

    def test_responses_appended_to_saga_history(self, mod):
        ch = mod.SagaChoreographer()
        p = mod.SagaParticipant("inventory_service")
        ch.register_handler("order_placed", p.execute)
        ev = mod.SagaEvent.create("s5", "order_placed", {})
        ch.start_saga("s5", ev)
        ch.publish_event(ev)
        events = ch.get_saga_events("s5")
        # initial event + 1 response
        assert len(events) == 2

    def test_publish_does_not_crash_without_saga(self, mod):
        ch = mod.SagaChoreographer()
        p = mod.SagaParticipant("svc")
        ch.register_handler("t", p.execute)
        orphan = mod.SagaEvent.create("orphan-saga", "t", {})
        # Should not raise even though saga was never started
        result = ch.publish_event(orphan)
        assert len(result) == 1

    def test_handler_responses_are_saga_events(self, mod):
        ch = mod.SagaChoreographer()
        p = mod.SagaParticipant("svc")
        ch.register_handler("ping", p.execute)
        ev = mod.SagaEvent.create("s", "ping", {})
        ch.start_saga("s", ev)
        responses = ch.publish_event(ev)
        for r in responses:
            assert isinstance(r, mod.SagaEvent)

    def test_handler_response_preserves_saga_id(self, mod):
        ch = mod.SagaChoreographer()
        p = mod.SagaParticipant("svc")
        ch.register_handler("t", p.execute)
        ev = mod.SagaEvent.create("my-saga", "t", {})
        ch.start_saga("my-saga", ev)
        responses = ch.publish_event(ev)
        assert responses[0].saga_id == "my-saga"


# ===========================================================================
# CompensationEngine — registration and recording
# ===========================================================================


class TestCompensationEngineSetup:
    def test_register_participant_no_error(self, mod):
        engine = mod.CompensationEngine()
        p = mod.SagaParticipant("svc")
        engine.register_participant(p)  # should not raise

    def test_record_execution_no_error(self, mod):
        engine = mod.CompensationEngine()
        p = mod.SagaParticipant("svc")
        engine.register_participant(p)
        ev = mod.SagaEvent.create("s", "t", {})
        engine.record_execution("svc", ev)  # should not raise

    def test_compensation_count_starts_at_zero(self, mod):
        engine = mod.CompensationEngine()
        assert engine.compensation_count() == 0

    def test_compensate_all_empty_returns_empty(self, mod):
        engine = mod.CompensationEngine()
        result = engine.compensate_all()
        assert result == []


# ===========================================================================
# CompensationEngine — compensate_all reverse order
# ===========================================================================


class TestCompensationEngineCompensateAll:
    def test_single_step_compensated(self, mod):
        engine = mod.CompensationEngine()
        p = mod.SagaParticipant("order_service")
        engine.register_participant(p)
        ev = mod.SagaEvent.create("s", "order_placed", {})
        engine.record_execution("order_service", ev)
        results = engine.compensate_all()
        assert len(results) == 1
        assert results[0].event_type == "order_service_compensated"

    def test_reverse_order_three_steps(self, mod):
        engine = mod.CompensationEngine()
        p1 = mod.SagaParticipant("step1")
        p2 = mod.SagaParticipant("step2")
        p3 = mod.SagaParticipant("step3")
        engine.register_participant(p1)
        engine.register_participant(p2)
        engine.register_participant(p3)

        ev1 = mod.SagaEvent.create("s", "t", {"step": 1})
        ev2 = mod.SagaEvent.create("s", "t", {"step": 2})
        ev3 = mod.SagaEvent.create("s", "t", {"step": 3})

        engine.record_execution("step1", ev1)
        engine.record_execution("step2", ev2)
        engine.record_execution("step3", ev3)

        results = engine.compensate_all()

        assert len(results) == 3
        # First compensation result should be step3's (last executed, first compensated)
        assert results[0].event_type == "step3_compensated"
        assert results[1].event_type == "step2_compensated"
        assert results[2].event_type == "step1_compensated"

    def test_compensation_count_increments(self, mod):
        engine = mod.CompensationEngine()
        p = mod.SagaParticipant("svc")
        engine.register_participant(p)
        ev = mod.SagaEvent.create("s", "t", {})
        engine.record_execution("svc", ev)
        engine.compensate_all()
        assert engine.compensation_count() == 1

    def test_compensation_count_accumulates_across_calls(self, mod):
        engine = mod.CompensationEngine()
        p = mod.SagaParticipant("svc")
        engine.register_participant(p)
        ev1 = mod.SagaEvent.create("s", "t", {})
        ev2 = mod.SagaEvent.create("s", "t", {})
        engine.record_execution("svc", ev1)
        engine.record_execution("svc", ev2)
        engine.compensate_all()
        # count should be 2 after compensating both
        assert engine.compensation_count() == 2

    def test_compensate_updates_participant_count(self, mod):
        engine = mod.CompensationEngine()
        p = mod.SagaParticipant("svc")
        engine.register_participant(p)
        ev = mod.SagaEvent.create("s", "t", {})
        engine.record_execution("svc", ev)
        engine.compensate_all()
        assert p.compensated_count() == 1

    def test_unknown_participant_name_skipped(self, mod):
        engine = mod.CompensationEngine()
        ev = mod.SagaEvent.create("s", "t", {})
        engine.record_execution("ghost_service", ev)
        # No participant registered — should not raise; returns empty
        result = engine.compensate_all()
        assert result == []

    def test_payload_references_original_event(self, mod):
        engine = mod.CompensationEngine()
        p = mod.SagaParticipant("svc")
        engine.register_participant(p)
        ev = mod.SagaEvent.create("s", "t", {})
        engine.record_execution("svc", ev)
        results = engine.compensate_all()
        assert results[0].payload["original"] == ev.event_id


# ===========================================================================
# SagaMonitor — start/end recording
# ===========================================================================


class TestSagaMonitorRecording:
    def test_record_start_no_error(self, mod):
        monitor = mod.SagaMonitor()
        monitor.record_saga_start("saga-1")  # should not raise

    def test_record_end_no_error(self, mod):
        monitor = mod.SagaMonitor()
        monitor.record_saga_start("saga-1")
        monitor.record_saga_end("saga-1", "completed")  # should not raise

    def test_is_timed_out_false_for_unknown(self, mod):
        monitor = mod.SagaMonitor()
        assert monitor.is_timed_out("no-such-saga") is False

    def test_is_timed_out_false_immediately_after_start(self, mod):
        monitor = mod.SagaMonitor(timeout_seconds=60.0)
        monitor.record_saga_start("saga-new")
        assert monitor.is_timed_out("saga-new") is False

    def test_is_timed_out_false_after_completion(self, mod):
        monitor = mod.SagaMonitor(timeout_seconds=0.001)
        monitor.record_saga_start("saga-done")
        monitor.record_saga_end("saga-done", "completed")
        # Even if time has passed, a completed saga is NOT timed out
        assert monitor.is_timed_out("saga-done") is False

    def test_is_timed_out_true_when_elapsed_exceeds_threshold(self, mod):
        monitor = mod.SagaMonitor(timeout_seconds=0.0)  # zero → always timed out
        monitor.record_saga_start("saga-slow")
        # Minimal sleep to ensure time.time() advances
        time.sleep(0.01)
        assert monitor.is_timed_out("saga-slow") is True

    def test_get_timed_out_sagas_empty_when_none(self, mod):
        monitor = mod.SagaMonitor(timeout_seconds=60.0)
        monitor.record_saga_start("fast")
        monitor.record_saga_end("fast", "completed")
        assert monitor.get_timed_out_sagas() == []

    def test_get_timed_out_sagas_returns_slow_saga(self, mod):
        monitor = mod.SagaMonitor(timeout_seconds=0.0)
        monitor.record_saga_start("slow-saga")
        time.sleep(0.01)
        timed_out = monitor.get_timed_out_sagas()
        assert "slow-saga" in timed_out


# ===========================================================================
# SagaMonitor — get_stats
# ===========================================================================


class TestSagaMonitorStats:
    def test_empty_monitor_all_zero(self, mod):
        monitor = mod.SagaMonitor()
        stats = monitor.get_stats()
        assert stats["total"] == 0
        assert stats["completed"] == 0
        assert stats["failed"] == 0
        assert stats["running"] == 0
        assert stats["timed_out"] == 0

    def test_stats_keys_present(self, mod):
        monitor = mod.SagaMonitor()
        stats = monitor.get_stats()
        for key in ("total", "completed", "failed", "running", "timed_out"):
            assert key in stats

    def test_running_saga_counted(self, mod):
        monitor = mod.SagaMonitor(timeout_seconds=60.0)
        monitor.record_saga_start("r1")
        stats = monitor.get_stats()
        assert stats["running"] == 1
        assert stats["total"] == 1

    def test_completed_saga_counted(self, mod):
        monitor = mod.SagaMonitor()
        monitor.record_saga_start("c1")
        monitor.record_saga_end("c1", "completed")
        stats = monitor.get_stats()
        assert stats["completed"] == 1
        assert stats["total"] == 1

    def test_failed_saga_counted(self, mod):
        monitor = mod.SagaMonitor()
        monitor.record_saga_start("f1")
        monitor.record_saga_end("f1", "failed")
        stats = monitor.get_stats()
        assert stats["failed"] == 1
        assert stats["total"] == 1

    def test_timed_out_saga_counted(self, mod):
        monitor = mod.SagaMonitor(timeout_seconds=0.0)
        monitor.record_saga_start("to1")
        time.sleep(0.01)
        stats = monitor.get_stats()
        assert stats["timed_out"] == 1

    def test_mixed_saga_states(self, mod):
        monitor = mod.SagaMonitor(timeout_seconds=60.0)
        monitor.record_saga_start("r1")
        monitor.record_saga_start("c1")
        monitor.record_saga_end("c1", "completed")
        monitor.record_saga_start("f1")
        monitor.record_saga_end("f1", "failed")
        stats = monitor.get_stats()
        assert stats["total"] == 3
        assert stats["running"] == 1
        assert stats["completed"] == 1
        assert stats["failed"] == 1

    def test_total_matches_sum_of_states(self, mod):
        monitor = mod.SagaMonitor(timeout_seconds=60.0)
        monitor.record_saga_start("a")
        monitor.record_saga_end("a", "completed")
        monitor.record_saga_start("b")
        monitor.record_saga_end("b", "failed")
        monitor.record_saga_start("c")
        stats = monitor.get_stats()
        assert stats["total"] == stats["completed"] + stats["failed"] + stats["running"] + stats["timed_out"]


# ===========================================================================
# End-to-end integration test
# ===========================================================================


class TestEndToEndSagaFlow:
    def test_happy_path_order_saga(self, mod):
        """Full saga lifecycle: start → publish → complete."""
        order_svc = mod.SagaParticipant("order_service")
        payment_svc = mod.SagaParticipant("payment_service")
        inventory_svc = mod.SagaParticipant("inventory_service")

        ch = mod.SagaChoreographer()
        ch.register_handler("order_placed", order_svc.execute)
        ch.register_handler("order_placed", payment_svc.execute)
        ch.register_handler("order_placed", inventory_svc.execute)

        monitor = mod.SagaMonitor(timeout_seconds=30.0)
        saga_id = str(uuid.uuid4())

        initial = mod.SagaEvent.create(saga_id, "order_placed", {"item": "widget"})
        ch.start_saga(saga_id, initial)
        monitor.record_saga_start(saga_id)

        responses = ch.publish_event(initial)

        assert len(responses) == 3
        ch.complete_saga(saga_id)
        monitor.record_saga_end(saga_id, "completed")

        assert ch.get_saga_status(saga_id) == "completed"
        events = ch.get_saga_events(saga_id)
        # initial + 3 responses
        assert len(events) == 4
        assert monitor.is_timed_out(saga_id) is False
        stats = monitor.get_stats()
        assert stats["completed"] == 1

    def test_compensation_rollback_saga(self, mod):
        """Compensation path: execute 3 steps, then roll back in reverse."""
        p1 = mod.SagaParticipant("step_a")
        p2 = mod.SagaParticipant("step_b")
        p3 = mod.SagaParticipant("step_c")

        engine = mod.CompensationEngine()
        engine.register_participant(p1)
        engine.register_participant(p2)
        engine.register_participant(p3)

        saga_id = str(uuid.uuid4())
        ev1 = mod.SagaEvent.create(saga_id, "e", {}, sequence=0)
        ev2 = mod.SagaEvent.create(saga_id, "e", {}, sequence=1)
        ev3 = mod.SagaEvent.create(saga_id, "e", {}, sequence=2)

        engine.record_execution("step_a", ev1)
        engine.record_execution("step_b", ev2)
        engine.record_execution("step_c", ev3)

        comp_events = engine.compensate_all()

        assert len(comp_events) == 3
        # reverse order
        assert comp_events[0].event_type == "step_c_compensated"
        assert comp_events[1].event_type == "step_b_compensated"
        assert comp_events[2].event_type == "step_a_compensated"

        assert p3.compensated_count() == 1
        assert p2.compensated_count() == 1
        assert p1.compensated_count() == 1
        assert engine.compensation_count() == 3
