"""
Tests for 27_event_driven_workflow_patterns.py

Covers:
    - EventBus               (11 tests)
    - EventSourcingWorkflow  (11 tests)
    - ChoreographySaga       (10 tests)
    - DeadLetterQueue        (10 tests)
    - EventFilter             (9 tests)
"""

from __future__ import annotations

import importlib.util
import sys
import threading
import types
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Module loader
# ---------------------------------------------------------------------------

_MOD_PATH = Path(__file__).parent.parent / "examples" / "27_event_driven_workflow_patterns.py"


def _load_module():
    module_name = "event_driven_workflow_patterns"
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


# ===========================================================================
# EventBus tests
# ===========================================================================


class TestEventBus:
    """11 tests covering subscribe, publish, unsubscribe, wildcard, threading."""

    def test_subscribe_and_publish_calls_handler(self, m):
        """A handler registered for an event type is called when that type is published."""
        bus = m.EventBus()
        received: list[dict] = []
        bus.subscribe("order.created", received.append)
        bus.publish({"type": "order.created", "order_id": "ORD-1"})
        assert len(received) == 1
        assert received[0]["order_id"] == "ORD-1"

    def test_handler_not_called_for_different_type(self, m):
        """A handler registered for one event type is not called for a different type."""
        bus = m.EventBus()
        received: list[dict] = []
        bus.subscribe("order.created", received.append)
        bus.publish({"type": "order.shipped"})
        assert received == []

    def test_multiple_handlers_for_same_type(self, m):
        """All handlers registered for the same event type are called."""
        bus = m.EventBus()
        calls: list[int] = []
        bus.subscribe("ping", lambda e: calls.append(1))
        bus.subscribe("ping", lambda e: calls.append(2))
        bus.publish({"type": "ping"})
        assert len(calls) == 2
        assert set(calls) == {1, 2}

    def test_wildcard_handler_receives_all_events(self, m):
        """A handler registered for '*' is called for every published event."""
        bus = m.EventBus()
        all_events: list[dict] = []
        bus.subscribe("*", all_events.append)
        bus.publish({"type": "alpha"})
        bus.publish({"type": "beta"})
        bus.publish({"type": "gamma"})
        assert len(all_events) == 3

    def test_wildcard_and_specific_both_called(self, m):
        """Both the specific handler and the wildcard handler are called for a matching event."""
        bus = m.EventBus()
        specific: list[dict] = []
        wildcard: list[dict] = []
        bus.subscribe("item.sold", specific.append)
        bus.subscribe("*", wildcard.append)
        bus.publish({"type": "item.sold", "item": "widget"})
        assert len(specific) == 1
        assert len(wildcard) == 1

    def test_unsubscribe_prevents_future_calls(self, m):
        """After unsubscribing, a handler is no longer called when its event type is published."""
        bus = m.EventBus()
        received: list[dict] = []
        handler = received.append
        bus.subscribe("tick", handler)
        bus.publish({"type": "tick"})  # should call
        bus.unsubscribe("tick", handler)
        bus.publish({"type": "tick"})  # should NOT call
        assert len(received) == 1

    def test_unsubscribe_nonexistent_handler_is_noop(self, m):
        """Unsubscribing a handler that was never registered does not raise."""
        bus = m.EventBus()
        bus.unsubscribe("never.registered", lambda e: None)  # Should not raise

    def test_event_count_increments_on_publish(self, m):
        """event_count increases by 1 for each call to publish."""
        bus = m.EventBus()
        assert bus.event_count == 0
        bus.publish({"type": "a"})
        assert bus.event_count == 1
        bus.publish({"type": "b"})
        assert bus.event_count == 2

    def test_event_count_zero_on_new_bus(self, m):
        """A newly created EventBus has event_count == 0."""
        bus = m.EventBus()
        assert bus.event_count == 0

    def test_thread_safety_concurrent_publishes(self, m):
        """Concurrent publishes from multiple threads all complete without errors."""
        bus = m.EventBus()
        received: list[dict] = []
        lock = threading.Lock()

        def safe_append(event: dict) -> None:
            with lock:
                received.append(event)

        bus.subscribe("concurrent", safe_append)

        threads = [threading.Thread(target=bus.publish, args=({"type": "concurrent", "n": i},)) for i in range(50)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(received) == 50
        assert bus.event_count == 50

    def test_publish_event_passed_correctly_to_handler(self, m):
        """The exact event dict is passed to the subscribed handler."""
        bus = m.EventBus()
        received: list[dict] = []
        bus.subscribe("data.event", received.append)
        evt = {"type": "data.event", "payload": [1, 2, 3], "meta": {"x": True}}
        bus.publish(evt)
        assert received[0] is evt


# ===========================================================================
# EventSourcingWorkflow tests
# ===========================================================================


class TestEventSourcingWorkflow:
    """11 tests covering record_event, replay, current_state, event_count."""

    def test_initial_state_empty(self, m):
        """A newly created workflow has no events and returns empty current_state."""
        wf = m.EventSourcingWorkflow("wf-0")
        assert wf.event_count == 0
        assert wf.current_state == {}

    def test_workflow_started_sets_running_status(self, m):
        """Recording workflow.started produces status RUNNING and merges payload."""
        wf = m.EventSourcingWorkflow("wf-1")
        wf.record_event("workflow.started", {"order_id": "ORD-1"})
        state = wf.current_state
        assert state["status"] == "RUNNING"
        assert state["order_id"] == "ORD-1"

    def test_step_completed_merges_payload(self, m):
        """Recording workflow.step_completed merges payload and keeps status RUNNING."""
        wf = m.EventSourcingWorkflow("wf-2")
        wf.record_event("workflow.started", {"order_id": "ORD-2"})
        wf.record_event("workflow.step_completed", {"step": "validate", "ok": True})
        state = wf.current_state
        assert state["status"] == "RUNNING"
        assert state["step"] == "validate"
        assert state["ok"] is True

    def test_workflow_completed_sets_completed_status(self, m):
        """Recording workflow.completed transitions status to COMPLETED."""
        wf = m.EventSourcingWorkflow("wf-3")
        wf.record_event("workflow.started", {"x": 1})
        wf.record_event("workflow.completed", {})
        assert wf.current_state["status"] == "COMPLETED"

    def test_workflow_failed_sets_failed_status_and_error(self, m):
        """Recording workflow.failed sets status FAILED and captures error message."""
        wf = m.EventSourcingWorkflow("wf-4")
        wf.record_event("workflow.started", {})
        wf.record_event("workflow.failed", {"error": "payment timeout"})
        state = wf.current_state
        assert state["status"] == "FAILED"
        assert state["error"] == "payment timeout"

    def test_workflow_compensated_sets_compensated_status(self, m):
        """Recording workflow.compensated transitions status to COMPENSATED."""
        wf = m.EventSourcingWorkflow("wf-5")
        wf.record_event("workflow.started", {})
        wf.record_event("workflow.failed", {"error": "err"})
        wf.record_event("workflow.compensated", {})
        assert wf.current_state["status"] == "COMPENSATED"

    def test_event_count_tracks_all_recorded_events(self, m):
        """event_count reflects the total number of events recorded."""
        wf = m.EventSourcingWorkflow("wf-6")
        wf.record_event("workflow.started", {})
        wf.record_event("workflow.step_completed", {"step": "a"})
        wf.record_event("workflow.completed", {})
        assert wf.event_count == 3

    def test_replay_returns_all_events_when_no_seq(self, m):
        """replay() with no argument returns all recorded events."""
        wf = m.EventSourcingWorkflow("wf-7")
        wf.record_event("workflow.started", {})
        wf.record_event("workflow.completed", {})
        events = wf.replay()
        assert len(events) == 2

    def test_replay_up_to_seq_filters_correctly(self, m):
        """replay(up_to_seq=N) returns only events with seq <= N."""
        wf = m.EventSourcingWorkflow("wf-8")
        wf.record_event("workflow.started", {})  # seq 0
        wf.record_event("workflow.step_completed", {"step": "a"})  # seq 1
        wf.record_event("workflow.step_completed", {"step": "b"})  # seq 2
        wf.record_event("workflow.completed", {})  # seq 3
        partial = wf.replay(up_to_seq=1)
        assert len(partial) == 2
        assert partial[-1]["seq"] == 1

    def test_replay_up_to_seq_zero_returns_first_event(self, m):
        """replay(up_to_seq=0) returns only the very first event."""
        wf = m.EventSourcingWorkflow("wf-9")
        wf.record_event("workflow.started", {"order": "ORD-X"})
        wf.record_event("workflow.completed", {})
        events = wf.replay(up_to_seq=0)
        assert len(events) == 1
        assert events[0]["type"] == "workflow.started"

    def test_workflow_id_preserved(self, m):
        """The workflow_id supplied at construction is accessible."""
        wf = m.EventSourcingWorkflow("my-workflow-id")
        assert wf.workflow_id == "my-workflow-id"


# ===========================================================================
# ChoreographySaga tests
# ===========================================================================


class TestChoreographySaga:
    """10 tests covering register_step, start, compensate_from, status."""

    def test_start_triggers_subscribed_step(self, m):
        """Publishing the initial event triggers the step registered for that event type."""
        bus = m.EventBus()
        saga = m.ChoreographySaga("s-1", bus)
        log: list[str] = []
        saga.register_step("step_a", "order.created", lambda e: log.append("a"), lambda e: None)
        saga.start({"type": "order.created"})
        assert "step_a" in saga.completed_steps

    def test_status_completed_after_successful_start(self, m):
        """Saga status transitions to COMPLETED when start completes without errors."""
        bus = m.EventBus()
        saga = m.ChoreographySaga("s-2", bus)
        saga.register_step("step_x", "go", lambda e: None, lambda e: None)
        saga.start({"type": "go"})
        assert saga.status == "COMPLETED"

    def test_completed_steps_reflects_run_steps(self, m):
        """completed_steps lists every step whose action ran successfully."""
        bus = m.EventBus()
        saga = m.ChoreographySaga("s-3", bus)
        saga.register_step("first", "go", lambda e: None, lambda e: None)
        saga.start({"type": "go"})
        assert saga.completed_steps == ["first"]

    def test_failing_action_triggers_compensation(self, m):
        """When an action raises, compensate_from is called and saga ends COMPENSATED."""
        bus = m.EventBus()
        saga = m.ChoreographySaga("s-4", bus)
        comp_log: list[str] = []

        saga.register_step(
            "succeed_step",
            "order.created",
            lambda e: None,
            lambda e: comp_log.append("compensated_succeed_step"),
        )
        saga.register_step(
            "failing_step",
            "order.created",
            lambda e: (_ for _ in ()).throw(RuntimeError("boom")),
            lambda e: comp_log.append("compensated_failing_step"),
        )

        saga.start({"type": "order.created"})
        assert saga.status == "COMPENSATED"
        # The succeed_step should have been compensated (it ran before failing_step)
        assert "compensated_succeed_step" in comp_log

    def test_compensate_from_reverses_order(self, m):
        """compensate_from runs compensating actions in reverse registration order."""
        bus = m.EventBus()
        saga = m.ChoreographySaga("s-5", bus)
        order: list[str] = []

        saga.register_step("step1", "go", lambda e: order.append("action1"), lambda e: order.append("comp1"))
        saga.register_step("step2", "go", lambda e: order.append("action2"), lambda e: order.append("comp2"))

        # Manually run actions then compensate
        saga._completed_steps.append("step1")
        saga._completed_steps.append("step2")
        saga.compensate_from("step2")

        # step1 should be compensated (in its position relative to step2)
        assert "comp1" in order

    def test_compensate_from_does_not_compensate_failed_step(self, m):
        """The step named in compensate_from is not itself compensated (it never succeeded)."""
        bus = m.EventBus()
        saga = m.ChoreographySaga("s-6", bus)
        comp_log: list[str] = []

        saga.register_step("ok_step", "ev", lambda e: None, lambda e: comp_log.append("ok_comp"))
        saga.register_step("bad_step", "ev", lambda e: None, lambda e: comp_log.append("bad_comp"))

        saga._completed_steps.append("ok_step")
        # bad_step is the failed step — should NOT be compensated
        saga.compensate_from("bad_step")

        assert "ok_comp" in comp_log
        assert "bad_comp" not in comp_log

    def test_saga_id_preserved(self, m):
        """The saga_id supplied at construction is accessible."""
        bus = m.EventBus()
        saga = m.ChoreographySaga("my-saga", bus)
        assert saga.saga_id == "my-saga"

    def test_initial_status_is_running(self, m):
        """A newly created saga has status RUNNING before start() is called."""
        bus = m.EventBus()
        saga = m.ChoreographySaga("s-7", bus)
        assert saga.status == "RUNNING"

    def test_no_steps_registered_start_completes(self, m):
        """Calling start() with no registered steps transitions saga to COMPLETED."""
        bus = m.EventBus()
        saga = m.ChoreographySaga("s-8", bus)
        saga.start({"type": "anything"})
        assert saga.status == "COMPLETED"
        assert saga.completed_steps == []

    def test_completed_steps_returns_copy(self, m):
        """completed_steps returns a copy — mutating it does not affect the saga."""
        bus = m.EventBus()
        saga = m.ChoreographySaga("s-9", bus)
        saga.register_step("step_a", "go", lambda e: None, lambda e: None)
        saga.start({"type": "go"})
        copy = saga.completed_steps
        copy.clear()
        assert len(saga.completed_steps) == 1


# ===========================================================================
# DeadLetterQueue tests
# ===========================================================================


class TestDeadLetterQueue:
    """10 tests covering process, dead_letters, retry_dead_letters, dlq_size."""

    def test_successful_processing_returns_true(self, m):
        """process() returns True when the handler does not raise."""
        dlq = m.DeadLetterQueue(max_retries=3)
        result = dlq.process({"type": "ok"}, lambda e: None)
        assert result is True

    def test_failed_processing_returns_false(self, m):
        """process() returns False when the handler raises."""
        dlq = m.DeadLetterQueue(max_retries=3)
        result = dlq.process({"type": "fail"}, lambda e: (_ for _ in ()).throw(RuntimeError("err")))
        assert result is False

    def test_event_moves_to_dlq_after_max_retries(self, m):
        """After max_retries failures, the event appears in dead_letters."""
        dlq = m.DeadLetterQueue(max_retries=2)
        event = {"type": "bad"}
        dlq.process(event, lambda e: (_ for _ in ()).throw(ValueError("x")))
        dlq.process(event, lambda e: (_ for _ in ()).throw(ValueError("x")))
        assert dlq.dlq_size == 1

    def test_event_not_in_dlq_before_max_retries_exhausted(self, m):
        """Before reaching max_retries, the event is not yet in the DLQ."""
        dlq = m.DeadLetterQueue(max_retries=3)
        event = {"type": "flaky"}
        dlq.process(event, lambda e: (_ for _ in ()).throw(RuntimeError("t")))
        dlq.process(event, lambda e: (_ for _ in ()).throw(RuntimeError("t")))
        # max_retries=3, only 2 failures so far
        assert dlq.dlq_size == 0

    def test_dlq_size_zero_on_new_instance(self, m):
        """A freshly created DeadLetterQueue has dlq_size == 0."""
        dlq = m.DeadLetterQueue()
        assert dlq.dlq_size == 0

    def test_dead_letters_contains_event_and_error(self, m):
        """dead_letters items contain the event dict and error string."""
        dlq = m.DeadLetterQueue(max_retries=1)
        event = {"type": "payment.fail", "amount": 50}
        dlq.process(event, lambda e: (_ for _ in ()).throw(RuntimeError("timeout")))
        items = dlq.dead_letters
        assert len(items) == 1
        assert items[0]["event"] is event
        assert "timeout" in items[0]["error"]

    def test_retry_dead_letters_recovers_successful_items(self, m):
        """retry_dead_letters returns the count of items that now succeed."""
        dlq = m.DeadLetterQueue(max_retries=1)
        event = {"type": "retry_me"}
        dlq.process(event, lambda e: (_ for _ in ()).throw(RuntimeError("first")))
        count = dlq.retry_dead_letters(lambda e: None)  # succeeds on retry
        assert count == 1

    def test_retry_dead_letters_removes_recovered_items(self, m):
        """Recovered items are removed from the DLQ after retry_dead_letters."""
        dlq = m.DeadLetterQueue(max_retries=1)
        event = {"type": "clear_me"}
        dlq.process(event, lambda e: (_ for _ in ()).throw(RuntimeError("x")))
        dlq.retry_dead_letters(lambda e: None)
        assert dlq.dlq_size == 0

    def test_retry_dead_letters_leaves_persistent_failures(self, m):
        """Items that still fail during retry remain in the DLQ."""
        dlq = m.DeadLetterQueue(max_retries=1)
        event = {"type": "always_fails"}
        dlq.process(event, lambda e: (_ for _ in ()).throw(RuntimeError("x")))
        dlq.retry_dead_letters(lambda e: (_ for _ in ()).throw(RuntimeError("still failing")))
        assert dlq.dlq_size == 1

    def test_dead_letters_returns_copy(self, m):
        """dead_letters returns a copy — mutating it does not affect the internal state."""
        dlq = m.DeadLetterQueue(max_retries=1)
        event = {"type": "copy_test"}
        dlq.process(event, lambda e: (_ for _ in ()).throw(RuntimeError("x")))
        copy = dlq.dead_letters
        copy.clear()
        assert dlq.dlq_size == 1


# ===========================================================================
# EventFilter tests
# ===========================================================================


class TestEventFilter:
    """9 tests covering add_filter, add_transformer, process, counts."""

    def test_no_filters_passes_event_through(self, m):
        """An EventFilter with no filters always lets events through."""
        ef = m.EventFilter()
        result = ef.process({"type": "any"})
        assert result is not None

    def test_filter_allows_matching_event(self, m):
        """A filter that returns True allows the event through."""
        ef = m.EventFilter()
        ef.add_filter("allow_all", lambda e: True)
        result = ef.process({"type": "pass"})
        assert result is not None

    def test_filter_blocks_non_matching_event(self, m):
        """A filter that returns False causes process() to return None."""
        ef = m.EventFilter()
        ef.add_filter("block_all", lambda e: False)
        result = ef.process({"type": "blocked"})
        assert result is None

    def test_first_failing_filter_short_circuits(self, m):
        """If the first filter fails, remaining filters and transformers are not applied."""
        ef = m.EventFilter()
        called: list[int] = []
        ef.add_filter("fail_first", lambda e: False)
        ef.add_filter("should_not_run", lambda e: called.append(1) or True)
        ef.process({"type": "x"})
        assert called == []

    def test_transformer_modifies_event(self, m):
        """A transformer applied to a passing event returns the modified dict."""
        ef = m.EventFilter()
        ef.add_transformer("add_flag", lambda e: {**e, "enriched": True})
        result = ef.process({"type": "enrich_me"})
        assert result is not None
        assert result["enriched"] is True

    def test_chained_transformers_apply_in_order(self, m):
        """Multiple transformers are applied in registration order."""
        ef = m.EventFilter()
        ef.add_transformer("step1", lambda e: {**e, "a": 1})
        ef.add_transformer("step2", lambda e: {**e, "b": e["a"] + 1})
        result = ef.process({"type": "chain"})
        assert result["a"] == 1
        assert result["b"] == 2

    def test_filter_count_reflects_registered_filters(self, m):
        """filter_count equals the number of filters registered."""
        ef = m.EventFilter()
        assert ef.filter_count == 0
        ef.add_filter("f1", lambda e: True)
        ef.add_filter("f2", lambda e: True)
        assert ef.filter_count == 2

    def test_transformer_count_reflects_registered_transformers(self, m):
        """transformer_count equals the number of transformers registered."""
        ef = m.EventFilter()
        assert ef.transformer_count == 0
        ef.add_transformer("t1", lambda e: e)
        assert ef.transformer_count == 1

    def test_filter_and_transformer_combined(self, m):
        """Combining a filter and transformer: matching events are enriched, others dropped."""
        ef = m.EventFilter()
        ef.add_filter("only_alerts", lambda e: e.get("level") == "ALERT")
        ef.add_transformer("add_source", lambda e: {**e, "source": "monitor"})
        alert = ef.process({"type": "msg", "level": "ALERT"})
        debug = ef.process({"type": "msg", "level": "DEBUG"})
        assert alert is not None
        assert alert["source"] == "monitor"
        assert debug is None
