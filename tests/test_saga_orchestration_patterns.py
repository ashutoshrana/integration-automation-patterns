"""
Tests for 15_saga_orchestration_patterns.py

Four distributed saga patterns:
  1. Orchestration-Based Saga
  2. Choreography-Based Saga (event bus)
  3. Transactional Outbox
  4. Dead Letter Queue
"""

import importlib.util
import sys
import threading
import time
import types
import uuid
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Module loading
# ---------------------------------------------------------------------------

_MOD_PATH = (
    Path(__file__).parent.parent / "examples" / "15_saga_orchestration_patterns.py"
)


def _load_module():
    module_name = "saga_orchestration"
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
# Pattern 1 — Orchestration-Based Saga
# ---------------------------------------------------------------------------

class TestSagaOrchestrator:
    def test_all_steps_succeed_returns_completed(self, m):
        orchestrator = m.SagaOrchestrator()
        steps = [
            m.SagaStep("step1", lambda ctx: ctx.update({"s1": True}), lambda ctx: None),
            m.SagaStep("step2", lambda ctx: ctx.update({"s2": True}), lambda ctx: None),
        ]
        result = orchestrator.execute(steps)
        assert result.status == m.SagaStatus.COMPLETED

    def test_completed_steps_recorded(self, m):
        orchestrator = m.SagaOrchestrator()
        steps = [
            m.SagaStep("step1", lambda ctx: None, lambda ctx: None),
            m.SagaStep("step2", lambda ctx: None, lambda ctx: None),
        ]
        result = orchestrator.execute(steps)
        assert "step1" in result.completed_steps
        assert "step2" in result.completed_steps

    def test_first_step_fail_no_compensation_needed(self, m):
        orchestrator = m.SagaOrchestrator()
        comp_calls = []
        steps = [
            m.SagaStep(
                "step1",
                lambda ctx: (_ for _ in ()).throw(ValueError("fail")),
                lambda ctx: comp_calls.append("comp1"),
            ),
        ]
        result = orchestrator.execute(steps)
        assert result.status == m.SagaStatus.COMPENSATED
        assert comp_calls == []  # Nothing to compensate since step1 itself failed

    def test_second_step_fail_compensates_first(self, m):
        orchestrator = m.SagaOrchestrator()
        comp_calls = []
        steps = [
            m.SagaStep(
                "step1",
                lambda ctx: ctx.update({"s1": True}),
                lambda ctx: comp_calls.append("comp1"),
            ),
            m.SagaStep(
                "step2",
                lambda ctx: (_ for _ in ()).throw(ValueError("step2 fail")),
                lambda ctx: comp_calls.append("comp2"),
            ),
        ]
        result = orchestrator.execute(steps)
        assert result.status == m.SagaStatus.COMPENSATED
        assert "comp1" in comp_calls
        # step2 failed, so its compensation is NOT called (it never completed)
        assert "comp2" not in comp_calls

    def test_compensation_in_reverse_order(self, m):
        orchestrator = m.SagaOrchestrator()
        comp_order = []
        steps = [
            m.SagaStep("s1", lambda ctx: None, lambda ctx: comp_order.append("c1")),
            m.SagaStep("s2", lambda ctx: None, lambda ctx: comp_order.append("c2")),
            m.SagaStep(
                "s3",
                lambda ctx: (_ for _ in ()).throw(ValueError("fail")),
                lambda ctx: comp_order.append("c3"),
            ),
        ]
        result = orchestrator.execute(steps)
        assert result.status == m.SagaStatus.COMPENSATED
        assert comp_order == ["c2", "c1"]

    def test_failure_step_recorded(self, m):
        orchestrator = m.SagaOrchestrator()
        steps = [
            m.SagaStep("s1", lambda ctx: None, lambda ctx: None),
            m.SagaStep(
                "s2",
                lambda ctx: (_ for _ in ()).throw(RuntimeError("db error")),
                lambda ctx: None,
            ),
        ]
        result = orchestrator.execute(steps)
        assert result.failure_step == "s2"
        assert "db error" in result.failure_reason

    def test_compensation_failure_results_in_failed_status(self, m):
        orchestrator = m.SagaOrchestrator()
        steps = [
            m.SagaStep(
                "s1",
                lambda ctx: None,
                lambda ctx: (_ for _ in ()).throw(RuntimeError("comp failed")),
            ),
            m.SagaStep(
                "s2",
                lambda ctx: (_ for _ in ()).throw(ValueError("step failed")),
                lambda ctx: None,
            ),
        ]
        result = orchestrator.execute(steps)
        assert result.status == m.SagaStatus.FAILED

    def test_context_passed_between_steps(self, m):
        orchestrator = m.SagaOrchestrator()
        steps = [
            m.SagaStep("s1", lambda ctx: ctx.update({"token": "abc"}), lambda ctx: None),
            m.SagaStep(
                "s2",
                lambda ctx: ctx.update({"used_token": ctx.get("token")}),
                lambda ctx: None,
            ),
        ]
        result = orchestrator.execute(steps, initial_context={})
        assert result.context.get("used_token") == "abc"

    def test_initial_context_available(self, m):
        orchestrator = m.SagaOrchestrator()
        captured = {}
        steps = [
            m.SagaStep(
                "s1",
                lambda ctx: captured.update({"order_id": ctx.get("order_id")}),
                lambda ctx: None,
            ),
        ]
        orchestrator.execute(steps, initial_context={"order_id": "ORD-42"})
        assert captured["order_id"] == "ORD-42"

    def test_empty_steps_returns_completed(self, m):
        orchestrator = m.SagaOrchestrator()
        result = orchestrator.execute([])
        assert result.status == m.SagaStatus.COMPLETED

    def test_saga_id_is_unique(self, m):
        orchestrator = m.SagaOrchestrator()
        r1 = orchestrator.execute([])
        r2 = orchestrator.execute([])
        assert r1.saga_id != r2.saga_id

    def test_step_status_completed_after_success(self, m):
        orchestrator = m.SagaOrchestrator()
        step = m.SagaStep("s1", lambda ctx: None, lambda ctx: None)
        orchestrator.execute([step])
        assert step.status == m.SagaStepStatus.COMPLETED

    def test_step_status_failed_after_action_raises(self, m):
        orchestrator = m.SagaOrchestrator()
        step = m.SagaStep(
            "s1",
            lambda ctx: (_ for _ in ()).throw(ValueError("x")),
            lambda ctx: None,
        )
        orchestrator.execute([step])
        assert step.status == m.SagaStepStatus.FAILED

    def test_step_status_compensated_after_compensation(self, m):
        orchestrator = m.SagaOrchestrator()
        s1 = m.SagaStep("s1", lambda ctx: None, lambda ctx: None)
        s2 = m.SagaStep(
            "s2",
            lambda ctx: (_ for _ in ()).throw(ValueError("x")),
            lambda ctx: None,
        )
        orchestrator.execute([s1, s2])
        assert s1.status == m.SagaStepStatus.COMPENSATED


# ---------------------------------------------------------------------------
# Pattern 2 — Choreography-Based Saga / Event Bus
# ---------------------------------------------------------------------------

class TestChoreographyEventBus:
    def test_single_subscriber_receives_event(self, m):
        bus = m.ChoreographyEventBus()
        received = []
        bus.subscribe("order.created", lambda e: received.append(e.aggregate_id))
        bus.emit(m.ChoreographyEvent(
            event_id=str(uuid.uuid4()),
            event_type="order.created",
            aggregate_id="ORD-001",
            payload={},
        ))
        assert received == ["ORD-001"]

    def test_multiple_subscribers_all_receive(self, m):
        bus = m.ChoreographyEventBus()
        r1, r2 = [], []
        bus.subscribe("payment.processed", lambda e: r1.append(e.event_id))
        bus.subscribe("payment.processed", lambda e: r2.append(e.event_id))
        eid = str(uuid.uuid4())
        bus.emit(m.ChoreographyEvent(
            event_id=eid,
            event_type="payment.processed",
            aggregate_id="PAY-001",
            payload={},
        ))
        assert eid in r1
        assert eid in r2

    def test_unsubscribed_handler_not_called(self, m):
        bus = m.ChoreographyEventBus()
        called = []
        handler = lambda e: called.append(e.event_id)
        bus.subscribe("order.created", handler)
        bus.unsubscribe("order.created", handler)
        bus.emit(m.ChoreographyEvent(
            event_id=str(uuid.uuid4()),
            event_type="order.created",
            aggregate_id="ORD-002",
            payload={},
        ))
        assert called == []

    def test_handler_for_wrong_event_not_called(self, m):
        bus = m.ChoreographyEventBus()
        called = []
        bus.subscribe("order.shipped", lambda e: called.append(True))
        bus.emit(m.ChoreographyEvent(
            event_id=str(uuid.uuid4()),
            event_type="order.created",
            aggregate_id="ORD-003",
            payload={},
        ))
        assert called == []

    def test_handler_exception_does_not_break_other_handlers(self, m):
        bus = m.ChoreographyEventBus()
        good = []
        bus.subscribe("evt", lambda e: (_ for _ in ()).throw(ValueError("boom")))
        bus.subscribe("evt", lambda e: good.append(True))
        bus.emit(m.ChoreographyEvent(
            event_id=str(uuid.uuid4()),
            event_type="evt",
            aggregate_id="X",
            payload={},
        ))
        assert good == [True]

    def test_published_events_recorded(self, m):
        bus = m.ChoreographyEventBus()
        bus.subscribe("a", lambda e: None)
        for i in range(3):
            bus.emit(m.ChoreographyEvent(
                event_id=str(uuid.uuid4()),
                event_type="a",
                aggregate_id=f"AGG-{i}",
                payload={},
            ))
        assert len(bus.published_events) == 3

    def test_thread_safety_concurrent_emit(self, m):
        bus = m.ChoreographyEventBus()
        received = []
        lock = threading.Lock()
        bus.subscribe("concurrent", lambda e: (lock.acquire(), received.append(True), lock.release()))

        threads = [
            threading.Thread(
                target=bus.emit,
                args=(m.ChoreographyEvent(
                    event_id=str(uuid.uuid4()),
                    event_type="concurrent",
                    aggregate_id="X",
                    payload={},
                ),),
            )
            for _ in range(10)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(received) == 10

    def test_unsubscribe_nonexistent_handler_is_noop(self, m):
        bus = m.ChoreographyEventBus()
        # Should not raise
        bus.unsubscribe("never_subscribed", lambda e: None)

    def test_payload_delivered_to_handler(self, m):
        bus = m.ChoreographyEventBus()
        payloads = []
        bus.subscribe("x", lambda e: payloads.append(e.payload))
        bus.emit(m.ChoreographyEvent(
            event_id=str(uuid.uuid4()),
            event_type="x",
            aggregate_id="A",
            payload={"amount": 99.0},
        ))
        assert payloads[0]["amount"] == 99.0


# ---------------------------------------------------------------------------
# Pattern 3 — Transactional Outbox
# ---------------------------------------------------------------------------

class TestOutboxStore:
    def _msg(self, m, message_id="MSG-001"):
        return m.OutboxMessage(
            message_id=message_id,
            aggregate_type="Order",
            aggregate_id="ORD-001",
            event_type="order.created",
            payload={"total": 50.0},
        )

    def test_save_and_retrieve_pending(self, m):
        store = m.OutboxStore()
        msg = self._msg(m)
        store.save(msg)
        pending = store.get_pending()
        assert len(pending) == 1
        assert pending[0].message_id == "MSG-001"

    def test_mark_published(self, m):
        store = m.OutboxStore()
        store.save(self._msg(m))
        store.mark_published("MSG-001")
        pending = store.get_pending()
        assert len(pending) == 0
        all_msgs = store.all_messages()
        assert all_msgs[0].status == m.OutboxMessageStatus.PUBLISHED

    def test_mark_failed_increments_retry_count(self, m):
        store = m.OutboxStore()
        store.save(self._msg(m))
        store.mark_failed("MSG-001")
        msg = store.all_messages()[0]
        assert msg.retry_count == 1
        store.mark_failed("MSG-001")
        msg = store.all_messages()[0]
        assert msg.retry_count == 2

    def test_batch_size_respected(self, m):
        store = m.OutboxStore()
        for i in range(5):
            store.save(self._msg(m, f"MSG-{i:03d}"))
        pending = store.get_pending(batch_size=3)
        assert len(pending) == 3

    def test_published_not_in_pending(self, m):
        store = m.OutboxStore()
        store.save(self._msg(m, "MSG-P"))
        store.save(self._msg(m, "MSG-F"))
        store.mark_published("MSG-P")
        pending = store.get_pending()
        ids = [p.message_id for p in pending]
        assert "MSG-P" not in ids
        assert "MSG-F" in ids


class TestOutboxPoller:
    def _make_store_with_messages(self, m, count=3):
        store = m.OutboxStore()
        for i in range(count):
            store.save(m.OutboxMessage(
                message_id=f"MSG-{i:03d}",
                aggregate_type="Order",
                aggregate_id=f"ORD-{i:03d}",
                event_type="order.created",
                payload={"index": i},
            ))
        return store

    def test_poll_publishes_all_pending(self, m):
        published = []
        store = self._make_store_with_messages(m, 3)
        poller = m.OutboxPoller(store, lambda msg: published.append(msg.message_id))
        count = poller.poll_once()
        assert count == 3
        assert len(published) == 3

    def test_poll_marks_published_in_store(self, m):
        store = self._make_store_with_messages(m, 2)
        poller = m.OutboxPoller(store, lambda msg: None)
        poller.poll_once()
        all_msgs = store.all_messages()
        assert all(msg.status == m.OutboxMessageStatus.PUBLISHED for msg in all_msgs)

    def test_poll_marks_failed_on_publisher_error(self, m):
        store = self._make_store_with_messages(m, 1)

        def bad_publisher(msg):
            raise ConnectionError("broker down")

        poller = m.OutboxPoller(store, bad_publisher, max_retries=3)
        count = poller.poll_once()
        assert count == 0
        msg = store.all_messages()[0]
        assert msg.status == m.OutboxMessageStatus.FAILED

    def test_poll_skips_max_retries_exceeded(self, m):
        store = m.OutboxStore()
        msg = m.OutboxMessage(
            message_id="MAXED",
            aggregate_type="Order",
            aggregate_id="ORD-X",
            event_type="order.created",
            payload={},
            retry_count=3,   # already at max
        )
        store.save(msg)
        published = []
        poller = m.OutboxPoller(store, lambda m: published.append(m.message_id), max_retries=3)
        count = poller.poll_once()
        assert count == 0
        assert published == []
        assert store.all_messages()[0].status == m.OutboxMessageStatus.FAILED

    def test_poll_empty_store_returns_zero(self, m):
        store = m.OutboxStore()
        poller = m.OutboxPoller(store, lambda msg: None)
        count = poller.poll_once()
        assert count == 0


# ---------------------------------------------------------------------------
# Pattern 4 — Dead Letter Queue
# ---------------------------------------------------------------------------

class TestDeadLetterQueue:
    def _msg(self, m, dlq_id="DLQ-001", queue_name="payments"):
        return m.DLQMessage(
            dlq_id=dlq_id,
            original_message_id="MSG-001",
            queue_name=queue_name,
            payload={"amount": 100.0},
            failure_reason="Timeout",
            attempt_count=3,
        )

    def test_enqueue_and_size(self, m):
        dlq = m.DeadLetterQueue()
        dlq.enqueue(self._msg(m))
        assert dlq.size() == 1

    def test_size_by_status(self, m):
        dlq = m.DeadLetterQueue()
        dlq.enqueue(self._msg(m, "DLQ-001"))
        dlq.enqueue(self._msg(m, "DLQ-002"))
        assert dlq.size(m.DLQMessageStatus.DEAD) == 2
        assert dlq.size(m.DLQMessageStatus.REPLAYED) == 0

    def test_replay_success_marks_replayed(self, m):
        dlq = m.DeadLetterQueue()
        dlq.enqueue(self._msg(m))
        replayed = []
        result = dlq.replay("DLQ-001", lambda msg: replayed.append(msg.dlq_id))
        assert result is True
        assert "DLQ-001" in replayed
        assert dlq.size(m.DLQMessageStatus.REPLAYED) == 1
        assert dlq.size(m.DLQMessageStatus.DEAD) == 0

    def test_replay_handler_exception_returns_false(self, m):
        dlq = m.DeadLetterQueue()
        dlq.enqueue(self._msg(m))

        def failing_handler(msg):
            raise ValueError("handler error")

        result = dlq.replay("DLQ-001", failing_handler)
        assert result is False
        # Message should remain DEAD
        assert dlq.size(m.DLQMessageStatus.DEAD) == 1

    def test_replay_nonexistent_message_returns_false(self, m):
        dlq = m.DeadLetterQueue()
        result = dlq.replay("NONEXISTENT", lambda msg: None)
        assert result is False

    def test_replay_already_replayed_message_returns_false(self, m):
        dlq = m.DeadLetterQueue()
        dlq.enqueue(self._msg(m))
        dlq.replay("DLQ-001", lambda msg: None)
        # Second replay attempt should fail (message is now REPLAYED, not DEAD)
        result = dlq.replay("DLQ-001", lambda msg: None)
        assert result is False

    def test_discard_marks_discarded(self, m):
        dlq = m.DeadLetterQueue()
        dlq.enqueue(self._msg(m))
        result = dlq.discard("DLQ-001")
        assert result is True
        assert dlq.size(m.DLQMessageStatus.DISCARDED) == 1
        assert dlq.size(m.DLQMessageStatus.DEAD) == 0

    def test_discard_nonexistent_returns_false(self, m):
        dlq = m.DeadLetterQueue()
        assert dlq.discard("NOPE") is False

    def test_list_messages_all(self, m):
        dlq = m.DeadLetterQueue()
        for i in range(3):
            dlq.enqueue(self._msg(m, f"DLQ-{i:03d}"))
        msgs = dlq.list_messages()
        assert len(msgs) == 3

    def test_list_messages_filter_by_status(self, m):
        dlq = m.DeadLetterQueue()
        dlq.enqueue(self._msg(m, "DLQ-001"))
        dlq.enqueue(self._msg(m, "DLQ-002"))
        dlq.replay("DLQ-001", lambda msg: None)
        dead = dlq.list_messages(status=m.DLQMessageStatus.DEAD)
        assert len(dead) == 1
        replayed = dlq.list_messages(status=m.DLQMessageStatus.REPLAYED)
        assert len(replayed) == 1

    def test_list_messages_filter_by_queue(self, m):
        dlq = m.DeadLetterQueue()
        dlq.enqueue(self._msg(m, "DLQ-001", queue_name="payments"))
        dlq.enqueue(self._msg(m, "DLQ-002", queue_name="shipments"))
        payments = dlq.list_messages(queue_name="payments")
        assert len(payments) == 1
        assert payments[0].queue_name == "payments"

    def test_replayed_at_set_after_replay(self, m):
        dlq = m.DeadLetterQueue()
        dlq.enqueue(self._msg(m))
        before = time.time()
        dlq.replay("DLQ-001", lambda msg: None)
        msgs = dlq.list_messages(status=m.DLQMessageStatus.REPLAYED)
        assert msgs[0].replayed_at is not None
        assert msgs[0].replayed_at >= before

    def test_thread_safe_concurrent_enqueue(self, m):
        dlq = m.DeadLetterQueue()
        threads = []
        for i in range(20):
            msg = m.DLQMessage(
                dlq_id=f"DLQ-{i:03d}",
                original_message_id=f"MSG-{i:03d}",
                queue_name="payments",
                payload={},
                failure_reason="err",
                attempt_count=3,
            )
            threads.append(threading.Thread(target=dlq.enqueue, args=(msg,)))
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert dlq.size() == 20
