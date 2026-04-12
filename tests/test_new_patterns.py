"""
Tests for new integration-automation-patterns modules:
circuit_breaker, saga, outbox, webhook_handler, cdc_event, kafka_envelope.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import time

import pytest

from integration_automation_patterns.cdc_event import CDCEvent, CDCOperation, CDCSourceMetadata
from integration_automation_patterns.circuit_breaker import CircuitBreaker, CircuitOpenError, CircuitState
from integration_automation_patterns.kafka_envelope import KafkaEventEnvelope
from integration_automation_patterns.outbox import OutboxProcessor, OutboxRecord
from integration_automation_patterns.saga import SagaOrchestrator
from integration_automation_patterns.webhook_handler import (
    WebhookHandler,
    WebhookReplayError,
    WebhookSignatureError,
)

# ===========================================================================
# CircuitBreaker
# ===========================================================================


def _raise_runtime() -> None:
    raise RuntimeError("fail")


class TestCircuitBreaker:
    def test_initial_state_is_closed(self) -> None:
        cb = CircuitBreaker(name="test", failure_threshold=3)
        assert cb.state == CircuitState.CLOSED

    def test_successful_call_passes_through(self) -> None:
        cb = CircuitBreaker(name="test", failure_threshold=3)
        result = cb.execute(lambda: "ok")
        assert result == "ok"

    def test_trips_open_after_threshold(self) -> None:
        cb = CircuitBreaker(name="test", failure_threshold=2)
        for _ in range(2):
            with pytest.raises(RuntimeError):
                cb.execute(_raise_runtime)
        assert cb.state == CircuitState.OPEN

    def test_open_circuit_raises_circuit_open_error(self) -> None:
        cb = CircuitBreaker(name="test", failure_threshold=1)
        with pytest.raises(RuntimeError):
            cb.execute(_raise_runtime)
        with pytest.raises(CircuitOpenError):
            cb.execute(lambda: "ok")

    def test_reset_returns_to_closed(self) -> None:
        cb = CircuitBreaker(name="test", failure_threshold=1)
        with pytest.raises(RuntimeError):
            cb.execute(_raise_runtime)
        cb.reset()
        assert cb.state == CircuitState.CLOSED

    def test_stats_returns_snapshot(self) -> None:
        cb = CircuitBreaker(name="my-service", failure_threshold=3)
        stats = cb.stats()
        assert stats.name == "my-service"
        assert stats.state == CircuitState.CLOSED
        assert stats.failure_count == 0

    def test_decorator_wraps_function(self) -> None:
        cb = CircuitBreaker(name="test", failure_threshold=3)

        @cb.call
        def add(a: int, b: int) -> int:
            return a + b

        assert add(1, 2) == 3  # type: ignore[call-arg]

    def test_half_open_after_recovery_timeout(self) -> None:
        cb = CircuitBreaker(name="test", failure_threshold=1, recovery_timeout=0.01)
        with pytest.raises(RuntimeError):
            cb.execute(_raise_runtime)
        time.sleep(0.02)
        # Next state check should transition to HALF_OPEN
        assert cb.state == CircuitState.HALF_OPEN


# ===========================================================================
# SagaOrchestrator
# ===========================================================================


class TestSagaOrchestrator:
    def test_all_steps_succeed(self) -> None:
        log: list[str] = []
        saga = SagaOrchestrator("test-saga")
        saga.add_step("step1", action=lambda: log.append("a"), compensate=lambda: log.append("undo-a"))
        saga.add_step("step2", action=lambda: log.append("b"), compensate=lambda: log.append("undo-b"))
        result = saga.execute()
        assert result.succeeded
        assert result.failed_step is None
        assert result.compensated_steps == []
        assert log == ["a", "b"]

    def test_failure_triggers_compensation(self) -> None:
        log: list[str] = []
        saga = SagaOrchestrator("test-saga")
        saga.add_step("step1", action=lambda: log.append("a"), compensate=lambda: log.append("undo-a"))

        def _fail() -> None:
            raise ValueError("boom")

        saga.add_step("step2", action=_fail, compensate=lambda: log.append("undo-b"))
        result = saga.execute()
        assert not result.succeeded
        assert result.failed_step == "step2"
        assert "step1" in result.compensated_steps
        assert "undo-a" in log

    def test_compensation_in_reverse_order(self) -> None:
        log: list[str] = []
        saga = SagaOrchestrator("order-saga")
        saga.add_step("s1", action=lambda: log.append("s1"), compensate=lambda: log.append("c1"))
        saga.add_step("s2", action=lambda: log.append("s2"), compensate=lambda: log.append("c2"))

        def _fail3() -> None:
            raise RuntimeError("fail")

        saga.add_step("s3", action=_fail3, compensate=lambda: log.append("c3"))
        saga.execute()
        assert log.index("c2") < log.index("c1")  # reverse order

    def test_method_chaining(self) -> None:
        saga = (
            SagaOrchestrator("chain")
            .add_step("a", action=lambda: None, compensate=lambda: None)
            .add_step("b", action=lambda: None, compensate=lambda: None)
        )
        assert isinstance(saga, SagaOrchestrator)

    def test_step_results_captured(self) -> None:
        saga = SagaOrchestrator("results")
        saga.add_step("fetch", action=lambda: {"id": 1}, compensate=lambda: None)
        result = saga.execute()
        assert result.step_results["fetch"] == {"id": 1}


# ===========================================================================
# OutboxRecord + OutboxProcessor
# ===========================================================================


class TestOutboxRecord:
    def test_create_factory(self) -> None:
        r = OutboxRecord.create("agg-1", "AccountCreated", {"name": "Acme"})
        assert r.aggregate_id == "agg-1"
        assert r.event_type == "AccountCreated"
        assert r.is_pending

    def test_to_event_dict_has_required_keys(self) -> None:
        r = OutboxRecord.create("agg-1", "Evt", {})
        d = r.to_event_dict()
        for key in ("record_id", "aggregate_id", "event_type", "payload", "created_at"):
            assert key in d

    def test_to_event_dict_is_json_serialisable(self) -> None:
        r = OutboxRecord.create("agg-1", "Evt", {"x": 1})
        json.dumps(r.to_event_dict())  # should not raise


class TestOutboxProcessor:
    def test_publishes_pending_records(self) -> None:
        records = [OutboxRecord.create("a1", "Evt", {}), OutboxRecord.create("a2", "Evt", {})]
        published: list[dict] = []
        marked: list[str] = []

        processor = OutboxProcessor(
            fetch_pending=lambda n: records[:n],
            publish=published.append,
            mark_published=marked.append,
        )
        pub, fail = processor.process_batch(batch_size=10)
        assert pub == 2
        assert fail == 0
        assert len(published) == 2

    def test_handles_publish_failure(self) -> None:
        records = [OutboxRecord.create("a1", "Evt", {})]
        failed_ids: list[str] = []

        def fail_publish(_: dict) -> None:
            raise RuntimeError("broker down")

        processor = OutboxProcessor(
            fetch_pending=lambda n: records[:n],
            publish=fail_publish,
            mark_published=lambda _: None,
            mark_failed=lambda rid, err, cnt: failed_ids.append(rid),
        )
        pub, fail = processor.process_batch()
        assert pub == 0
        assert fail == 1
        assert len(failed_ids) == 1

    def test_empty_batch_returns_zeros(self) -> None:
        processor = OutboxProcessor(
            fetch_pending=lambda n: [],
            publish=lambda _: None,
            mark_published=lambda _: None,
        )
        assert processor.process_batch() == (0, 0)


# ===========================================================================
# WebhookHandler
# ===========================================================================


def _make_signed_payload(secret: str, body: dict, tolerance_seconds: float = 300.0) -> tuple[bytes, str]:
    """Helper: create a signed payload bytes + signature header."""
    body_bytes = json.dumps(body).encode("utf-8")
    sig = hmac.new(secret.encode("utf-8"), body_bytes, hashlib.sha256).hexdigest()
    return body_bytes, f"sha256={sig}"


class TestWebhookHandler:
    SECRET = "test-secret-abc"

    def test_valid_signature_parses_event(self) -> None:
        body = {"id": "evt_001", "type": "account.updated", "data": {}}
        payload, sig = _make_signed_payload(self.SECRET, body)
        handler = WebhookHandler(secret=self.SECRET, tolerance_seconds=None)
        event = handler.parse(payload=payload, signature_header=sig)
        assert event.event_id == "evt_001"
        assert event.event_type == "account.updated"

    def test_invalid_signature_raises(self) -> None:
        body = {"id": "1", "type": "test"}
        payload = json.dumps(body).encode("utf-8")
        handler = WebhookHandler(secret=self.SECRET, tolerance_seconds=None)
        with pytest.raises(WebhookSignatureError):
            handler.parse(payload=payload, signature_header="sha256=badhex")

    def test_stale_timestamp_raises_replay_error(self) -> None:
        old_ts = time.time() - 600  # 10 minutes ago
        body = {"id": "1", "type": "test", "timestamp": old_ts}
        payload, sig = _make_signed_payload(self.SECRET, body)
        handler = WebhookHandler(secret=self.SECRET, tolerance_seconds=300)
        with pytest.raises(WebhookReplayError):
            handler.parse(payload=payload, signature_header=sig)

    def test_fresh_timestamp_passes(self) -> None:
        body = {"id": "1", "type": "test", "timestamp": time.time()}
        payload, sig = _make_signed_payload(self.SECRET, body)
        handler = WebhookHandler(secret=self.SECRET, tolerance_seconds=300)
        event = handler.parse(payload=payload, signature_header=sig)
        assert event.event_type == "test"

    def test_v1_format_signature(self) -> None:
        body = {"id": "2", "type": "stripe.evt"}
        payload = json.dumps(body).encode("utf-8")
        sig_hex = hmac.new(self.SECRET.encode(), payload, hashlib.sha256).hexdigest()
        handler = WebhookHandler(secret=self.SECRET, tolerance_seconds=None)
        event = handler.parse(payload=payload, signature_header=f"v1={sig_hex}")
        assert event.event_id == "2"


# ===========================================================================
# CDCEvent
# ===========================================================================


class TestCDCEvent:
    def test_from_debezium_insert(self) -> None:
        payload = {
            "op": "c",
            "before": None,
            "after": {"id": 1, "name": "Alice"},
            "source": {"db": "mydb", "schema": "public", "table": "users", "ts_ms": 1000000000000},
        }
        event = CDCEvent.from_debezium(payload)
        assert event.operation == CDCOperation.INSERT
        assert event.after == {"id": 1, "name": "Alice"}
        assert event.before is None

    def test_from_debezium_update(self) -> None:
        payload = {
            "op": "u",
            "before": {"id": 1, "name": "Alice"},
            "after": {"id": 1, "name": "Bob"},
            "source": {"db": "db", "schema": "s", "table": "t"},
        }
        event = CDCEvent.from_debezium(payload)
        assert event.operation == CDCOperation.UPDATE
        assert event.changed_fields() == {"name"}

    def test_from_debezium_delete(self) -> None:
        payload = {
            "op": "d",
            "before": {"id": 2, "name": "Bob"},
            "after": None,
            "source": {"db": "db", "schema": "s", "table": "t"},
        }
        event = CDCEvent.from_debezium(payload)
        assert event.operation == CDCOperation.DELETE

    def test_unknown_op_raises(self) -> None:
        payload = {"op": "x", "source": {"db": "", "schema": "", "table": ""}}
        with pytest.raises(ValueError, match="Unrecognised"):
            CDCEvent.from_debezium(payload)

    def test_changed_fields_empty_for_insert(self) -> None:
        src = CDCSourceMetadata(database="db", schema="s", table="t")
        event = CDCEvent(operation=CDCOperation.INSERT, source=src, after={"a": 1})
        assert event.changed_fields() == set()

    def test_to_audit_dict_json_serialisable(self) -> None:
        payload = {
            "op": "u",
            "before": {"id": 1, "v": "old"},
            "after": {"id": 1, "v": "new"},
            "source": {"db": "db", "schema": "s", "table": "t"},
        }
        event = CDCEvent.from_debezium(payload)
        json.dumps(event.to_audit_dict())  # should not raise


# ===========================================================================
# KafkaEventEnvelope
# ===========================================================================


class TestKafkaEventEnvelope:
    def test_create_factory(self) -> None:
        env = KafkaEventEnvelope.create(
            event_type="AccountUpdated",
            source_system="salesforce",
            payload={"id": "001"},
            topic="crm.accounts.v1",
            partition_key="001",
        )
        assert env.event_type == "AccountUpdated"
        assert env.topic == "crm.accounts.v1"
        assert env.partition_key == "001"

    def test_to_producer_record_structure(self) -> None:
        env = KafkaEventEnvelope.create("Evt", "sys", {}, "t1", "key1")
        rec = env.to_producer_record()
        assert rec["topic"] == "t1"
        assert rec["key"] == "key1"
        assert isinstance(rec["value"], bytes)

    def test_roundtrip_producer_to_consumer(self) -> None:
        env = KafkaEventEnvelope.create("OrderPlaced", "oms", {"order_id": "42"}, "orders.v1", "42")
        rec = env.to_producer_record()
        received = KafkaEventEnvelope.from_consumer_record(
            topic=rec["topic"], partition=0, offset=100, key=rec["key"], value=rec["value"]
        )
        assert received.event_type == "OrderPlaced"
        assert received.payload == {"order_id": "42"}
        assert received.partition == 0
        assert received.offset == 100

    def test_mark_dead_letter(self) -> None:
        env = KafkaEventEnvelope.create("Evt", "sys", {}, "orders.v1", "k1")
        dlq = env.mark_dead_letter("parse error")
        assert dlq.dead_letter_reason == "parse error"
        assert dlq.topic == "orders.v1.dlq"
        # Original unchanged
        assert env.dead_letter_reason is None
