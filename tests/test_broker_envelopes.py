"""
Tests for broker-specific event envelope adapters:
SQSEventEnvelope, AzureServiceBusEnvelope, GCPPubSubEnvelope, RabbitMQEnvelope.
"""

from __future__ import annotations

import base64
import json
from datetime import datetime, timezone

from integration_automation_patterns.azure_servicebus_envelope import AzureServiceBusEnvelope
from integration_automation_patterns.gcp_pubsub_envelope import GCPPubSubEnvelope
from integration_automation_patterns.rabbitmq_envelope import RabbitMQEnvelope
from integration_automation_patterns.sqs_envelope import SQSEventEnvelope

# ===========================================================================
# SQSEventEnvelope
# ===========================================================================


class TestSQSEventEnvelopeCreate:
    def test_create_sets_required_fields(self) -> None:
        env = SQSEventEnvelope.create(
            event_type="OrderPlaced",
            source_system="order-service",
            payload={"order_id": "ORD-001"},
            queue_url="https://sqs.us-east-1.amazonaws.com/123/orders",
        )
        assert env.event_type == "OrderPlaced"
        assert env.source_system == "order-service"
        assert env.payload == {"order_id": "ORD-001"}
        assert env.queue_url == "https://sqs.us-east-1.amazonaws.com/123/orders"
        assert env.schema_version == "1.0"
        assert len(env.event_id) == 36  # UUID

    def test_create_standard_queue_no_group(self) -> None:
        env = SQSEventEnvelope.create(
            event_type="X",
            source_system="s",
            payload={},
            queue_url="https://sqs.us-east-1.amazonaws.com/123/q",
        )
        assert env.message_group_id is None
        assert env.message_deduplication_id is None
        assert env.is_fifo() is False

    def test_create_fifo_queue_sets_group_and_dedup(self) -> None:
        env = SQSEventEnvelope.create(
            event_type="X",
            source_system="s",
            payload={},
            queue_url="https://sqs.us-east-1.amazonaws.com/123/q.fifo",
            message_group_id="customer-123",
        )
        assert env.message_group_id == "customer-123"
        assert env.message_deduplication_id == env.event_id
        assert env.is_fifo() is True

    def test_create_fifo_explicit_dedup_id(self) -> None:
        env = SQSEventEnvelope.create(
            event_type="X",
            source_system="s",
            payload={},
            queue_url="https://sqs.us-east-1.amazonaws.com/123/q.fifo",
            message_group_id="g",
            message_deduplication_id="custom-dedup-id",
        )
        assert env.message_deduplication_id == "custom-dedup-id"

    def test_create_unique_event_ids(self) -> None:
        e1 = SQSEventEnvelope.create("X", "s", {}, "https://sqs.example.com/q")
        e2 = SQSEventEnvelope.create("X", "s", {}, "https://sqs.example.com/q")
        assert e1.event_id != e2.event_id


class TestSQSEventEnvelopeToSendParams:
    def _make(self, fifo: bool = False) -> SQSEventEnvelope:
        return SQSEventEnvelope.create(
            event_type="InvoicePaid",
            source_system="billing",
            payload={"invoice_id": "INV-001"},
            queue_url="https://sqs.us-east-1.amazonaws.com/123/invoices",
            message_group_id="INV-001" if fifo else None,
        )

    def test_send_params_has_required_keys(self) -> None:
        params = self._make().to_send_params()
        assert "QueueUrl" in params
        assert "MessageBody" in params
        assert "MessageAttributes" in params

    def test_send_params_queue_url_matches(self) -> None:
        env = self._make()
        assert env.to_send_params()["QueueUrl"] == env.queue_url

    def test_message_body_is_valid_json(self) -> None:
        params = self._make().to_send_params()
        body = json.loads(params["MessageBody"])
        assert body["event_type"] == "InvoicePaid"
        assert body["payload"]["invoice_id"] == "INV-001"

    def test_message_attributes_event_type(self) -> None:
        params = self._make().to_send_params()
        attrs = params["MessageAttributes"]
        assert attrs["EventType"]["StringValue"] == "InvoicePaid"
        assert attrs["SourceSystem"]["StringValue"] == "billing"

    def test_fifo_params_include_group_and_dedup(self) -> None:
        params = self._make(fifo=True).to_send_params()
        assert params["MessageGroupId"] == "INV-001"
        assert "MessageDeduplicationId" in params

    def test_standard_params_no_group(self) -> None:
        params = self._make(fifo=False).to_send_params()
        assert "MessageGroupId" not in params

    def test_ensure_ascii_false_in_body(self) -> None:
        env = SQSEventEnvelope.create(
            event_type="X",
            source_system="s",
            payload={"name": "José García"},
            queue_url="https://sqs.example.com/q",
        )
        body_str = env.to_send_params()["MessageBody"]
        assert "José" in body_str


class TestSQSEventEnvelopeFromSQSMessage:
    def _make_sqs_msg(self, group_id: str | None = None) -> dict:
        body: dict = {
            "event_id": "eid-001",
            "event_type": "OrderShipped",
            "source_system": "fulfillment",
            "payload": {"order_id": "ORD-002"},
            "queue_url": "https://sqs.example.com/q",
            "schema_version": "1.0",
            "created_at": "2026-04-13T00:00:00+00:00",
        }
        return {
            "Body": json.dumps(body),
            "ReceiptHandle": "receipt-handle-xyz",
            "Attributes": {
                "ApproximateReceiveCount": "3",
                **({"MessageGroupId": group_id} if group_id else {}),
            },
        }

    def test_from_sqs_message_fields(self) -> None:
        env = SQSEventEnvelope.from_sqs_message(self._make_sqs_msg())
        assert env.event_id == "eid-001"
        assert env.event_type == "OrderShipped"
        assert env.receipt_handle == "receipt-handle-xyz"
        assert env.approximate_receive_count == 3

    def test_from_sqs_message_fifo_group(self) -> None:
        env = SQSEventEnvelope.from_sqs_message(self._make_sqs_msg(group_id="g-1"))
        assert env.message_group_id == "g-1"

    def test_from_sns_wrapped_message(self) -> None:
        inner: dict = {
            "event_id": "eid-sns",
            "event_type": "UserCreated",
            "source_system": "auth",
            "payload": {"user_id": "u-1"},
            "queue_url": "",
            "schema_version": "1.0",
            "created_at": "2026-04-13T00:00:00+00:00",
        }
        sns_wrapper = {
            "Type": "Notification",
            "Message": json.dumps(inner),
            "Subject": "UserCreated",
        }
        sqs_msg = {"Body": json.dumps(sns_wrapper), "Attributes": {}}
        env = SQSEventEnvelope.from_sqs_message(sqs_msg)
        assert env.event_id == "eid-sns"
        assert env.event_type == "UserCreated"


class TestSQSEventEnvelopeMarkDead:
    def test_mark_dead_letter_sets_reason(self) -> None:
        env = SQSEventEnvelope.create("X", "s", {}, "https://sqs.example.com/q")
        dlq = env.mark_dead_letter("parse_error")
        assert dlq.dead_letter_reason == "parse_error"
        assert env.dead_letter_reason is None  # original unchanged


# ===========================================================================
# AzureServiceBusEnvelope
# ===========================================================================


class TestAzureServiceBusEnvelopeCreate:
    def test_create_sets_fields(self) -> None:
        env = AzureServiceBusEnvelope.create(
            event_type="InvoiceCreated",
            source_system="erp",
            payload={"invoice_id": "INV-001"},
            queue_or_topic="invoices",
        )
        assert env.event_type == "InvoiceCreated"
        assert env.source_system == "erp"
        assert env.queue_or_topic == "invoices"
        assert env.session_id is None
        assert env.is_session_enabled() is False

    def test_create_with_session(self) -> None:
        env = AzureServiceBusEnvelope.create(
            event_type="X",
            source_system="s",
            payload={},
            queue_or_topic="q",
            session_id="cust-123",
        )
        assert env.session_id == "cust-123"
        assert env.is_session_enabled() is True

    def test_create_with_correlation_id(self) -> None:
        env = AzureServiceBusEnvelope.create(
            event_type="X",
            source_system="s",
            payload={},
            queue_or_topic="q",
            correlation_id="corr-abc",
        )
        assert env.correlation_id == "corr-abc"

    def test_create_with_scheduled_enqueue(self) -> None:
        future = datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        env = AzureServiceBusEnvelope.create(
            event_type="X",
            source_system="s",
            payload={},
            queue_or_topic="q",
            enqueue_time_utc=future,
        )
        assert env.is_scheduled() is True
        assert env.enqueue_time_utc == future


class TestAzureServiceBusEnvelopeToMessage:
    def _make(self, session: bool = False) -> AzureServiceBusEnvelope:
        return AzureServiceBusEnvelope.create(
            event_type="PaymentProcessed",
            source_system="payment",
            payload={"amount": 99.99},
            queue_or_topic="payments",
            session_id="cust-1" if session else None,
        )

    def test_to_service_bus_message_keys(self) -> None:
        params = self._make().to_service_bus_message()
        assert "body" in params
        assert "message_id" in params
        assert params["content_type"] == "application/json"

    def test_message_id_matches_event_id(self) -> None:
        env = self._make()
        assert env.to_service_bus_message()["message_id"] == env.event_id

    def test_body_is_valid_json(self) -> None:
        params = self._make().to_service_bus_message()
        body = json.loads(params["body"])
        assert body["event_type"] == "PaymentProcessed"
        assert body["payload"]["amount"] == 99.99

    def test_session_id_present_when_set(self) -> None:
        params = self._make(session=True).to_service_bus_message()
        assert params["session_id"] == "cust-1"

    def test_no_session_id_when_not_set(self) -> None:
        params = self._make(session=False).to_service_bus_message()
        assert "session_id" not in params

    def test_application_properties_event_type(self) -> None:
        params = self._make().to_service_bus_message()
        assert params["application_properties"]["EventType"] == "PaymentProcessed"

    def test_ensure_ascii_false_in_body(self) -> None:
        env = AzureServiceBusEnvelope.create(
            event_type="X",
            source_system="s",
            payload={"name": "François"},
            queue_or_topic="q",
        )
        body_str = env.to_service_bus_message()["body"]
        assert "François" in body_str


class TestAzureServiceBusEnvelopeFromMessage:
    def _make_dict_msg(self, session_id: str | None = None) -> dict:
        body: dict = {
            "event_id": "eid-sb",
            "event_type": "ContractSigned",
            "source_system": "legal",
            "payload": {"contract_id": "CTR-001"},
            "queue_or_topic": "contracts",
            "schema_version": "2.0",
            "created_at": "2026-04-13T00:00:00+00:00",
        }
        return {
            "body": json.dumps(body).encode("utf-8"),
            "application_properties": {"EventType": "ContractSigned"},
            "session_id": session_id,
            "delivery_count": 2,
            "lock_token": "lock-abc",
        }

    def test_from_dict_message_fields(self) -> None:
        env = AzureServiceBusEnvelope.from_service_bus_message(self._make_dict_msg())
        assert env.event_id == "eid-sb"
        assert env.event_type == "ContractSigned"
        assert env.delivery_count == 2
        assert env.lock_token == "lock-abc"

    def test_from_dict_with_session(self) -> None:
        env = AzureServiceBusEnvelope.from_service_bus_message(self._make_dict_msg(session_id="s-1"))
        assert env.session_id == "s-1"

    def test_schema_version_preserved(self) -> None:
        env = AzureServiceBusEnvelope.from_service_bus_message(self._make_dict_msg())
        assert env.schema_version == "2.0"


class TestAzureServiceBusEnvelopeMarkDead:
    def test_mark_dead_letter(self) -> None:
        env = AzureServiceBusEnvelope.create("X", "s", {}, "q")
        dlq = env.mark_dead_letter("poison_message", "Failed schema validation")
        assert dlq.dead_letter_reason == "poison_message"
        assert dlq.dead_letter_error_description == "Failed schema validation"
        assert env.dead_letter_reason is None


# ===========================================================================
# GCPPubSubEnvelope
# ===========================================================================


class TestGCPPubSubEnvelopeCreate:
    def test_create_sets_fields(self) -> None:
        env = GCPPubSubEnvelope.create(
            event_type="UserRegistered",
            source_system="auth",
            payload={"user_id": "u-001"},
            topic="projects/my-project/topics/users",
        )
        assert env.event_type == "UserRegistered"
        assert env.source_system == "auth"
        assert env.topic == "projects/my-project/topics/users"
        assert env.ordering_key is None
        assert env.is_ordered() is False

    def test_create_with_ordering_key(self) -> None:
        env = GCPPubSubEnvelope.create(
            event_type="X",
            source_system="s",
            payload={},
            topic="projects/p/topics/t",
            ordering_key="user-123",
        )
        assert env.ordering_key == "user-123"
        assert env.is_ordered() is True


class TestGCPPubSubEnvelopeToPublish:
    def _make(self) -> GCPPubSubEnvelope:
        return GCPPubSubEnvelope.create(
            event_type="OrderFulfilled",
            source_system="fulfillment",
            payload={"order_id": "ORD-003"},
            topic="projects/myproj/topics/orders",
        )

    def test_to_publish_data_is_bytes(self) -> None:
        data = self._make().to_publish_data()
        assert isinstance(data, bytes)

    def test_to_publish_data_is_valid_json(self) -> None:
        data = self._make().to_publish_data()
        body = json.loads(data.decode("utf-8"))
        assert body["event_type"] == "OrderFulfilled"
        assert body["payload"]["order_id"] == "ORD-003"

    def test_to_publish_attributes_keys(self) -> None:
        attrs = self._make().to_publish_attributes()
        assert "event_type" in attrs
        assert "source_system" in attrs
        assert "schema_version" in attrs
        assert "event_id" in attrs

    def test_attributes_event_type_value(self) -> None:
        attrs = self._make().to_publish_attributes()
        assert attrs["event_type"] == "OrderFulfilled"

    def test_ensure_ascii_false_in_data(self) -> None:
        env = GCPPubSubEnvelope.create(
            event_type="X",
            source_system="s",
            payload={"city": "Zürich"},
            topic="projects/p/topics/t",
        )
        assert "Zürich" in env.to_publish_data().decode("utf-8")


class TestGCPPubSubEnvelopeFromPubsubMessage:
    def _make_msg_dict(self, ordering_key: str | None = None, delivery_attempt: int = 0) -> dict:
        body: dict = {
            "event_id": "eid-ps",
            "event_type": "ItemShipped",
            "source_system": "warehouse",
            "payload": {"item_id": "ITEM-001"},
            "topic": "projects/p/topics/t",
            "schema_version": "1.0",
            "created_at": "2026-04-13T00:00:00+00:00",
        }
        return {
            "data": json.dumps(body).encode("utf-8"),
            "attributes": {"event_type": "ItemShipped"},
            "delivery_attempt": delivery_attempt,
            "ordering_key": ordering_key,
            "message_id": "msg-123",
        }

    def test_from_pubsub_message_fields(self) -> None:
        env = GCPPubSubEnvelope.from_pubsub_message(self._make_msg_dict())
        assert env.event_id == "eid-ps"
        assert env.event_type == "ItemShipped"
        assert env.message_id == "msg-123"
        assert env.delivery_attempt == 0

    def test_delivery_attempt_tracked(self) -> None:
        env = GCPPubSubEnvelope.from_pubsub_message(self._make_msg_dict(delivery_attempt=3))
        assert env.delivery_attempt == 3
        assert env.exceeds_delivery_attempts(2) is True
        assert env.exceeds_delivery_attempts(5) is False

    def test_ordering_key_preserved(self) -> None:
        env = GCPPubSubEnvelope.from_pubsub_message(self._make_msg_dict(ordering_key="user-1"))
        assert env.ordering_key == "user-1"
        assert env.is_ordered() is True


class TestGCPPubSubEnvelopeFromPushPayload:
    def test_from_push_payload_unwraps_base64(self) -> None:
        body: dict = {
            "event_id": "eid-push",
            "event_type": "PushEvent",
            "source_system": "svc",
            "payload": {"key": "val"},
            "topic": "projects/p/topics/t",
            "schema_version": "1.0",
            "created_at": "2026-04-13T00:00:00+00:00",
        }
        encoded = base64.b64encode(json.dumps(body).encode()).decode()
        push_payload = {
            "message": {
                "data": encoded,
                "attributes": {},
                "messageId": "push-msg-1",
                "publishTime": "2026-04-13T00:00:00Z",
            }
        }
        env = GCPPubSubEnvelope.from_push_payload(push_payload)
        assert env.event_id == "eid-push"
        assert env.event_type == "PushEvent"


# ===========================================================================
# RabbitMQEnvelope
# ===========================================================================


class TestRabbitMQEnvelopeCreate:
    def test_create_sets_fields(self) -> None:
        env = RabbitMQEnvelope.create(
            event_type="PaymentFailed",
            source_system="billing",
            payload={"invoice_id": "INV-002"},
            exchange="payments",
            routing_key="payments.failed.eu",
        )
        assert env.event_type == "PaymentFailed"
        assert env.exchange == "payments"
        assert env.routing_key == "payments.failed.eu"
        assert env.ttl_ms is None
        assert env.priority is None

    def test_create_with_ttl_and_priority(self) -> None:
        env = RabbitMQEnvelope.create(
            event_type="X",
            source_system="s",
            payload={},
            exchange="e",
            routing_key="rk",
            ttl_ms=5000,
            priority=8,
        )
        assert env.ttl_ms == 5000
        assert env.priority == 8

    def test_create_with_reply_to(self) -> None:
        env = RabbitMQEnvelope.create(
            event_type="X",
            source_system="s",
            payload={},
            exchange="",
            routing_key="rpc.queue",
            reply_to="rpc.reply",
            correlation_id="corr-1",
        )
        assert env.reply_to == "rpc.reply"
        assert env.correlation_id == "corr-1"


class TestRabbitMQEnvelopeToAMQP:
    def _make(self, ttl: int | None = None, priority: int | None = None) -> RabbitMQEnvelope:
        return RabbitMQEnvelope.create(
            event_type="StockUpdated",
            source_system="warehouse",
            payload={"sku": "SKU-001", "qty": 100},
            exchange="inventory",
            routing_key="inventory.stock.updated",
            ttl_ms=ttl,
            priority=priority,
        )

    def test_to_amqp_body_is_bytes(self) -> None:
        assert isinstance(self._make().to_amqp_body(), bytes)

    def test_to_amqp_body_is_valid_json(self) -> None:
        body = json.loads(self._make().to_amqp_body().decode("utf-8"))
        assert body["event_type"] == "StockUpdated"
        assert body["payload"]["sku"] == "SKU-001"

    def test_to_amqp_properties_delivery_mode_2(self) -> None:
        props = self._make().to_amqp_properties()
        assert props["delivery_mode"] == 2

    def test_to_amqp_properties_content_type(self) -> None:
        assert self._make().to_amqp_properties()["content_type"] == "application/json"

    def test_to_amqp_properties_message_id_is_event_id(self) -> None:
        env = self._make()
        assert env.to_amqp_properties()["message_id"] == env.event_id

    def test_to_amqp_properties_headers(self) -> None:
        props = self._make().to_amqp_properties()
        assert props["headers"]["event_type"] == "StockUpdated"

    def test_expiration_set_when_ttl(self) -> None:
        props = self._make(ttl=3000).to_amqp_properties()
        assert props["expiration"] == "3000"

    def test_no_expiration_without_ttl(self) -> None:
        props = self._make().to_amqp_properties()
        assert "expiration" not in props

    def test_priority_in_props_when_set(self) -> None:
        props = self._make(priority=5).to_amqp_properties()
        assert props["priority"] == 5

    def test_ensure_ascii_false_in_body(self) -> None:
        env = RabbitMQEnvelope.create(
            event_type="X",
            source_system="s",
            payload={"city": "Straßburg"},
            exchange="",
            routing_key="rk",
        )
        assert "Straßburg" in env.to_amqp_body().decode("utf-8")


class TestRabbitMQEnvelopeFromAMQP:
    def _make_props_dict(self, headers: dict | None = None) -> dict:
        return {
            "headers": headers or {"event_type": "ShipmentCreated", "source_system": "logistics"},
            "message_id": "eid-rmq",
            "correlation_id": "corr-rmq",
            "reply_to": None,
            "priority": None,
        }

    def _make_body(self) -> bytes:
        body: dict = {
            "event_id": "eid-rmq",
            "event_type": "ShipmentCreated",
            "source_system": "logistics",
            "payload": {"shipment_id": "SHP-001"},
            "schema_version": "1.0",
            "created_at": "2026-04-13T00:00:00+00:00",
        }
        return json.dumps(body).encode("utf-8")

    def test_from_amqp_message_fields(self) -> None:
        env = RabbitMQEnvelope.from_amqp_message(
            body=self._make_body(),
            properties=self._make_props_dict(),
            delivery_tag=42,
            redelivered=False,
            exchange="logistics",
            routing_key="shipments.created",
        )
        assert env.event_id == "eid-rmq"
        assert env.event_type == "ShipmentCreated"
        assert env.delivery_tag == 42
        assert env.exchange == "logistics"
        assert env.correlation_id == "corr-rmq"

    def test_redelivered_flag(self) -> None:
        env = RabbitMQEnvelope.from_amqp_message(
            body=self._make_body(),
            properties=self._make_props_dict(),
            redelivered=True,
        )
        assert env.redelivered is True

    def test_x_death_header_count(self) -> None:
        props = self._make_props_dict(headers={"x-death": [{"count": 2, "queue": "q"}, {"count": 1, "queue": "dlq"}]})
        env = RabbitMQEnvelope.from_amqp_message(
            body=self._make_body(),
            properties=props,
        )
        assert env.death_count == 3
        assert env.is_dead_lettered() is True

    def test_no_x_death_not_dead_lettered(self) -> None:
        env = RabbitMQEnvelope.from_amqp_message(
            body=self._make_body(),
            properties=self._make_props_dict(),
        )
        assert env.death_count == 0
        assert env.is_dead_lettered() is False

    def test_no_properties_uses_body_fields(self) -> None:
        env = RabbitMQEnvelope.from_amqp_message(body=self._make_body(), properties=None)
        assert env.event_id == "eid-rmq"
        assert env.event_type == "ShipmentCreated"


class TestRabbitMQEnvelopeMarkDead:
    def test_mark_dead_letter_sets_reason(self) -> None:
        env = RabbitMQEnvelope.create("X", "s", {}, "e", "rk")
        dlq = env.mark_dead_letter("max_retries_exceeded")
        assert dlq.dead_letter_reason == "max_retries_exceeded"
        assert env.dead_letter_reason is None


# ===========================================================================
# Roundtrip tests: produce → consume
# ===========================================================================


class TestBrokerEnvelopeRoundtrips:
    def test_sqs_roundtrip(self) -> None:
        """Produce → to_send_params → from_sqs_message preserves event_type and payload."""
        original = SQSEventEnvelope.create(
            event_type="OrderCancelled",
            source_system="order-svc",
            payload={"order_id": "ORD-999", "reason": "customer_request"},
            queue_url="https://sqs.us-east-1.amazonaws.com/123/orders",
        )
        params = original.to_send_params()
        sqs_msg = {
            "Body": params["MessageBody"],
            "ReceiptHandle": "rh-001",
            "Attributes": {"ApproximateReceiveCount": "1"},
        }
        received = SQSEventEnvelope.from_sqs_message(sqs_msg)
        assert received.event_id == original.event_id
        assert received.event_type == original.event_type
        assert received.payload == original.payload

    def test_azure_sb_roundtrip(self) -> None:
        original = AzureServiceBusEnvelope.create(
            event_type="ContractExpired",
            source_system="legal",
            payload={"contract_id": "CTR-002"},
            queue_or_topic="contracts",
        )
        params = original.to_service_bus_message()
        msg_dict = {
            "body": params["body"].encode("utf-8"),
            "application_properties": params["application_properties"],
            "session_id": None,
            "delivery_count": 0,
            "lock_token": "lt-001",
        }
        received = AzureServiceBusEnvelope.from_service_bus_message(msg_dict)
        assert received.event_id == original.event_id
        assert received.event_type == original.event_type
        assert received.payload == original.payload

    def test_gcp_pubsub_roundtrip(self) -> None:
        original = GCPPubSubEnvelope.create(
            event_type="ReviewSubmitted",
            source_system="review-svc",
            payload={"review_id": "REV-001", "rating": 5},
            topic="projects/p/topics/reviews",
        )
        data = original.to_publish_data()
        msg_dict = {
            "data": data,
            "attributes": original.to_publish_attributes(),
            "message_id": "srv-msg-1",
        }
        received = GCPPubSubEnvelope.from_pubsub_message(msg_dict)
        assert received.event_id == original.event_id
        assert received.event_type == original.event_type
        assert received.payload == original.payload

    def test_rabbitmq_roundtrip(self) -> None:
        original = RabbitMQEnvelope.create(
            event_type="InventoryLow",
            source_system="warehouse",
            payload={"sku": "SKU-002", "qty": 3},
            exchange="inventory",
            routing_key="inventory.low",
        )
        body = original.to_amqp_body()
        props = original.to_amqp_properties()
        # Simulate pika dict properties
        received = RabbitMQEnvelope.from_amqp_message(
            body=body,
            properties=props,
            delivery_tag=1,
            exchange="inventory",
            routing_key="inventory.low",
        )
        assert received.event_id == original.event_id
        assert received.event_type == original.event_type
        assert received.payload == original.payload
