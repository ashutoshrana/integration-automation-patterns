"""
azure_servicebus_envelope.py — Azure Service Bus message envelope.

Wraps business events in an Azure Service Bus-compatible envelope, supporting:
- Standard queues and topics (fan-out via subscriptions)
- Sessions (ordered, grouped message processing — analogous to Kafka partitions)
- Dead-letter queue (DLQ) routing with delivery count tracking
- Scheduled message delivery (enqueue_time_utc)
- Idempotency via message_id (Service Bus de-duplication window)

Follows the same envelope-carries-metadata pattern as ``KafkaEventEnvelope``
and ``SQSEventEnvelope``: routing metadata lives in the envelope, business
data lives in the payload.

Usage::

    # Produce to a queue
    envelope = AzureServiceBusEnvelope.create(
        event_type="InvoiceCreated",
        source_system="erp",
        payload={"invoice_id": "INV-2026-001", "amount": 1250.00},
        queue_or_topic="invoices",
    )
    sender.send_messages(envelope.to_service_bus_message())

    # Produce with session (FIFO ordering per customer_id)
    envelope = AzureServiceBusEnvelope.create(
        event_type="OrderUpdated",
        source_system="order-service",
        payload={"order_id": "ORD-001", "status": "fulfilled"},
        queue_or_topic="orders",
        session_id="customer-123",
    )
    sender.send_messages(envelope.to_service_bus_message())

    # Consume — deserialise from received ServiceBusMessage
    received_envelope = AzureServiceBusEnvelope.from_service_bus_message(sb_message)
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass
class AzureServiceBusEnvelope:
    """
    Azure Service Bus-compatible event envelope.

    Attributes:
        event_id: UUID for idempotency; maps to Service Bus ``message_id``.
        event_type: Domain event discriminator.
        source_system: Name of the producing system.
        payload: Business event data (JSON-serialisable dict).
        queue_or_topic: Target or source queue / topic name.
        schema_version: Payload schema version string.
        created_at: UTC timestamp when this envelope was created.
        session_id: Service Bus session ID for ordered, grouped processing.
            ``None`` for non-session-enabled queues/topics.
        correlation_id: Optional correlation ID for request/reply patterns.
        enqueue_time_utc: Schedule delivery time. ``None`` = immediate.
        delivery_count: Number of delivery attempts (populated on consume).
        lock_token: Service Bus lock token (populated on consume; required for
            ``complete_message()`` and ``dead_letter_message()``).
        dead_letter_reason: If non-None, marks this envelope for DLQ routing.
        dead_letter_error_description: Optional extended DLQ description.
    """

    event_id: str
    event_type: str
    source_system: str
    payload: dict[str, Any]
    queue_or_topic: str
    schema_version: str = "1.0"
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    session_id: str | None = None
    correlation_id: str | None = None
    enqueue_time_utc: datetime | None = None
    delivery_count: int = 0
    lock_token: str | None = None
    dead_letter_reason: str | None = None
    dead_letter_error_description: str | None = None

    @classmethod
    def create(
        cls,
        event_type: str,
        source_system: str,
        payload: dict[str, Any],
        queue_or_topic: str,
        schema_version: str = "1.0",
        session_id: str | None = None,
        correlation_id: str | None = None,
        enqueue_time_utc: datetime | None = None,
    ) -> AzureServiceBusEnvelope:
        """
        Factory for a new outbound AzureServiceBusEnvelope.

        Args:
            event_type: Domain event discriminator.
            source_system: Name of the producing system.
            payload: Business event data.
            queue_or_topic: Target queue or topic name.
            schema_version: Payload schema version string.
            session_id: Service Bus session ID for ordered delivery.
            correlation_id: Optional correlation ID (for request/reply tracing).
            enqueue_time_utc: Scheduled delivery time (``None`` = send now).

        Returns:
            A new ``AzureServiceBusEnvelope`` ready to send.
        """
        return cls(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            source_system=source_system,
            payload=payload,
            queue_or_topic=queue_or_topic,
            schema_version=schema_version,
            session_id=session_id,
            correlation_id=correlation_id,
            enqueue_time_utc=enqueue_time_utc,
        )

    @classmethod
    def from_service_bus_message(cls, message: Any) -> AzureServiceBusEnvelope:
        """
        Deserialise from a ``ServiceBusReceivedMessage`` (azure-servicebus SDK).

        Accepts either an SDK ``ServiceBusReceivedMessage`` object (reads
        ``.body`` bytes and ``.application_properties``) or a plain dict in
        the same shape (useful for testing without the SDK installed).

        Args:
            message: SDK ``ServiceBusReceivedMessage`` or equivalent dict.

        Returns:
            A populated ``AzureServiceBusEnvelope``.
        """
        # Support both the real SDK object and a test-friendly dict
        if isinstance(message, dict):
            raw_body = message.get("body", b"{}")
            session_id = message.get("session_id")
            correlation_id = message.get("correlation_id")
            delivery_count = int(message.get("delivery_count", 0))
            lock_token = message.get("lock_token")
        else:
            body_raw = message.body
            if hasattr(body_raw, "__iter__") and not isinstance(body_raw, (bytes, str)):
                raw_body = b"".join(body_raw)
            else:
                raw_body = body_raw
            session_id = getattr(message, "session_id", None)
            correlation_id = getattr(message, "correlation_id", None)
            delivery_count = int(getattr(message, "delivery_count", 0))
            lock_token = str(getattr(message, "lock_token", None) or "")

        body_str = raw_body.decode("utf-8") if isinstance(raw_body, bytes) else raw_body
        data: dict[str, Any] = json.loads(body_str)

        return cls(
            event_id=data.get("event_id", str(uuid.uuid4())),
            event_type=data.get("event_type", ""),
            source_system=data.get("source_system", ""),
            payload=data.get("payload", {}),
            queue_or_topic=data.get("queue_or_topic", ""),
            schema_version=data.get("schema_version", "1.0"),
            created_at=(
                datetime.fromisoformat(data["created_at"]) if "created_at" in data else datetime.now(timezone.utc)
            ),
            session_id=session_id,
            correlation_id=correlation_id,
            delivery_count=delivery_count,
            lock_token=lock_token,
        )

    def to_service_bus_message(self) -> dict[str, Any]:
        """
        Serialise to a dict suitable for constructing a ``ServiceBusMessage``.

        The returned dict can be passed to
        ``azure.servicebus.ServiceBusMessage(**envelope.to_service_bus_message())``.

        Returns:
            Dict with ``body``, ``message_id``, ``content_type``,
            ``application_properties``, and optionally ``session_id``,
            ``correlation_id``, ``scheduled_enqueue_time_utc``.
        """
        body: dict[str, Any] = {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "source_system": self.source_system,
            "payload": self.payload,
            "queue_or_topic": self.queue_or_topic,
            "schema_version": self.schema_version,
            "created_at": self.created_at.isoformat(),
        }
        params: dict[str, Any] = {
            "body": json.dumps(body, ensure_ascii=False),
            "message_id": self.event_id,
            "content_type": "application/json",
            "application_properties": {
                "EventType": self.event_type,
                "SourceSystem": self.source_system,
                "SchemaVersion": self.schema_version,
            },
        }
        if self.session_id is not None:
            params["session_id"] = self.session_id
        if self.correlation_id is not None:
            params["correlation_id"] = self.correlation_id
        if self.enqueue_time_utc is not None:
            params["scheduled_enqueue_time_utc"] = self.enqueue_time_utc
        return params

    def mark_dead_letter(
        self,
        reason: str,
        error_description: str | None = None,
    ) -> AzureServiceBusEnvelope:
        """
        Return a copy of this envelope flagged for DLQ routing.

        Args:
            reason: Short reason code (maps to Service Bus ``DeadLetterReason``).
            error_description: Optional extended description.

        Returns:
            New envelope with ``dead_letter_reason`` and optionally
            ``dead_letter_error_description`` set.
        """
        import dataclasses

        return dataclasses.replace(
            self,
            dead_letter_reason=reason,
            dead_letter_error_description=error_description,
        )

    def is_session_enabled(self) -> bool:
        """Return ``True`` if this envelope targets a session-enabled entity."""
        return self.session_id is not None

    def is_scheduled(self) -> bool:
        """Return ``True`` if this envelope has a scheduled enqueue time."""
        return self.enqueue_time_utc is not None
