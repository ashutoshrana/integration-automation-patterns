"""
sqs_envelope.py — AWS SQS/SNS message envelope for enterprise event-driven integration.

Wraps business events in an AWS SQS-compatible envelope, supporting:
- Standard queues and FIFO queues (MessageGroupId / MessageDeduplicationId)
- SNS fan-out pass-through (unwraps the SNS wrapper transparently)
- Dead-letter queue (DLQ) routing with approximate receive count tracking
- Idempotency key propagation via MessageAttributes

Follows the same envelope-carries-metadata pattern as ``KafkaEventEnvelope``:
the envelope holds routing/broker metadata; the payload holds business data.

Usage::

    # Produce to SQS standard queue
    envelope = SQSEventEnvelope.create(
        event_type="OrderPlaced",
        source_system="order-service",
        payload={"order_id": "ORD-001", "total": 99.95},
        queue_url="https://sqs.us-east-1.amazonaws.com/123456789012/orders",
    )
    client.send_message(**envelope.to_send_params())

    # Produce to SQS FIFO queue — ordering within order_id
    envelope = SQSEventEnvelope.create(
        event_type="OrderUpdated",
        source_system="order-service",
        payload={"order_id": "ORD-001", "status": "shipped"},
        queue_url="https://sqs.us-east-1.amazonaws.com/123456789012/orders.fifo",
        message_group_id="ORD-001",
    )
    client.send_message(**envelope.to_send_params())

    # Consume from SQS
    for msg in sqs.receive_message(...)["Messages"]:
        envelope = SQSEventEnvelope.from_sqs_message(msg)
        # SNS-wrapped messages are unwrapped automatically
        process(envelope)
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass
class SQSEventEnvelope:
    """
    AWS SQS-compatible event envelope.

    Attributes:
        event_id: UUID for idempotency deduplication.
        event_type: Domain event discriminator (e.g. ``"OrderPlaced"``).
        source_system: Name of the producing system.
        payload: Business event data (JSON-serialisable dict).
        queue_url: Target or source SQS queue URL.
        schema_version: Payload schema version string.
        created_at: UTC timestamp when this envelope was created.
        message_group_id: FIFO queue message group (ordering key). ``None`` for
            standard queues.
        message_deduplication_id: FIFO queue content-based deduplication ID.
            Auto-set to ``event_id`` when ``message_group_id`` is provided and
            this field is ``None``.
        receipt_handle: Populated on consume; required for ``delete_message``.
        approximate_receive_count: Number of times this message has been received
            (from SQS ``ApproximateReceiveCount`` attribute).
        dead_letter_reason: If non-None, marks this envelope for DLQ routing.
    """

    event_id: str
    event_type: str
    source_system: str
    payload: dict[str, Any]
    queue_url: str
    schema_version: str = "1.0"
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    message_group_id: str | None = None
    message_deduplication_id: str | None = None
    receipt_handle: str | None = None
    approximate_receive_count: int = 0
    dead_letter_reason: str | None = None

    @classmethod
    def create(
        cls,
        event_type: str,
        source_system: str,
        payload: dict[str, Any],
        queue_url: str,
        schema_version: str = "1.0",
        message_group_id: str | None = None,
        message_deduplication_id: str | None = None,
    ) -> SQSEventEnvelope:
        """
        Factory for creating a new outbound SQSEventEnvelope.

        Args:
            event_type: Domain event discriminator.
            source_system: Name of the producing system.
            payload: Business event data.
            queue_url: Target SQS queue URL.
            schema_version: Payload schema version string.
            message_group_id: FIFO queue ordering key. Provide for FIFO queues;
                omit for standard queues.
            message_deduplication_id: FIFO deduplication ID. Defaults to
                ``event_id`` when ``message_group_id`` is provided.

        Returns:
            A new ``SQSEventEnvelope`` ready to send.
        """
        event_id = str(uuid.uuid4())
        dedup_id = message_deduplication_id or (event_id if message_group_id else None)
        return cls(
            event_id=event_id,
            event_type=event_type,
            source_system=source_system,
            payload=payload,
            queue_url=queue_url,
            schema_version=schema_version,
            message_group_id=message_group_id,
            message_deduplication_id=dedup_id,
        )

    @classmethod
    def from_sqs_message(cls, message: dict[str, Any]) -> SQSEventEnvelope:
        """
        Deserialise an SQS message dict (as returned by ``receive_message``).

        Handles both direct SQS messages and SNS-wrapped fan-out messages
        transparently — if the body is an SNS notification envelope, the inner
        ``Message`` field is unwrapped automatically.

        Args:
            message: Raw SQS message dict from ``receive_message``.

        Returns:
            A populated ``SQSEventEnvelope``.
        """
        raw_body = message.get("Body", "{}")
        body: dict[str, Any] = json.loads(raw_body)

        # Unwrap SNS fan-out: SNS wraps the original message in a JSON envelope
        # with a ``Type`` field set to ``"Notification"``
        if body.get("Type") == "Notification":
            body = json.loads(body.get("Message", "{}"))

        attrs = message.get("Attributes", {})
        receive_count = int(attrs.get("ApproximateReceiveCount", 0))

        return cls(
            event_id=body.get("event_id", str(uuid.uuid4())),
            event_type=body.get("event_type", ""),
            source_system=body.get("source_system", ""),
            payload=body.get("payload", {}),
            queue_url=body.get("queue_url", ""),
            schema_version=body.get("schema_version", "1.0"),
            created_at=(
                datetime.fromisoformat(body["created_at"].replace("Z", "+00:00"))
                if "created_at" in body
                else datetime.now(timezone.utc)
            ),
            message_group_id=message.get("Attributes", {}).get("MessageGroupId"),
            receipt_handle=message.get("ReceiptHandle"),
            approximate_receive_count=receive_count,
        )

    def to_send_params(self) -> dict[str, Any]:
        """
        Serialise to a dict suitable for ``boto3`` ``sqs.send_message(**params)``.

        Returns:
            Dict with ``QueueUrl``, ``MessageBody``, ``MessageAttributes``, and
            (for FIFO queues) ``MessageGroupId`` / ``MessageDeduplicationId``.
        """
        body: dict[str, Any] = {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "source_system": self.source_system,
            "payload": self.payload,
            "queue_url": self.queue_url,
            "schema_version": self.schema_version,
            "created_at": self.created_at.isoformat(),
        }
        params: dict[str, Any] = {
            "QueueUrl": self.queue_url,
            "MessageBody": json.dumps(body, ensure_ascii=False),
            "MessageAttributes": {
                "EventType": {"DataType": "String", "StringValue": self.event_type},
                "SourceSystem": {"DataType": "String", "StringValue": self.source_system},
                "SchemaVersion": {"DataType": "String", "StringValue": self.schema_version},
                "EventId": {"DataType": "String", "StringValue": self.event_id},
            },
        }
        if self.message_group_id is not None:
            params["MessageGroupId"] = self.message_group_id
        if self.message_deduplication_id is not None:
            params["MessageDeduplicationId"] = self.message_deduplication_id
        return params

    def mark_dead_letter(self, reason: str) -> SQSEventEnvelope:
        """
        Return a copy of this envelope flagged for DLQ routing.

        Args:
            reason: Human-readable reason for DLQ routing.

        Returns:
            New envelope with ``dead_letter_reason`` set.
        """
        import dataclasses

        return dataclasses.replace(self, dead_letter_reason=reason)

    def is_fifo(self) -> bool:
        """Return ``True`` if this envelope targets a FIFO queue."""
        return self.message_group_id is not None
