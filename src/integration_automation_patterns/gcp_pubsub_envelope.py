"""
gcp_pubsub_envelope.py — GCP Pub/Sub message envelope for enterprise integration.

Wraps business events in a GCP Pub/Sub-compatible envelope, supporting:
- Publisher attribute propagation (event_type, source_system, schema_version)
- Push subscription delivery (base64-encoded body in ``{"message": {...}}`` wrapper)
- Pull subscription delivery (PubsubMessage dict from ``subscriber.pull()``)
- Dead-letter topic routing via ``delivery_attempt`` tracking
- Ordering keys for per-key sequential delivery (analogous to Kafka partition keys)

GCP Pub/Sub encodes message data as base64 and carries metadata in string
``attributes`` — this envelope handles the encoding/decoding transparently.

Usage::

    # Produce
    envelope = GCPPubSubEnvelope.create(
        event_type="UserCreated",
        source_system="auth-service",
        payload={"user_id": "u-001", "email": "user@example.com"},
        topic="projects/my-project/topics/user-events",
    )
    publisher.publish(
        envelope.topic,
        data=envelope.to_publish_data(),
        **envelope.to_publish_attributes(),
    )

    # Consume from pull subscription
    for received_msg in subscriber.pull(...).received_messages:
        envelope = GCPPubSubEnvelope.from_pubsub_message(received_msg.message)
        process(envelope)
        subscriber.acknowledge(ack_ids=[received_msg.ack_id])

    # Consume from push subscription (HTTP endpoint receives JSON)
    envelope = GCPPubSubEnvelope.from_push_payload(request.json)
"""

from __future__ import annotations

import base64
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass
class GCPPubSubEnvelope:
    """
    GCP Pub/Sub-compatible event envelope.

    Attributes:
        event_id: UUID for idempotency; stored as ``message_id`` on publish.
        event_type: Domain event discriminator; stored as Pub/Sub attribute.
        source_system: Name of the producing system; stored as Pub/Sub attribute.
        payload: Business event data (JSON-serialisable dict).
        topic: Full Pub/Sub topic path (``projects/<project>/topics/<name>``).
        schema_version: Payload schema version string; stored as Pub/Sub attribute.
        created_at: UTC timestamp when this envelope was created.
        ordering_key: Pub/Sub ordering key for per-key sequential delivery.
            ``None`` for unordered messages.
        publish_time: Populated on consume from ``PubsubMessage.publish_time``.
        delivery_attempt: Number of delivery attempts (from
            ``PubsubMessage.delivery_attempt``; non-zero only with dead-letter
            policy enabled on the subscription).
        ack_id: Populated on pull consume; required for ``acknowledge()``.
        message_id: Server-assigned Pub/Sub message ID (populated on consume).
    """

    event_id: str
    event_type: str
    source_system: str
    payload: dict[str, Any]
    topic: str
    schema_version: str = "1.0"
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    ordering_key: str | None = None
    publish_time: datetime | None = None
    delivery_attempt: int = 0
    ack_id: str | None = None
    message_id: str | None = None

    @classmethod
    def create(
        cls,
        event_type: str,
        source_system: str,
        payload: dict[str, Any],
        topic: str,
        schema_version: str = "1.0",
        ordering_key: str | None = None,
    ) -> GCPPubSubEnvelope:
        """
        Factory for creating a new outbound GCPPubSubEnvelope.

        Args:
            event_type: Domain event discriminator.
            source_system: Name of the producing system.
            payload: Business event data.
            topic: Full Pub/Sub topic path.
            schema_version: Payload schema version string.
            ordering_key: Optional ordering key. Messages with the same key are
                delivered in order within a subscription. Requires the topic to
                have ``message_ordering`` enabled.

        Returns:
            A new ``GCPPubSubEnvelope`` ready to publish.
        """
        return cls(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            source_system=source_system,
            payload=payload,
            topic=topic,
            schema_version=schema_version,
            ordering_key=ordering_key,
        )

    @classmethod
    def from_pubsub_message(cls, message: Any) -> GCPPubSubEnvelope:
        """
        Deserialise from a Pub/Sub ``PubsubMessage`` (pull subscription).

        Accepts either a ``google.cloud.pubsub_v1`` ``PubsubMessage`` object or
        a plain dict in the same shape (useful for testing without the SDK).

        Args:
            message: ``PubsubMessage`` object or equivalent dict.

        Returns:
            A populated ``GCPPubSubEnvelope``.
        """
        if isinstance(message, dict):
            data_bytes: bytes = message.get("data", b"")
            attributes: dict[str, str] = dict(message.get("attributes", {}))
            publish_time_raw = message.get("publish_time")
            delivery_attempt = int(message.get("delivery_attempt", 0))
            message_id = message.get("message_id") or message.get("messageId")
            ordering_key = message.get("ordering_key") or message.get("orderingKey")
        else:
            data_bytes = bytes(getattr(message, "data", b""))
            attributes = dict(getattr(message, "attributes", {}) or {})
            publish_time_raw = getattr(message, "publish_time", None)
            delivery_attempt = int(getattr(message, "delivery_attempt", 0))
            message_id = getattr(message, "message_id", None)
            ordering_key = getattr(message, "ordering_key", None) or None

        body_str = data_bytes.decode("utf-8")
        data: dict[str, Any] = json.loads(body_str)

        publish_time: datetime | None = None
        if publish_time_raw is not None:
            if isinstance(publish_time_raw, str):
                publish_time = datetime.fromisoformat(publish_time_raw)
            elif hasattr(publish_time_raw, "isoformat"):
                publish_time = publish_time_raw

        return cls(
            event_id=data.get("event_id", str(uuid.uuid4())),
            event_type=data.get("event_type", attributes.get("event_type", "")),
            source_system=data.get("source_system", attributes.get("source_system", "")),
            payload=data.get("payload", {}),
            topic=data.get("topic", ""),
            schema_version=data.get("schema_version", attributes.get("schema_version", "1.0")),
            created_at=(
                datetime.fromisoformat(data["created_at"]) if "created_at" in data else datetime.now(timezone.utc)
            ),
            ordering_key=ordering_key or None,
            publish_time=publish_time,
            delivery_attempt=delivery_attempt,
            message_id=message_id,
        )

    @classmethod
    def from_push_payload(cls, push_payload: dict[str, Any]) -> GCPPubSubEnvelope:
        """
        Deserialise from a Pub/Sub push subscription HTTP payload.

        Push subscriptions deliver ``{"message": {"data": "<base64>",
        "attributes": {...}, "messageId": "...", "publishTime": "..."}}``
        to the registered endpoint.

        Args:
            push_payload: Parsed JSON body of the push delivery HTTP request.

        Returns:
            A populated ``GCPPubSubEnvelope``.
        """
        msg = push_payload.get("message", {})
        raw_data = msg.get("data", "")
        data_bytes = base64.b64decode(raw_data) if raw_data else b"{}"
        return cls.from_pubsub_message(
            {
                "data": data_bytes,
                "attributes": msg.get("attributes", {}),
                "publish_time": msg.get("publishTime"),
                "message_id": msg.get("messageId"),
                "ordering_key": msg.get("orderingKey"),
            }
        )

    def to_publish_data(self) -> bytes:
        """
        Serialise the envelope body to bytes for ``publisher.publish(data=...)``.

        Returns:
            UTF-8 encoded JSON bytes.
        """
        body: dict[str, Any] = {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "source_system": self.source_system,
            "payload": self.payload,
            "topic": self.topic,
            "schema_version": self.schema_version,
            "created_at": self.created_at.isoformat(),
        }
        return json.dumps(body, ensure_ascii=False).encode("utf-8")

    def to_publish_attributes(self) -> dict[str, str]:
        """
        Return Pub/Sub message attributes for ``publisher.publish(**attrs)``.

        Attributes are stored as string key→value pairs and are searchable
        via Pub/Sub subscription filters without deserialising the payload.

        Returns:
            Dict of string attributes.
        """
        attrs: dict[str, str] = {
            "event_type": self.event_type,
            "source_system": self.source_system,
            "schema_version": self.schema_version,
            "event_id": self.event_id,
        }
        return attrs

    def is_ordered(self) -> bool:
        """Return ``True`` if this envelope has an ordering key."""
        return self.ordering_key is not None

    def exceeds_delivery_attempts(self, max_attempts: int) -> bool:
        """
        Return ``True`` if delivery attempts exceed the configured threshold.

        Useful for implementing manual DLQ routing when GCP dead-letter policies
        are not enabled on the subscription.

        Args:
            max_attempts: Maximum allowed delivery attempts.
        """
        return self.delivery_attempt >= max_attempts
