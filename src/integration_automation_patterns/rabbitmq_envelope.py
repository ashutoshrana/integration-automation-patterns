"""
rabbitmq_envelope.py — RabbitMQ AMQP message envelope for enterprise integration.

Wraps business events in a RabbitMQ-compatible envelope, supporting:
- Direct, fanout, topic, and headers exchange routing
- Dead-letter exchange (DLX) routing with x-death header tracking
- Publisher confirms (delivery_tag tracking for acknowledgement)
- Per-message TTL and priority
- Correlation ID for request/reply and tracing patterns
- Content-type and content-encoding metadata

AMQP routing model:
  Producer → Exchange (direct/fanout/topic/headers) → Queue → Consumer
This envelope carries both the business payload and the AMQP routing metadata.

Usage::

    # Produce to a topic exchange
    envelope = RabbitMQEnvelope.create(
        event_type="OrderPlaced",
        source_system="order-service",
        payload={"order_id": "ORD-001"},
        exchange="orders",
        routing_key="orders.placed.eu",
    )
    channel.basic_publish(
        exchange=envelope.exchange,
        routing_key=envelope.routing_key,
        body=envelope.to_amqp_body(),
        properties=envelope.to_amqp_properties(),
    )

    # Produce to a direct exchange with TTL
    envelope = RabbitMQEnvelope.create(
        event_type="SessionExpiry",
        source_system="auth",
        payload={"session_id": "s-001"},
        exchange="auth",
        routing_key="session.expire",
        ttl_ms=30_000,   # message expires after 30 s if not consumed
    )

    # Consume — reconstruct from pika BasicProperties + body
    def on_message(channel, method, properties, body):
        envelope = RabbitMQEnvelope.from_amqp_message(
            body=body,
            properties=properties,
            delivery_tag=method.delivery_tag,
        )
        process(envelope)
        channel.basic_ack(delivery_tag=method.delivery_tag)
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass
class RabbitMQEnvelope:
    """
    RabbitMQ AMQP-compatible event envelope.

    Attributes:
        event_id: UUID for idempotency; stored as AMQP ``message_id``.
        event_type: Domain event discriminator; stored as AMQP header.
        source_system: Name of the producing system; stored as AMQP header.
        payload: Business event data (JSON-serialisable dict).
        exchange: AMQP exchange to publish to (or empty string for default exchange).
        routing_key: AMQP routing key (queue name for direct/default exchange;
            pattern for topic exchange; binding key for headers exchange).
        schema_version: Payload schema version string; stored as AMQP header.
        created_at: UTC timestamp when this envelope was created.
        correlation_id: Optional AMQP correlation ID for request/reply tracing.
        reply_to: Optional AMQP reply-to queue name.
        ttl_ms: Per-message TTL in milliseconds. ``None`` = no TTL (queue policy applies).
        priority: AMQP message priority (0–255). ``None`` = default priority.
        delivery_tag: Populated on consume; required for ``basic_ack()``/``basic_nack()``.
        redelivered: ``True`` if this is a redelivery (consumer crash/reject).
        death_count: Number of times this message has been dead-lettered
            (from ``x-death`` header; 0 if never dead-lettered).
        dead_letter_reason: If non-None, marks this envelope for DLX routing.
    """

    event_id: str
    event_type: str
    source_system: str
    payload: dict[str, Any]
    exchange: str
    routing_key: str
    schema_version: str = "1.0"
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    correlation_id: str | None = None
    reply_to: str | None = None
    ttl_ms: int | None = None
    priority: int | None = None
    delivery_tag: int | None = None
    redelivered: bool = False
    death_count: int = 0
    dead_letter_reason: str | None = None

    @classmethod
    def create(
        cls,
        event_type: str,
        source_system: str,
        payload: dict[str, Any],
        exchange: str,
        routing_key: str,
        schema_version: str = "1.0",
        correlation_id: str | None = None,
        reply_to: str | None = None,
        ttl_ms: int | None = None,
        priority: int | None = None,
    ) -> RabbitMQEnvelope:
        """
        Factory for creating a new outbound RabbitMQEnvelope.

        Args:
            event_type: Domain event discriminator.
            source_system: Name of the producing system.
            payload: Business event data.
            exchange: AMQP exchange name (``""`` for default exchange).
            routing_key: AMQP routing key.
            schema_version: Payload schema version string.
            correlation_id: Optional correlation ID for tracing.
            reply_to: Optional reply-to queue for request/reply patterns.
            ttl_ms: Per-message TTL in milliseconds.
            priority: Message priority (0–255).

        Returns:
            A new ``RabbitMQEnvelope`` ready to publish.
        """
        return cls(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            source_system=source_system,
            payload=payload,
            exchange=exchange,
            routing_key=routing_key,
            schema_version=schema_version,
            correlation_id=correlation_id,
            reply_to=reply_to,
            ttl_ms=ttl_ms,
            priority=priority,
        )

    @classmethod
    def from_amqp_message(
        cls,
        body: bytes | str,
        properties: Any | None = None,
        delivery_tag: int | None = None,
        redelivered: bool = False,
        exchange: str = "",
        routing_key: str = "",
    ) -> RabbitMQEnvelope:
        """
        Deserialise from raw AMQP message parts (pika or compatible AMQP library).

        Accepts either real ``pika.BasicProperties`` or a plain dict for
        ``properties`` — useful for testing without pika installed.

        Args:
            body: Raw AMQP message body (bytes or str).
            properties: ``pika.BasicProperties`` or dict with ``headers``,
                ``message_id``, ``correlation_id``, ``reply_to``, ``priority``.
            delivery_tag: Delivery tag from ``method.delivery_tag``.
            redelivered: Whether this is a redelivery.
            exchange: Exchange the message arrived on.
            routing_key: Routing key the message arrived on.

        Returns:
            A populated ``RabbitMQEnvelope``.
        """
        body_str = body.decode("utf-8") if isinstance(body, bytes) else body
        data: dict[str, Any] = json.loads(body_str)

        # Handle both pika.BasicProperties and dict
        if isinstance(properties, dict):
            headers: dict[str, Any] = dict(properties.get("headers") or {})
            message_id: str = properties.get("message_id") or data.get("event_id", str(uuid.uuid4()))
            correlation_id = properties.get("correlation_id")
            reply_to = properties.get("reply_to")
            priority = properties.get("priority")
        elif properties is not None:
            headers = dict(getattr(properties, "headers", None) or {})
            message_id = getattr(properties, "message_id", None) or data.get("event_id", str(uuid.uuid4()))
            correlation_id = getattr(properties, "correlation_id", None)
            reply_to = getattr(properties, "reply_to", None)
            priority = getattr(properties, "priority", None)
        else:
            headers = {}
            message_id = data.get("event_id", str(uuid.uuid4()))
            correlation_id = None
            reply_to = None
            priority = None

        # x-death header: list of death records from RabbitMQ DLX
        x_death = headers.get("x-death", [])
        death_count = sum(int(d.get("count", 1)) for d in x_death) if isinstance(x_death, list) else 0

        return cls(
            event_id=message_id,
            event_type=data.get("event_type", headers.get("event_type", "")),
            source_system=data.get("source_system", headers.get("source_system", "")),
            payload=data.get("payload", {}),
            exchange=exchange,
            routing_key=routing_key,
            schema_version=data.get("schema_version", headers.get("schema_version", "1.0")),
            created_at=(
                datetime.fromisoformat(data["created_at"].replace("Z", "+00:00"))
                if "created_at" in data
                else datetime.now(timezone.utc)
            ),
            correlation_id=correlation_id,
            reply_to=reply_to,
            priority=int(priority) if priority is not None else None,
            delivery_tag=delivery_tag,
            redelivered=redelivered,
            death_count=death_count,
        )

    def to_amqp_body(self) -> bytes:
        """
        Serialise the envelope to UTF-8 JSON bytes for ``channel.basic_publish(body=...)``.

        Returns:
            UTF-8 encoded JSON bytes.
        """
        body: dict[str, Any] = {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "source_system": self.source_system,
            "payload": self.payload,
            "schema_version": self.schema_version,
            "created_at": self.created_at.isoformat(),
        }
        return json.dumps(body, ensure_ascii=False).encode("utf-8")

    def to_amqp_properties(self) -> dict[str, Any]:
        """
        Return AMQP properties dict for ``pika.BasicProperties(**props)``.

        Returns:
            Dict with ``message_id``, ``content_type``, ``delivery_mode``
            (persistent=2), ``headers``, and optional ``correlation_id``,
            ``reply_to``, ``expiration``, ``priority``.
        """
        props: dict[str, Any] = {
            "message_id": self.event_id,
            "content_type": "application/json",
            "delivery_mode": 2,  # persistent
            "headers": {
                "event_type": self.event_type,
                "source_system": self.source_system,
                "schema_version": self.schema_version,
            },
        }
        if self.correlation_id is not None:
            props["correlation_id"] = self.correlation_id
        if self.reply_to is not None:
            props["reply_to"] = self.reply_to
        if self.ttl_ms is not None:
            props["expiration"] = str(self.ttl_ms)
        if self.priority is not None:
            props["priority"] = self.priority
        return props

    def mark_dead_letter(self, reason: str) -> RabbitMQEnvelope:
        """
        Return a copy of this envelope flagged for dead-letter exchange routing.

        Args:
            reason: Human-readable reason for DLX routing.

        Returns:
            New envelope with ``dead_letter_reason`` set.
        """
        import dataclasses

        return dataclasses.replace(self, dead_letter_reason=reason)

    def is_dead_lettered(self) -> bool:
        """Return ``True`` if this message has been dead-lettered at least once."""
        return self.death_count > 0
