"""
kafka_envelope.py — Kafka-aware event envelope for enterprise event-driven integration.

Extends the base ``EventEnvelope`` with Kafka-specific routing metadata:
partition key, topic, consumer group offset tracking, and dead-letter routing.

Follows the enterprise messaging pattern where the envelope carries routing
metadata separately from business payload, enabling broker-agnostic downstream
processing while preserving Kafka-specific observability.

Usage::

    envelope = KafkaEventEnvelope.create(
        event_type="AccountUpdated",
        source_system="salesforce",
        payload={"account_id": "001XX000003GYk1", "status": "Active"},
        topic="crm.accounts.v1",
        partition_key="001XX000003GYk1",  # route by account_id for ordering
    )

    # Serialise for producer
    msg = envelope.to_producer_record()
    producer.send(topic=msg["topic"], key=msg["key"], value=msg["value"])

    # Deserialise on consumer side
    received = KafkaEventEnvelope.from_consumer_record(topic, partition, offset, key, value)
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass
class KafkaEventEnvelope:
    """
    Kafka-aware event envelope combining business payload with broker routing metadata.

    Attributes:
        event_id: UUID for idempotency deduplication across consumer restarts.
        event_type: Domain event discriminator (e.g. ``"AccountUpdated"``).
        source_system: Name of the producing system (``"salesforce"``, ``"sap"``, etc.).
        payload: Business event data (JSON-serialisable dict).
        topic: Kafka topic this event is destined for or arrived from.
        partition_key: Key used for Kafka partition assignment.  Should ensure
            ordering for related events (e.g. same account_id).
        schema_version: Payload schema version for consumer compatibility.
        created_at: UTC timestamp when this envelope was created.
        headers: Optional Kafka message headers dict.
        partition: Assigned Kafka partition (set on consume; ``None`` on produce).
        offset: Kafka offset within the partition (set on consume; ``None`` on produce).
        dead_letter_reason: If non-None, this envelope should be routed to the DLQ.
    """

    event_id: str
    event_type: str
    source_system: str
    payload: dict[str, Any]
    topic: str
    partition_key: str
    schema_version: str = "1.0"
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    headers: dict[str, str] = field(default_factory=dict)
    partition: int | None = None
    offset: int | None = None
    dead_letter_reason: str | None = None

    @classmethod
    def create(
        cls,
        event_type: str,
        source_system: str,
        payload: dict[str, Any],
        topic: str,
        partition_key: str,
        schema_version: str = "1.0",
        headers: dict[str, str] | None = None,
    ) -> KafkaEventEnvelope:
        """
        Factory for creating a new outbound KafkaEventEnvelope.

        Args:
            event_type: Domain event discriminator.
            source_system: Name of the producing system.
            payload: Business event data.
            topic: Target Kafka topic.
            partition_key: Key for partition assignment (use a stable entity ID
                to guarantee ordering within that entity).
            schema_version: Payload schema version string.
            headers: Optional Kafka headers (string key→value).

        Returns:
            A new ``KafkaEventEnvelope`` ready for production.
        """
        return cls(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            source_system=source_system,
            payload=payload,
            topic=topic,
            partition_key=partition_key,
            schema_version=schema_version,
            headers=headers or {},
        )

    @classmethod
    def from_consumer_record(
        cls,
        topic: str,
        partition: int,
        offset: int,
        key: str | bytes | None,
        value: bytes | str,
    ) -> KafkaEventEnvelope:
        """
        Deserialise a consumed Kafka record into a KafkaEventEnvelope.

        Args:
            topic: Kafka topic the record arrived from.
            partition: Kafka partition number.
            offset: Record offset within the partition.
            key: Record key (bytes or str); used as partition_key.
            value: Record value bytes or string (must be JSON-encoded envelope).

        Returns:
            A ``KafkaEventEnvelope`` with partition/offset set.
        """
        raw = value if isinstance(value, str) else value.decode("utf-8")
        data: dict[str, Any] = json.loads(raw)

        partition_key = (key.decode("utf-8") if isinstance(key, bytes) else (key or ""))

        return cls(
            event_id=data.get("event_id", str(uuid.uuid4())),
            event_type=data.get("event_type", ""),
            source_system=data.get("source_system", ""),
            payload=data.get("payload", {}),
            topic=topic,
            partition_key=partition_key,
            schema_version=data.get("schema_version", "1.0"),
            created_at=(
                datetime.fromisoformat(data["created_at"]) if "created_at" in data else datetime.now(timezone.utc)
            ),
            headers=data.get("headers", {}),
            partition=partition,
            offset=offset,
        )

    def to_producer_record(self) -> dict[str, Any]:
        """
        Serialise to a dict suitable for a Kafka producer ``send()`` call.

        Returns:
            Dict with ``topic``, ``key`` (str), ``value`` (JSON bytes), and ``headers``.
        """
        body: dict[str, Any] = {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "source_system": self.source_system,
            "payload": self.payload,
            "topic": self.topic,
            "schema_version": self.schema_version,
            "created_at": self.created_at.isoformat(),
            "headers": self.headers,
        }
        return {
            "topic": self.topic,
            "key": self.partition_key,
            "value": json.dumps(body).encode("utf-8"),
            "headers": [(k, v.encode("utf-8")) for k, v in self.headers.items()],
        }

    def mark_dead_letter(self, reason: str) -> KafkaEventEnvelope:
        """
        Return a copy of this envelope flagged for dead-letter routing.

        Args:
            reason: Human-readable reason for DLQ routing.

        Returns:
            New envelope with ``dead_letter_reason`` set and ``topic`` updated
            to ``<original_topic>.dlq``.
        """
        import dataclasses

        return dataclasses.replace(
            self,
            dead_letter_reason=reason,
            topic=f"{self.topic}.dlq",
        )
