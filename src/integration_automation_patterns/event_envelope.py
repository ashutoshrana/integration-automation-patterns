"""
event_envelope.py — Reliable Event Handling for Enterprise Integration

Enterprise integration fails most often at the transport layer — not because
the business logic is wrong, but because events are dropped, duplicated, or
processed out of order when systems restart, network partitions occur, or
downstream services are temporarily unavailable.

This module provides the structural primitives for building reliable,
replay-safe event handling in enterprise integration workflows. The patterns
apply regardless of the message broker (Kafka, SQS, Azure Service Bus,
GCP Pub/Sub, RabbitMQ, MQ Series) or the enterprise platforms being integrated
(CRM, ERP, ITSM, custom services).

Design goals:
  - Every event carries enough context to be replayed safely
  - Retry behavior is explicit and bounded, not implicit
  - Delivery status is inspectable without querying the broker
  - Deduplication is structural (idempotency key), not a side effect of storage
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class DeliveryStatus(Enum):
    """
    Tracks the lifecycle of one event through the integration pipeline.

    Events move forward through this lifecycle. An event that has reached
    FAILED should not be silently dropped — it should be routed to a dead
    letter queue or an explicit remediation path.
    """

    PENDING = "pending"  # Created, not yet dispatched
    DISPATCHED = "dispatched"  # Sent to broker or downstream system
    ACKNOWLEDGED = "acknowledged"  # Downstream confirmed receipt
    PROCESSED = "processed"  # Business logic completed successfully
    RETRYING = "retrying"  # Failed at least once, within retry budget
    FAILED = "failed"  # Retry budget exhausted; requires intervention
    SKIPPED = "skipped"  # Intentionally not processed (e.g., duplicate)


@dataclass(slots=True)
class RetryPolicy:
    """
    Defines retry behavior for a failed event delivery attempt.

    Explicit retry policies prevent two common failure modes:
    1. Infinite retry loops that mask persistent downstream failures
    2. Retry storms that overwhelm recovering services

    Attributes:
        max_attempts: Total number of delivery attempts allowed (including
            the first). After this many attempts, the event moves to FAILED.
        backoff_seconds: Base wait time between retries in seconds.
            Actual wait = backoff_seconds * (2 ** attempt_number) when
            exponential = True.
        exponential: If True, use exponential backoff. If False, use
            fixed-interval retry.
        max_backoff_seconds: Upper bound on wait time when using exponential
            backoff. Prevents unbounded delays.
    """

    max_attempts: int = 3
    backoff_seconds: int = 30
    exponential: bool = True
    max_backoff_seconds: int = 3600

    def wait_seconds_for_attempt(self, attempt_number: int) -> int:
        """
        Calculate the wait time before the given attempt number.

        Args:
            attempt_number: Zero-indexed attempt count (0 = first retry,
                not the original attempt).

        Returns:
            Seconds to wait before attempting delivery again.
        """
        if not self.exponential:
            return self.backoff_seconds
        delay: int = int(self.backoff_seconds * (2**attempt_number))
        return min(delay, self.max_backoff_seconds)

    def is_exhausted(self, attempt_number: int) -> bool:
        """Return True if the attempt budget is exhausted."""
        return attempt_number >= self.max_attempts


@dataclass
class EventEnvelope:
    """
    A transport container for one unit of work in an enterprise integration flow.

    An EventEnvelope carries the event payload alongside the metadata needed
    to route, deduplicate, retry, and audit it. Separating envelope metadata
    from business payload means integration infrastructure can handle
    reliability concerns without touching business logic.

    Design principle: The envelope must contain everything needed to replay
    the event safely, without requiring coordination with the original sender.

    Attributes:
        event_id: Globally unique identifier for this event. Used as the
            idempotency key — processing the same event_id twice should
            produce the same result as processing it once.
        event_type: A namespaced string identifying what happened.
            Convention: "<domain>.<entity>.<action>" (e.g., "enrollment.student.updated")
        source_system: Identifier for the system that originated the event.
            Used for routing, filtering, and audit.
        payload: The business data associated with the event. Keep payloads
            small — prefer IDs + change summary over full record snapshots.
            Full records should be fetched from the system of record at
            processing time, not embedded in the event.
        correlation_id: Links this event to a parent workflow, request, or
            transaction. Enables tracing across system boundaries.
        schema_version: Semantic version of the payload schema. Consumers
            should reject events with unexpected versions rather than
            silently misinterpreting the payload.
        created_at: Wall clock time when the event was created. Stored in
            UTC. Do not use for business ordering — use a sequence or
            logical clock for that.
        status: Current delivery status. Updated as the event moves through
            the pipeline.
        attempt_count: Number of delivery attempts made. Starts at 0,
            incremented before each attempt.
        retry_policy: Retry behavior for failed delivery attempts.
        tags: Arbitrary key-value metadata for routing, filtering, or
            observability. Not used in business logic.
    """

    event_id: str
    event_type: str
    source_system: str
    payload: dict[str, Any]
    correlation_id: str = ""
    schema_version: str = "1.0"
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    status: DeliveryStatus = DeliveryStatus.PENDING
    attempt_count: int = 0
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    tags: dict[str, str] = field(default_factory=dict)

    def mark_dispatched(self) -> None:
        """Record that this event has been sent to the broker or downstream system."""
        self.attempt_count += 1
        self.status = DeliveryStatus.DISPATCHED

    def mark_acknowledged(self) -> None:
        """Record that the downstream system confirmed receipt."""
        self.status = DeliveryStatus.ACKNOWLEDGED

    def mark_processed(self) -> None:
        """Record that business processing completed successfully."""
        self.status = DeliveryStatus.PROCESSED

    def mark_failed(self) -> None:
        """
        Record a failed delivery attempt and advance status accordingly.

        If the retry budget is not exhausted, status becomes RETRYING.
        If the budget is exhausted, status becomes FAILED.
        """
        if self.retry_policy.is_exhausted(self.attempt_count):
            self.status = DeliveryStatus.FAILED
        else:
            self.status = DeliveryStatus.RETRYING

    def mark_skipped(self, reason: str = "") -> None:
        """Record that this event was intentionally skipped (e.g., duplicate)."""
        self.status = DeliveryStatus.SKIPPED
        if reason:
            self.tags["skip_reason"] = reason

    def next_retry_wait_seconds(self) -> int:
        """
        Return the wait time before the next delivery attempt.

        Returns 0 if the event is not in a retryable state.
        """
        if self.status != DeliveryStatus.RETRYING:
            return 0
        return self.retry_policy.wait_seconds_for_attempt(self.attempt_count)

    def is_terminal(self) -> bool:
        """Return True if the event has reached a state that requires no further processing."""
        return self.status in {
            DeliveryStatus.PROCESSED,
            DeliveryStatus.FAILED,
            DeliveryStatus.SKIPPED,
        }

    def to_audit_line(self) -> str:
        """Format a structured log line for integration observability systems."""
        return (
            f"[INTEGRATION_EVENT] event_id={self.event_id} "
            f"type={self.event_type} "
            f"source={self.source_system} "
            f"status={self.status.value} "
            f"attempts={self.attempt_count} "
            f"correlation={self.correlation_id or 'none'} "
            f"schema={self.schema_version} "
            f"created={self.created_at.isoformat()}"
        )
