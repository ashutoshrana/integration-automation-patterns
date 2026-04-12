"""
outbox.py — Transactional Outbox pattern for reliable event publishing.

Ensures events are published exactly once even when the message broker is
temporarily unavailable.  The application writes events to an outbox table in
the same database transaction as the business operation, then a separate relay
process polls the outbox and publishes to the broker.

This module provides the data structures and processor logic.  Persistence of
``OutboxRecord`` objects is the caller's responsibility (typically a repository
layer wrapping a relational database).

Usage::

    # In your business logic (same DB transaction):
    record = OutboxRecord.create(
        aggregate_id="account-001",
        event_type="AccountUpdated",
        payload={"account_id": "001", "status": "active"},
    )
    outbox_repo.save(record)

    # In your relay process:
    processor = OutboxProcessor(
        fetch_pending=outbox_repo.get_pending,
        publish=kafka_producer.send,
        mark_published=outbox_repo.mark_published,
    )
    processor.process_batch(batch_size=50)
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class OutboxRecord:
    """
    A single pending event in the transactional outbox.

    Attributes:
        record_id: UUID identifying this outbox entry.
        aggregate_id: ID of the domain object that produced the event.
        event_type: String event discriminator (e.g. ``"AccountUpdated"``).
        payload: Event data as a JSON-serialisable dict.
        created_at: UTC timestamp when the record was inserted.
        published_at: UTC timestamp when the event was confirmed published, or ``None``.
        attempt_count: Number of publish attempts made.
        last_error: Last error message, or ``None``.
    """

    aggregate_id: str
    event_type: str
    payload: dict[str, Any]
    record_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    published_at: datetime | None = None
    attempt_count: int = 0
    last_error: str | None = None

    @classmethod
    def create(
        cls,
        aggregate_id: str,
        event_type: str,
        payload: dict[str, Any],
    ) -> OutboxRecord:
        """
        Factory method for creating a new outbox entry.

        Args:
            aggregate_id: ID of the domain aggregate that generated the event.
            event_type: String discriminator for the event type.
            payload: Event data dict (must be JSON-serialisable).

        Returns:
            A new ``OutboxRecord`` with ``published_at=None``.
        """
        return cls(aggregate_id=aggregate_id, event_type=event_type, payload=payload)

    @property
    def is_pending(self) -> bool:
        """True if the event has not yet been successfully published."""
        return self.published_at is None

    def to_event_dict(self) -> dict[str, Any]:
        """
        Return a dict representation suitable for publishing to a message broker.

        Includes ``record_id`` for idempotency deduplication on the consumer side.
        """
        return {
            "record_id": self.record_id,
            "aggregate_id": self.aggregate_id,
            "event_type": self.event_type,
            "payload": self.payload,
            "created_at": self.created_at.isoformat(),
        }


@dataclass
class OutboxProcessor:
    """
    Polls an outbox table and relays pending events to a message broker.

    All three callables are injected so the processor is storage- and
    broker-agnostic.

    Args:
        fetch_pending: Returns a list of unpublished ``OutboxRecord`` objects.
            Signature: ``(batch_size: int) -> list[OutboxRecord]``
        publish: Publishes an event dict to the broker.
            Signature: ``(event: dict) -> None``
            Should raise on failure.
        mark_published: Marks a record as successfully published.
            Signature: ``(record_id: str) -> None``
        mark_failed: Optional; records a failed attempt with error message.
            Signature: ``(record_id: str, error: str, attempt_count: int) -> None``
    """

    fetch_pending: Callable[[int], list[OutboxRecord]]
    publish: Callable[[dict[str, Any]], None]
    mark_published: Callable[[str], None]
    mark_failed: Callable[[str, str, int], None] | None = None

    def process_batch(self, batch_size: int = 100) -> tuple[int, int]:
        """
        Process one batch of pending outbox records.

        Args:
            batch_size: Maximum number of records to process in this call.

        Returns:
            Tuple of ``(published_count, failed_count)``.
        """
        records = self.fetch_pending(batch_size)
        if not records:
            return 0, 0

        published = 0
        failed = 0

        for record in records:
            try:
                event = record.to_event_dict()
                self.publish(event)
                self.mark_published(record.record_id)
                published += 1
                logger.debug(
                    "[OUTBOX] Published record_id=%s event_type=%s aggregate_id=%s",
                    record.record_id,
                    record.event_type,
                    record.aggregate_id,
                )
            except Exception as exc:
                failed += 1
                error_msg = str(exc)
                logger.warning(
                    "[OUTBOX] Publish failed record_id=%s event_type=%s error=%s attempt=%d",
                    record.record_id,
                    record.event_type,
                    error_msg,
                    record.attempt_count + 1,
                )
                if self.mark_failed is not None:
                    try:
                        self.mark_failed(record.record_id, error_msg, record.attempt_count + 1)
                    except Exception as mark_exc:
                        logger.error("[OUTBOX] Failed to mark record as failed: %s", mark_exc)

        logger.info("[OUTBOX] Batch complete published=%d failed=%d", published, failed)
        return published, failed


@dataclass
class AsyncOutboxProcessor:
    """
    Async variant of ``OutboxProcessor`` for use with asyncio-based applications
    (FastAPI, aiohttp, Starlette) where the broker client exposes a coroutine API.

    All callables may be regular functions or coroutines — ``AsyncOutboxProcessor``
    detects this at call time using ``inspect.iscoroutinefunction`` and awaits
    accordingly. This lets you mix async and sync dependencies without ceremony.

    Args:
        fetch_pending: Returns pending records.  May be sync or async.
            Signature: ``(batch_size: int) -> list[OutboxRecord]``
        publish: Publishes an event to the broker.  May be sync or async.
            Signature: ``(event: dict) -> None``
        mark_published: Marks a record published.  May be sync or async.
            Signature: ``(record_id: str) -> None``
        mark_failed: Optional failure recorder.  May be sync or async.
            Signature: ``(record_id: str, error: str, attempt_count: int) -> None``

    Usage::

        processor = AsyncOutboxProcessor(
            fetch_pending=db.fetch_pending_outbox,    # async def
            publish=kafka.send,                        # async def
            mark_published=db.mark_published,          # async def
        )

        # In an asyncio background task:
        published, failed = await processor.process_batch(batch_size=50)
    """

    fetch_pending: Any  # Callable[[int], Awaitable[list[OutboxRecord]] | list[OutboxRecord]]
    publish: Any  # Callable[[dict], Awaitable[None] | None]
    mark_published: Any  # Callable[[str], Awaitable[None] | None]
    mark_failed: Any | None = None

    async def process_batch(self, batch_size: int = 100) -> tuple[int, int]:
        """
        Async variant of ``OutboxProcessor.process_batch``.

        Args:
            batch_size: Maximum records to process per call.

        Returns:
            Tuple of ``(published_count, failed_count)``.
        """
        import inspect

        if inspect.iscoroutinefunction(self.fetch_pending):
            records = await self.fetch_pending(batch_size)
        else:
            records = self.fetch_pending(batch_size)

        if not records:
            return 0, 0

        published = 0
        failed = 0

        for record in records:
            try:
                event = record.to_event_dict()

                if inspect.iscoroutinefunction(self.publish):
                    await self.publish(event)
                else:
                    self.publish(event)

                if inspect.iscoroutinefunction(self.mark_published):
                    await self.mark_published(record.record_id)
                else:
                    self.mark_published(record.record_id)

                published += 1
                logger.debug(
                    "[OUTBOX] Published record_id=%s event_type=%s aggregate_id=%s",
                    record.record_id,
                    record.event_type,
                    record.aggregate_id,
                )
            except Exception as exc:
                failed += 1
                error_msg = str(exc)
                logger.warning(
                    "[OUTBOX] Publish failed record_id=%s event_type=%s error=%s attempt=%d",
                    record.record_id,
                    record.event_type,
                    error_msg,
                    record.attempt_count + 1,
                )
                if self.mark_failed is not None:
                    try:
                        if inspect.iscoroutinefunction(self.mark_failed):
                            await self.mark_failed(record.record_id, error_msg, record.attempt_count + 1)
                        else:
                            self.mark_failed(record.record_id, error_msg, record.attempt_count + 1)
                    except Exception as mark_exc:
                        logger.error("[OUTBOX] Failed to mark record as failed: %s", mark_exc)

        logger.info("[OUTBOX] Batch complete published=%d failed=%d", published, failed)
        return published, failed
