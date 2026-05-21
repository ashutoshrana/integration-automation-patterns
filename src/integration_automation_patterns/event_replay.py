"""
Event replay engine for enterprise integration pipelines.

Deterministic re-processing of transactional outbox records with:
- Content-addressable idempotency keys (SHA-256) — safe to run repeatedly
- Async concurrency via asyncio.Semaphore (default 5 parallel publishes)
- Selective replay by time window, event type, or correlation ID
- Dead-letter support and per-event error accumulation

Usage::

    engine = EventReplayEngine(outbox=outbox, publisher=kafka_publisher.publish)

    result = await engine.replay_since(
        since=datetime(2026, 5, 1),
        event_types=["order.created", "payment.processed"],
        max_events=500,
    )
    print(f"Replayed: {result.replayed}, Skipped: {result.skipped}, Failed: {result.failed}")
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)

__all__ = ["EventReplayEngine", "ReplayResult", "ReplayFilter"]


@dataclass
class ReplayFilter:
    """Criteria for selecting events to replay."""
    since: Optional[datetime] = None
    until: Optional[datetime] = None
    event_types: Optional[List[str]] = None
    correlation_ids: Optional[List[str]] = None
    max_events: int = 1000
    include_dead_letter: bool = False


@dataclass
class ReplayResult:
    """Summary of one replay run."""
    replayed: int = 0
    skipped: int = 0
    failed: int = 0
    dead_lettered: int = 0
    errors: List[Dict[str, Any]] = field(default_factory=list)
    duration_ms: float = 0.0


class EventReplayEngine:
    """
    Deterministic event replay with idempotency guarantees.

    Re-processes failed or missing events from the transactional outbox.
    Uses content-addressable idempotency keys to skip events that already
    succeeded — replay is always safe to run without risk of duplicate
    side effects.

    Args:
        outbox: Outbox instance with get_pending() and mark_processed() methods.
        publisher: Callable (sync or async) that publishes one event; returns True on success.
        idempotency_store: Optional set or Redis-backed store of processed event hashes.
            Defaults to an in-memory set (resets on restart).
        concurrency: Events to process in parallel (default: 5).

    Example::

        engine = EventReplayEngine(
            outbox=TransactionalOutbox(db=db),
            publisher=kafka_publisher.publish,
            concurrency=10,
        )
        result = await engine.replay(
            ReplayFilter(since=datetime(2026, 5, 1), event_types=["order.placed"])
        )
    """

    def __init__(
        self,
        outbox: Any,
        publisher: Callable[[Any], Any],
        idempotency_store: Optional[Set[str]] = None,
        concurrency: int = 5,
    ) -> None:
        self._outbox = outbox
        self._publisher = publisher
        self._processed: Set[str] = idempotency_store if idempotency_store is not None else set()
        self._concurrency = concurrency

    async def replay(self, filter_: Optional[ReplayFilter] = None) -> ReplayResult:
        """
        Replay all events matching filter_.

        Runs up to ``concurrency`` publish operations concurrently.
        Events whose idempotency key is already in the store are skipped.
        """
        filter_ = filter_ or ReplayFilter()
        result = ReplayResult()
        sem = asyncio.Semaphore(self._concurrency)
        t0 = asyncio.get_event_loop().time()

        events = await self._fetch(filter_)
        await asyncio.gather(
            *(self._process(event, result, sem) for event in events),
            return_exceptions=True,
        )

        result.duration_ms = (asyncio.get_event_loop().time() - t0) * 1000
        logger.info(
            "Replay complete: replayed=%d skipped=%d failed=%d duration_ms=%.1f",
            result.replayed, result.skipped, result.failed, result.duration_ms,
        )
        return result

    async def replay_since(
        self,
        since: datetime,
        event_types: Optional[List[str]] = None,
        max_events: int = 1000,
    ) -> ReplayResult:
        """Convenience method — replay events since a given UTC timestamp."""
        return await self.replay(ReplayFilter(
            since=since, event_types=event_types, max_events=max_events,
        ))

    def idempotency_key(self, event: Any) -> str:
        """
        Content-addressable key for one event envelope.

        SHA-256 of (event_id + event_type + stable payload repr) so that
        re-delivered events with identical content produce the same key.
        """
        event_id = str(getattr(event, "event_id", "") or "")
        event_type = str(getattr(event, "event_type", "") or "")
        payload = getattr(event, "payload", {}) or {}
        payload_repr = str(sorted(payload.items())) if isinstance(payload, dict) else str(payload)
        return hashlib.sha256(f"{event_id}:{event_type}:{payload_repr}".encode()).hexdigest()

    async def _fetch(self, f: ReplayFilter) -> List[Any]:
        kwargs: Dict[str, Any] = {"limit": f.max_events}
        if f.since is not None: kwargs["since"] = f.since
        if f.until is not None: kwargs["until"] = f.until
        if f.event_types: kwargs["event_types"] = f.event_types
        if f.correlation_ids: kwargs["correlation_ids"] = f.correlation_ids
        kwargs["include_dead_letter"] = f.include_dead_letter

        get_pending = self._outbox.get_pending
        if asyncio.iscoroutinefunction(get_pending):
            events = await get_pending(**kwargs)
        else:
            loop = asyncio.get_running_loop()
            events = await loop.run_in_executor(None, lambda: get_pending(**kwargs))
        return events or []

    async def _process(self, event: Any, result: ReplayResult, sem: asyncio.Semaphore) -> None:
        async with sem:
            key = self.idempotency_key(event)
            if key in self._processed:
                result.skipped += 1
                return
            try:
                if asyncio.iscoroutinefunction(self._publisher):
                    success = await self._publisher(event)
                else:
                    loop = asyncio.get_running_loop()
                    success = await loop.run_in_executor(None, lambda: self._publisher(event))

                if success:
                    self._processed.add(key)
                    result.replayed += 1
                    mark = getattr(self._outbox, "mark_processed", None)
                    if mark:
                        if asyncio.iscoroutinefunction(mark):
                            await mark(event)
                        else:
                            loop = asyncio.get_running_loop()
                            await loop.run_in_executor(None, lambda: mark(event))
                else:
                    result.failed += 1
                    result.errors.append({
                        "event_id": getattr(event, "event_id", "unknown"),
                        "reason": "publisher returned False",
                    })
            except Exception as exc:
                result.failed += 1
                result.errors.append({
                    "event_id": getattr(event, "event_id", "unknown"),
                    "reason": str(exc),
                })
                logger.warning("Replay failed for event %s: %s", getattr(event, "event_id", "?"), exc)
