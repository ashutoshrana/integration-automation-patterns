"""
Event replay engine for enterprise integration pipelines.

Deterministic re-processing of transactional outbox records with:
- Canonical JSON idempotency keys (SHA-256) with optional durable markers
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
import inspect
import json
import logging
import sqlite3
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

__all__ = ["EventReplayEngine", "ReplayResult", "ReplayFilter", "SQLiteReplayStore"]


@dataclass
class ReplayFilter:
    """Criteria for selecting events to replay."""

    since: datetime | None = None
    until: datetime | None = None
    event_types: list[str] | None = None
    correlation_ids: list[str] | None = None
    max_events: int = 1000
    include_dead_letter: bool = False


@dataclass
class ReplayResult:
    """Summary of one replay run."""

    replayed: int = 0
    skipped: int = 0
    failed: int = 0
    dead_lettered: int = 0
    errors: list[dict[str, Any]] = field(default_factory=list)
    duration_ms: float = 0.0


class SQLiteReplayStore:
    """Opt-in durable replay markers; one writer serializes publishers per database.

    Use a dedicated database, separate from the outbox/business database. A crash
    after external publication but before marker commit can still cause redelivery.
    Publishers must use bounded I/O timeouts; lock acquisition waits at most 30s.
    """

    def __init__(self, path: str | Path) -> None:
        self.path = str(path)
        if self.path == ":memory:":
            raise ValueError("durable file path required")
        db = sqlite3.connect(self.path)
        try:
            with db:
                db.execute("CREATE TABLE IF NOT EXISTS replay_completed (key TEXT PRIMARY KEY)")
        finally:
            db.close()

    @asynccontextmanager
    async def claim(self, key: str) -> AsyncIterator[tuple[bool, Callable[[], None]]]:
        db = sqlite3.connect(self.path, timeout=0)
        deadline = asyncio.get_running_loop().time() + 30
        try:
            while True:
                try:
                    db.execute("BEGIN IMMEDIATE")
                    break
                except sqlite3.OperationalError as exc:
                    if str(exc) not in ("database is locked", "database table is locked"):
                        raise
                    if asyncio.get_running_loop().time() >= deadline:
                        raise TimeoutError("replay claim timed out") from exc
                    await asyncio.sleep(0.01)
            done = db.execute("SELECT 1 FROM replay_completed WHERE key=?", (key,)).fetchone() is not None

            def complete() -> None:
                db.execute("INSERT INTO replay_completed VALUES (?)", (key,))

            yield done, complete
            db.commit()
        finally:
            db.close()


class EventReplayEngine:
    """
    Event replay with serialized publication and retryable acknowledgments.

    Re-processes failed or missing events from the transactional outbox.
    In-memory markers reset on restart. Supply SQLiteReplayStore for durable
    coordination between independent runners. Delivery remains at least once:
    external publication and local marker commit are separate operations.
    Consumers must deduplicate business effects transactionally.

    Args:
        outbox: Outbox instance with get_pending() and mark_processed() methods.
        publisher: Callable (sync or async) that publishes one event; returns True on success.
        idempotency_store: Optional set of processed hashes or SQLiteReplayStore.
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
        idempotency_store: set[str] | SQLiteReplayStore | None = None,
        concurrency: int = 5,
    ) -> None:
        self._outbox = outbox
        self._publisher = publisher
        self._store = idempotency_store if isinstance(idempotency_store, SQLiteReplayStore) else None
        self._processed: set[str] = idempotency_store if isinstance(idempotency_store, set) else set()
        self._locks: dict[str, asyncio.Lock] = {}
        if concurrency < 1:
            raise ValueError("concurrency must be positive")
        self._concurrency = concurrency

    async def replay(self, filter_: ReplayFilter | None = None) -> ReplayResult:
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
        outcomes = await asyncio.gather(
            *(self._process(event, result, sem) for event in events),
            return_exceptions=True,
        )
        for outcome in outcomes:
            if isinstance(outcome, BaseException):
                raise outcome

        result.duration_ms = (asyncio.get_event_loop().time() - t0) * 1000
        logger.info(
            "Replay complete: replayed=%d skipped=%d failed=%d duration_ms=%.1f",
            result.replayed,
            result.skipped,
            result.failed,
            result.duration_ms,
        )
        return result

    async def replay_since(
        self,
        since: datetime,
        event_types: list[str] | None = None,
        max_events: int = 1000,
    ) -> ReplayResult:
        """Convenience method — replay events since a given UTC timestamp."""
        return await self.replay(
            ReplayFilter(
                since=since,
                event_types=event_types,
                max_events=max_events,
            )
        )

    def idempotency_key(self, event: Any) -> str:
        """
        Content-addressable key for one event envelope.

        SHA-256 of (event_id + event_type + stable payload repr).
        """
        event_id = str(getattr(event, "event_id", "") or "")
        event_type = str(getattr(event, "event_type", "") or "")
        payload = getattr(event, "payload", {}) or {}
        if not event_id:
            raise ValueError("event_id is required for replay")
        encoded = json.dumps([event_id, event_type, payload], sort_keys=True, separators=(",", ":"), allow_nan=False)
        return hashlib.sha256(encoded.encode()).hexdigest()

    async def _fetch(self, f: ReplayFilter) -> list[Any]:
        kwargs: dict[str, Any] = {"limit": f.max_events}
        if f.since is not None:
            kwargs["since"] = f.since
        if f.until is not None:
            kwargs["until"] = f.until
        if f.event_types:
            kwargs["event_types"] = f.event_types
        if f.correlation_ids:
            kwargs["correlation_ids"] = f.correlation_ids
        kwargs["include_dead_letter"] = f.include_dead_letter

        get_pending = self._outbox.get_pending
        if inspect.iscoroutinefunction(get_pending):
            events = await get_pending(**kwargs)
        else:
            loop = asyncio.get_running_loop()
            events = await loop.run_in_executor(None, lambda: get_pending(**kwargs))
        return events or []

    @asynccontextmanager
    async def _claim(self, key: str) -> AsyncIterator[tuple[bool, Callable[[], None]]]:
        if self._store is not None:
            async with self._store.claim(key) as claim:
                yield claim
        else:
            async with self._locks.setdefault(key, asyncio.Lock()):
                yield key in self._processed, lambda: self._processed.add(key)

    async def _call(self, callback: Callable[..., Any], *args: Any) -> Any:
        if inspect.iscoroutinefunction(callback):
            return await callback(*args)
        value = await asyncio.get_running_loop().run_in_executor(None, lambda: callback(*args))
        return await value if inspect.isawaitable(value) else value

    async def _process(self, event: Any, result: ReplayResult, sem: asyncio.Semaphore) -> None:
        async with sem:
            try:
                key = self.idempotency_key(event)
                if self._store is None:
                    # Read legacy markers during upgrades; write only canonical keys.
                    payload = getattr(event, "payload", {}) or {}
                    legacy_payload = str(sorted(payload.items())) if isinstance(payload, dict) else str(payload)
                    legacy = hashlib.sha256(
                        f"{getattr(event, 'event_id', '')}:{getattr(event, 'event_type', '')}:{legacy_payload}".encode()
                    ).hexdigest()
                    if legacy in self._processed:
                        self._processed.add(key)
                async with self._claim(key) as (done, complete):
                    if not done:
                        if not await self._call(self._publisher, event):
                            raise RuntimeError("publisher returned False")
                        complete()
                # Persist publication before acknowledging; a failed ack is retried
                # without publishing again, including after a durable-store restart.
                mark = getattr(self._outbox, "mark_processed", None)
                if mark:
                    await self._call(mark, event)
                if done:
                    result.skipped += 1
                else:
                    result.replayed += 1
            except Exception as exc:
                result.failed += 1
                result.errors.append({"event_id": getattr(event, "event_id", "unknown"), "reason": str(exc)})
                logger.warning("Replay failed for event %s: %s", getattr(event, "event_id", "?"), exc)
