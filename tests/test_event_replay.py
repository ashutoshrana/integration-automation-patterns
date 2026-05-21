"""Tests for EventReplayEngine."""
from __future__ import annotations
import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List
import pytest
from integration_automation_patterns.event_replay import EventReplayEngine, ReplayFilter


@dataclass
class FakeEvent:
    event_id: str
    event_type: str
    payload: dict
    created_at: datetime = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.utcnow()


class FakeOutbox:
    def __init__(self, events: List[FakeEvent]):
        self._events = events
        self.processed: List[str] = []

    def get_pending(self, since=None, until=None, event_types=None, limit=1000, **kwargs):
        evts = self._events
        if since: evts = [e for e in evts if e.created_at >= since]
        if event_types: evts = [e for e in evts if e.event_type in event_types]
        return evts[:limit]

    def mark_processed(self, event: FakeEvent):
        self.processed.append(event.event_id)


async def _ok_publisher(event) -> bool:
    return True


async def _fail_publisher(event) -> bool:
    return False


def _make_events(n: int = 5) -> List[FakeEvent]:
    return [FakeEvent(f"evt-{i}", "order.created", {"amount": i * 10}) for i in range(n)]


@pytest.mark.asyncio
async def test_replay_all_events():
    events = _make_events(5)
    outbox = FakeOutbox(events)
    engine = EventReplayEngine(outbox=outbox, publisher=_ok_publisher)
    result = await engine.replay()
    assert result.replayed == 5
    assert result.skipped == 0
    assert result.failed == 0


@pytest.mark.asyncio
async def test_idempotency_skips_already_processed():
    events = _make_events(3)
    outbox = FakeOutbox(events)
    engine = EventReplayEngine(outbox=outbox, publisher=_ok_publisher)
    await engine.replay()
    result = await engine.replay()
    assert result.skipped == 3
    assert result.replayed == 0


@pytest.mark.asyncio
async def test_failed_publisher_counted():
    events = _make_events(3)
    outbox = FakeOutbox(events)
    engine = EventReplayEngine(outbox=outbox, publisher=_fail_publisher)
    result = await engine.replay()
    assert result.failed == 3
    assert result.replayed == 0


@pytest.mark.asyncio
async def test_replay_since_filters_by_time():
    old = FakeEvent("old-1", "order.created", {}, created_at=datetime(2026, 1, 1))
    new = FakeEvent("new-1", "order.created", {}, created_at=datetime(2026, 5, 20))
    outbox = FakeOutbox([old, new])
    engine = EventReplayEngine(outbox=outbox, publisher=_ok_publisher)
    result = await engine.replay_since(since=datetime(2026, 5, 1))
    assert result.replayed == 1


@pytest.mark.asyncio
async def test_concurrent_replay_safe():
    events = _make_events(20)
    outbox = FakeOutbox(events)
    engine = EventReplayEngine(outbox=outbox, publisher=_ok_publisher, concurrency=10)
    result = await engine.replay()
    assert result.replayed == 20
    assert result.failed == 0


@pytest.mark.asyncio
async def test_idempotency_key_deterministic():
    engine = EventReplayEngine(outbox=FakeOutbox([]), publisher=_ok_publisher)
    evt = FakeEvent("evt-1", "order.created", {"amount": 100})
    key1 = engine.idempotency_key(evt)
    key2 = engine.idempotency_key(evt)
    assert key1 == key2
    assert len(key1) == 64


@pytest.mark.asyncio
async def test_mark_processed_called_on_success():
    events = _make_events(2)
    outbox = FakeOutbox(events)
    engine = EventReplayEngine(outbox=outbox, publisher=_ok_publisher)
    await engine.replay()
    assert len(outbox.processed) == 2
