"""Tests for EventReplayEngine."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import pytest

from integration_automation_patterns.event_replay import EventReplayEngine


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
    def __init__(self, events: list[FakeEvent]):
        self._events = events
        self.processed: list[str] = []

    def get_pending(self, since=None, until=None, event_types=None, limit=1000, **kwargs):
        evts = self._events
        if since:
            evts = [e for e in evts if e.created_at >= since]
        if event_types:
            evts = [e for e in evts if e.event_type in event_types]
        return evts[:limit]

    def mark_processed(self, event: FakeEvent):
        self.processed.append(event.event_id)


async def _ok_publisher(event) -> bool:
    return True


async def _fail_publisher(event) -> bool:
    return False


def _make_events(n: int = 5) -> list[FakeEvent]:
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


@pytest.mark.asyncio
async def test_duplicate_events_and_overlapping_runs_publish_once():
    import asyncio

    event = _make_events(1)[0]
    calls = []

    async def publish(event):
        calls.append(event.event_id)
        await asyncio.sleep(0.01)
        return True

    engine = EventReplayEngine(FakeOutbox([event, event]), publish)
    await asyncio.gather(engine.replay(), engine.replay())
    assert calls == [event.event_id]


def test_nested_payload_identity_is_canonical():
    engine = EventReplayEngine(FakeOutbox([]), _ok_publisher)
    a = FakeEvent("evt", "type", {"nested": {"a": 1, "b": 2}})
    b = FakeEvent("evt", "type", {"nested": {"b": 2, "a": 1}})
    assert engine.idempotency_key(a) == engine.idempotency_key(b)


@pytest.mark.asyncio
async def test_failed_ack_is_retried_without_republishing():
    event = _make_events(1)[0]

    class RecoveringOutbox(FakeOutbox):
        def mark_processed(self, event):
            if not self.processed:
                self.processed.append("failed")
                raise RuntimeError("ack unavailable")
            super().mark_processed(event)

    outbox = RecoveringOutbox([event])
    engine = EventReplayEngine(outbox, _ok_publisher)
    first = await engine.replay()
    assert (first.replayed, first.failed) == (0, 1)
    second = await engine.replay()
    assert second.skipped == 1
    assert outbox.processed == ["failed", event.event_id]


@pytest.mark.asyncio
async def test_independent_durable_runners_and_ack_recovery(tmp_path):
    import asyncio

    from integration_automation_patterns.event_replay import SQLiteReplayStore

    event = _make_events(1)[0]
    path = tmp_path / "replay.sqlite"
    calls = []

    async def publish(event):
        calls.append(event.event_id)
        await asyncio.sleep(0.03)
        return True

    class FlakyOutbox(FakeOutbox):
        def mark_processed(self, event):
            raise RuntimeError("ack unavailable")

    engines = [EventReplayEngine(FlakyOutbox([event]), publish, SQLiteReplayStore(path)) for _ in range(2)]
    results = await asyncio.gather(*(engine.replay() for engine in engines))
    assert calls == [event.event_id]
    assert sum(result.failed for result in results) == 2
    recovered = FakeOutbox([event])
    result = await EventReplayEngine(recovered, publish, SQLiteReplayStore(path)).replay()
    assert (result.skipped, result.replayed, result.failed) == (1, 0, 0)
    assert calls == [event.event_id]
    assert recovered.processed == [event.event_id]


@pytest.mark.asyncio
async def test_durable_claim_rolls_back_after_process_crash(tmp_path):
    import asyncio
    import subprocess
    import sys

    from integration_automation_patterns.event_replay import SQLiteReplayStore

    path = tmp_path / "replay.sqlite"
    script = """
import asyncio, os, sys
from integration_automation_patterns.event_replay import SQLiteReplayStore
async def crash():
    async with SQLiteReplayStore(sys.argv[1]).claim('event') as (done, complete):
        complete()
        os._exit(7)
asyncio.run(crash())
"""
    result = await asyncio.to_thread(subprocess.run, [sys.executable, "-c", script, str(path)])
    assert result.returncode == 7
    async with SQLiteReplayStore(path).claim("event") as (done, complete):
        assert not done
        complete()
    async with SQLiteReplayStore(path).claim("event") as (done, _):
        assert done


@pytest.mark.asyncio
async def test_invalid_identity_is_reported_and_concurrency_is_bounded():
    with pytest.raises(ValueError, match="concurrency"):
        EventReplayEngine(FakeOutbox([]), _ok_publisher, concurrency=0)
    result = await EventReplayEngine(FakeOutbox([FakeEvent("", "type", {})]), _ok_publisher).replay()
    assert result.failed == 1


@pytest.mark.asyncio
async def test_crash_after_external_publish_can_redeliver(tmp_path):
    import asyncio
    import subprocess
    import sys

    from integration_automation_patterns.event_replay import SQLiteReplayStore

    path, effects = tmp_path / "replay.sqlite", tmp_path / "effects.txt"
    script = """
import asyncio, os, sys
from pathlib import Path
from types import SimpleNamespace
from integration_automation_patterns.event_replay import EventReplayEngine, SQLiteReplayStore
class Outbox:
    def get_pending(self, **kwargs):
        return [SimpleNamespace(event_id='evt-0', event_type='order.created', payload={'amount': 0})]
async def publish(event):
    Path(sys.argv[2]).write_text('published\\n')
    os._exit(7)
asyncio.run(EventReplayEngine(Outbox(), publish, SQLiteReplayStore(sys.argv[1])).replay())
"""
    crashed = await asyncio.to_thread(subprocess.run, [sys.executable, "-c", script, str(path), str(effects)])
    assert crashed.returncode == 7
    assert effects.read_text() == "published\n"

    async def publish(event):
        with effects.open("a") as output:
            output.write("published\n")
        return True

    result = await EventReplayEngine(FakeOutbox(_make_events(1)), publish, SQLiteReplayStore(path)).replay()
    assert result.replayed == 1
    assert effects.read_text().splitlines() == ["published", "published"]


@pytest.mark.asyncio
async def test_legacy_external_set_prevents_republication():
    import hashlib

    event = _make_events(1)[0]
    legacy = hashlib.sha256(
        f"{event.event_id}:{event.event_type}:{str(sorted(event.payload.items()))}".encode()
    ).hexdigest()
    markers = {legacy}

    async def unexpected_publish(event):
        raise AssertionError("legacy marker must suppress publication")

    outbox = FakeOutbox([event])
    engine = EventReplayEngine(outbox, unexpected_publish, markers)
    result = await engine.replay()
    assert (result.skipped, result.failed) == (1, 0)
    assert engine.idempotency_key(event) in markers
    assert outbox.processed == [event.event_id]


@pytest.mark.asyncio
async def test_child_publisher_cancellation_propagates_and_claim_can_retry(tmp_path):
    import asyncio

    from integration_automation_patterns.event_replay import SQLiteReplayStore

    event = _make_events(1)[0]
    outbox = FakeOutbox([event])
    store = SQLiteReplayStore(tmp_path / "cancel.sqlite")

    async def cancelled(event):
        raise asyncio.CancelledError("publisher cancelled")

    with pytest.raises(asyncio.CancelledError):
        await EventReplayEngine(outbox, cancelled, store).replay()
    assert outbox.processed == []
    result = await EventReplayEngine(outbox, _ok_publisher, store).replay()
    assert (result.replayed, result.failed) == (1, 0)
    assert outbox.processed == [event.event_id]


@pytest.mark.asyncio
async def test_outer_replay_cancellation_propagates_without_ack(tmp_path):
    import asyncio

    from integration_automation_patterns.event_replay import SQLiteReplayStore

    started = asyncio.Event()
    outbox = FakeOutbox(_make_events(1))
    store = SQLiteReplayStore(tmp_path / "outer-cancel.sqlite")

    async def blocked(event):
        started.set()
        await asyncio.Event().wait()
        return True

    task = asyncio.create_task(EventReplayEngine(outbox, blocked, store).replay())
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert outbox.processed == []
    assert (await EventReplayEngine(outbox, _ok_publisher, store).replay()).replayed == 1
