"""
tests/test_async_patterns.py — Tests for async-capable outbox processor.

Covers:
- AsyncOutboxProcessor with async fetch/publish/mark callables
- AsyncOutboxProcessor with sync callables (mixed mode)
- Duplicate suppression (empty batch short-circuit)
- Failure path: publish raises, mark_failed is called
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from integration_automation_patterns.outbox import AsyncOutboxProcessor, OutboxRecord

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_record(event_type: str = "AccountUpdated", aggregate_id: str = "acct-1") -> OutboxRecord:
    return OutboxRecord.create(aggregate_id=aggregate_id, event_type=event_type, payload={"status": "active"})


# ---------------------------------------------------------------------------
# AsyncOutboxProcessor — happy path with async callables
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_async_processor_publishes_all_records() -> None:
    records = [_make_record(f"Event{i}", f"agg-{i}") for i in range(3)]

    fetch = AsyncMock(return_value=records)
    publish = AsyncMock()
    mark_published = AsyncMock()

    processor = AsyncOutboxProcessor(
        fetch_pending=fetch,
        publish=publish,
        mark_published=mark_published,
    )

    published, failed = await processor.process_batch(batch_size=10)

    assert published == 3
    assert failed == 0
    assert fetch.call_count == 1
    assert publish.call_count == 3
    assert mark_published.call_count == 3


@pytest.mark.asyncio
async def test_async_processor_empty_batch_returns_zero() -> None:
    fetch = AsyncMock(return_value=[])
    publish = AsyncMock()
    mark_published = AsyncMock()

    processor = AsyncOutboxProcessor(
        fetch_pending=fetch,
        publish=publish,
        mark_published=mark_published,
    )

    published, failed = await processor.process_batch()

    assert published == 0
    assert failed == 0
    publish.assert_not_called()
    mark_published.assert_not_called()


# ---------------------------------------------------------------------------
# AsyncOutboxProcessor — mixed sync/async callables
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_async_processor_accepts_sync_callables() -> None:
    records = [_make_record()]

    published_ids: list[str] = []

    def sync_fetch(batch_size: int) -> list[OutboxRecord]:
        return records

    def sync_publish(event: dict) -> None:
        pass

    def sync_mark(record_id: str) -> None:
        published_ids.append(record_id)

    processor = AsyncOutboxProcessor(
        fetch_pending=sync_fetch,
        publish=sync_publish,
        mark_published=sync_mark,
    )

    published, failed = await processor.process_batch()

    assert published == 1
    assert failed == 0
    assert records[0].record_id in published_ids


# ---------------------------------------------------------------------------
# AsyncOutboxProcessor — failure path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_async_processor_calls_mark_failed_on_publish_error() -> None:
    record = _make_record()
    fetch = AsyncMock(return_value=[record])

    async def failing_publish(event: dict) -> None:
        raise RuntimeError("broker unavailable")

    mark_published = AsyncMock()
    mark_failed = AsyncMock()

    processor = AsyncOutboxProcessor(
        fetch_pending=fetch,
        publish=failing_publish,
        mark_published=mark_published,
        mark_failed=mark_failed,
    )

    published, failed = await processor.process_batch()

    assert published == 0
    assert failed == 1
    mark_published.assert_not_called()
    mark_failed.assert_called_once()
    call_args = mark_failed.call_args
    assert call_args[0][0] == record.record_id
    assert "broker unavailable" in call_args[0][1]
    assert call_args[0][2] == 1  # attempt_count + 1


@pytest.mark.asyncio
async def test_async_processor_continues_after_individual_failure() -> None:
    """A single failed record should not block remaining records."""
    records = [
        _make_record("FailEvent", "agg-fail"),
        _make_record("OkEvent", "agg-ok"),
    ]
    fetch = AsyncMock(return_value=records)

    call_count = 0

    async def selective_publish(event: dict) -> None:
        nonlocal call_count
        call_count += 1
        if event["event_type"] == "FailEvent":
            raise RuntimeError("fail")

    mark_published = AsyncMock()

    processor = AsyncOutboxProcessor(
        fetch_pending=fetch,
        publish=selective_publish,
        mark_published=mark_published,
    )

    published, failed = await processor.process_batch()

    assert published == 1
    assert failed == 1
    assert call_count == 2
    mark_published.assert_called_once()
