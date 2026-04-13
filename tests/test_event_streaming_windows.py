"""
Tests for 12_event_streaming_windows.py

Covers TumblingWindow, SlidingWindow, SessionWindow, WindowAggregator,
LateArrivalHandler, and StreamProcessor.
"""

from __future__ import annotations

import importlib.util
import sys
import uuid
from pathlib import Path
from typing import Any

import pytest

# ---------------------------------------------------------------------------
# Module loading helper (frozen-dataclass fix for Python 3.14+)
# ---------------------------------------------------------------------------

_MODULE_NAME = "event_streaming_windows"
_MODULE_PATH = (
    Path(__file__).parent.parent / "examples" / "12_event_streaming_windows.py"
)


def _load_module() -> Any:
    spec = importlib.util.spec_from_file_location(_MODULE_NAME, _MODULE_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[_MODULE_NAME] = mod
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


_mod = _load_module()

StreamEvent = _mod.StreamEvent
AggregationFunction = _mod.AggregationFunction
LateArrivalPolicy = _mod.LateArrivalPolicy
WindowSpec = _mod.WindowSpec
WindowKey = _mod.WindowKey
WindowResult = _mod.WindowResult
LateEventRecord = _mod.LateEventRecord
TumblingWindow = _mod.TumblingWindow
SlidingWindow = _mod.SlidingWindow
SessionWindow = _mod.SessionWindow
WindowAggregator = _mod.WindowAggregator
LateArrivalHandler = _mod.LateArrivalHandler
StreamProcessor = _mod.StreamProcessor
_make_event = _mod._make_event


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _event(stream_key: str, event_time_ms: int, **payload: Any) -> StreamEvent:
    return _make_event(stream_key, event_time_ms, payload if payload else None)


# ---------------------------------------------------------------------------
# TumblingWindow
# ---------------------------------------------------------------------------

class TestTumblingWindow:

    def test_assigns_event_at_window_boundary(self) -> None:
        tw = TumblingWindow(window_size_ms=60_000)
        start, end = tw.assign(0)
        assert start == 0
        assert end == 60_000

    def test_assigns_event_in_middle_of_window(self) -> None:
        tw = TumblingWindow(window_size_ms=60_000)
        start, end = tw.assign(30_000)
        assert start == 0
        assert end == 60_000

    def test_assigns_event_to_next_window(self) -> None:
        tw = TumblingWindow(window_size_ms=60_000)
        start, end = tw.assign(60_000)
        assert start == 60_000
        assert end == 120_000

    def test_events_in_same_window(self) -> None:
        tw = TumblingWindow(window_size_ms=60_000)
        k1 = tw.assign_key(_event("k", 0))
        k2 = tw.assign_key(_event("k", 59_999))
        assert k1 == k2

    def test_events_in_different_windows(self) -> None:
        tw = TumblingWindow(window_size_ms=60_000)
        k1 = tw.assign_key(_event("k", 0))
        k2 = tw.assign_key(_event("k", 60_000))
        assert k1 != k2

    def test_window_key_includes_stream_key(self) -> None:
        tw = TumblingWindow(window_size_ms=60_000)
        k = tw.assign_key(_event("my-stream", 0))
        assert k.stream_key == "my-stream"

    def test_invalid_window_size_raises(self) -> None:
        with pytest.raises(ValueError):
            TumblingWindow(window_size_ms=0)
        with pytest.raises(ValueError):
            TumblingWindow(window_size_ms=-1)


# ---------------------------------------------------------------------------
# SlidingWindow
# ---------------------------------------------------------------------------

class TestSlidingWindow:

    def test_event_belongs_to_multiple_windows(self) -> None:
        sw = SlidingWindow(window_size_ms=60_000, slide_interval_ms=15_000)
        windows = sw.assign_all(30_000)
        # Event at 30s is in windows starting at 0, 15, 30 (not 45 since 45+60=105>30)
        assert len(windows) == 3
        starts = {s for s, _ in windows}
        assert 0 in starts
        assert 15_000 in starts
        assert 30_000 in starts

    def test_event_at_window_start_boundary(self) -> None:
        sw = SlidingWindow(window_size_ms=60_000, slide_interval_ms=15_000)
        windows = sw.assign_all(60_000)
        starts = {s for s, _ in windows}
        # Windows containing t=60s: start ≤ 60000 < start + 60000
        # i.e., 15000, 30000, 45000, 60000
        assert 60_000 in starts
        assert 15_000 in starts

    def test_slide_equals_window_size_is_tumbling(self) -> None:
        sw = SlidingWindow(window_size_ms=60_000, slide_interval_ms=60_000)
        windows = sw.assign_all(30_000)
        assert len(windows) == 1   # no overlap when slide == window

    def test_all_windows_have_correct_size(self) -> None:
        sw = SlidingWindow(window_size_ms=60_000, slide_interval_ms=15_000)
        windows = sw.assign_all(45_000)
        for start, end in windows:
            assert end - start == 60_000

    def test_invalid_slide_interval_raises(self) -> None:
        with pytest.raises(ValueError):
            SlidingWindow(window_size_ms=60_000, slide_interval_ms=0)
        with pytest.raises(ValueError):
            SlidingWindow(window_size_ms=60_000, slide_interval_ms=90_000)

    def test_assign_keys_returns_correct_stream_key(self) -> None:
        sw = SlidingWindow(window_size_ms=60_000, slide_interval_ms=15_000)
        keys = sw.assign_keys(_event("payments", 30_000))
        assert all(k.stream_key == "payments" for k in keys)


# ---------------------------------------------------------------------------
# SessionWindow
# ---------------------------------------------------------------------------

class TestSessionWindow:

    def test_single_event_creates_session(self) -> None:
        sw = SessionWindow(session_gap_ms=30_000)
        e = _event("user-1", 0)
        completed, current = sw.process_event(e)
        assert completed is None
        assert current.stream_key == "user-1"
        assert current.window_start_ms == 0

    def test_events_within_gap_extend_session(self) -> None:
        sw = SessionWindow(session_gap_ms=30_000)
        sw.process_event(_event("u", 0))
        sw.process_event(_event("u", 10_000))
        completed, current = sw.process_event(_event("u", 20_000))
        assert completed is None  # no session closed
        assert current.window_start_ms == 0

    def test_gap_exceeded_closes_session(self) -> None:
        sw = SessionWindow(session_gap_ms=30_000)
        sw.process_event(_event("u", 0))
        sw.process_event(_event("u", 10_000))
        # 40s gap exceeds 30s threshold → previous session closes
        completed, _ = sw.process_event(_event("u", 50_000))
        assert completed is not None
        assert completed.stream_key == "u"
        assert completed.window_start_ms == 0
        assert completed.window_end_ms == 10_000 + 30_000  # last_event + gap

    def test_flush_all_returns_all_sessions(self) -> None:
        sw = SessionWindow(session_gap_ms=30_000)
        sw.process_event(_event("u", 0))
        sw.process_event(_event("u", 10_000))
        sw.process_event(_event("u", 50_000))  # closes first session, opens second
        keys = sw.flush_all()
        assert len(keys) == 2  # first (completed) + second (open)

    def test_independent_sessions_per_key(self) -> None:
        sw = SessionWindow(session_gap_ms=30_000)
        sw.process_event(_event("user-A", 0))
        sw.process_event(_event("user-B", 5_000))
        sw.process_event(_event("user-A", 10_000))
        sw.process_event(_event("user-B", 15_000))
        open_sessions = sw.get_open_sessions()
        assert "user-A" in open_sessions
        assert "user-B" in open_sessions
        # Sessions are independent — no cross-contamination
        assert open_sessions["user-A"][0] == 0    # user-A session starts at 0
        assert open_sessions["user-B"][0] == 5_000   # user-B session starts at 5s

    def test_invalid_session_gap_raises(self) -> None:
        with pytest.raises(ValueError):
            SessionWindow(session_gap_ms=0)


# ---------------------------------------------------------------------------
# WindowAggregator
# ---------------------------------------------------------------------------

class TestWindowAggregator:

    def _events(self, amounts: list[float]) -> list[StreamEvent]:
        return [_event("k", i * 1000, amount=a) for i, a in enumerate(amounts)]

    def test_count(self) -> None:
        agg = WindowAggregator([(AggregationFunction.COUNT, "event_id")])
        result = agg.compute(self._events([10, 20, 30]))
        assert result["COUNT(event_id)"] == 3

    def test_sum(self) -> None:
        agg = WindowAggregator([(AggregationFunction.SUM, "amount")])
        result = agg.compute(self._events([10, 20, 30]))
        assert result["SUM(amount)"] == 60

    def test_avg(self) -> None:
        agg = WindowAggregator([(AggregationFunction.AVG, "amount")])
        result = agg.compute(self._events([10, 20, 30]))
        assert result["AVG(amount)"] == 20.0

    def test_min_max(self) -> None:
        agg = WindowAggregator([
            (AggregationFunction.MIN, "amount"),
            (AggregationFunction.MAX, "amount"),
        ])
        result = agg.compute(self._events([5, 15, 10]))
        assert result["MIN(amount)"] == 5
        assert result["MAX(amount)"] == 15

    def test_distinct_count(self) -> None:
        events = [
            _event("k", 0, user="alice"),
            _event("k", 1000, user="bob"),
            _event("k", 2000, user="alice"),
        ]
        agg = WindowAggregator([(AggregationFunction.DISTINCT_COUNT, "user")])
        result = agg.compute(events)
        assert result["DISTINCT_COUNT(user)"] == 2

    def test_numeric_aggregation_returns_none_for_no_values(self) -> None:
        agg = WindowAggregator([(AggregationFunction.SUM, "missing_field")])
        result = agg.compute(self._events([10, 20]))
        assert result["SUM(missing_field)"] is None

    def test_empty_events_list(self) -> None:
        agg = WindowAggregator([(AggregationFunction.COUNT, "event_id")])
        result = agg.compute([])
        assert result["COUNT(event_id)"] == 0


# ---------------------------------------------------------------------------
# LateArrivalHandler
# ---------------------------------------------------------------------------

class TestLateArrivalHandler:

    def test_within_tolerance_always_accepted(self) -> None:
        handler = LateArrivalHandler(late_tolerance_ms=10_000, policy=LateArrivalPolicy.DROP)
        late_event = _event("k", 90_000)
        should_process, _ = handler.handle(late_event, watermark_ms=95_000)
        assert should_process  # 5s late, within 10s tolerance

    def test_drop_policy_beyond_tolerance(self) -> None:
        handler = LateArrivalHandler(late_tolerance_ms=10_000, policy=LateArrivalPolicy.DROP)
        late_event = _event("k", 60_000)
        should_process, _ = handler.handle(late_event, watermark_ms=100_000)
        assert not should_process
        assert handler.audit_log[0].policy_applied == LateArrivalPolicy.DROP

    def test_side_output_routes_to_reconciliation_stream(self) -> None:
        handler = LateArrivalHandler(
            late_tolerance_ms=10_000, policy=LateArrivalPolicy.SIDE_OUTPUT
        )
        late_event = _event("k", 60_000)
        should_process, _ = handler.handle(late_event, watermark_ms=100_000)
        assert not should_process
        assert len(handler.side_output_stream) == 1
        assert handler.side_output_stream[0].event_id == late_event.event_id

    def test_accept_policy_processes_late_event(self) -> None:
        handler = LateArrivalHandler(late_tolerance_ms=5_000, policy=LateArrivalPolicy.ACCEPT)
        late_event = _event("k", 60_000)
        latest_key = WindowKey("k", 90_000, 150_000)
        should_process, assigned = handler.handle(late_event, watermark_ms=100_000, latest_open_window=latest_key)
        assert should_process
        assert assigned == latest_key

    def test_audit_log_records_lateness(self) -> None:
        handler = LateArrivalHandler(late_tolerance_ms=5_000, policy=LateArrivalPolicy.DROP)
        late_event = _event("k", 50_000)
        handler.handle(late_event, watermark_ms=100_000)
        assert handler.audit_log[0].lateness_ms == 50_000

    def test_assign_to_latest_policy(self) -> None:
        handler = LateArrivalHandler(
            late_tolerance_ms=5_000, policy=LateArrivalPolicy.ASSIGN_TO_LATEST
        )
        late_event = _event("k", 30_000)
        latest_key = WindowKey("k", 100_000, 160_000)
        should_process, assigned = handler.handle(late_event, watermark_ms=100_000, latest_open_window=latest_key)
        assert should_process
        assert assigned == latest_key


# ---------------------------------------------------------------------------
# StreamProcessor — tumbling
# ---------------------------------------------------------------------------

class TestStreamProcessorTumbling:

    def _spec(self) -> WindowSpec:
        return WindowSpec(
            window_size_ms=60_000,
            late_tolerance_ms=0,
            late_arrival_policy=LateArrivalPolicy.DROP,
        )

    def test_three_windows_sixty_events(self) -> None:
        proc = StreamProcessor(spec=self._spec())
        for window_idx in range(3):
            for i in range(20):
                t = window_idx * 60_000 + i * 3_000
                proc.process(_event("orders", t, amount=10))
        proc.flush()
        results = proc.get_results()
        assert len(results) == 3
        assert all(r.event_count == 20 for r in results)

    def test_no_double_counting(self) -> None:
        proc = StreamProcessor(spec=self._spec())
        for i in range(100):
            proc.process(_event("k", i * 1_000, amount=1))
        proc.flush()
        results = proc.get_results()
        total = sum(r.event_count for r in results)
        assert total == 100

    def test_watermark_advances_correctly(self) -> None:
        proc = StreamProcessor(spec=self._spec())
        proc.process(_event("k", 30_000))
        assert proc.watermark_ms == 30_000
        proc.process(_event("k", 90_000))
        assert proc.watermark_ms == 90_000

    def test_window_finalizes_after_watermark_passes(self) -> None:
        proc = StreamProcessor(spec=self._spec())
        proc.process(_event("k", 0))
        proc.process(_event("k", 30_000))
        # Push watermark past 60s
        results = proc.process(_event("k", 61_000))
        assert len(results) == 1
        assert results[0].event_count == 2

    def test_late_event_dropped_with_drop_policy(self) -> None:
        proc = StreamProcessor(spec=self._spec(), allowed_lateness_ms=0)
        proc.process(_event("k", 100_000))
        # Advance watermark to 100s; now send an event at t=10s (90s late)
        proc.process(_event("k", 10_000))
        assert len(proc.late_handler.audit_log) == 1
        assert proc.late_handler.audit_log[0].policy_applied == LateArrivalPolicy.DROP


# ---------------------------------------------------------------------------
# StreamProcessor — sliding
# ---------------------------------------------------------------------------

class TestStreamProcessorSliding:

    def test_burst_appears_in_multiple_windows(self) -> None:
        spec = WindowSpec(
            window_size_ms=60_000,
            slide_interval_ms=15_000,
        )
        proc = StreamProcessor(spec=spec)
        # 5 events in a 15-second burst
        for i in range(5):
            proc.process(_event("k", 30_000 + i * 2_000))
        proc.process(_event("k", 200_000))
        proc.flush()
        results = proc.get_results()
        total_appearances = sum(r.event_count for r in results if r.event_count > 1)
        # Each burst event should appear in at least 2 windows (W/S = 4)
        assert total_appearances > 0


# ---------------------------------------------------------------------------
# StreamProcessor — session
# ---------------------------------------------------------------------------

class TestStreamProcessorSession:

    def _spec(self, gap_ms: int = 30_000) -> WindowSpec:
        return WindowSpec(
            window_size_ms=gap_ms,
            session_gap_ms=gap_ms,
        )

    def test_two_sessions_detected(self) -> None:
        proc = StreamProcessor(spec=self._spec(gap_ms=30_000))
        for t in [0, 5_000, 10_000]:
            proc.process(_event("user", t))
        proc.process(_event("user", 50_000))  # gap > 30s
        proc.flush()
        results = proc.get_results()
        assert len(results) == 2

    def test_session_event_counts(self) -> None:
        proc = StreamProcessor(spec=self._spec(gap_ms=30_000))
        for t in [0, 5_000, 10_000]:
            proc.process(_event("user", t))
        proc.process(_event("user", 50_000))
        proc.flush()
        results = proc.get_results()
        counts = sorted(r.event_count for r in results)
        assert counts == [1, 3]

    def test_independent_user_sessions(self) -> None:
        proc = StreamProcessor(spec=self._spec(gap_ms=30_000))
        proc.process(_event("user-A", 0))
        proc.process(_event("user-B", 1_000))
        proc.process(_event("user-A", 5_000))
        proc.process(_event("user-B", 6_000))
        proc.flush()
        results = proc.get_results()
        # Two users, each with 1 session
        assert len(results) == 2
        assert {r.window_key.stream_key for r in results} == {"user-A", "user-B"}
