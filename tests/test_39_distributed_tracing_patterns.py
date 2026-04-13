"""
Tests for 39_distributed_tracing_patterns.py

Covers all five patterns:
  - TraceContext (to_traceparent, from_traceparent, child_context)
  - Span (timing, events, status, to_dict)
  - Tracer (start/finish/get_trace/export)
  - SamplingStrategy (always_on/off/ratio/should_sample)
  - TraceExporter (export, filters)

Uses only the standard library.  No external dependencies required.
"""

from __future__ import annotations

import importlib.util
import sys
import threading
import time
import types
import uuid
from pathlib import Path

import pytest

_MOD_PATH = Path(__file__).parent.parent / "examples" / "39_distributed_tracing_patterns.py"


def _load_module() -> types.ModuleType:
    module_name = "distributed_tracing_patterns_39"
    if module_name in sys.modules:
        return sys.modules[module_name]
    spec = importlib.util.spec_from_file_location(module_name, _MOD_PATH)
    mod = types.ModuleType(module_name)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


@pytest.fixture(scope="module")
def mod() -> types.ModuleType:
    return _load_module()


# ===========================================================================
# TraceContext — init
# ===========================================================================


class TestTraceContextInit:
    def test_auto_generates_trace_id(self, mod):
        ctx = mod.TraceContext()
        assert ctx.trace_id is not None
        assert len(ctx.trace_id) == 32

    def test_auto_generates_span_id(self, mod):
        ctx = mod.TraceContext()
        assert ctx.span_id is not None
        assert len(ctx.span_id) == 16

    def test_two_contexts_have_different_trace_ids(self, mod):
        ctx1 = mod.TraceContext()
        ctx2 = mod.TraceContext()
        assert ctx1.trace_id != ctx2.trace_id

    def test_two_contexts_have_different_span_ids(self, mod):
        ctx1 = mod.TraceContext()
        ctx2 = mod.TraceContext()
        assert ctx1.span_id != ctx2.span_id

    def test_explicit_trace_id_preserved(self, mod):
        tid = "abcd1234" * 4
        ctx = mod.TraceContext(trace_id=tid)
        assert ctx.trace_id == tid

    def test_explicit_span_id_preserved(self, mod):
        sid = "cafebabe12345678"
        ctx = mod.TraceContext(span_id=sid)
        assert ctx.span_id == sid

    def test_parent_span_id_default_none(self, mod):
        ctx = mod.TraceContext()
        assert ctx.parent_span_id is None

    def test_sampling_flag_default_true(self, mod):
        ctx = mod.TraceContext()
        assert ctx.sampling_flag is True

    def test_sampling_flag_false(self, mod):
        ctx = mod.TraceContext(sampling_flag=False)
        assert ctx.sampling_flag is False

    def test_explicit_parent_span_id(self, mod):
        ctx = mod.TraceContext(parent_span_id="deadbeef12345678")
        assert ctx.parent_span_id == "deadbeef12345678"


# ===========================================================================
# TraceContext — to_traceparent
# ===========================================================================


class TestTraceContextToTraceparent:
    def test_format_sampled(self, mod):
        ctx = mod.TraceContext(trace_id="a" * 32, span_id="b" * 16, sampling_flag=True)
        header = ctx.to_traceparent()
        assert header == f"00-{'a' * 32}-{'b' * 16}-01"

    def test_format_not_sampled(self, mod):
        ctx = mod.TraceContext(trace_id="a" * 32, span_id="b" * 16, sampling_flag=False)
        header = ctx.to_traceparent()
        assert header == f"00-{'a' * 32}-{'b' * 16}-00"

    def test_four_dash_separated_parts(self, mod):
        ctx = mod.TraceContext()
        parts = ctx.to_traceparent().split("-")
        assert len(parts) == 4

    def test_version_is_00(self, mod):
        ctx = mod.TraceContext()
        assert ctx.to_traceparent().startswith("00-")

    def test_trace_id_in_header(self, mod):
        ctx = mod.TraceContext()
        assert ctx.trace_id in ctx.to_traceparent()

    def test_span_id_in_header(self, mod):
        ctx = mod.TraceContext()
        assert ctx.span_id in ctx.to_traceparent()

    def test_returns_string(self, mod):
        ctx = mod.TraceContext()
        assert isinstance(ctx.to_traceparent(), str)


# ===========================================================================
# TraceContext — from_traceparent
# ===========================================================================


class TestTraceContextFromTraceparent:
    def test_round_trip_sampled(self, mod):
        original = mod.TraceContext(trace_id="c" * 32, span_id="d" * 16, sampling_flag=True)
        parsed = mod.TraceContext.from_traceparent(original.to_traceparent())
        assert parsed.trace_id == original.trace_id
        assert parsed.span_id == original.span_id
        assert parsed.sampling_flag is True

    def test_round_trip_not_sampled(self, mod):
        original = mod.TraceContext(trace_id="e" * 32, span_id="f" * 16, sampling_flag=False)
        parsed = mod.TraceContext.from_traceparent(original.to_traceparent())
        assert parsed.sampling_flag is False

    def test_invalid_header_raises(self, mod):
        with pytest.raises(ValueError):
            mod.TraceContext.from_traceparent("bad-header")

    def test_wrong_version_raises(self, mod):
        with pytest.raises(ValueError):
            mod.TraceContext.from_traceparent(f"01-{'a' * 32}-{'b' * 16}-01")

    def test_too_many_parts_raises(self, mod):
        with pytest.raises(ValueError):
            mod.TraceContext.from_traceparent(f"00-{'a' * 32}-{'b' * 16}-01-extra")

    def test_too_few_parts_raises(self, mod):
        with pytest.raises(ValueError):
            mod.TraceContext.from_traceparent("00-abc")

    def test_parsed_parent_span_id_is_none(self, mod):
        header = f"00-{'a' * 32}-{'b' * 16}-01"
        parsed = mod.TraceContext.from_traceparent(header)
        assert parsed.parent_span_id is None

    def test_parsed_trace_id_correct(self, mod):
        tid = "1234abcd" * 4
        header = f"00-{tid}-{'f' * 16}-01"
        parsed = mod.TraceContext.from_traceparent(header)
        assert parsed.trace_id == tid


# ===========================================================================
# TraceContext — child_context
# ===========================================================================


class TestTraceContextChildContext:
    def test_child_inherits_trace_id(self, mod):
        parent = mod.TraceContext()
        child = parent.child_context()
        assert child.trace_id == parent.trace_id

    def test_child_has_new_span_id(self, mod):
        parent = mod.TraceContext()
        child = parent.child_context()
        assert child.span_id != parent.span_id

    def test_child_parent_span_id_is_parent_span(self, mod):
        parent = mod.TraceContext()
        child = parent.child_context()
        assert child.parent_span_id == parent.span_id

    def test_child_inherits_sampling_flag_true(self, mod):
        parent = mod.TraceContext(sampling_flag=True)
        child = parent.child_context()
        assert child.sampling_flag is True

    def test_child_inherits_sampling_flag_false(self, mod):
        parent = mod.TraceContext(sampling_flag=False)
        child = parent.child_context()
        assert child.sampling_flag is False

    def test_grandchild_trace_id(self, mod):
        root = mod.TraceContext()
        child = root.child_context()
        grandchild = child.child_context()
        assert grandchild.trace_id == root.trace_id

    def test_grandchild_parent_span_id(self, mod):
        root = mod.TraceContext()
        child = root.child_context()
        grandchild = child.child_context()
        assert grandchild.parent_span_id == child.span_id


# ===========================================================================
# Span — init and properties
# ===========================================================================


class TestSpanInit:
    def test_name_stored(self, mod):
        ctx = mod.TraceContext()
        span = mod.Span("my.op", ctx)
        assert span.name == "my.op"

    def test_trace_context_stored(self, mod):
        ctx = mod.TraceContext()
        span = mod.Span("op", ctx)
        assert span.trace_context is ctx

    def test_attributes_stored(self, mod):
        ctx = mod.TraceContext()
        span = mod.Span("op", ctx, attributes={"k": "v"})
        assert span.attributes["k"] == "v"

    def test_attributes_default_empty(self, mod):
        ctx = mod.TraceContext()
        span = mod.Span("op", ctx)
        assert span.attributes == {}

    def test_start_time_set(self, mod):
        before = time.time()
        ctx = mod.TraceContext()
        span = mod.Span("op", ctx)
        after = time.time()
        assert before <= span.start_time <= after

    def test_end_time_none_initially(self, mod):
        ctx = mod.TraceContext()
        span = mod.Span("op", ctx)
        assert span.end_time is None

    def test_status_unset_initially(self, mod):
        ctx = mod.TraceContext()
        span = mod.Span("op", ctx)
        assert span.status == "UNSET"

    def test_events_empty_initially(self, mod):
        ctx = mod.TraceContext()
        span = mod.Span("op", ctx)
        assert span.events == []


# ===========================================================================
# Span — finish and duration
# ===========================================================================


class TestSpanFinishAndDuration:
    def test_finish_sets_end_time(self, mod):
        ctx = mod.TraceContext()
        span = mod.Span("op", ctx)
        before = time.time()
        span.finish()
        after = time.time()
        assert before <= span.end_time <= after

    def test_duration_ms_positive_after_finish(self, mod):
        ctx = mod.TraceContext()
        span = mod.Span("op", ctx)
        time.sleep(0.005)
        span.finish()
        assert span.duration_ms() > 0

    def test_duration_ms_reasonable(self, mod):
        ctx = mod.TraceContext()
        span = mod.Span("op", ctx)
        time.sleep(0.01)
        span.finish()
        assert 5 <= span.duration_ms() <= 500

    def test_duration_ms_before_finish_non_negative(self, mod):
        ctx = mod.TraceContext()
        span = mod.Span("op", ctx)
        assert span.duration_ms() >= 0

    def test_duration_ms_returns_float(self, mod):
        ctx = mod.TraceContext()
        span = mod.Span("op", ctx)
        span.finish()
        assert isinstance(span.duration_ms(), float)


# ===========================================================================
# Span — events
# ===========================================================================


class TestSpanEvents:
    def test_add_event_appended(self, mod):
        ctx = mod.TraceContext()
        span = mod.Span("op", ctx)
        span.add_event("cache.miss")
        assert len(span.events) == 1

    def test_add_event_name(self, mod):
        ctx = mod.TraceContext()
        span = mod.Span("op", ctx)
        span.add_event("db.query")
        assert span.events[0].name == "db.query"

    def test_add_event_with_attributes(self, mod):
        ctx = mod.TraceContext()
        span = mod.Span("op", ctx)
        span.add_event("retry", attributes={"attempt": 2})
        assert span.events[0].attributes["attempt"] == 2

    def test_add_event_no_attributes_defaults_empty(self, mod):
        ctx = mod.TraceContext()
        span = mod.Span("op", ctx)
        span.add_event("ping")
        assert span.events[0].attributes == {}

    def test_multiple_events_ordered(self, mod):
        ctx = mod.TraceContext()
        span = mod.Span("op", ctx)
        span.add_event("start")
        span.add_event("end")
        assert span.events[0].name == "start"
        assert span.events[1].name == "end"

    def test_event_has_timestamp(self, mod):
        before = time.time()
        ctx = mod.TraceContext()
        span = mod.Span("op", ctx)
        span.add_event("tick")
        after = time.time()
        assert before <= span.events[0].timestamp <= after


# ===========================================================================
# Span — status
# ===========================================================================


class TestSpanStatus:
    def test_set_ok(self, mod):
        ctx = mod.TraceContext()
        span = mod.Span("op", ctx)
        span.set_status("OK")
        assert span.status == "OK"

    def test_set_error(self, mod):
        ctx = mod.TraceContext()
        span = mod.Span("op", ctx)
        span.set_status("ERROR")
        assert span.status == "ERROR"

    def test_set_unset(self, mod):
        ctx = mod.TraceContext()
        span = mod.Span("op", ctx)
        span.set_status("OK")
        span.set_status("UNSET")
        assert span.status == "UNSET"


# ===========================================================================
# Span — to_dict
# ===========================================================================


class TestSpanToDict:
    def _finished_span(self, mod, name="test.op", attributes=None):
        ctx = mod.TraceContext()
        span = mod.Span(name, ctx, attributes=attributes)
        span.set_status("OK")
        span.finish()
        return span

    def test_has_required_keys(self, mod):
        span = self._finished_span(mod)
        d = span.to_dict()
        required = {
            "trace_id",
            "span_id",
            "parent_span_id",
            "name",
            "start",
            "end",
            "duration_ms",
            "attributes",
            "events",
            "status",
        }
        assert required.issubset(d.keys())

    def test_trace_id_correct(self, mod):
        span = self._finished_span(mod)
        assert span.to_dict()["trace_id"] == span.trace_context.trace_id

    def test_span_id_correct(self, mod):
        span = self._finished_span(mod)
        assert span.to_dict()["span_id"] == span.trace_context.span_id

    def test_name_correct(self, mod):
        span = self._finished_span(mod, name="db.insert")
        assert span.to_dict()["name"] == "db.insert"

    def test_status_correct(self, mod):
        span = self._finished_span(mod)
        assert span.to_dict()["status"] == "OK"

    def test_duration_non_negative(self, mod):
        span = self._finished_span(mod)
        assert span.to_dict()["duration_ms"] >= 0

    def test_events_serialised(self, mod):
        ctx = mod.TraceContext()
        span = mod.Span("op", ctx)
        span.add_event("ev1")
        span.finish()
        events = span.to_dict()["events"]
        assert len(events) == 1
        assert events[0]["name"] == "ev1"

    def test_attributes_copied_not_referenced(self, mod):
        span = self._finished_span(mod, attributes={"x": 1})
        d = span.to_dict()
        d["attributes"]["x"] = 99
        assert span.attributes["x"] == 1

    def test_parent_span_id_in_dict(self, mod):
        parent_ctx = mod.TraceContext()
        child_ctx = parent_ctx.child_context()
        span = mod.Span("child.op", child_ctx)
        span.finish()
        assert span.to_dict()["parent_span_id"] == parent_ctx.span_id


# ===========================================================================
# Tracer — start and finish
# ===========================================================================


class TestTracerStartAndFinish:
    def test_start_span_returns_span(self, mod):
        tracer = mod.Tracer()
        span = tracer.start_span("op")
        assert span.name == "op"

    def test_start_span_no_parent_creates_root(self, mod):
        tracer = mod.Tracer()
        span = tracer.start_span("op")
        assert span.trace_context.parent_span_id is None

    def test_start_span_with_parent_inherits_trace_id(self, mod):
        tracer = mod.Tracer()
        parent = tracer.start_span("parent")
        child = tracer.start_span("child", parent_context=parent.trace_context)
        assert child.trace_context.trace_id == parent.trace_context.trace_id

    def test_start_span_with_parent_sets_parent_span_id(self, mod):
        tracer = mod.Tracer()
        parent = tracer.start_span("parent")
        child = tracer.start_span("child", parent_context=parent.trace_context)
        assert child.trace_context.parent_span_id == parent.trace_context.span_id

    def test_start_span_with_attributes(self, mod):
        tracer = mod.Tracer()
        span = tracer.start_span("op", attributes={"k": "v"})
        assert span.attributes["k"] == "v"

    def test_finish_span_sets_end_time(self, mod):
        tracer = mod.Tracer()
        span = tracer.start_span("op")
        tracer.finish_span(span)
        assert span.end_time is not None

    def test_get_span_count_zero_initially(self, mod):
        tracer = mod.Tracer()
        assert tracer.get_span_count() == 0

    def test_get_span_count_increments(self, mod):
        tracer = mod.Tracer()
        span = tracer.start_span("op")
        tracer.finish_span(span)
        assert tracer.get_span_count() == 1

    def test_get_span_count_multiple(self, mod):
        tracer = mod.Tracer()
        for i in range(5):
            s = tracer.start_span(f"op{i}")
            tracer.finish_span(s)
        assert tracer.get_span_count() == 5


# ===========================================================================
# Tracer — get_trace and export
# ===========================================================================


class TestTracerGetTrace:
    def test_get_trace_returns_correct_count(self, mod):
        tracer = mod.Tracer()
        root = tracer.start_span("root")
        child = tracer.start_span("child", parent_context=root.trace_context)
        tracer.finish_span(child)
        tracer.finish_span(root)
        trace = tracer.get_trace(root.trace_context.trace_id)
        assert len(trace) == 2

    def test_get_trace_excludes_other_traces(self, mod):
        tracer = mod.Tracer()
        s1 = tracer.start_span("a")
        s2 = tracer.start_span("b")
        tracer.finish_span(s1)
        tracer.finish_span(s2)
        trace1 = tracer.get_trace(s1.trace_context.trace_id)
        assert all(d["trace_id"] == s1.trace_context.trace_id for d in trace1)

    def test_get_trace_sorted_by_start(self, mod):
        tracer = mod.Tracer()
        root = tracer.start_span("root")
        child = tracer.start_span("child", parent_context=root.trace_context)
        tracer.finish_span(child)
        tracer.finish_span(root)
        spans = tracer.get_trace(root.trace_context.trace_id)
        starts = [s["start"] for s in spans]
        assert starts == sorted(starts)

    def test_get_trace_unknown_id_empty(self, mod):
        tracer = mod.Tracer()
        assert tracer.get_trace("nonexistent" * 3) == []

    def test_export_returns_all_completed(self, mod):
        tracer = mod.Tracer()
        for _ in range(3):
            s = tracer.start_span("op")
            tracer.finish_span(s)
        exported = tracer.export()
        assert len(exported) == 3

    def test_export_returns_dicts(self, mod):
        tracer = mod.Tracer()
        s = tracer.start_span("op")
        tracer.finish_span(s)
        exported = tracer.export()
        assert all(isinstance(d, dict) for d in exported)

    def test_concurrent_span_creation(self, mod):
        tracer = mod.Tracer()
        results = []

        def worker():
            s = tracer.start_span("concurrent")
            tracer.finish_span(s)
            results.append(True)

        threads = [threading.Thread(target=worker) for _ in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert tracer.get_span_count() == 20


# ===========================================================================
# SamplingStrategy — factories
# ===========================================================================


class TestSamplingStrategyFactories:
    def test_always_on_rate_is_one(self, mod):
        s = mod.SamplingStrategy.always_on()
        assert s.sample_rate == 1.0

    def test_always_off_rate_is_zero(self, mod):
        s = mod.SamplingStrategy.always_off()
        assert s.sample_rate == 0.0

    def test_ratio_rate_stored(self, mod):
        s = mod.SamplingStrategy.ratio(0.3)
        assert abs(s.sample_rate - 0.3) < 1e-9

    def test_clamp_above_one(self, mod):
        s = mod.SamplingStrategy(sample_rate=2.0)
        assert s.sample_rate == 1.0

    def test_clamp_below_zero(self, mod):
        s = mod.SamplingStrategy(sample_rate=-0.5)
        assert s.sample_rate == 0.0

    def test_default_rate_is_one(self, mod):
        s = mod.SamplingStrategy()
        assert s.sample_rate == 1.0


# ===========================================================================
# SamplingStrategy — should_sample
# ===========================================================================


class TestSamplingStrategyShouldSample:
    def test_always_on_samples_all(self, mod):
        s = mod.SamplingStrategy.always_on()
        results = [s.should_sample(mod.TraceContext().trace_id) for _ in range(20)]
        assert all(results)

    def test_always_off_samples_none(self, mod):
        s = mod.SamplingStrategy.always_off()
        results = [s.should_sample(mod.TraceContext().trace_id) for _ in range(20)]
        assert not any(results)

    def test_deterministic_same_trace_id(self, mod):
        s = mod.SamplingStrategy.ratio(0.5)
        trace_id = mod.TraceContext().trace_id
        r1 = s.should_sample(trace_id)
        r2 = s.should_sample(trace_id)
        assert r1 == r2

    def test_ratio_partial_sampling(self, mod):
        s = mod.SamplingStrategy.ratio(0.5)
        trace_ids = [uuid.uuid4().hex for _ in range(200)]
        sampled = sum(1 for tid in trace_ids if s.should_sample(tid))
        assert 50 <= sampled <= 150

    def test_returns_bool(self, mod):
        s = mod.SamplingStrategy.always_on()
        result = s.should_sample("a" * 32)
        assert isinstance(result, bool)

    def test_ratio_zero_samples_none(self, mod):
        s = mod.SamplingStrategy.ratio(0.0)
        results = [s.should_sample(uuid.uuid4().hex) for _ in range(20)]
        assert not any(results)

    def test_ratio_one_samples_all(self, mod):
        s = mod.SamplingStrategy.ratio(1.0)
        results = [s.should_sample(uuid.uuid4().hex) for _ in range(20)]
        assert all(results)


# ===========================================================================
# TraceExporter — export and clear
# ===========================================================================


class TestTraceExporterExport:
    def test_export_returns_count(self, mod):
        exp = mod.TraceExporter()
        tracer = mod.Tracer()
        spans_list = []
        for _ in range(3):
            s = tracer.start_span("op")
            tracer.finish_span(s)
            spans_list.append(s.to_dict())
        assert exp.export(spans_list) == 3

    def test_export_empty_list_returns_zero(self, mod):
        exp = mod.TraceExporter()
        assert exp.export([]) == 0

    def test_export_accumulates(self, mod):
        exp = mod.TraceExporter()
        tracer = mod.Tracer()
        s1 = tracer.start_span("a")
        tracer.finish_span(s1)
        s2 = tracer.start_span("b")
        tracer.finish_span(s2)
        exp.export([s1.to_dict()])
        exp.export([s2.to_dict()])
        # Both stored
        assert len(exp._spans) == 2

    def test_clear_removes_all_spans(self, mod):
        exp = mod.TraceExporter()
        tracer = mod.Tracer()
        s = tracer.start_span("op")
        tracer.finish_span(s)
        exp.export([s.to_dict()])
        exp.clear()
        assert exp._spans == []

    def test_clear_then_export_works(self, mod):
        exp = mod.TraceExporter()
        tracer = mod.Tracer()
        s = tracer.start_span("op")
        tracer.finish_span(s)
        exp.export([s.to_dict()])
        exp.clear()
        count = exp.export([s.to_dict()])
        assert count == 1
        assert len(exp._spans) == 1


# ===========================================================================
# TraceExporter — filters
# ===========================================================================


def _make_populated_exporter(mod):
    """Helper: build a TraceExporter with 3 spans of known properties."""
    tracer = mod.Tracer()
    exp = mod.TraceExporter()

    # span 1: service-a, OK, fast
    s1 = tracer.start_span("svc-a.fast", attributes={"service.name": "service-a"})
    s1.set_status("OK")
    tracer.finish_span(s1)

    # span 2: service-b, ERROR, fast
    s2 = tracer.start_span("svc-b.err", attributes={"service.name": "service-b"})
    s2.set_status("ERROR")
    tracer.finish_span(s2)

    # span 3: service-a, ERROR; inflate duration in dict
    s3 = tracer.start_span("svc-a.slow", attributes={"service.name": "service-a"})
    s3.set_status("ERROR")
    tracer.finish_span(s3)

    all_dicts = tracer.export()
    for d in all_dicts:
        if d["name"] == "svc-a.slow":
            d["duration_ms"] = 999.0

    exp.export(all_dicts)
    return exp


class TestTraceExporterFilters:
    def test_get_by_service_correct_count(self, mod):
        exp = _make_populated_exporter(mod)
        assert len(exp.get_by_service("service-a")) == 2

    def test_get_by_service_all_match(self, mod):
        exp = _make_populated_exporter(mod)
        spans = exp.get_by_service("service-a")
        assert all(s["attributes"]["service.name"] == "service-a" for s in spans)

    def test_get_by_service_excludes_other(self, mod):
        exp = _make_populated_exporter(mod)
        assert len(exp.get_by_service("service-b")) == 1

    def test_get_by_service_no_match_empty(self, mod):
        exp = _make_populated_exporter(mod)
        assert exp.get_by_service("nonexistent") == []

    def test_get_errors_count(self, mod):
        exp = _make_populated_exporter(mod)
        assert len(exp.get_errors()) == 2

    def test_get_errors_all_error_status(self, mod):
        exp = _make_populated_exporter(mod)
        assert all(s["status"] == "ERROR" for s in exp.get_errors())

    def test_get_errors_excludes_ok(self, mod):
        exp = _make_populated_exporter(mod)
        assert not any(s["status"] == "OK" for s in exp.get_errors())

    def test_get_slow_spans_threshold_matched(self, mod):
        exp = _make_populated_exporter(mod)
        slow = exp.get_slow_spans(threshold_ms=500.0)
        assert len(slow) == 1
        assert slow[0]["duration_ms"] == 999.0

    def test_get_slow_spans_none_above_threshold(self, mod):
        exp = _make_populated_exporter(mod)
        assert exp.get_slow_spans(threshold_ms=10_000.0) == []

    def test_get_slow_spans_all_below_threshold(self, mod):
        exp = _make_populated_exporter(mod)
        # All real spans have sub-ms durations; threshold = 0 catches the 999ms one
        slow = exp.get_slow_spans(threshold_ms=0.0)
        assert any(s["duration_ms"] == 999.0 for s in slow)

    def test_concurrent_export_and_filter(self, mod):
        exp = mod.TraceExporter()
        tracer = mod.Tracer()

        def producer():
            for _ in range(10):
                s = tracer.start_span("op", attributes={"service.name": "svc"})
                s.set_status("ERROR")
                tracer.finish_span(s)
                exp.export([s.to_dict()])

        def consumer():
            for _ in range(10):
                _ = exp.get_errors()

        t1 = threading.Thread(target=producer)
        t2 = threading.Thread(target=consumer)
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        assert len(exp.get_errors()) >= 10


# ===========================================================================
# Integration — end-to-end
# ===========================================================================


class TestEndToEnd:
    def test_full_trace_assembled(self, mod):
        tracer = mod.Tracer()
        exporter = mod.TraceExporter()
        strategy = mod.SamplingStrategy.always_on()

        root = tracer.start_span("http.request", attributes={"service.name": "gateway"})
        assert strategy.should_sample(root.trace_context.trace_id)
        root.add_event("request.received")

        db = tracer.start_span(
            "db.query",
            parent_context=root.trace_context,
            attributes={"service.name": "gateway"},
        )
        db.set_status("OK")
        tracer.finish_span(db)

        root.set_status("OK")
        tracer.finish_span(root)

        count = exporter.export(tracer.export())
        assert count == 2
        assert len(tracer.get_trace(root.trace_context.trace_id)) == 2
        assert len(exporter.get_by_service("gateway")) == 2
        assert exporter.get_errors() == []

    def test_error_span_event_preserved(self, mod):
        tracer = mod.Tracer()
        exporter = mod.TraceExporter()

        s = tracer.start_span("failing.op", attributes={"service.name": "worker"})
        s.set_status("ERROR")
        s.add_event("exception", attributes={"type": "ValueError"})
        tracer.finish_span(s)

        exporter.export(tracer.export())
        errors = exporter.get_errors()
        assert len(errors) == 1
        assert errors[0]["events"][0]["name"] == "exception"

    def test_traceparent_propagation_across_hops(self, mod):
        ctx_a = mod.TraceContext()
        header = ctx_a.to_traceparent()

        ctx_b_parent = mod.TraceContext.from_traceparent(header)
        ctx_b_child = ctx_b_parent.child_context()

        assert ctx_b_child.trace_id == ctx_a.trace_id
        assert ctx_b_child.parent_span_id == ctx_a.span_id

    def test_sampling_gates_trace_creation(self, mod):
        strategy = mod.SamplingStrategy.always_off()
        tracer = mod.Tracer()

        ctx = mod.TraceContext()
        if strategy.should_sample(ctx.trace_id):
            tracer.start_span("should-not-exist")

        assert tracer.get_span_count() == 0
