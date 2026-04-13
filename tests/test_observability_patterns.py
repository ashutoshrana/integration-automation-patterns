"""
Tests for 32_observability_patterns.py

Covers all five patterns (MetricRegistry, Histogram, SpanTracer, HealthCheck,
AlertRule / AlertManager) with 60 test cases using only the standard library.
No external dependencies required.
"""

from __future__ import annotations

import importlib.util
import sys
import threading
import time
import types
from pathlib import Path

import pytest

_MOD_PATH = (
    Path(__file__).parent.parent / "examples" / "32_observability_patterns.py"
)


def _load_module():
    module_name = "observability_patterns_32"
    if module_name in sys.modules:
        return sys.modules[module_name]
    spec = importlib.util.spec_from_file_location(module_name, _MOD_PATH)
    mod = types.ModuleType(module_name)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def mod():
    return _load_module()


# ===========================================================================
# MetricRegistry
# ===========================================================================


class TestMetricRegistry:
    # --- increment ---

    def test_increment_creates_counter_at_one(self, mod):
        registry = mod.MetricRegistry()
        registry.increment("req.total")
        assert registry.get("req.total") == 1.0

    def test_increment_adds_to_existing_value(self, mod):
        registry = mod.MetricRegistry()
        registry.increment("req.total", 3.0)
        registry.increment("req.total", 2.0)
        assert registry.get("req.total") == 5.0

    def test_increment_default_value_is_one(self, mod):
        registry = mod.MetricRegistry()
        registry.increment("hits")
        registry.increment("hits")
        assert registry.get("hits") == 2.0

    def test_increment_accepts_tags_without_error(self, mod):
        registry = mod.MetricRegistry()
        registry.increment("req.count", tags={"method": "GET"})
        assert registry.get("req.count") == 1.0

    def test_increment_different_metrics_are_independent(self, mod):
        registry = mod.MetricRegistry()
        registry.increment("a", 5.0)
        registry.increment("b", 2.0)
        assert registry.get("a") == 5.0
        assert registry.get("b") == 2.0

    # --- gauge ---

    def test_gauge_sets_value(self, mod):
        registry = mod.MetricRegistry()
        registry.gauge("mem.mb", 512.0)
        assert registry.get("mem.mb") == 512.0

    def test_gauge_replaces_previous_value(self, mod):
        registry = mod.MetricRegistry()
        registry.gauge("cpu.pct", 30.0)
        registry.gauge("cpu.pct", 80.0)
        assert registry.get("cpu.pct") == 80.0

    def test_gauge_accepts_tags_without_error(self, mod):
        registry = mod.MetricRegistry()
        registry.gauge("disk.gb", 200.0, tags={"host": "srv-1"})
        assert registry.get("disk.gb") == 200.0

    # --- get ---

    def test_get_unknown_metric_returns_zero(self, mod):
        registry = mod.MetricRegistry()
        assert registry.get("never.set") == 0.0

    def test_get_after_increment_returns_correct_value(self, mod):
        registry = mod.MetricRegistry()
        registry.increment("x", 42.0)
        assert registry.get("x") == 42.0

    # --- snapshot ---

    def test_snapshot_returns_all_metrics(self, mod):
        registry = mod.MetricRegistry()
        registry.increment("a", 1.0)
        registry.gauge("b", 2.0)
        snap = registry.snapshot()
        assert snap["a"] == 1.0
        assert snap["b"] == 2.0

    def test_snapshot_is_independent_copy(self, mod):
        registry = mod.MetricRegistry()
        registry.increment("counter", 5.0)
        snap = registry.snapshot()
        snap["counter"] = 999.0
        assert registry.get("counter") == 5.0

    def test_snapshot_empty_registry_returns_empty_dict(self, mod):
        registry = mod.MetricRegistry()
        assert registry.snapshot() == {}

    # --- reset ---

    def test_reset_zeros_metric(self, mod):
        registry = mod.MetricRegistry()
        registry.increment("hits", 10.0)
        registry.reset("hits")
        assert registry.get("hits") == 0.0

    def test_reset_unknown_metric_creates_zero_entry(self, mod):
        registry = mod.MetricRegistry()
        registry.reset("fresh")
        assert registry.get("fresh") == 0.0

    # --- thread safety ---

    def test_increment_thread_safe(self, mod):
        registry = mod.MetricRegistry()
        errors = []

        def worker():
            try:
                for _ in range(100):
                    registry.increment("shared")
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
        assert registry.get("shared") == 1000.0


# ===========================================================================
# Histogram
# ===========================================================================


class TestHistogram:
    # --- observe + count/sum/mean ---

    def test_observe_increments_count(self, mod):
        hist = mod.Histogram(boundaries=[10, 50, 100])
        hist.observe(5.0)
        assert hist.count() == 1

    def test_observe_multiple_increments_count(self, mod):
        hist = mod.Histogram(boundaries=[10, 50, 100])
        for v in [1, 2, 3, 4, 5]:
            hist.observe(v)
        assert hist.count() == 5

    def test_sum_is_accurate(self, mod):
        hist = mod.Histogram(boundaries=[100])
        hist.observe(10.0)
        hist.observe(20.0)
        hist.observe(30.0)
        assert hist.sum() == 60.0

    def test_mean_is_accurate(self, mod):
        hist = mod.Histogram(boundaries=[100])
        hist.observe(10.0)
        hist.observe(30.0)
        assert hist.mean() == 20.0

    def test_mean_empty_returns_zero(self, mod):
        hist = mod.Histogram(boundaries=[100])
        assert hist.mean() == 0.0

    def test_sum_empty_returns_zero(self, mod):
        hist = mod.Histogram(boundaries=[100])
        assert hist.sum() == 0.0

    def test_count_empty_returns_zero(self, mod):
        hist = mod.Histogram(boundaries=[100])
        assert hist.count() == 0

    # --- buckets (cumulative) ---

    def test_buckets_cumulative_counts(self, mod):
        hist = mod.Histogram(boundaries=[1, 5, 10, 50, 100])
        for v in [0.5, 1.0, 2.0, 4.5, 8.0, 11.0, 30.0, 60.0]:
            hist.observe(v)
        b = hist.buckets()
        # ≤1: 0.5, 1.0 → 2; ≤5: +2.0, 4.5 → 4; ≤10: +8.0 → 5; ≤50: +11.0, 30.0 → 7; ≤100: +60.0 → 8
        assert b[1] == 2
        assert b[5] == 4
        assert b[10] == 5
        assert b[50] == 7
        assert b[100] == 8

    def test_buckets_all_below_first_boundary(self, mod):
        hist = mod.Histogram(boundaries=[10, 50, 100])
        hist.observe(1.0)
        hist.observe(2.0)
        b = hist.buckets()
        assert b[10] == 2
        assert b[50] == 2
        assert b[100] == 2

    def test_buckets_none_below_first_boundary(self, mod):
        hist = mod.Histogram(boundaries=[1, 5])
        hist.observe(10.0)
        b = hist.buckets()
        assert b[1] == 0
        assert b[5] == 0

    def test_buckets_returns_all_configured_boundaries(self, mod):
        boundaries = [1, 5, 10, 50, 100]
        hist = mod.Histogram(boundaries=boundaries)
        hist.observe(3.0)
        b = hist.buckets()
        assert set(b.keys()) == set(boundaries)

    def test_buckets_boundary_sorting(self, mod):
        # Pass unsorted boundaries; should still work correctly
        hist = mod.Histogram(boundaries=[100, 10, 1])
        hist.observe(5.0)
        b = hist.buckets()
        assert b[1] == 0
        assert b[10] == 1
        assert b[100] == 1

    # --- percentile ---

    def test_percentile_p0_returns_minimum(self, mod):
        hist = mod.Histogram(boundaries=[1000])
        for v in [3, 1, 4, 1, 5, 9, 2, 6]:
            hist.observe(v)
        assert hist.percentile(0) == 1.0

    def test_percentile_p100_returns_maximum(self, mod):
        hist = mod.Histogram(boundaries=[1000])
        for v in [3, 1, 4, 1, 5, 9, 2, 6]:
            hist.observe(v)
        assert hist.percentile(100) == 9.0

    def test_percentile_p50_is_median_single_value(self, mod):
        hist = mod.Histogram(boundaries=[1000])
        hist.observe(7.0)
        assert hist.percentile(50) == 7.0

    def test_percentile_increases_with_p(self, mod):
        hist = mod.Histogram(boundaries=[1000])
        for v in range(1, 101):
            hist.observe(float(v))
        assert hist.percentile(25) < hist.percentile(50) < hist.percentile(75) < hist.percentile(95)

    def test_percentile_empty_raises_value_error(self, mod):
        hist = mod.Histogram(boundaries=[100])
        with pytest.raises(ValueError):
            hist.percentile(50)

    def test_percentile_below_zero_raises_value_error(self, mod):
        hist = mod.Histogram(boundaries=[100])
        hist.observe(1.0)
        with pytest.raises(ValueError):
            hist.percentile(-1)

    def test_percentile_above_100_raises_value_error(self, mod):
        hist = mod.Histogram(boundaries=[100])
        hist.observe(1.0)
        with pytest.raises(ValueError):
            hist.percentile(101)


# ===========================================================================
# SpanTracer
# ===========================================================================


class TestSpanTracer:
    # --- start_span ---

    def test_start_span_returns_span(self, mod):
        tracer = mod.SpanTracer()
        span = tracer.start_span("op")
        assert isinstance(span, mod.Span)

    def test_start_span_auto_generates_trace_id(self, mod):
        tracer = mod.SpanTracer()
        span = tracer.start_span("op")
        assert span.trace_id is not None
        assert len(span.trace_id) > 0

    def test_start_span_two_calls_get_different_trace_ids(self, mod):
        tracer = mod.SpanTracer()
        s1 = tracer.start_span("op")
        s2 = tracer.start_span("op")
        assert s1.trace_id != s2.trace_id

    def test_start_span_explicit_trace_id_preserved(self, mod):
        tracer = mod.SpanTracer()
        span = tracer.start_span("op", trace_id="my-trace-123")
        assert span.trace_id == "my-trace-123"

    def test_start_span_name_set_correctly(self, mod):
        tracer = mod.SpanTracer()
        span = tracer.start_span("db.query")
        assert span.name == "db.query"

    def test_start_span_tags_attached(self, mod):
        tracer = mod.SpanTracer()
        span = tracer.start_span("http.request", tags={"method": "POST"})
        assert span.tags["method"] == "POST"

    def test_start_span_no_tags_defaults_empty_dict(self, mod):
        tracer = mod.SpanTracer()
        span = tracer.start_span("op")
        assert span.tags == {}

    def test_start_span_end_time_is_none(self, mod):
        tracer = mod.SpanTracer()
        span = tracer.start_span("op")
        assert span.end_time is None

    # --- finish_span ---

    def test_finish_span_sets_end_time(self, mod):
        tracer = mod.SpanTracer()
        span = tracer.start_span("op")
        tracer.finish_span(span)
        assert span.end_time is not None

    def test_finish_span_duration_ms_is_non_negative(self, mod):
        tracer = mod.SpanTracer()
        span = tracer.start_span("op")
        time.sleep(0.001)
        tracer.finish_span(span)
        assert span.duration_ms >= 0.0

    def test_duration_ms_none_before_finish(self, mod):
        tracer = mod.SpanTracer()
        span = tracer.start_span("op")
        assert span.duration_ms is None

    # --- get_trace ---

    def test_get_trace_returns_spans_for_trace_id(self, mod):
        tracer = mod.SpanTracer()
        tid = "trace-abc"
        tracer.start_span("a", trace_id=tid)
        tracer.start_span("b", trace_id=tid)
        tracer.start_span("c", trace_id="other-trace")
        result = tracer.get_trace(tid)
        assert len(result) == 2
        assert all(s.trace_id == tid for s in result)

    def test_get_trace_ordered_by_start_time(self, mod):
        tracer = mod.SpanTracer()
        tid = "ordered-trace"
        s1 = tracer.start_span("first", trace_id=tid)
        time.sleep(0.001)
        s2 = tracer.start_span("second", trace_id=tid)
        result = tracer.get_trace(tid)
        assert result[0].span_id == s1.span_id
        assert result[1].span_id == s2.span_id

    def test_get_trace_unknown_trace_returns_empty(self, mod):
        tracer = mod.SpanTracer()
        assert tracer.get_trace("no-such-trace") == []

    # --- completed_spans ---

    def test_completed_spans_only_finished(self, mod):
        tracer = mod.SpanTracer()
        s_open = tracer.start_span("open")
        s_done = tracer.start_span("done")
        tracer.finish_span(s_done)
        completed = tracer.completed_spans()
        assert s_done in completed
        assert s_open not in completed

    def test_completed_spans_empty_when_none_finished(self, mod):
        tracer = mod.SpanTracer()
        tracer.start_span("unfinished")
        assert tracer.completed_spans() == []

    # --- clear ---

    def test_clear_removes_all_spans(self, mod):
        tracer = mod.SpanTracer()
        for _ in range(5):
            tracer.start_span("op")
        tracer.clear()
        assert tracer.completed_spans() == []
        assert tracer.get_trace("any") == []


# ===========================================================================
# HealthCheck
# ===========================================================================


class TestHealthCheck:
    # --- register + run ---

    def test_run_healthy_check_returns_healthy_true(self, mod):
        hc = mod.HealthCheck()
        hc.register("db", lambda: True)
        result = hc.run("db")
        assert result.healthy is True

    def test_run_unhealthy_check_returns_healthy_false(self, mod):
        hc = mod.HealthCheck()
        hc.register("cache", lambda: False)
        result = hc.run("cache")
        assert result.healthy is False

    def test_run_check_name_in_result(self, mod):
        hc = mod.HealthCheck()
        hc.register("service-x", lambda: True)
        result = hc.run("service-x")
        assert result.name == "service-x"

    def test_run_exception_in_check_fn_returns_unhealthy(self, mod):
        hc = mod.HealthCheck()
        hc.register("broken", lambda: (_ for _ in ()).throw(RuntimeError("conn failed")))
        result = hc.run("broken")
        assert result.healthy is False
        assert "conn failed" in result.message

    def test_run_unknown_check_raises_key_error(self, mod):
        hc = mod.HealthCheck()
        with pytest.raises(KeyError):
            hc.run("no-such-check")

    def test_run_result_has_checked_at_timestamp(self, mod):
        hc = mod.HealthCheck()
        hc.register("t", lambda: True)
        before = time.time()
        result = hc.run("t")
        after = time.time()
        assert before <= result.checked_at <= after

    # --- run_all ---

    def test_run_all_returns_all_registered_checks(self, mod):
        hc = mod.HealthCheck()
        hc.register("a", lambda: True)
        hc.register("b", lambda: False)
        hc.register("c", lambda: True)
        results = hc.run_all()
        assert set(results.keys()) == {"a", "b", "c"}

    def test_run_all_empty_registry_returns_empty_dict(self, mod):
        hc = mod.HealthCheck()
        assert hc.run_all() == {}

    # --- is_healthy ---

    def test_is_healthy_true_when_all_pass(self, mod):
        hc = mod.HealthCheck()
        hc.register("x", lambda: True)
        hc.register("y", lambda: True)
        assert hc.is_healthy() is True

    def test_is_healthy_false_when_any_fails(self, mod):
        hc = mod.HealthCheck()
        hc.register("ok", lambda: True)
        hc.register("fail", lambda: False)
        assert hc.is_healthy() is False

    def test_is_healthy_true_when_no_checks_registered(self, mod):
        hc = mod.HealthCheck()
        assert hc.is_healthy() is True

    # --- summary ---

    def test_summary_healthy_key_reflects_overall_status(self, mod):
        hc = mod.HealthCheck()
        hc.register("p", lambda: True)
        hc.register("q", lambda: False)
        summ = hc.summary()
        assert summ["healthy"] is False

    def test_summary_checks_key_has_per_check_info(self, mod):
        hc = mod.HealthCheck()
        hc.register("alpha", lambda: True)
        summ = hc.summary()
        assert "alpha" in summ["checks"]
        assert "healthy" in summ["checks"]["alpha"]
        assert "message" in summ["checks"]["alpha"]

    def test_summary_all_healthy_returns_true(self, mod):
        hc = mod.HealthCheck()
        hc.register("a", lambda: True)
        hc.register("b", lambda: True)
        assert hc.summary()["healthy"] is True


# ===========================================================================
# AlertRule + AlertManager
# ===========================================================================


class TestAlertRule:
    # --- condition: gt ---

    def test_gt_fires_when_above_threshold(self, mod):
        rule = mod.AlertRule("test", "cpu", 80.0, "gt")
        assert rule.evaluate(81.0) is not None

    def test_gt_no_fire_when_equal(self, mod):
        rule = mod.AlertRule("test", "cpu", 80.0, "gt")
        assert rule.evaluate(80.0) is None

    def test_gt_no_fire_when_below(self, mod):
        rule = mod.AlertRule("test", "cpu", 80.0, "gt")
        assert rule.evaluate(79.0) is None

    # --- condition: lt ---

    def test_lt_fires_when_below_threshold(self, mod):
        rule = mod.AlertRule("test", "disk", 10.0, "lt")
        assert rule.evaluate(9.0) is not None

    def test_lt_no_fire_when_equal(self, mod):
        rule = mod.AlertRule("test", "disk", 10.0, "lt")
        assert rule.evaluate(10.0) is None

    def test_lt_no_fire_when_above(self, mod):
        rule = mod.AlertRule("test", "disk", 10.0, "lt")
        assert rule.evaluate(11.0) is None

    # --- condition: gte ---

    def test_gte_fires_when_equal(self, mod):
        rule = mod.AlertRule("test", "m", 5.0, "gte")
        assert rule.evaluate(5.0) is not None

    def test_gte_fires_when_above(self, mod):
        rule = mod.AlertRule("test", "m", 5.0, "gte")
        assert rule.evaluate(6.0) is not None

    def test_gte_no_fire_when_below(self, mod):
        rule = mod.AlertRule("test", "m", 5.0, "gte")
        assert rule.evaluate(4.9) is None

    # --- condition: lte ---

    def test_lte_fires_when_equal(self, mod):
        rule = mod.AlertRule("test", "m", 5.0, "lte")
        assert rule.evaluate(5.0) is not None

    def test_lte_fires_when_below(self, mod):
        rule = mod.AlertRule("test", "m", 5.0, "lte")
        assert rule.evaluate(4.0) is not None

    def test_lte_no_fire_when_above(self, mod):
        rule = mod.AlertRule("test", "m", 5.0, "lte")
        assert rule.evaluate(5.1) is None

    # --- condition: eq ---

    def test_eq_fires_when_equal(self, mod):
        rule = mod.AlertRule("test", "m", 42.0, "eq")
        assert rule.evaluate(42.0) is not None

    def test_eq_no_fire_when_not_equal(self, mod):
        rule = mod.AlertRule("test", "m", 42.0, "eq")
        assert rule.evaluate(43.0) is None

    # --- Alert dataclass fields ---

    def test_alert_fields_populated(self, mod):
        rule = mod.AlertRule("cpu-alert", "cpu.pct", 90.0, "gt")
        alert = rule.evaluate(95.0)
        assert alert.rule_name == "cpu-alert"
        assert alert.metric_name == "cpu.pct"
        assert alert.current_value == 95.0
        assert alert.threshold == 90.0
        assert alert.condition == "gt"

    def test_alert_fired_at_is_recent(self, mod):
        rule = mod.AlertRule("r", "m", 0.0, "gt")
        before = time.time()
        alert = rule.evaluate(1.0)
        after = time.time()
        assert before <= alert.fired_at <= after

    # --- invalid condition ---

    def test_invalid_condition_raises_value_error(self, mod):
        with pytest.raises(ValueError):
            mod.AlertRule("r", "m", 1.0, "neq")


class TestAlertManager:
    def test_add_and_evaluate_one_firing_rule(self, mod):
        manager = mod.AlertManager()
        manager.add_rule(mod.AlertRule("hi-cpu", "cpu", 80.0, "gt"))
        alerts = manager.evaluate_all({"cpu": 90.0})
        assert len(alerts) == 1
        assert alerts[0].rule_name == "hi-cpu"

    def test_evaluate_all_no_fire_returns_empty(self, mod):
        manager = mod.AlertManager()
        manager.add_rule(mod.AlertRule("hi-cpu", "cpu", 80.0, "gt"))
        alerts = manager.evaluate_all({"cpu": 50.0})
        assert alerts == []

    def test_evaluate_all_two_rules_one_fires(self, mod):
        manager = mod.AlertManager()
        manager.add_rule(mod.AlertRule("hi-cpu", "cpu", 80.0, "gt"))
        manager.add_rule(mod.AlertRule("lo-disk", "disk", 10.0, "lt"))
        alerts = manager.evaluate_all({"cpu": 85.0, "disk": 20.0})
        assert len(alerts) == 1
        assert alerts[0].rule_name == "hi-cpu"

    def test_evaluate_all_both_rules_fire(self, mod):
        manager = mod.AlertManager()
        manager.add_rule(mod.AlertRule("hi-cpu", "cpu", 80.0, "gt"))
        manager.add_rule(mod.AlertRule("lo-disk", "disk", 10.0, "lt"))
        alerts = manager.evaluate_all({"cpu": 95.0, "disk": 5.0})
        assert len(alerts) == 2

    def test_evaluate_all_missing_metric_skips_rule(self, mod):
        manager = mod.AlertManager()
        manager.add_rule(mod.AlertRule("r", "absent_metric", 10.0, "gt"))
        alerts = manager.evaluate_all({"other": 100.0})
        assert alerts == []

    def test_fired_count_accumulates_across_calls(self, mod):
        manager = mod.AlertManager()
        manager.add_rule(mod.AlertRule("r", "m", 5.0, "gt"))
        manager.evaluate_all({"m": 10.0})
        manager.evaluate_all({"m": 10.0})
        assert manager.fired_count == 2

    def test_fired_count_starts_at_zero(self, mod):
        manager = mod.AlertManager()
        assert manager.fired_count == 0

    def test_evaluate_all_empty_manager_returns_empty(self, mod):
        manager = mod.AlertManager()
        alerts = manager.evaluate_all({"cpu": 95.0})
        assert alerts == []
