"""
32_observability_patterns.py — Observability Patterns

Pure-Python implementation of core observability primitives using only the
standard library (``dataclasses``, ``threading``, ``time``, ``uuid``,
``typing``).  No external monitoring or tracing libraries are required.

    Pattern 1 — MetricRegistry (metrics collection):
                Thread-safe registry for named counters, gauges, and timing
                observations.  Supports increment, gauge-set, snapshot, reset,
                and thread-safe concurrent access via ``threading.Lock``.

    Pattern 2 — Histogram (value distribution tracking):
                Records observations into configurable bucket boundaries and
                computes cumulative bucket counts, total count/sum/mean, and
                percentile estimates via linear interpolation across buckets.

    Pattern 3 — SpanTracer (lightweight distributed tracing):
                Creates and finishes tracing spans with auto-generated
                trace IDs (``uuid.uuid4``), records start/end wall-clock
                times, and retrieves spans by trace ID or completion status.

    Pattern 4 — HealthCheck (periodic health probe registry):
                Registers named check functions, executes them (catching
                exceptions as unhealthy), and provides aggregated health
                summary suitable for liveness/readiness endpoints.

    Pattern 5 — AlertRule / AlertManager (threshold-based alerting):
                Evaluates metric values against configurable threshold
                conditions (gt, lt, gte, lte, eq) and fires ``Alert``
                dataclass instances when conditions are met.

No external dependencies required.

Run:
    python examples/32_observability_patterns.py
"""

from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field

# ===========================================================================
# Pattern 1 — MetricRegistry
# ===========================================================================


class MetricRegistry:
    """Thread-safe registry for named counters, gauges, and timing observations.

    All metric values are stored as ``float``.  Counters are *additive*
    (``increment`` adds to the existing value); gauges are *absolute*
    (``gauge`` replaces the current value).

    Example::

        registry = MetricRegistry()
        registry.increment("requests.total", tags={"method": "GET"})
        registry.gauge("memory.used_mb", 512.0)
        snap = registry.snapshot()
        print(snap["requests.total"])   # 1.0
    """

    def __init__(self) -> None:
        self._metrics: dict[str, float] = {}
        self._lock: threading.Lock = threading.Lock()

    # ------------------------------------------------------------------
    # Mutations
    # ------------------------------------------------------------------

    def increment(
        self,
        name: str,
        value: float = 1.0,
        tags: dict[str, str] | None = None,  # noqa: ARG002  (reserved for future use)
    ) -> None:
        """Add *value* to the counter named *name*.

        Args:
            name:  Metric name.
            value: Amount to add (default ``1.0``).
            tags:  Optional tag dict (stored for caller convenience; not
                   used internally in this implementation).
        """
        with self._lock:
            self._metrics[name] = self._metrics.get(name, 0.0) + value

    def gauge(
        self,
        name: str,
        value: float,
        tags: dict[str, str] | None = None,  # noqa: ARG002
    ) -> None:
        """Set the gauge named *name* to *value* (replaces any existing value).

        Args:
            name:  Metric name.
            value: New gauge reading.
            tags:  Optional tag dict (reserved for future use).
        """
        with self._lock:
            self._metrics[name] = value

    def reset(self, name: str) -> None:
        """Zero the metric *name*.

        Args:
            name: Metric name to reset.
        """
        with self._lock:
            self._metrics[name] = 0.0

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    def get(self, name: str) -> float:
        """Return the current value of metric *name*.

        Args:
            name: Metric name.

        Returns:
            Current value, or ``0.0`` if the metric has never been set.
        """
        with self._lock:
            return self._metrics.get(name, 0.0)

    def snapshot(self) -> dict[str, float]:
        """Return a copy of all current metric values.

        Returns:
            A ``dict`` mapping metric name → current value.  Mutations to
            the returned dict do not affect the registry.
        """
        with self._lock:
            return dict(self._metrics)


# ===========================================================================
# Pattern 2 — Histogram
# ===========================================================================


class Histogram:
    """Tracks value distributions using configurable bucket boundaries.

    Bucket boundaries are sorted upper bounds.  Each call to :meth:`observe`
    records a value; :meth:`buckets` returns cumulative counts (i.e. how many
    observations fell at or below each upper bound).

    Example::

        hist = Histogram(boundaries=[1, 5, 10, 50, 100])
        for v in [0.5, 3, 7, 12, 75]:
            hist.observe(v)
        print(hist.buckets())
        # {1: 1, 5: 2, 10: 3, 50: 4, 100: 5}
        print(hist.percentile(50))   # estimated median
    """

    def __init__(self, boundaries: list[float]) -> None:
        """Initialise the histogram with sorted bucket upper bounds.

        Args:
            boundaries: List of upper bound values (will be sorted ascending).
        """
        self._boundaries: list[float] = sorted(boundaries)
        # Store raw observations for accurate percentile computation
        self._observations: list[float] = []
        self._total_sum: float = 0.0
        self._lock: threading.Lock = threading.Lock()

    # ------------------------------------------------------------------
    # Recording
    # ------------------------------------------------------------------

    def observe(self, value: float) -> None:
        """Record a single observation.

        Args:
            value: The measured value to record.
        """
        with self._lock:
            self._observations.append(value)
            self._total_sum += value

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def buckets(self) -> dict[float, int]:
        """Return cumulative bucket counts.

        For each configured upper bound ``b``, the count is the number of
        observations whose value is ``≤ b``.

        Returns:
            Dict mapping upper bound → cumulative observation count.
        """
        with self._lock:
            obs = sorted(self._observations)
        result: dict[float, int] = {}
        for bound in self._boundaries:
            result[bound] = sum(1 for v in obs if v <= bound)
        return result

    def count(self) -> int:
        """Total number of observations recorded.

        Returns:
            Integer observation count.
        """
        with self._lock:
            return len(self._observations)

    def sum(self) -> float:
        """Sum of all observations.

        Returns:
            Float sum; ``0.0`` if no observations.
        """
        with self._lock:
            return self._total_sum

    def mean(self) -> float:
        """Arithmetic mean of all observations.

        Returns:
            Mean value; ``0.0`` if no observations.
        """
        with self._lock:
            n = len(self._observations)
            return self._total_sum / n if n > 0 else 0.0

    def percentile(self, p: float) -> float:
        """Estimate the *p*-th percentile using linear interpolation.

        Uses the sorted observation list directly (not bucket boundaries)
        for accurate percentile estimation.

        Args:
            p: Percentile to compute (0–100 inclusive).

        Returns:
            Estimated value at the *p*-th percentile.

        Raises:
            ValueError: If *p* is outside the range [0, 100].
            ValueError: If no observations have been recorded.
        """
        if not 0 <= p <= 100:
            raise ValueError(f"Percentile p must be in [0, 100]; got {p}")
        with self._lock:
            n = len(self._observations)
            if n == 0:
                raise ValueError("Cannot compute percentile: no observations recorded")
            sorted_obs = sorted(self._observations)

        if p == 0:
            return sorted_obs[0]
        if p == 100:
            return sorted_obs[-1]

        # Linear interpolation
        index = (p / 100.0) * (n - 1)
        lower = int(index)
        upper = lower + 1
        if upper >= n:
            return sorted_obs[-1]
        frac = index - lower
        return sorted_obs[lower] + frac * (sorted_obs[upper] - sorted_obs[lower])


# ===========================================================================
# Pattern 3 — SpanTracer
# ===========================================================================


@dataclass
class Span:
    """A single distributed-tracing span.

    Attributes:
        trace_id:   ID grouping related spans into one trace.
        span_id:    Unique identifier for this individual span.
        name:       Human-readable operation name.
        start_time: Wall-clock timestamp (``time.time()``) when the span started.
        end_time:   Wall-clock timestamp when the span finished; ``None`` if still
                    in progress.
        tags:       Arbitrary key-value metadata attached to the span.
    """

    trace_id: str
    span_id: str
    name: str
    start_time: float
    end_time: float | None = None
    tags: dict[str, str] = field(default_factory=dict)

    @property
    def duration_ms(self) -> float | None:
        """Elapsed time in milliseconds, or ``None`` if not yet finished.

        Returns:
            Duration in ms if ``end_time`` is set, else ``None``.
        """
        if self.end_time is None:
            return None
        return (self.end_time - self.start_time) * 1000.0


class SpanTracer:
    """Lightweight distributed-tracing span recorder.

    Spans are created with :meth:`start_span` and completed with
    :meth:`finish_span`.  All spans are held in memory and accessible via
    :meth:`get_trace` and :meth:`completed_spans`.

    Example::

        tracer = SpanTracer()
        span = tracer.start_span("db.query", tags={"table": "users"})
        # … do work …
        tracer.finish_span(span)
        print(span.duration_ms)
    """

    def __init__(self) -> None:
        self._spans: list[Span] = []
        self._lock: threading.Lock = threading.Lock()

    # ------------------------------------------------------------------
    # Span lifecycle
    # ------------------------------------------------------------------

    def start_span(
        self,
        name: str,
        trace_id: str | None = None,
        tags: dict[str, str] | None = None,
    ) -> Span:
        """Create a new span, append it to internal storage, and return it.

        Args:
            name:     Operation name for the span.
            trace_id: Trace group identifier.  Auto-generated from
                      ``uuid.uuid4()`` if ``None``.
            tags:     Optional tag dict attached to the span.

        Returns:
            The newly created :class:`Span` (still in progress).
        """
        span = Span(
            trace_id=trace_id if trace_id is not None else str(uuid.uuid4()),
            span_id=str(uuid.uuid4()),
            name=name,
            start_time=time.time(),
            tags=tags if tags is not None else {},
        )
        with self._lock:
            self._spans.append(span)
        return span

    def finish_span(self, span: Span) -> None:
        """Mark *span* as finished by setting its ``end_time``.

        Args:
            span: The :class:`Span` to finish.
        """
        span.end_time = time.time()

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def get_trace(self, trace_id: str) -> list[Span]:
        """Return all spans belonging to *trace_id*, ordered by start_time.

        Args:
            trace_id: Trace group identifier to filter by.

        Returns:
            List of :class:`Span` objects for the trace, sorted by
            ``start_time`` ascending; may be empty.
        """
        with self._lock:
            matching = [s for s in self._spans if s.trace_id == trace_id]
        return sorted(matching, key=lambda s: s.start_time)

    def completed_spans(self) -> list[Span]:
        """Return all spans that have been finished (``end_time`` is set).

        Returns:
            List of completed :class:`Span` objects.
        """
        with self._lock:
            return [s for s in self._spans if s.end_time is not None]

    def clear(self) -> None:
        """Remove all spans from internal storage."""
        with self._lock:
            self._spans.clear()


# ===========================================================================
# Pattern 4 — HealthCheck
# ===========================================================================


@dataclass
class CheckResult:
    """Result of executing a single health check.

    Attributes:
        name:       Registered name of the check.
        healthy:    ``True`` if the check passed, ``False`` otherwise.
        message:    Human-readable status message.
        checked_at: Wall-clock timestamp when the check was executed.
    """

    name: str
    healthy: bool
    message: str
    checked_at: float = field(default_factory=time.time)


class HealthCheck:
    """Periodic health check registry.

    Caller registers named callable checks; the registry executes them on
    demand and aggregates results into a summary suitable for a
    ``/healthz`` endpoint.

    Example::

        hc = HealthCheck()
        hc.register("db", lambda: True, description="Database connectivity")
        hc.register("cache", lambda: False, description="Redis cache")
        print(hc.summary())
        # {'healthy': False, 'checks': {'db': {...}, 'cache': {...}}}
    """

    def __init__(self) -> None:
        # name → (check_fn, description)
        self._checks: dict[str, tuple] = {}

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register(
        self,
        name: str,
        check_fn: Callable[[], bool],
        description: str = "",
    ) -> None:
        """Register a named health check function.

        Args:
            name:        Unique name for this check.
            check_fn:    Zero-argument callable returning ``bool``.
            description: Optional human-readable description.
        """
        self._checks[name] = (check_fn, description)

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def run(self, name: str) -> CheckResult:
        """Execute the check named *name* and return a :class:`CheckResult`.

        If ``check_fn`` raises an exception the check is treated as
        unhealthy and the exception message is captured.

        Args:
            name: Registered check name.

        Returns:
            :class:`CheckResult` with outcome and timestamp.

        Raises:
            KeyError: If no check with *name* has been registered.
        """
        if name not in self._checks:
            raise KeyError(f"No health check registered with name {name!r}")
        check_fn, _description = self._checks[name]
        try:
            healthy = bool(check_fn())
            message = "OK" if healthy else "FAIL"
        except Exception as exc:  # noqa: BLE001
            healthy = False
            message = f"Exception: {exc}"
        return CheckResult(name=name, healthy=healthy, message=message)

    def run_all(self) -> dict[str, CheckResult]:
        """Execute all registered checks and return results keyed by name.

        Returns:
            Dict mapping check name → :class:`CheckResult`.
        """
        return {name: self.run(name) for name in self._checks}

    # ------------------------------------------------------------------
    # Aggregation
    # ------------------------------------------------------------------

    def is_healthy(self) -> bool:
        """Return ``True`` only if ALL registered checks pass.

        Returns:
            ``True`` when every check is healthy; ``False`` if any fail.
            ``True`` when no checks are registered (vacuously healthy).
        """
        return all(result.healthy for result in self.run_all().values())

    def summary(self) -> dict:
        """Return an aggregated health summary.

        Returns:
            Dict with keys ``"healthy"`` (bool) and ``"checks"`` (dict
            mapping name → ``{"healthy": bool, "message": str}``).
        """
        results = self.run_all()
        checks_summary = {name: {"healthy": r.healthy, "message": r.message} for name, r in results.items()}
        overall = all(r.healthy for r in results.values())
        return {"healthy": overall, "checks": checks_summary}


# ===========================================================================
# Pattern 5 — AlertRule / AlertManager
# ===========================================================================


@dataclass
class Alert:
    """A fired alert instance.

    Attributes:
        rule_name:     Name of the :class:`AlertRule` that triggered.
        metric_name:   Name of the metric that was evaluated.
        current_value: The metric value at the time of firing.
        threshold:     The configured threshold value.
        condition:     The condition string (``"gt"``, ``"lt"``, etc.).
        fired_at:      Wall-clock timestamp when the alert fired.
    """

    rule_name: str
    metric_name: str
    current_value: float
    threshold: float
    condition: str
    fired_at: float = field(default_factory=time.time)


class AlertRule:
    """Threshold-based alert rule evaluated against a single metric.

    Supported *condition* values:

    * ``"gt"``  — fires when ``value > threshold``
    * ``"lt"``  — fires when ``value < threshold``
    * ``"gte"`` — fires when ``value >= threshold``
    * ``"lte"`` — fires when ``value <= threshold``
    * ``"eq"``  — fires when ``value == threshold``

    Example::

        rule = AlertRule("high-latency", "request.latency_ms", 500.0, "gt")
        alert = rule.evaluate(750.0)
        assert alert is not None
        assert alert.rule_name == "high-latency"
    """

    _CONDITIONS = frozenset({"gt", "lt", "gte", "lte", "eq"})

    def __init__(
        self,
        name: str,
        metric: str,
        threshold: float,
        condition: str,
    ) -> None:
        """Initialise an :class:`AlertRule`.

        Args:
            name:      Human-readable rule name.
            metric:    Metric name this rule monitors.
            threshold: Threshold value to compare against.
            condition: Comparison operator: ``"gt"``, ``"lt"``, ``"gte"``,
                       ``"lte"``, or ``"eq"``.

        Raises:
            ValueError: If *condition* is not a recognised operator.
        """
        if condition not in self._CONDITIONS:
            raise ValueError(f"Unknown condition {condition!r}; must be one of {sorted(self._CONDITIONS)}")
        self.name = name
        self.metric = metric
        self.threshold = threshold
        self.condition = condition

    # ------------------------------------------------------------------
    # Evaluation
    # ------------------------------------------------------------------

    def evaluate(self, value: float) -> Alert | None:
        """Evaluate *value* against this rule's threshold and condition.

        Args:
            value: Current metric value to test.

        Returns:
            An :class:`Alert` if the condition is triggered, ``None``
            otherwise.
        """
        triggered = False
        if self.condition == "gt":
            triggered = value > self.threshold
        elif self.condition == "lt":
            triggered = value < self.threshold
        elif self.condition == "gte":
            triggered = value >= self.threshold
        elif self.condition == "lte":
            triggered = value <= self.threshold
        elif self.condition == "eq":
            triggered = value == self.threshold

        if not triggered:
            return None
        return Alert(
            rule_name=self.name,
            metric_name=self.metric,
            current_value=value,
            threshold=self.threshold,
            condition=self.condition,
        )


class AlertManager:
    """Manages a collection of :class:`AlertRule` objects and evaluates them.

    Example::

        manager = AlertManager()
        manager.add_rule(AlertRule("cpu-high", "cpu.usage_pct", 90.0, "gt"))
        manager.add_rule(AlertRule("mem-low", "mem.free_mb", 256.0, "lt"))
        alerts = manager.evaluate_all({"cpu.usage_pct": 95.0, "mem.free_mb": 512.0})
        print(len(alerts))       # 1  (only cpu-high fired)
        print(manager.fired_count)  # 1
    """

    def __init__(self) -> None:
        self._rules: list[AlertRule] = []
        self._fired_count: int = 0

    # ------------------------------------------------------------------
    # Rule management
    # ------------------------------------------------------------------

    def add_rule(self, rule: AlertRule) -> None:
        """Add *rule* to the manager.

        Args:
            rule: The :class:`AlertRule` to add.
        """
        self._rules.append(rule)

    # ------------------------------------------------------------------
    # Evaluation
    # ------------------------------------------------------------------

    def evaluate_all(self, metrics: dict[str, float]) -> list[Alert]:
        """Evaluate all rules against *metrics* and return fired alerts.

        Rules whose ``metric`` key is absent from *metrics* are skipped
        silently (treated as no value available).

        Args:
            metrics: Dict mapping metric name → current value.

        Returns:
            List of :class:`Alert` objects for rules that fired.
        """
        fired: list[Alert] = []
        for rule in self._rules:
            if rule.metric not in metrics:
                continue
            alert = rule.evaluate(metrics[rule.metric])
            if alert is not None:
                fired.append(alert)
        self._fired_count += len(fired)
        return fired

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def fired_count(self) -> int:
        """Total number of alerts fired across all :meth:`evaluate_all` calls."""
        return self._fired_count


# ===========================================================================
# Smoke-test helpers (used only in __main__)
# ===========================================================================


def _separator(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def _run_metric_registry_demo() -> None:
    """Scenario 1: Increment counters, set gauges, snapshot."""
    _separator("Scenario 1 — MetricRegistry: counters + gauges + snapshot")

    registry = MetricRegistry()

    # Counter: request counts
    registry.increment("requests.total")
    registry.increment("requests.total")
    registry.increment("requests.total", value=3.0, tags={"method": "GET"})
    print(f"requests.total after increments: {registry.get('requests.total')}")
    assert registry.get("requests.total") == 5.0

    # Gauge: current latency
    registry.gauge("latency.p99_ms", 142.7)
    print(f"latency.p99_ms gauge: {registry.get('latency.p99_ms')}")
    assert registry.get("latency.p99_ms") == 142.7

    # Unknown metric defaults to 0.0
    assert registry.get("never.set") == 0.0
    print("unknown metric → 0.0  ✓")

    # Snapshot is a copy
    snap = registry.snapshot()
    snap["requests.total"] = 999.0
    assert registry.get("requests.total") == 5.0
    print("snapshot is independent copy  ✓")

    # Reset
    registry.reset("requests.total")
    assert registry.get("requests.total") == 0.0
    print("reset to 0.0  ✓")

    print("PASS")


def _run_histogram_demo() -> None:
    """Scenario 2: Observe latency values, compute percentiles and buckets."""
    _separator("Scenario 2 — Histogram: latency distribution + percentiles")

    hist = Histogram(boundaries=[1, 5, 10, 50, 100, 500])
    latencies = [0.5, 1.0, 2.0, 4.5, 8.0, 11.0, 30.0, 60.0, 200.0, 450.0]
    for v in latencies:
        hist.observe(v)

    print(f"count : {hist.count()}")
    print(f"sum   : {hist.sum()}")
    print(f"mean  : {hist.mean():.2f}")
    assert hist.count() == 10
    assert hist.sum() == sum(latencies)

    p50 = hist.percentile(50)
    p95 = hist.percentile(95)
    print(f"p50   : {p50:.2f} ms")
    print(f"p95   : {p95:.2f} ms")
    assert p50 > 0
    assert p95 > p50

    buckets = hist.buckets()
    print("bucket counts (cumulative):")
    for bound, cnt in sorted(buckets.items()):
        print(f"  ≤ {bound:>5} : {cnt}")

    # Cumulative: ≤1 → 2 (0.5 and 1.0), ≤5 → 4 (+ 2.0, 4.5)
    assert buckets[1] == 2
    assert buckets[5] == 4
    print("PASS")


def _run_span_tracer_demo() -> None:
    """Scenario 3: Start and finish 3 spans in the same trace."""
    _separator("Scenario 3 — SpanTracer: 3 spans in one trace")

    tracer = SpanTracer()
    trace_id = str(uuid.uuid4())

    span_names = ["http.request", "db.query", "cache.lookup"]
    spans = []
    for name in span_names:
        s = tracer.start_span(name, trace_id=trace_id, tags={"service": "api"})
        time.sleep(0.001)  # small delay so durations > 0
        tracer.finish_span(s)
        spans.append(s)
        print(f"  {name}: {s.duration_ms:.3f} ms")
        assert s.duration_ms is not None and s.duration_ms >= 0

    # All spans in same trace
    trace_spans = tracer.get_trace(trace_id)
    assert len(trace_spans) == 3
    print(f"Spans retrieved for trace: {len(trace_spans)}")

    # All completed
    completed = tracer.completed_spans()
    assert len(completed) == 3
    print(f"Completed spans: {len(completed)}")

    print("PASS")


def _run_health_check_demo() -> None:
    """Scenario 4: Register 2 checks (one pass, one fail), run_all, summary."""
    _separator("Scenario 4 — HealthCheck: register + run_all + summary")

    hc = HealthCheck()
    hc.register("database", lambda: True, description="PostgreSQL connectivity")
    hc.register("cache", lambda: False, description="Redis connectivity")

    results = hc.run_all()
    print(f"database healthy: {results['database'].healthy}")
    print(f"cache    healthy: {results['cache'].healthy}")
    assert results["database"].healthy is True
    assert results["cache"].healthy is False

    assert hc.is_healthy() is False
    print("is_healthy() = False (cache failed)  ✓")

    summ = hc.summary()
    print(f"summary overall healthy: {summ['healthy']}")
    assert summ["healthy"] is False
    assert "database" in summ["checks"]
    assert "cache" in summ["checks"]
    print("PASS")


def _run_alert_manager_demo() -> None:
    """Scenario 5: AlertManager fires exactly one of two rules."""
    _separator("Scenario 5 — AlertManager: threshold alerting")

    manager = AlertManager()
    manager.add_rule(AlertRule("high-cpu", "cpu.usage_pct", 80.0, "gt"))
    manager.add_rule(AlertRule("low-disk", "disk.free_gb", 10.0, "lt"))

    # cpu = 95 (> 80 → fires), disk = 20 (not < 10 → no fire)
    metrics = {"cpu.usage_pct": 95.0, "disk.free_gb": 20.0}
    alerts = manager.evaluate_all(metrics)

    print(f"Alerts fired: {len(alerts)}")
    assert len(alerts) == 1
    assert alerts[0].rule_name == "high-cpu"
    assert alerts[0].current_value == 95.0
    print(
        f"  Rule: {alerts[0].rule_name}  value={alerts[0].current_value}  "
        f"threshold={alerts[0].threshold}  condition={alerts[0].condition}"
    )
    print(f"fired_count property: {manager.fired_count}")
    assert manager.fired_count == 1
    print("PASS")


# ===========================================================================
# Entry point
# ===========================================================================


if __name__ == "__main__":
    print("Observability Patterns — Smoke Test Suite")
    print("Patterns: MetricRegistry | Histogram | SpanTracer | HealthCheck | AlertRule / AlertManager")

    _run_metric_registry_demo()
    _run_histogram_demo()
    _run_span_tracer_demo()
    _run_health_check_demo()
    _run_alert_manager_demo()

    print("\n" + "=" * 60)
    print("  All scenarios passed.")
    print("=" * 60)
