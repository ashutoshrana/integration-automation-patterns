"""
39_distributed_tracing_patterns.py — Distributed Tracing and Observability Patterns

Pure-Python implementation of five distributed tracing and observability primitives
using only the standard library (``dataclasses``, ``threading``, ``time``,
``uuid``, ``collections``, ``typing``).  No external dependencies are required —
no opentelemetry, no jaeger, no zipkin.

    Pattern 1 — TraceContext:
                W3C traceparent-compatible context propagation carrier.  A
                ``TraceContext`` holds the identifiers required by the W3C
                Trace Context specification (trace-id, span-id, parent-span-id,
                and a sampling flag).  :meth:`to_traceparent` serialises the
                context to the canonical ``00-{trace_id}-{span_id}-{flags}``
                header string.  :meth:`from_traceparent` is a class-method
                factory that parses such a header back into a ``TraceContext``.
                :meth:`child_context` creates a new ``TraceContext`` that
                inherits the same ``trace_id`` and promotes the current
                ``span_id`` to ``parent_span_id``, while generating a fresh
                ``span_id`` — exactly the fork semantics required to build a
                parent–child span relationship across service hops.

    Pattern 2 — Span:
                Individual unit of work within a trace.  A ``Span`` records
                its start time at construction, its ``TraceContext``, a
                ``name``, an arbitrary ``attributes`` dict, and an ordered
                list of ``SpanEvent`` occurrences.  :meth:`finish` captures
                the wall-clock end time; :meth:`duration_ms` computes elapsed
                milliseconds.  :meth:`add_event` appends a time-stamped
                ``SpanEvent`` (name + optional attributes) to the span's
                event log.  :meth:`set_status` records the outcome of the
                span ("OK", "ERROR", or "UNSET").  :meth:`to_dict` serialises
                the complete span state to a plain dict suitable for JSON
                export or database storage.

    Pattern 3 — Tracer:
                In-memory span collector and trace assembler.  :meth:`start_span`
                creates a ``Span`` with a fresh ``TraceContext`` or — when a
                *parent_context* is supplied — with a child context derived from
                the parent, preserving the trace-id chain.  :meth:`finish_span`
                records the span's end time and moves it to the completed-span
                store.  :meth:`get_trace` retrieves all completed spans for a
                given ``trace_id`` sorted chronologically by start time.
                :meth:`get_span_count` returns the total number of completed
                spans held in memory.  :meth:`export` returns every completed
                span as a list of dicts.  All state is protected by a
                ``threading.Lock`` for safe concurrent use.

    Pattern 4 — SamplingStrategy:
                Configurable, deterministic trace sampling.  The ``sample_rate``
                (0.0 – 1.0) controls what fraction of traces are kept.
                :meth:`should_sample` uses ``hash(trace_id) % 100`` so the
                sampling decision for a given ``trace_id`` is stable across
                calls and processes.  Three factory class-methods cover the
                most common configurations: :meth:`always_on` (rate = 1.0),
                :meth:`always_off` (rate = 0.0), and :meth:`ratio` (arbitrary
                rate).  This avoids the need for any random state while
                preserving head-based sampling semantics.

    Pattern 5 — TraceExporter:
                Accumulates exported spans and provides filter views.
                :meth:`export` appends a list of span dicts and returns the
                number of spans stored.  :meth:`get_by_service` filters spans
                whose ``attributes["service.name"]`` matches the supplied name.
                :meth:`get_errors` returns spans with ``status == "ERROR"``.
                :meth:`get_slow_spans` returns spans whose ``duration_ms``
                exceeds a configurable threshold.  :meth:`clear` resets the
                internal store — useful between test runs.  All mutation is
                protected by a ``threading.Lock``.

Usage example
-------------
::

    tracer = Tracer()
    strategy = SamplingStrategy.ratio(0.5)
    exporter = TraceExporter()

    root_ctx = TraceContext()
    if strategy.should_sample(root_ctx.trace_id):
        root_span = tracer.start_span("http.request", attributes={"service.name": "api-gateway"})
        root_span.add_event("request.received")

        child_span = tracer.start_span(
            "db.query",
            parent_context=root_span.trace_context,
            attributes={"service.name": "api-gateway", "db.statement": "SELECT ..."},
        )
        child_span.set_status("OK")
        tracer.finish_span(child_span)

        root_span.set_status("OK")
        tracer.finish_span(root_span)

        exported_count = exporter.export(tracer.export())
        slow = exporter.get_slow_spans(threshold_ms=5.0)
"""

from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _new_trace_id() -> str:
    """Return a 32-character lowercase hex trace-id (128-bit UUID, no dashes)."""
    return uuid.uuid4().hex  # 32 hex chars


def _new_span_id() -> str:
    """Return a 16-character lowercase hex span-id (64-bit UUID fragment)."""
    return uuid.uuid4().hex[:16]  # 16 hex chars


# ---------------------------------------------------------------------------
# Pattern 1 — TraceContext
# ---------------------------------------------------------------------------


@dataclass
class TraceContext:
    """W3C traceparent-compatible context propagation carrier.

    Parameters
    ----------
    trace_id:
        128-bit trace identifier as 32 lowercase hex chars.  Auto-generated
        when ``None``.
    span_id:
        64-bit span identifier as 16 lowercase hex chars.  Auto-generated
        when ``None``.
    parent_span_id:
        Optional parent span identifier.  ``None`` for root spans.
    sampling_flag:
        When ``True`` the span is sampled and the trace-flags byte is ``01``;
        when ``False`` the byte is ``00``.
    """

    trace_id: str = field(default_factory=_new_trace_id)
    span_id: str = field(default_factory=_new_span_id)
    parent_span_id: str | None = None
    sampling_flag: bool = True

    def to_traceparent(self) -> str:
        """Serialise to W3C traceparent header format.

        Returns
        -------
        str
            ``"00-{trace_id}-{span_id}-01"`` when sampled, ``"…-00"`` otherwise.
        """
        flags = "01" if self.sampling_flag else "00"
        return f"00-{self.trace_id}-{self.span_id}-{flags}"

    @classmethod
    def from_traceparent(cls, header: str) -> TraceContext:
        """Parse a W3C traceparent header into a ``TraceContext``.

        Parameters
        ----------
        header:
            A string of the form ``"00-<trace_id>-<span_id>-<flags>"``.

        Raises
        ------
        ValueError
            If the header does not have exactly four dash-separated parts or
            the version field is not ``"00"``.
        """
        parts = header.strip().split("-")
        if len(parts) != 4:
            raise ValueError(f"Invalid traceparent header: {header!r}")
        version, trace_id, span_id, flags = parts
        if version != "00":
            raise ValueError(f"Unsupported traceparent version: {version!r}")
        sampling_flag = flags == "01"
        return cls(trace_id=trace_id, span_id=span_id, sampling_flag=sampling_flag)

    def child_context(self) -> TraceContext:
        """Create a child ``TraceContext`` inheriting this trace.

        The child shares ``trace_id``, promotes the current ``span_id`` to
        ``parent_span_id``, and gets a fresh ``span_id``.
        """
        return TraceContext(
            trace_id=self.trace_id,
            span_id=_new_span_id(),
            parent_span_id=self.span_id,
            sampling_flag=self.sampling_flag,
        )


# ---------------------------------------------------------------------------
# Pattern 2 — Span
# ---------------------------------------------------------------------------


@dataclass
class SpanEvent:
    """A time-stamped annotation attached to a :class:`Span`."""

    name: str
    timestamp: float
    attributes: dict[str, Any] = field(default_factory=dict)


class Span:
    """Individual unit of work within a distributed trace.

    Parameters
    ----------
    name:
        Human-readable operation name (e.g. ``"http.request"``).
    trace_context:
        The :class:`TraceContext` that identifies this span's position in the
        trace graph.
    attributes:
        Optional key-value metadata attached at construction time.
    """

    def __init__(
        self,
        name: str,
        trace_context: TraceContext,
        attributes: dict[str, Any] | None = None,
    ) -> None:
        self.name: str = name
        self.trace_context: TraceContext = trace_context
        self.attributes: dict[str, Any] = dict(attributes) if attributes else {}
        self.start_time: float = time.time()
        self.end_time: float | None = None
        self.events: list[SpanEvent] = []
        self.status: str = "UNSET"

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def finish(self) -> None:
        """Record the span's end time (wall-clock ``time.time()``)."""
        self.end_time = time.time()

    # ------------------------------------------------------------------
    # Measurements
    # ------------------------------------------------------------------

    def duration_ms(self) -> float:
        """Return elapsed duration in milliseconds.

        Uses ``end_time`` if the span is finished, otherwise measures from
        ``start_time`` to now — so it is safe to call on an in-flight span.
        """
        end = self.end_time if self.end_time is not None else time.time()
        return (end - self.start_time) * 1000.0

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    def add_event(self, name: str, attributes: dict[str, Any] | None = None) -> None:
        """Append a named, time-stamped event to this span."""
        self.events.append(SpanEvent(name=name, timestamp=time.time(), attributes=attributes or {}))

    def set_status(self, status: str) -> None:
        """Set the outcome status of this span.

        Parameters
        ----------
        status:
            One of ``"OK"``, ``"ERROR"``, or ``"UNSET"``.
        """
        self.status = status

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        """Serialise the span to a plain dict.

        Returns
        -------
        dict
            Keys: ``trace_id``, ``span_id``, ``parent_span_id``, ``name``,
            ``start``, ``end``, ``duration_ms``, ``attributes``, ``events``,
            ``status``.
        """
        return {
            "trace_id": self.trace_context.trace_id,
            "span_id": self.trace_context.span_id,
            "parent_span_id": self.trace_context.parent_span_id,
            "name": self.name,
            "start": self.start_time,
            "end": self.end_time,
            "duration_ms": self.duration_ms(),
            "attributes": dict(self.attributes),
            "events": [
                {
                    "name": e.name,
                    "timestamp": e.timestamp,
                    "attributes": dict(e.attributes),
                }
                for e in self.events
            ],
            "status": self.status,
        }


# ---------------------------------------------------------------------------
# Pattern 3 — Tracer
# ---------------------------------------------------------------------------


class Tracer:
    """In-memory span collector and trace assembler.

    Spans are stored in two buckets:

    * ``_active_spans`` — spans that have been started but not finished.
    * ``_completed_spans`` — spans moved here by :meth:`finish_span`.

    All public methods are thread-safe via an internal ``threading.Lock``.
    """

    def __init__(self) -> None:
        self._active_spans: list[Span] = []
        self._completed_spans: list[Span] = []
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Span lifecycle
    # ------------------------------------------------------------------

    def start_span(
        self,
        name: str,
        parent_context: TraceContext | None = None,
        attributes: dict[str, Any] | None = None,
    ) -> Span:
        """Create and track a new span.

        Parameters
        ----------
        name:
            Operation name.
        parent_context:
            When supplied a child ``TraceContext`` is derived from it,
            preserving the trace-id chain.  When ``None`` a fresh root
            context is created.
        attributes:
            Key-value metadata for the span.
        """
        ctx = parent_context.child_context() if parent_context is not None else TraceContext()
        span = Span(name=name, trace_context=ctx, attributes=attributes)
        with self._lock:
            self._active_spans.append(span)
        return span

    def finish_span(self, span: Span) -> None:
        """Finish a span and move it to the completed store."""
        span.finish()
        with self._lock:
            try:
                self._active_spans.remove(span)
            except ValueError:
                pass  # span was never in active list (already finished or external)
            self._completed_spans.append(span)

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def get_trace(self, trace_id: str) -> list[dict[str, Any]]:
        """Return all completed spans for *trace_id* sorted by start time.

        Parameters
        ----------
        trace_id:
            The 32-char hex trace identifier.
        """
        with self._lock:
            spans = [s for s in self._completed_spans if s.trace_context.trace_id == trace_id]
        return sorted([s.to_dict() for s in spans], key=lambda d: d["start"])

    def get_span_count(self) -> int:
        """Return the total number of completed spans held in memory."""
        with self._lock:
            return len(self._completed_spans)

    def export(self) -> list[dict[str, Any]]:
        """Return all completed spans as a list of dicts."""
        with self._lock:
            return [s.to_dict() for s in self._completed_spans]


# ---------------------------------------------------------------------------
# Pattern 4 — SamplingStrategy
# ---------------------------------------------------------------------------


class SamplingStrategy:
    """Configurable, deterministic head-based trace sampling.

    Parameters
    ----------
    sample_rate:
        Fraction of traces to keep.  ``1.0`` keeps all; ``0.0`` keeps none.
        Values outside [0.0, 1.0] are clamped silently.
    """

    def __init__(self, sample_rate: float = 1.0) -> None:
        self.sample_rate: float = max(0.0, min(1.0, sample_rate))

    # ------------------------------------------------------------------
    # Sampling decision
    # ------------------------------------------------------------------

    def should_sample(self, trace_id: str) -> bool:
        """Return ``True`` when the trace should be sampled.

        The decision is deterministic: ``hash(trace_id) % 100 < sample_rate * 100``.
        The same ``trace_id`` always returns the same answer.
        """
        bucket = abs(hash(trace_id)) % 100
        return bucket < self.sample_rate * 100

    # ------------------------------------------------------------------
    # Factories
    # ------------------------------------------------------------------

    @classmethod
    def always_on(cls) -> SamplingStrategy:
        """Return a strategy that samples every trace (rate = 1.0)."""
        return cls(sample_rate=1.0)

    @classmethod
    def always_off(cls) -> SamplingStrategy:
        """Return a strategy that samples no traces (rate = 0.0)."""
        return cls(sample_rate=0.0)

    @classmethod
    def ratio(cls, rate: float) -> SamplingStrategy:
        """Return a strategy with an arbitrary sampling *rate*."""
        return cls(sample_rate=rate)


# ---------------------------------------------------------------------------
# Pattern 5 — TraceExporter
# ---------------------------------------------------------------------------


class TraceExporter:
    """Accumulates exported span dicts and provides filter views.

    Spans are accepted as plain dicts (as produced by :meth:`Span.to_dict` or
    :meth:`Tracer.export`) and held in an internal list.  All mutation is
    protected by a ``threading.Lock`` for safe concurrent use.
    """

    def __init__(self) -> None:
        self._spans: list[dict[str, Any]] = []
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Ingest
    # ------------------------------------------------------------------

    def export(self, spans: list[dict[str, Any]]) -> int:
        """Store *spans* and return the number of spans added.

        Parameters
        ----------
        spans:
            List of span dicts (typically from :meth:`Tracer.export`).
        """
        with self._lock:
            self._spans.extend(spans)
            return len(spans)

    # ------------------------------------------------------------------
    # Filters
    # ------------------------------------------------------------------

    def get_by_service(self, service_name: str) -> list[dict[str, Any]]:
        """Return spans whose ``attributes["service.name"]`` equals *service_name*."""
        with self._lock:
            return [s for s in self._spans if s.get("attributes", {}).get("service.name") == service_name]

    def get_errors(self) -> list[dict[str, Any]]:
        """Return spans with ``status == "ERROR"``."""
        with self._lock:
            return [s for s in self._spans if s.get("status") == "ERROR"]

    def get_slow_spans(self, threshold_ms: float) -> list[dict[str, Any]]:
        """Return spans whose ``duration_ms`` exceeds *threshold_ms*."""
        with self._lock:
            return [s for s in self._spans if s.get("duration_ms", 0.0) > threshold_ms]

    # ------------------------------------------------------------------
    # Maintenance
    # ------------------------------------------------------------------

    def clear(self) -> None:
        """Remove all stored spans."""
        with self._lock:
            self._spans.clear()
