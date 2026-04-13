"""
24_distributed_tracing_patterns.py — Distributed tracing patterns for
propagating trace context across microservice boundaries, collecting spans,
sampling traces, and exporting telemetry data.

Demonstrates seven complementary patterns that together implement a production-
grade distributed tracing pipeline compatible with the OpenTelemetry (OTel)
specification and the W3C Trace Context recommendation:

    Pattern 1 — TraceContext (W3C Trace Context propagation):
                Carries the four fields mandated by the W3C Trace Context Level-1
                specification (https://www.w3.org/TR/trace-context/): a 128-bit
                trace_id identifying the entire distributed trace, a 64-bit
                parent_id (span_id of the sending span), an 8-bit trace_flags
                bitmask where bit 0 is the "sampled" flag, and an optional
                tracestate string for vendor-specific metadata.  Serialises to
                and from the canonical traceparent HTTP header format
                "00-{trace_id}-{parent_id}-{flags:02x}".  This is the primary
                carrier format used by OpenTelemetry, Jaeger, Zipkin B3 (v2),
                AWS X-Ray (with propagator shim), and Google Cloud Trace.

    Pattern 2 — Span (unit of work in a trace):
                Records a single named unit of work within a distributed trace:
                its unique span_id, the trace context it belongs to, the
                operation name and service name, start/end times, a status
                (OK / ERROR / UNSET), a flat key-value attribute dictionary, and
                an append-only events log.  Serialises to an OTLP-compatible dict
                that maps directly to the OpenTelemetry Protocol (OTLP) proto
                schema used by the OTel Collector, Jaeger OTLP receiver, and
                Grafana Tempo.  Modelling spans as dataclasses with an explicit
                finish() lifecycle matches the Span API defined in the OTel
                specification and mirrors the implementation in opentelemetry-sdk
                (Python), go.opentelemetry.io/otel, and opentelemetry-java.

    Pattern 3 — Tracer (span factory and propagation helper):
                Creates spans associated with a named service, injects W3C
                traceparent headers into outgoing HTTP request dictionaries, and
                extracts TraceContext from incoming HTTP headers so that child
                services can attach their spans to the same trace.  The inject /
                extract API is identical to the OpenTelemetry Propagator API
                (opentelemetry.propagators.composite) and the Jaeger HTTP Headers
                propagator.  Tracers also register finished spans with their
                parent TracerProvider so that spans can be batched and exported
                asynchronously.

    Pattern 4 — Sampler (head-based trace sampling):
                Three sampling policies following the OpenTelemetry Sampler
                specification: AlwaysSample (always record), NeverSample (never
                record), and RatioSampler (consistent probability sampling using
                the deterministic formula int(trace_id[:8], 16) % 10000 <
                ratio * 10000 so that all spans within a trace share the same
                sampling decision).  The ratio sampler corresponds to
                opentelemetry.sdk.trace.sampling.TraceIdRatioBased sampler in
                the OTel Python SDK, the parentbased_traceidratio sampler in the
                OTel Collector, and Jaeger's probabilistic sampler.

    Pattern 5 — SpanExporter and InMemorySpanExporter:
                The SpanExporter Protocol defines the single export(spans) method
                that all exporters must implement.  InMemorySpanExporter is the
                testing-friendly implementation that accumulates finished spans
                in a list; it is the direct equivalent of
                opentelemetry.sdk.trace.export.in_memory_span_exporter
                .InMemorySpanExporter in the OTel Python SDK and is the standard
                tool for asserting on spans in unit tests without a running
                backend.  Production exporters would target OTLP gRPC/HTTP (OTel
                Collector), Jaeger Thrift, Zipkin JSON, AWS X-Ray, or Google
                Cloud Trace.

    Pattern 6 — TracerProvider (central span registry and exporter driver):
                Acts as the root factory for Tracers and the sink for finished
                spans.  Maintains an internal buffer of completed spans and
                flushes them to the configured SpanExporter on force_flush() or
                shutdown(), mirroring the BatchSpanProcessor in the OTel Python
                SDK (opentelemetry.sdk.trace.export.BatchSpanProcessor) and the
                SimpleSpanProcessor used in testing.  The provider is the central
                configuration point for the entire tracing pipeline, matching
                the TracerProvider API defined in opentelemetry-api and the
                SdkTracerProvider in opentelemetry-sdk.

    Pattern 7 — Baggage (W3C Baggage propagation):
                Carries arbitrary key-value metadata alongside the trace context
                through the distributed system, serialised to and parsed from
                the W3C Baggage HTTP header format "key1=val1,key2=val2".
                Baggage values are read by downstream services to make routing,
                sampling, or business-logic decisions without querying an
                upstream service.  This is the W3C Baggage specification
                (https://www.w3.org/TR/baggage/) implemented in the OTel Python
                SDK as opentelemetry.baggage and in the OTel Collector's baggage
                propagator.  The immutable set() pattern (returns a new Baggage
                rather than mutating in place) prevents accidental cross-request
                sharing of baggage state in concurrent servers.

Relationship to commercial and open-source tracing platforms:

    - OpenTelemetry SDK (Python) : TraceContext ≈ SpanContext; Tracer ≈ Tracer;
                                   TracerProvider ≈ SdkTracerProvider;
                                   InMemorySpanExporter ≈ InMemorySpanExporter;
                                   Baggage ≈ opentelemetry.baggage API.
    - Jaeger                     : Span.to_dict() keys match Jaeger OTLP JSON;
                                   Tracer.inject/extract ≈ Jaeger HTTP Headers
                                   propagator; RatioSampler ≈ probabilistic
                                   sampler (param: 0.0–1.0).
    - Zipkin                     : traceparent ≈ B3 single-header format
                                   (different encoding, same semantics);
                                   InMemorySpanExporter ≈ Zipkin in-process
                                   reporter used in tests.
    - AWS X-Ray                  : TraceContext.trace_id ≈ X-Ray trace ID
                                   (different format but same purpose);
                                   Tracer.inject ≈ X-Ray propagation via
                                   X-Amzn-Trace-Id header.
    - Google Cloud Trace         : TracerProvider + OTLP exporter ≈ Cloud Trace
                                   OTel exporter (google-cloud-trace-exporter);
                                   Baggage ≈ Cloud Trace HTTP header
                                   propagation for custom attributes.
    - Grafana Tempo              : Span.to_dict() OTLP dict ≈ Tempo OTLP HTTP
                                   receiver payload; TracerProvider.force_flush
                                   ≈ BatchSpanProcessor flush before shutdown.

No external dependencies required.
"""

from __future__ import annotations

import secrets
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Protocol, runtime_checkable


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class InvalidTraceparent(ValueError):
    """Raised when a traceparent header cannot be parsed."""


# ---------------------------------------------------------------------------
# Pattern 1 — TraceContext (W3C Trace Context propagation)
# ---------------------------------------------------------------------------


@dataclass
class TraceContext:
    """Carries the four W3C Trace Context fields across service boundaries.

    The traceparent header format is::

        00-{trace_id}-{parent_id}-{flags:02x}

    where ``trace_id`` is a 32-character lowercase hex string (128 bits),
    ``parent_id`` is a 16-character lowercase hex string (64 bits), and
    ``flags`` is a zero-padded two-character hex byte.

    Attributes:
        trace_id:    128-bit trace identifier as a 32-char lowercase hex string.
        parent_id:   64-bit span (parent) identifier as a 16-char lowercase hex.
        trace_flags: 8-bit flags bitmask.  Bit 0 (0x01) is the "sampled" flag.
        trace_state: Vendor-specific key=value pairs (W3C tracestate header).
    """

    trace_id: str
    parent_id: str
    trace_flags: int
    trace_state: str = ""

    # ------------------------------------------------------------------
    # Factory helpers
    # ------------------------------------------------------------------

    @classmethod
    def new_root(cls) -> "TraceContext":
        """Create a new root span context, generating fresh random IDs.

        Returns:
            A TraceContext with a new 128-bit trace_id, a new 64-bit parent_id,
            and the sampled flag (0x01) set by default.
        """
        return cls(
            trace_id=secrets.token_hex(16),   # 32-char hex = 128 bits
            parent_id=secrets.token_hex(8),   # 16-char hex = 64 bits
            trace_flags=0x01,
        )

    def child(self) -> "TraceContext":
        """Create a child span context inheriting this trace's trace_id.

        Returns:
            A new TraceContext with the same trace_id and trace_flags, but a
            freshly generated parent_id representing the new child span.
        """
        return TraceContext(
            trace_id=self.trace_id,
            parent_id=secrets.token_hex(8),
            trace_flags=self.trace_flags,
            trace_state=self.trace_state,
        )

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def to_traceparent(self) -> str:
        """Serialise to the W3C traceparent header string.

        Returns:
            A string in the form ``00-{trace_id}-{parent_id}-{flags:02x}``.
        """
        return f"00-{self.trace_id}-{self.parent_id}-{self.trace_flags:02x}"

    @classmethod
    def from_traceparent(cls, header: str) -> "TraceContext":
        """Parse a W3C traceparent header.

        Args:
            header: String in the form ``version-trace_id-parent_id-flags``.

        Returns:
            A TraceContext populated from the header fields.

        Raises:
            InvalidTraceparent: If the header does not conform to the spec.
        """
        parts = header.strip().split("-")
        if len(parts) != 4:
            raise InvalidTraceparent(
                f"traceparent must have 4 dash-separated fields, got: {header!r}"
            )
        version, trace_id, parent_id, flags_str = parts
        if len(trace_id) != 32:
            raise InvalidTraceparent(
                f"trace_id must be 32 hex chars, got {len(trace_id)}: {trace_id!r}"
            )
        if len(parent_id) != 16:
            raise InvalidTraceparent(
                f"parent_id must be 16 hex chars, got {len(parent_id)}: {parent_id!r}"
            )
        if len(flags_str) != 2:
            raise InvalidTraceparent(
                f"flags must be 2 hex chars, got {len(flags_str)}: {flags_str!r}"
            )
        try:
            trace_flags = int(flags_str, 16)
        except ValueError as exc:
            raise InvalidTraceparent(f"flags not valid hex: {flags_str!r}") from exc
        # Validate hex chars in IDs
        try:
            int(trace_id, 16)
            int(parent_id, 16)
        except ValueError as exc:
            raise InvalidTraceparent(f"trace_id/parent_id not valid hex") from exc
        return cls(
            trace_id=trace_id,
            parent_id=parent_id,
            trace_flags=trace_flags,
        )

    # ------------------------------------------------------------------
    # Sampling
    # ------------------------------------------------------------------

    def is_sampled(self) -> bool:
        """Return True if the sampled bit (0x01) is set in trace_flags."""
        return bool(self.trace_flags & 0x01)


# ---------------------------------------------------------------------------
# Pattern 2 — Span (unit of work)
# ---------------------------------------------------------------------------


class SpanStatus(Enum):
    """Lifecycle status of a span, aligned with the OTel StatusCode enum."""

    OK = "OK"
    ERROR = "ERROR"
    UNSET = "UNSET"


@dataclass
class Span:
    """Records a single named operation within a distributed trace.

    Attributes:
        span_id:        64-bit unique identifier for this span (16-char hex).
        trace_context:  The W3C TraceContext this span belongs to.
        operation_name: Human-readable name for the operation (e.g. ``GET /users``).
        service_name:   Name of the service that produced this span.
        start_time:     UTC datetime when the span started.
        end_time:       UTC datetime when the span finished, or None if active.
        status:         SpanStatus.OK / ERROR / UNSET.
        attributes:     Arbitrary key-value metadata (OTel Attributes).
        events:         Ordered log of timestamped events within the span.
    """

    span_id: str
    trace_context: TraceContext
    operation_name: str
    service_name: str
    start_time: datetime
    end_time: datetime | None = None
    status: SpanStatus = SpanStatus.UNSET
    attributes: dict = field(default_factory=dict)
    events: list[dict] = field(default_factory=list)

    # ------------------------------------------------------------------
    # Mutation helpers
    # ------------------------------------------------------------------

    def add_event(self, name: str, attributes: dict | None = None) -> None:
        """Append a timestamped event to the span's event log.

        Args:
            name:       Short descriptive name for the event.
            attributes: Optional key-value metadata attached to the event.
        """
        self.events.append(
            {
                "name": name,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "attributes": attributes or {},
            }
        )

    def set_attribute(self, key: str, value) -> None:
        """Store a key-value attribute on the span.

        Args:
            key:   Attribute name.
            value: Attribute value (any JSON-serialisable type).
        """
        self.attributes[key] = value

    def finish(self, status: SpanStatus = SpanStatus.OK) -> None:
        """Mark the span as finished.

        Sets ``end_time`` to the current UTC time and stores the final status.

        Args:
            status: The outcome of the operation (default: SpanStatus.OK).
        """
        self.end_time = datetime.now(timezone.utc)
        self.status = status

    # ------------------------------------------------------------------
    # Computed properties
    # ------------------------------------------------------------------

    def duration_ms(self) -> float | None:
        """Return the span duration in milliseconds, or None if not finished.

        Returns:
            Duration as a float (milliseconds), or None if end_time is unset.
        """
        if self.end_time is None:
            return None
        delta = self.end_time - self.start_time
        return delta.total_seconds() * 1000.0

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def to_dict(self) -> dict:
        """Serialise the span to an OTLP-compatible dictionary.

        The returned structure maps directly to the OpenTelemetry Protocol
        (OTLP) span JSON schema:

        - ``traceId`` / ``spanId`` — hex strings matching OTel encoding.
        - ``name`` — operation name.
        - ``startTimeUnixNano`` / ``endTimeUnixNano`` — nanosecond epoch ints.
        - ``status`` — dict with ``code`` key.
        - ``attributes`` — list of ``{key, value}`` dicts.

        Returns:
            Dictionary with OTLP-compatible field names and types.
        """
        def _to_nano(dt: datetime | None) -> int | None:
            if dt is None:
                return None
            return int(dt.timestamp() * 1e9)

        return {
            "traceId": self.trace_context.trace_id,
            "spanId": self.span_id,
            "parentSpanId": self.trace_context.parent_id,
            "name": self.operation_name,
            "serviceName": self.service_name,
            "startTimeUnixNano": _to_nano(self.start_time),
            "endTimeUnixNano": _to_nano(self.end_time),
            "status": {"code": self.status.value},
            "attributes": [{"key": k, "value": v} for k, v in self.attributes.items()],
            "events": self.events,
            "traceFlags": self.trace_context.trace_flags,
        }


# ---------------------------------------------------------------------------
# Pattern 4 — Sampler (head-based sampling policies)
# ---------------------------------------------------------------------------


@runtime_checkable
class Sampler(Protocol):
    """Protocol that all sampler implementations must satisfy."""

    def should_sample(self, trace_id: str) -> bool:
        """Return True if this trace should be recorded and exported.

        Args:
            trace_id: The 32-char hex trace identifier.

        Returns:
            True to record the trace, False to drop it.
        """
        ...


class AlwaysSample:
    """Sampler that records every trace — useful in development."""

    def should_sample(self, trace_id: str) -> bool:
        return True


class NeverSample:
    """Sampler that drops every trace — useful for disabling tracing."""

    def should_sample(self, trace_id: str) -> bool:
        return False


class RatioSampler:
    """Probability sampler that deterministically samples ``ratio`` of traces.

    Sampling is consistent: all spans within a trace share the same decision
    because the decision is derived purely from the trace_id.  The formula::

        int(trace_id[:8], 16) % 10000 < ratio * 10000

    maps the first 8 hex characters of the trace_id to a uniform integer in
    [0, 65535] and then uses a modulo to project onto [0, 9999], giving a
    distribution close enough to uniform for practical sampling purposes.

    Args:
        ratio: Fraction of traces to sample, in the range [0.0, 1.0].
    """

    def __init__(self, ratio: float) -> None:
        if not 0.0 <= ratio <= 1.0:
            raise ValueError(f"ratio must be between 0.0 and 1.0, got {ratio}")
        self._ratio = ratio
        self._threshold = int(ratio * 10000)

    def should_sample(self, trace_id: str) -> bool:
        """Deterministic sampling based on trace_id prefix.

        Args:
            trace_id: The 32-char hex trace identifier.

        Returns:
            True if the trace falls within the configured sampling ratio.
        """
        bucket = int(trace_id[:8], 16) % 10000
        return bucket < self._threshold


# ---------------------------------------------------------------------------
# Pattern 5 — SpanExporter and InMemorySpanExporter
# ---------------------------------------------------------------------------


@runtime_checkable
class SpanExporter(Protocol):
    """Protocol for span exporter backends."""

    def export(self, spans: list[Span]) -> None:
        """Send a batch of finished spans to the tracing backend.

        Args:
            spans: List of finished Span objects to export.
        """
        ...


class InMemorySpanExporter:
    """Test-friendly exporter that stores spans in an in-process list.

    This is the standard tool for asserting on spans in unit tests without
    a running tracing backend.  It mirrors
    ``opentelemetry.sdk.trace.export.in_memory_span_exporter
    .InMemorySpanExporter`` from the OTel Python SDK.
    """

    def __init__(self) -> None:
        self._spans: list[Span] = []

    def export(self, spans: list[Span]) -> None:
        """Append ``spans`` to the internal buffer.

        Args:
            spans: Finished spans to store.
        """
        self._spans.extend(spans)

    def get_finished_spans(self) -> list[Span]:
        """Return a snapshot of all stored spans.

        Returns:
            List of Span objects in the order they were exported.
        """
        return list(self._spans)

    def clear(self) -> None:
        """Empty the internal span buffer."""
        self._spans.clear()


# ---------------------------------------------------------------------------
# Pattern 3 — Tracer (span factory and propagation helper)
# ---------------------------------------------------------------------------


class Tracer:
    """Creates spans, injects, and extracts W3C trace context.

    Each Tracer is scoped to a single ``service_name`` and optionally uses a
    ``Sampler`` to control whether spans are forwarded to the exporter.
    Finished sampled spans are registered with the parent ``TracerProvider``
    for batched export.

    Args:
        service_name: The name of the service owning this tracer.
        sampler:      Optional sampling policy (default: AlwaysSample).
        provider:     Back-reference to the TracerProvider for span registration.
    """

    def __init__(
        self,
        service_name: str,
        sampler: "Sampler | None" = None,
        provider: "TracerProvider | None" = None,
    ) -> None:
        self._service_name = service_name
        self._sampler: Sampler = sampler if sampler is not None else AlwaysSample()
        self._provider = provider

    # ------------------------------------------------------------------
    # Span lifecycle
    # ------------------------------------------------------------------

    def start_span(
        self,
        operation_name: str,
        parent_context: TraceContext | None = None,
    ) -> Span:
        """Create and start a new span.

        If ``parent_context`` is provided the new span is a child of that
        context (same trace_id, new span_id).  Otherwise a new root trace is
        created.

        The sampling decision is derived from the trace_id: if the sampler
        returns False the span is created but ``trace_flags`` is cleared
        (0x00) to signal "not sampled".

        Args:
            operation_name:  Human-readable name for the operation.
            parent_context:  Optional parent TraceContext for child spans.

        Returns:
            A new Span in UNSET status with start_time set to now (UTC).
        """
        if parent_context is not None:
            ctx = parent_context.child()
        else:
            ctx = TraceContext.new_root()

        # Apply sampling decision
        if not self._sampler.should_sample(ctx.trace_id):
            ctx = TraceContext(
                trace_id=ctx.trace_id,
                parent_id=ctx.parent_id,
                trace_flags=0x00,
            )

        span = Span(
            span_id=secrets.token_hex(8),
            trace_context=ctx,
            operation_name=operation_name,
            service_name=self._service_name,
            start_time=datetime.now(timezone.utc),
        )
        return span

    def finish_span(self, span: Span, status: SpanStatus = SpanStatus.OK) -> None:
        """Finish the span and register it with the provider for export.

        Args:
            span:   The span to finish.
            status: Final status (default OK).
        """
        span.finish(status)
        if self._provider is not None and span.trace_context.is_sampled():
            self._provider._register_span(span)  # noqa: SLF001

    # ------------------------------------------------------------------
    # Propagation helpers
    # ------------------------------------------------------------------

    def inject(self, span: Span, headers: dict) -> dict:
        """Inject the W3C traceparent header into an HTTP headers dict.

        Mutates *and* returns ``headers`` for convenience so callers can write::

            outbound_headers = tracer.inject(span, {})

        Args:
            span:    The active span whose context should be propagated.
            headers: Mutable dict of HTTP headers.

        Returns:
            The same ``headers`` dict with the ``traceparent`` key added/updated.
        """
        headers["traceparent"] = span.trace_context.to_traceparent()
        return headers

    def extract(self, headers: dict) -> TraceContext | None:
        """Extract a TraceContext from an HTTP headers dict.

        Args:
            headers: Incoming HTTP headers dict (case-insensitive key lookup
                     attempted for ``traceparent``).

        Returns:
            A TraceContext if a valid ``traceparent`` header is present,
            otherwise None.
        """
        raw = headers.get("traceparent") or headers.get("Traceparent")
        if raw is None:
            return None
        try:
            return TraceContext.from_traceparent(raw)
        except InvalidTraceparent:
            return None


# ---------------------------------------------------------------------------
# Pattern 6 — TracerProvider (central span registry and exporter driver)
# ---------------------------------------------------------------------------


class TracerProvider:
    """Root factory for Tracers and sink for finished spans.

    Maintains an internal buffer of completed spans and flushes them to the
    configured ``SpanExporter`` on ``force_flush()`` or ``shutdown()``.

    Args:
        exporter: Optional SpanExporter backend.  If None, spans are buffered
                  but never forwarded anywhere.
    """

    def __init__(self, exporter: "SpanExporter | None" = None) -> None:
        self._exporter = exporter
        self._buffer: list[Span] = []

    # ------------------------------------------------------------------
    # Internal registration (called by Tracer.finish_span)
    # ------------------------------------------------------------------

    def _register_span(self, span: Span) -> None:
        self._buffer.append(span)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_tracer(
        self,
        service_name: str,
        sampler: "Sampler | None" = None,
    ) -> Tracer:
        """Return a Tracer scoped to ``service_name``.

        Args:
            service_name: The logical service name.
            sampler:      Optional sampling policy.

        Returns:
            A new Tracer that registers finished spans with this provider.
        """
        return Tracer(service_name=service_name, sampler=sampler, provider=self)

    def force_flush(self) -> None:
        """Immediately export all buffered spans to the exporter.

        Clears the internal buffer after export.
        """
        if self._exporter is not None and self._buffer:
            self._exporter.export(list(self._buffer))
        self._buffer.clear()

    def shutdown(self) -> None:
        """Flush buffered spans and tear down the provider.

        Identical to ``force_flush()`` in this implementation; production
        providers would additionally close exporter connections.
        """
        self.force_flush()


# ---------------------------------------------------------------------------
# Pattern 7 — Baggage (W3C Baggage propagation)
# ---------------------------------------------------------------------------


@dataclass
class Baggage:
    """Immutable key-value metadata that propagates alongside the trace.

    Serialises to and from the W3C Baggage HTTP header format::

        key1=value1,key2=value2

    The immutable ``set()`` pattern returns a *new* Baggage instance rather
    than mutating the existing one, preventing accidental sharing of state
    between concurrent requests.

    Attributes:
        items: Mapping of baggage key strings to value strings.
    """

    items: dict[str, str] = field(default_factory=dict)

    def set(self, key: str, value: str) -> "Baggage":
        """Return a new Baggage with ``key`` set to ``value``.

        Args:
            key:   Baggage entry name.
            value: Baggage entry value.

        Returns:
            A new Baggage instance with the updated entry.
        """
        new_items = dict(self.items)
        new_items[key] = value
        return Baggage(items=new_items)

    def get(self, key: str) -> str | None:
        """Return the value for ``key``, or None if not present.

        Args:
            key: Baggage entry name.

        Returns:
            The string value, or None.
        """
        return self.items.get(key)

    def to_header(self) -> str:
        """Serialise to the W3C Baggage header format ``k=v,k=v``.

        Returns:
            A comma-separated string of ``key=value`` pairs.
        """
        return ",".join(f"{k}={v}" for k, v in self.items.items())

    @classmethod
    def from_header(cls, header: str) -> "Baggage":
        """Parse a W3C Baggage header string.

        Args:
            header: Comma-separated ``key=value`` pairs.

        Returns:
            A Baggage instance populated from the header.
        """
        items: dict[str, str] = {}
        if not header.strip():
            return cls(items=items)
        for pair in header.split(","):
            pair = pair.strip()
            if "=" in pair:
                k, _, v = pair.partition("=")
                items[k.strip()] = v.strip()
        return cls(items=items)


# ---------------------------------------------------------------------------
# __main__ — realistic multi-service trace demonstration
# ---------------------------------------------------------------------------


if __name__ == "__main__":
    print("=" * 70)
    print("Distributed Tracing Patterns — multi-service trace demo")
    print("=" * 70)

    # -----------------------------------------------------------------------
    # Set up the provider with an in-memory exporter
    # -----------------------------------------------------------------------
    exporter = InMemorySpanExporter()
    provider = TracerProvider(exporter=exporter)

    # Each service gets its own Tracer, all sharing the same provider
    gateway_tracer = provider.get_tracer("api-gateway")
    auth_tracer = provider.get_tracer("auth-service")
    user_tracer = provider.get_tracer("user-service")

    # -----------------------------------------------------------------------
    # Step 1 — API Gateway creates the root span
    # -----------------------------------------------------------------------
    print("\n[api-gateway] Handling incoming request: GET /api/v1/users/42")
    gateway_span = gateway_tracer.start_span("GET /api/v1/users/42")
    gateway_span.set_attribute("http.method", "GET")
    gateway_span.set_attribute("http.url", "/api/v1/users/42")
    gateway_span.set_attribute("http.flavor", "1.1")
    gateway_span.add_event("request_received")

    print(f"  trace_id  : {gateway_span.trace_context.trace_id}")
    print(f"  span_id   : {gateway_span.span_id}")
    print(f"  sampled   : {gateway_span.trace_context.is_sampled()}")

    # Inject trace context into outbound HTTP headers
    outbound_headers: dict = {}
    gateway_tracer.inject(gateway_span, outbound_headers)
    print(f"  traceparent: {outbound_headers['traceparent']}")

    # Attach baggage for downstream services
    baggage = Baggage().set("user-tenant", "acme-corp").set("request-tier", "premium")
    outbound_headers["baggage"] = baggage.to_header()
    print(f"  baggage   : {outbound_headers['baggage']}")

    # -----------------------------------------------------------------------
    # Step 2 — auth-service extracts context, creates child span, finishes OK
    # -----------------------------------------------------------------------
    print("\n[auth-service] Validating JWT token...")
    auth_context = auth_tracer.extract(outbound_headers)
    auth_span = auth_tracer.start_span("validate_jwt", parent_context=auth_context)
    auth_span.set_attribute("auth.method", "JWT")
    auth_span.set_attribute("auth.algorithm", "RS256")
    auth_span.add_event("token_decoded")

    # Simulate a small amount of work
    time.sleep(0.005)

    auth_span.add_event("token_valid", {"user_id": "user-42"})
    auth_tracer.finish_span(auth_span, SpanStatus.OK)
    print(f"  span_id   : {auth_span.span_id}")
    print(f"  shared trace_id: {auth_span.trace_context.trace_id == gateway_span.trace_context.trace_id}")
    print(f"  duration  : {auth_span.duration_ms():.2f} ms")
    print(f"  status    : {auth_span.status.value}")

    # Build headers for next hop
    auth_headers: dict = {}
    auth_tracer.inject(auth_span, auth_headers)

    # -----------------------------------------------------------------------
    # Step 3 — user-service extracts context, creates child span, finishes ERROR
    # -----------------------------------------------------------------------
    print("\n[user-service] Fetching user profile (will fail — profile locked)...")
    user_context = user_tracer.extract(auth_headers)
    user_span = user_tracer.start_span("GET user_profile", parent_context=user_context)
    user_span.set_attribute("db.system", "postgresql")
    user_span.set_attribute("db.operation", "SELECT")
    user_span.set_attribute("user.id", "user-42")
    user_span.add_event("db_query_started")

    time.sleep(0.003)

    # Simulate an error
    user_span.add_event(
        "db_query_failed",
        {"error": "AccountLockedException", "user_id": "user-42"},
    )
    user_span.set_attribute("error", True)
    user_span.set_attribute("error.message", "User account is locked")
    user_tracer.finish_span(user_span, SpanStatus.ERROR)

    print(f"  span_id   : {user_span.span_id}")
    print(f"  shared trace_id: {user_span.trace_context.trace_id == gateway_span.trace_context.trace_id}")
    print(f"  duration  : {user_span.duration_ms():.2f} ms")
    print(f"  status    : {user_span.status.value}")

    # -----------------------------------------------------------------------
    # Step 4 — API Gateway span finishes (propagates the error)
    # -----------------------------------------------------------------------
    print("\n[api-gateway] Returning 403 Forbidden to client...")
    gateway_span.add_event("sending_response", {"http.status_code": 403})
    gateway_span.set_attribute("http.status_code", 403)
    gateway_tracer.finish_span(gateway_span, SpanStatus.ERROR)
    print(f"  duration  : {gateway_span.duration_ms():.2f} ms")
    print(f"  status    : {gateway_span.status.value}")

    # -----------------------------------------------------------------------
    # Step 5 — Flush provider and print all spans
    # -----------------------------------------------------------------------
    provider.force_flush()
    finished = exporter.get_finished_spans()

    print("\n" + "=" * 70)
    print(f"Exported {len(finished)} spans for trace {finished[0].trace_context.trace_id}")
    print("=" * 70)
    header = f"{'Service':<22} {'Operation':<30} {'ms':>8}  {'Status'}"
    print(header)
    print("-" * 70)
    for span in finished:
        dur = span.duration_ms()
        dur_str = f"{dur:.2f}" if dur is not None else "N/A"
        print(f"{span.service_name:<22} {span.operation_name:<30} {dur_str:>8}  {span.status.value}")

    # -----------------------------------------------------------------------
    # Demonstrate Baggage round-trip
    # -----------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("Baggage round-trip demo")
    print("=" * 70)
    tenant = Baggage.from_header(outbound_headers["baggage"]).get("user-tenant")
    tier = Baggage.from_header(outbound_headers["baggage"]).get("request-tier")
    print(f"  user-tenant  : {tenant}")
    print(f"  request-tier : {tier}")

    # -----------------------------------------------------------------------
    # Demonstrate RatioSampler
    # -----------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("RatioSampler demo (50% sampling)")
    print("=" * 70)
    sampler_50 = RatioSampler(0.5)
    sampled_count = sum(
        1 for _ in range(100)
        if sampler_50.should_sample(secrets.token_hex(16))
    )
    print(f"  Sampled {sampled_count}/100 random traces (expected ~50)")

    print("\nDone.")
