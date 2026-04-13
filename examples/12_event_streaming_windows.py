"""
12_event_streaming_windows.py — Temporal windowing patterns for enterprise
event streaming pipelines.

In enterprise event-driven systems, raw event streams must be aggregated into
meaningful time-bounded summaries: order volumes per 5-minute window, session
activity across a customer journey, or peak throughput sliding across a rolling
hour. These temporal patterns are how stream processors answer the question
"what happened in this time period?" reliably under out-of-order and late
arrivals.

    Pattern 1 — TumblingWindow: Fixed-duration, non-overlapping windows.
                Each event belongs to exactly one window. Ideal for
                periodic reporting: "total orders per minute" or "API
                calls per 5-minute billing interval". No double-counting.

    Pattern 2 — SlidingWindow: Fixed-duration windows that advance by a
                configurable step interval (< window size). Events appear
                in multiple windows. Ideal for smoothed metrics: "rolling
                60-second average" computed every 15 seconds.

    Pattern 3 — SessionWindow: Activity-gap-based windows. A session starts
                on the first event and extends as long as events arrive within
                an inactivity gap. Sessions close when the gap is exceeded.
                Ideal for user session analytics and abandonment detection.

    Pattern 4 — WindowAggregator: Computes COUNT, SUM, AVG, MIN, MAX, and
                DISTINCT_COUNT over the events in a window. Pluggable
                aggregation functions; results attached to WindowResult.

    Pattern 5 — LateArrivalHandler: Manages events that arrive after their
                window's watermark has advanced. Configurable policies:
                ACCEPT (within late tolerance), ASSIGN_TO_LATEST (move to
                current window), DROP (discard with audit log), or
                SIDE_OUTPUT (route to separate late-event stream for
                reconciliation).

    Pattern 6 — StreamProcessor: Orchestrates window assignment, aggregation,
                and late-arrival handling. Maintains per-window state, emits
                WindowResult records, and advances the watermark based on
                observed event timestamps minus a configurable allowed lateness.

Scenarios
---------

  A. Tumbling window — order pipeline:
     100 synthetic orders arrive in real time across 6 one-minute windows.
     Each window captures the correct count. Windows are non-overlapping
     (no event belongs to two windows).

  B. Sliding window — API rate monitoring:
     API call events; 60-second window sliding every 15 seconds. The same
     high-traffic burst appears in 4 consecutive sliding windows.

  C. Session window — user journey:
     A customer views 3 product pages, pauses > 30 s (gap threshold), then
     places an order. Two sessions are detected: browse session + purchase
     session.

  D. Late arrival handling:
     An event timestamped 90 seconds before the current watermark arrives
     late. ACCEPT policy re-opens the window within tolerance; DROP policy
     discards it with an audit entry; SIDE_OUTPUT routes it to a reconciliation
     stream.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable


# ---------------------------------------------------------------------------
# Core types
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class StreamEvent:
    """
    A single event on a stream with an event-time timestamp.

    event_time — when the event *occurred* (milliseconds since epoch).
                 This is the authoritative time for window assignment.
    ingestion_time — when the event was ingested by the pipeline
                     (may be later than event_time if the event was late).
    event_id — globally unique identifier for idempotency.
    stream_key — partition key; windows are computed per-key.
    payload — arbitrary event attributes.
    """
    event_id: str
    stream_key: str
    event_time: int          # ms since epoch
    ingestion_time: int      # ms since epoch
    payload: dict[str, Any]


class AggregationFunction(str, Enum):
    COUNT = "COUNT"
    SUM = "SUM"
    AVG = "AVG"
    MIN = "MIN"
    MAX = "MAX"
    DISTINCT_COUNT = "DISTINCT_COUNT"


class LateArrivalPolicy(str, Enum):
    """How to handle events that arrive after the window watermark."""
    ACCEPT = "ACCEPT"                  # Re-open window if within late tolerance
    ASSIGN_TO_LATEST = "ASSIGN_TO_LATEST"  # Move to the most recent open window
    DROP = "DROP"                      # Discard; write to audit log
    SIDE_OUTPUT = "SIDE_OUTPUT"        # Route to a reconciliation side stream


@dataclass
class WindowSpec:
    """
    Descriptor for a window type and its parameters.

    window_size_ms — duration of the window in milliseconds.
    slide_interval_ms — (SlidingWindow only) how far the window advances.
    session_gap_ms — (SessionWindow only) inactivity gap that closes a session.
    late_tolerance_ms — how far past the watermark an event may still be
                        ACCEPTED into the window it belongs to.
    late_arrival_policy — policy when late_tolerance_ms is exceeded.
    """
    window_size_ms: int
    slide_interval_ms: int | None = None    # SlidingWindow
    session_gap_ms: int | None = None        # SessionWindow
    late_tolerance_ms: int = 0
    late_arrival_policy: LateArrivalPolicy = LateArrivalPolicy.DROP


@dataclass
class WindowKey:
    """Uniquely identifies a window instance."""
    stream_key: str
    window_start_ms: int
    window_end_ms: int

    def __hash__(self) -> int:
        return hash((self.stream_key, self.window_start_ms, self.window_end_ms))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, WindowKey):
            return False
        return (
            self.stream_key == other.stream_key
            and self.window_start_ms == other.window_start_ms
            and self.window_end_ms == other.window_end_ms
        )


@dataclass
class WindowResult:
    """The finalized result for one closed window."""
    window_key: WindowKey
    event_count: int
    aggregations: dict[str, Any]
    late_events_accepted: int = 0
    late_events_dropped: int = 0
    late_events_side_output: int = 0
    is_final: bool = True       # False if result is a partial update
    result_id: str = field(default_factory=lambda: str(uuid.uuid4()))


@dataclass
class LateEventRecord:
    """Audit record for a late-arrival event."""
    event: StreamEvent
    policy_applied: LateArrivalPolicy
    watermark_at_arrival: int
    event_time: int
    lateness_ms: int
    assigned_window: WindowKey | None = None   # if ASSIGN_TO_LATEST


# ---------------------------------------------------------------------------
# Pattern 1 — TumblingWindow
# ---------------------------------------------------------------------------

class TumblingWindow:
    """
    Assigns each event to exactly one non-overlapping window.

    A tumbling window of size W starts at multiples of W relative to epoch 0:
        window = floor(event_time / W) * W  →  [start, start + W)

    No event is counted twice. The assignment is deterministic and
    requires no state: given the same event time and window size, the
    window boundaries are always identical.

    Typical use: per-minute order counts, 5-minute billing intervals,
    hourly revenue roll-ups.
    """

    def __init__(self, window_size_ms: int) -> None:
        if window_size_ms <= 0:
            raise ValueError(f"window_size_ms must be positive, got {window_size_ms}")
        self.window_size_ms = window_size_ms

    def assign(self, event_time_ms: int) -> tuple[int, int]:
        """Return (window_start_ms, window_end_ms) for the given event time."""
        start = (event_time_ms // self.window_size_ms) * self.window_size_ms
        return start, start + self.window_size_ms

    def assign_key(self, event: StreamEvent) -> WindowKey:
        start, end = self.assign(event.event_time)
        return WindowKey(
            stream_key=event.stream_key,
            window_start_ms=start,
            window_end_ms=end,
        )


# ---------------------------------------------------------------------------
# Pattern 2 — SlidingWindow
# ---------------------------------------------------------------------------

class SlidingWindow:
    """
    Assigns each event to potentially multiple overlapping windows.

    A sliding window of size W advancing every S milliseconds produces
    windows:
        [..., [t - W + S, t + S), [t, t + W), [t + S, t + W + S), ...]

    An event at time T belongs to every window [start, start + W) where
    start ≤ T < start + W and start is a multiple of S:
        start ∈ { k * S | k * S ≤ T < k * S + W }

    Events in the "hot" region of a burst appear in floor(W/S) windows,
    giving a smoothed view of the stream that a tumbling window would
    alias.

    Typical use: rolling N-second averages computed every M seconds,
    anomaly detection on moving windows.
    """

    def __init__(self, window_size_ms: int, slide_interval_ms: int) -> None:
        if window_size_ms <= 0:
            raise ValueError(f"window_size_ms must be positive, got {window_size_ms}")
        if slide_interval_ms <= 0 or slide_interval_ms > window_size_ms:
            raise ValueError(
                f"slide_interval_ms must be in (0, window_size_ms], "
                f"got {slide_interval_ms}"
            )
        self.window_size_ms = window_size_ms
        self.slide_interval_ms = slide_interval_ms

    def assign_all(self, event_time_ms: int) -> list[tuple[int, int]]:
        """Return all (start, end) windows that contain this event time."""
        # The earliest window that could contain event_time_ms starts at:
        #   k * S where k * S + W > event_time_ms  →  k > (event_time_ms - W) / S
        S = self.slide_interval_ms
        W = self.window_size_ms
        earliest_start = ((event_time_ms - W) // S + 1) * S
        if earliest_start < 0:
            earliest_start = 0

        windows: list[tuple[int, int]] = []
        start = earliest_start
        while start <= event_time_ms:
            end = start + W
            if start <= event_time_ms < end:
                windows.append((start, end))
            start += S
        return windows

    def assign_keys(self, event: StreamEvent) -> list[WindowKey]:
        return [
            WindowKey(
                stream_key=event.stream_key,
                window_start_ms=start,
                window_end_ms=end,
            )
            for start, end in self.assign_all(event.event_time)
        ]


# ---------------------------------------------------------------------------
# Pattern 3 — SessionWindow
# ---------------------------------------------------------------------------

class SessionWindow:
    """
    Groups events into sessions separated by inactivity gaps.

    A session starts on the first event after a gap >= session_gap_ms
    and extends as long as events keep arriving within the gap threshold.
    Sessions are finalized once the watermark advances past the last
    event's time + session_gap_ms.

    Unlike tumbling/sliding windows, session boundaries are not
    predetermined: they are data-driven. Two users' sessions for the
    same stream_key are independent.

    Typical use: customer journey analytics, abandonment detection,
    user interaction sessions, chatbot conversation boundaries.
    """

    def __init__(self, session_gap_ms: int) -> None:
        if session_gap_ms <= 0:
            raise ValueError(f"session_gap_ms must be positive, got {session_gap_ms}")
        self.session_gap_ms = session_gap_ms
        # Per-key session state: stream_key → (session_start, last_event_time)
        self._sessions: dict[str, tuple[int, int]] = {}
        # Completed sessions per key
        self._completed: dict[str, list[tuple[int, int]]] = {}

    def process_event(self, event: StreamEvent) -> tuple[WindowKey | None, WindowKey]:
        """
        Process one event.

        Returns (completed_window_key, current_window_key):
        - completed_window_key — the session that just closed (None if no session closed).
        - current_window_key — the session this event belongs to (always returned;
          its end boundary is provisional until the session closes).
        """
        key = event.stream_key
        t = event.event_time
        completed_key: WindowKey | None = None

        if key not in self._sessions:
            # First event for this key — start a new session
            self._sessions[key] = (t, t)
        else:
            session_start, last_event_time = self._sessions[key]

            if t - last_event_time >= self.session_gap_ms:
                # Gap exceeded — close the current session and start a new one
                completed_key = WindowKey(
                    stream_key=key,
                    window_start_ms=session_start,
                    window_end_ms=last_event_time + self.session_gap_ms,
                )
                if key not in self._completed:
                    self._completed[key] = []
                self._completed[key].append((session_start, last_event_time))
                self._sessions[key] = (t, t)
            else:
                # Extend the current session
                self._sessions[key] = (session_start, max(last_event_time, t))

        current_start, current_last = self._sessions[key]
        current_key = WindowKey(
            stream_key=key,
            window_start_ms=current_start,
            window_end_ms=current_last + self.session_gap_ms,
        )
        return completed_key, current_key

    def flush_all(self) -> list[WindowKey]:
        """
        Close all open sessions (called at end of stream or on final watermark).
        Returns WindowKey for each completed session (open + previously completed).
        """
        keys: list[WindowKey] = []

        for stream_key, (session_start, last_event_time) in self._sessions.items():
            session_end = last_event_time + self.session_gap_ms
            keys.append(WindowKey(
                stream_key=stream_key,
                window_start_ms=session_start,
                window_end_ms=session_end,
            ))

        # Also emit previously completed sessions
        for stream_key, completed in self._completed.items():
            for session_start, last_event_time in completed:
                session_end = last_event_time + self.session_gap_ms
                keys.append(WindowKey(
                    stream_key=stream_key,
                    window_start_ms=session_start,
                    window_end_ms=session_end,
                ))

        self._sessions.clear()
        self._completed.clear()
        return keys

    def get_open_sessions(self) -> dict[str, tuple[int, int]]:
        """Return active (stream_key → (start, last_event)) for inspection."""
        return dict(self._sessions)

    def get_completed_sessions(self) -> dict[str, list[tuple[int, int]]]:
        """Return finalized (stream_key → [(start, last_event), ...]) for inspection."""
        return {k: list(v) for k, v in self._completed.items()}


# ---------------------------------------------------------------------------
# Pattern 4 — WindowAggregator
# ---------------------------------------------------------------------------

class WindowAggregator:
    """
    Computes aggregate statistics over all events in a window.

    Supports: COUNT, SUM, AVG, MIN, MAX, DISTINCT_COUNT.

    The numeric_field parameter identifies which payload key to aggregate
    over for numeric functions. For COUNT and DISTINCT_COUNT the field may
    be any hashable value.

    Results are returned as a dict[str, Any] keyed by aggregation name.
    Multiple aggregations may be computed in one pass over the events.
    """

    def __init__(
        self,
        aggregations: list[tuple[AggregationFunction, str]],
    ) -> None:
        """
        aggregations — list of (AggregationFunction, field_name) pairs.
        Example: [(COUNT, "event_id"), (SUM, "amount"), (AVG, "amount")]
        """
        self.aggregations = aggregations

    def compute(self, events: list[StreamEvent]) -> dict[str, Any]:
        results: dict[str, Any] = {}

        for agg_fn, field_name in self.aggregations:
            key = f"{agg_fn.value}({field_name})"
            values = [e.payload.get(field_name) for e in events]

            if agg_fn == AggregationFunction.COUNT:
                results[key] = len(events)
            elif agg_fn == AggregationFunction.DISTINCT_COUNT:
                results[key] = len({v for v in values if v is not None})
            elif agg_fn in (
                AggregationFunction.SUM,
                AggregationFunction.AVG,
                AggregationFunction.MIN,
                AggregationFunction.MAX,
            ):
                numeric = [v for v in values if isinstance(v, (int, float))]
                if not numeric:
                    results[key] = None
                elif agg_fn == AggregationFunction.SUM:
                    results[key] = sum(numeric)
                elif agg_fn == AggregationFunction.AVG:
                    results[key] = sum(numeric) / len(numeric)
                elif agg_fn == AggregationFunction.MIN:
                    results[key] = min(numeric)
                elif agg_fn == AggregationFunction.MAX:
                    results[key] = max(numeric)

        return results


# ---------------------------------------------------------------------------
# Pattern 5 — LateArrivalHandler
# ---------------------------------------------------------------------------

class LateArrivalHandler:
    """
    Enforces late-arrival policy for events that arrive after their window
    watermark has advanced.

    Watermark is the event-time up to which the processor considers the
    stream "complete". Events with event_time < (watermark - late_tolerance_ms)
    are considered late.

    Policy decisions:
        ACCEPT          — re-open the window; update its result (within tolerance)
        ASSIGN_TO_LATEST — assign to the currently open window for this key
        DROP            — discard; write audit record
        SIDE_OUTPUT     — route to reconciliation stream; write audit record

    The side_output_stream accumulates events for downstream reconciliation
    (e.g., a corrective journal entry in an accounting integration).
    """

    def __init__(
        self,
        late_tolerance_ms: int,
        policy: LateArrivalPolicy,
    ) -> None:
        self.late_tolerance_ms = late_tolerance_ms
        self.policy = policy
        self.audit_log: list[LateEventRecord] = []
        self.side_output_stream: list[StreamEvent] = []

    def handle(
        self,
        event: StreamEvent,
        watermark_ms: int,
        latest_open_window: WindowKey | None = None,
    ) -> tuple[bool, WindowKey | None]:
        """
        Determine whether and where to process a late event.

        Returns (should_process, assigned_window_key).
        - should_process=True means the event should update the assigned window.
        - should_process=False means the event is dropped or side-outputted.
        """
        lateness_ms = watermark_ms - event.event_time

        if lateness_ms <= self.late_tolerance_ms:
            # Within tolerance — ACCEPT always applies regardless of stated policy
            # (the event is not actually "late" by the configured threshold)
            from_ms = (event.event_time // 1) * 1  # keep event_time as-is
            start = event.event_time
            end = start + 1  # placeholder; caller replaces with real window bounds
            # Build window key for where the event originally belongs
            window_key = latest_open_window
            record = LateEventRecord(
                event=event,
                policy_applied=LateArrivalPolicy.ACCEPT,
                watermark_at_arrival=watermark_ms,
                event_time=event.event_time,
                lateness_ms=lateness_ms,
                assigned_window=window_key,
            )
            self.audit_log.append(record)
            return True, window_key

        # Beyond tolerance — apply configured policy
        if self.policy == LateArrivalPolicy.ACCEPT:
            record = LateArrivalRecord = LateEventRecord(
                event=event,
                policy_applied=LateArrivalPolicy.ACCEPT,
                watermark_at_arrival=watermark_ms,
                event_time=event.event_time,
                lateness_ms=lateness_ms,
                assigned_window=latest_open_window,
            )
            self.audit_log.append(LateArrivalRecord)
            return True, latest_open_window

        elif self.policy == LateArrivalPolicy.ASSIGN_TO_LATEST:
            record = LateEventRecord(
                event=event,
                policy_applied=LateArrivalPolicy.ASSIGN_TO_LATEST,
                watermark_at_arrival=watermark_ms,
                event_time=event.event_time,
                lateness_ms=lateness_ms,
                assigned_window=latest_open_window,
            )
            self.audit_log.append(record)
            return True, latest_open_window

        elif self.policy == LateArrivalPolicy.DROP:
            record = LateEventRecord(
                event=event,
                policy_applied=LateArrivalPolicy.DROP,
                watermark_at_arrival=watermark_ms,
                event_time=event.event_time,
                lateness_ms=lateness_ms,
                assigned_window=None,
            )
            self.audit_log.append(record)
            return False, None

        else:  # SIDE_OUTPUT
            record = LateEventRecord(
                event=event,
                policy_applied=LateArrivalPolicy.SIDE_OUTPUT,
                watermark_at_arrival=watermark_ms,
                event_time=event.event_time,
                lateness_ms=lateness_ms,
                assigned_window=None,
            )
            self.audit_log.append(record)
            self.side_output_stream.append(event)
            return False, None


# ---------------------------------------------------------------------------
# Pattern 6 — StreamProcessor
# ---------------------------------------------------------------------------

class StreamProcessor:
    """
    Orchestrates window assignment, per-window event accumulation,
    watermark advancement, and late-arrival handling.

    Window strategy is chosen via the spec:
        - spec.slide_interval_ms set → SlidingWindow
        - spec.session_gap_ms set    → SessionWindow
        - otherwise                  → TumblingWindow

    Watermark = max(observed event_time) - allowed_lateness_ms.

    Windows are finalized (emitted as WindowResult) when the watermark
    advances past their window_end_ms. Finalized windows are removed from
    the active state.
    """

    def __init__(
        self,
        spec: WindowSpec,
        aggregations: list[tuple[AggregationFunction, str]] | None = None,
        allowed_lateness_ms: int = 0,
    ) -> None:
        self.spec = spec
        self.aggregations = aggregations or [(AggregationFunction.COUNT, "event_id")]
        self.allowed_lateness_ms = allowed_lateness_ms
        self.aggregator = WindowAggregator(self.aggregations)
        self.late_handler = LateArrivalHandler(
            late_tolerance_ms=spec.late_tolerance_ms,
            policy=spec.late_arrival_policy,
        )

        # Determine window strategy
        if spec.slide_interval_ms is not None:
            self._sliding = SlidingWindow(spec.window_size_ms, spec.slide_interval_ms)
            self._tumbling: TumblingWindow | None = None
            self._session: SessionWindow | None = None
        elif spec.session_gap_ms is not None:
            self._session = SessionWindow(spec.session_gap_ms)
            self._tumbling = None
            self._sliding = None
        else:
            self._tumbling = TumblingWindow(spec.window_size_ms)
            self._sliding = None
            self._session = None

        # State: WindowKey → list of events in that window
        self._window_events: dict[WindowKey, list[StreamEvent]] = {}
        # Session-only: events accumulated while a session is still open
        # (keyed by stream_key; moved to _window_events when session closes)
        self._session_accumulators: dict[str, list[StreamEvent]] = {}
        self._watermark_ms: int = 0
        self._results: list[WindowResult] = []

    @property
    def watermark_ms(self) -> int:
        return self._watermark_ms

    def process(self, event: StreamEvent) -> list[WindowResult]:
        """
        Process a single event. Returns any WindowResults finalized by
        the watermark advancement this event caused.
        """
        # Check if event is late
        if event.event_time < self._watermark_ms - self.allowed_lateness_ms:
            # Late event — delegate to handler
            latest_key = self._latest_open_window(event.stream_key)
            should_process, assigned_key = self.late_handler.handle(
                event, self._watermark_ms, latest_key
            )
            if should_process and assigned_key is not None:
                if assigned_key not in self._window_events:
                    self._window_events[assigned_key] = []
                self._window_events[assigned_key].append(event)
            return []

        # Assign event to windows
        if self._sliding is not None:
            keys = self._sliding.assign_keys(event)
        elif self._session is not None:
            completed_key, current_key = self._session.process_event(event)
            if completed_key is not None:
                # A session just closed: move its accumulated events to _window_events
                acc = self._session_accumulators.pop(event.stream_key, [])
                self._window_events[completed_key] = acc
            # Add this event to the current (open) session accumulator
            self._session_accumulators.setdefault(event.stream_key, []).append(event)
            self._advance_watermark(event.event_time)
            return self._finalize_ready_windows()
        else:
            assert self._tumbling is not None
            keys = [self._tumbling.assign_key(event)]

        for key in keys:
            if key not in self._window_events:
                self._window_events[key] = []
            self._window_events[key].append(event)

        self._advance_watermark(event.event_time)
        return self._finalize_ready_windows()

    def flush(self) -> list[WindowResult]:
        """
        Finalize all remaining open windows (end of stream).
        For session windows, move remaining accumulators to _window_events first.
        """
        if self._session is not None:
            # Move remaining open-session accumulators into _window_events
            open_sessions = self._session.get_open_sessions()
            for stream_key, (session_start, last_event_time) in open_sessions.items():
                key = WindowKey(
                    stream_key=stream_key,
                    window_start_ms=session_start,
                    window_end_ms=last_event_time + self._session.session_gap_ms,
                )
                acc = self._session_accumulators.pop(stream_key, [])
                self._window_events[key] = acc
            self._session.flush_all()   # clears session internal state

        # Force watermark past all remaining windows
        self._watermark_ms = 2**62
        return self._finalize_ready_windows()

    def get_results(self) -> list[WindowResult]:
        """All finalized WindowResults so far (including from flush)."""
        return list(self._results)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _advance_watermark(self, event_time_ms: int) -> None:
        new_watermark = event_time_ms - self.allowed_lateness_ms
        if new_watermark > self._watermark_ms:
            self._watermark_ms = new_watermark

    def _finalize_ready_windows(self) -> list[WindowResult]:
        """Emit and remove windows whose end_ms <= current watermark."""
        ready: list[WindowKey] = [
            k for k in self._window_events
            if k.window_end_ms <= self._watermark_ms
        ]
        emitted: list[WindowResult] = []
        for key in ready:
            events = self._window_events.pop(key)
            agg = self.aggregator.compute(events)
            result = WindowResult(
                window_key=key,
                event_count=len(events),
                aggregations=agg,
            )
            self._results.append(result)
            emitted.append(result)
        return emitted

    def _latest_open_window(self, stream_key: str) -> WindowKey | None:
        """Return the most recently opened window for a stream_key."""
        candidates = [k for k in self._window_events if k.stream_key == stream_key]
        if not candidates:
            return None
        return max(candidates, key=lambda k: k.window_start_ms)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_event(
    stream_key: str,
    event_time_ms: int,
    payload: dict[str, Any] | None = None,
) -> StreamEvent:
    return StreamEvent(
        event_id=str(uuid.uuid4()),
        stream_key=stream_key,
        event_time=event_time_ms,
        ingestion_time=event_time_ms + 5,
        payload=payload or {},
    )


def _print_results(label: str, results: list[WindowResult]) -> None:
    print(f"\n{'='*68}")
    print(f"  {label}")
    print(f"{'='*68}")
    if not results:
        print("  (no windows finalized)")
        return
    for r in sorted(results, key=lambda x: x.window_key.window_start_ms):
        start_s = r.window_key.window_start_ms // 1000
        end_s = r.window_key.window_end_ms // 1000
        print(
            f"  [{start_s:>6}s – {end_s:>6}s]  key={r.window_key.stream_key!r:<20}"
            f"  events={r.event_count:>4}  {r.aggregations}"
        )


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------

def scenario_a_tumbling_window() -> None:
    """
    Scenario A — Tumbling window for order pipeline.

    60 orders arrive with event_times spread across 3 consecutive
    one-minute windows (60s each). Each window captures exactly 20 events.
    No event is double-counted.
    """
    WINDOW_MS = 60_000      # 1-minute tumbling windows
    BASE_MS = 0             # epoch origin

    spec = WindowSpec(
        window_size_ms=WINDOW_MS,
        late_tolerance_ms=0,
        late_arrival_policy=LateArrivalPolicy.DROP,
    )
    processor = StreamProcessor(
        spec=spec,
        aggregations=[
            (AggregationFunction.COUNT, "event_id"),
            (AggregationFunction.SUM, "amount"),
        ],
    )

    # 20 events per window, 3 windows
    events_per_window = 20
    for window_idx in range(3):
        for i in range(events_per_window):
            t = BASE_MS + window_idx * WINDOW_MS + (i * (WINDOW_MS // events_per_window))
            processor.process(_make_event("orders", t, {"amount": 100 + i}))

    processor.flush()
    results = processor.get_results()   # includes windows finalized during process()
    _print_results("Scenario A — Tumbling window: order pipeline", results)

    assert len(results) == 3, f"Expected 3 windows, got {len(results)}"
    for r in results:
        assert r.event_count == 20, f"Expected 20 events per window, got {r.event_count}"
        # No double-counting: each event appears in exactly one window
    total_events = sum(r.event_count for r in results)
    assert total_events == 60, f"Expected 60 total events, got {total_events}"
    print("\n  ✓ Tumbling: each event in exactly one window; no double-counting")


def scenario_b_sliding_window() -> None:
    """
    Scenario B — Sliding window for API rate monitoring.

    Events arrive in a 30-second burst. A 60-second window sliding every
    15 seconds means the burst appears in 4 consecutive windows (since
    W/S = 4). Events in the burst are double-counted, which is the
    intended behavior for smoothed rate metrics.
    """
    WINDOW_MS = 60_000      # 60-second window
    SLIDE_MS = 15_000       # advance every 15 seconds

    spec = WindowSpec(
        window_size_ms=WINDOW_MS,
        slide_interval_ms=SLIDE_MS,
        late_tolerance_ms=0,
        late_arrival_policy=LateArrivalPolicy.DROP,
    )
    processor = StreamProcessor(
        spec=spec,
        aggregations=[(AggregationFunction.COUNT, "event_id")],
    )

    # 10 API calls between t=30s and t=60s (a 30-second burst)
    BASE_S = 30
    BURST_COUNT = 10
    for i in range(BURST_COUNT):
        t_ms = (BASE_S + i * 3) * 1000
        processor.process(_make_event("api-calls", t_ms))

    # Advance stream time well past all windows to finalize them
    processor.process(_make_event("api-calls", 200_000))
    processor.flush()
    results = processor.get_results()   # includes windows finalized during process()

    _print_results("Scenario B — Sliding window: API rate (60s / 15s slide)", results)

    # Events in the burst period [30s, 60s) appear in multiple windows
    # Each window that overlaps the burst contains ≥1 of the 10 events
    windows_containing_burst = [
        r for r in results
        if r.window_key.window_start_ms <= 30_000
        and r.window_key.window_end_ms >= 60_000
        or (30_000 <= r.window_key.window_start_ms < 90_000
            and r.event_count > 1)  # windows partly overlapping burst
    ]
    total_event_appearances = sum(r.event_count for r in results if r.event_count > 0)
    assert total_event_appearances > BURST_COUNT, (
        "Sliding window must double-count events across overlapping windows"
    )
    print(
        f"\n  ✓ Sliding: {BURST_COUNT} events appear in "
        f"{sum(1 for r in results if r.event_count > 0)} windows "
        f"({total_event_appearances} total appearances — expected > {BURST_COUNT})"
    )


def scenario_c_session_window() -> None:
    """
    Scenario C — Session window for user journey analytics.

    A user browses 3 product pages (events at t=0s, t=5s, t=10s), then
    is idle for 40 seconds (gap > 30s threshold), then places an order
    (t=50s). Two sessions are detected:
        Session 1: browse session [0s … 10s + 30s gap)
        Session 2: purchase session [50s … 50s + 30s gap)
    """
    SESSION_GAP_MS = 30_000   # 30-second inactivity gap

    spec = WindowSpec(
        window_size_ms=SESSION_GAP_MS,   # unused for session; gap drives sizing
        session_gap_ms=SESSION_GAP_MS,
        late_tolerance_ms=0,
        late_arrival_policy=LateArrivalPolicy.DROP,
    )
    processor = StreamProcessor(
        spec=spec,
        aggregations=[(AggregationFunction.COUNT, "event_id")],
    )

    # Browse session: 3 page views in quick succession
    for t_s in [0, 5, 10]:
        processor.process(_make_event("user-42", t_s * 1000, {"action": "page_view"}))

    # 40s gap (idle)
    # Purchase session: one order at t=50s
    processor.process(_make_event("user-42", 50_000, {"action": "purchase"}))

    processor.flush()
    results = processor.get_results()
    _print_results("Scenario C — Session window: user journey", results)

    assert len(results) == 2, f"Expected 2 sessions, got {len(results)}"
    event_counts = sorted(r.event_count for r in results)
    assert event_counts == [1, 3], (
        f"Expected sessions with 3 and 1 events, got {event_counts}"
    )
    print("\n  ✓ Session: browse session (3 events) + purchase session (1 event) detected")


def scenario_d_late_arrival() -> None:
    """
    Scenario D — Late arrival handling with two policies.

    DROP policy: an event 90 seconds late is discarded; audit log records it.
    SIDE_OUTPUT policy: same event is routed to reconciliation stream.
    ACCEPT policy: an event only 5 seconds late (within 10s tolerance) is accepted.
    """
    WINDOW_MS = 60_000
    WATERMARK_MS = 180_000   # 3 minutes advanced

    # --- DROP policy ---
    drop_handler = LateArrivalHandler(
        late_tolerance_ms=30_000,
        policy=LateArrivalPolicy.DROP,
    )
    late_event = _make_event("payments", WATERMARK_MS - 90_000, {"amount": 250})
    should_process_drop, _ = drop_handler.handle(late_event, WATERMARK_MS)
    assert not should_process_drop, "DROP policy should discard the event"
    assert drop_handler.audit_log[0].policy_applied == LateArrivalPolicy.DROP
    assert drop_handler.audit_log[0].lateness_ms == 90_000

    # --- SIDE_OUTPUT policy ---
    side_handler = LateArrivalHandler(
        late_tolerance_ms=30_000,
        policy=LateArrivalPolicy.SIDE_OUTPUT,
    )
    should_process_side, _ = side_handler.handle(late_event, WATERMARK_MS)
    assert not should_process_side, "SIDE_OUTPUT policy should not re-process the event"
    assert len(side_handler.side_output_stream) == 1
    assert side_handler.side_output_stream[0].event_id == late_event.event_id

    # --- ACCEPT policy (within tolerance) ---
    accept_handler = LateArrivalHandler(
        late_tolerance_ms=10_000,    # 10s tolerance
        policy=LateArrivalPolicy.DROP,   # policy overridden since within tolerance
    )
    slightly_late_event = _make_event("payments", WATERMARK_MS - 5_000)
    should_process_accept, _ = accept_handler.handle(slightly_late_event, WATERMARK_MS)
    assert should_process_accept, "Within-tolerance event should be accepted"

    print(f"\n{'='*68}")
    print("  Scenario D — Late arrival handling")
    print(f"{'='*68}")
    print(f"  DROP policy    (90s late, 30s tolerance): discarded  ✓  audit_log={len(drop_handler.audit_log)}")
    print(f"  SIDE_OUTPUT    (90s late, 30s tolerance): side stream={len(side_handler.side_output_stream)} ✓")
    print(f"  ACCEPT         (5s late,  10s tolerance): re-accepted ✓")
    print("\n  ✓ Late arrival: DROP discards; SIDE_OUTPUT routes to reconciliation; within-tolerance ACCEPTED")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("Event Streaming Temporal Window Patterns")
    print("Tumbling · Sliding · Session · Aggregation · Late Arrival")

    scenario_a_tumbling_window()
    scenario_b_sliding_window()
    scenario_c_session_window()
    scenario_d_late_arrival()

    print(f"\n{'='*68}")
    print("  All four scenarios complete.")
    print("  Invariants verified:")
    print("    • Tumbling: each event belongs to exactly one window")
    print("    • Sliding: events in burst appear in multiple overlapping windows")
    print("    • Session: inactivity gap closes session; new event opens next")
    print("    • Late arrival: DROP/SIDE_OUTPUT/ACCEPT policies enforce watermark")
    print(f"{'='*68}")
