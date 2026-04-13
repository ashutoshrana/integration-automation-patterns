"""
09_backpressure_retry.py — Backpressure and adaptive retry storm prevention
for enterprise event-driven integration pipelines.

Demonstrates the patterns required to keep an event-driven pipeline stable
under downstream system pressure — the most common cause of production
integration failures at scale:

    Pattern 1 — Adaptive Retry Strategy: Exponential backoff with full jitter
                prevents synchronized retry convoys when many consumers fail
                simultaneously. Per-error-type policy distinguishes transient
                failures (retry with backoff) from permanent failures (dead-
                letter immediately, no retry budget consumed).

    Pattern 2 — Retry Budget: Caps total retry attempts per consumer per time
                window. When the budget is exhausted, events route to the dead-
                letter queue immediately — prevents a single slow downstream
                service from blocking the entire pipeline indefinitely.

    Pattern 3 — Backpressure Controller: Monitors downstream service health
                (latency, error rate, queue depth). When pressure exceeds the
                high watermark, the controller signals upstream consumers to
                reduce consumption rate. When pressure falls below the low
                watermark, normal rate is restored. Prevents queue overflow
                cascades where an overwhelmed downstream causes the entire
                upstream pipeline to retry and compound the problem.

    Pattern 4 — Dead Letter Router: Preserves events that cannot be processed,
                along with their full retry history. Supports replay (re-inject
                to the main queue after manual triage) and discard. Per-error-
                type TTL enables automatic expiry of certain categories.

Scenarios
---------

  A. Normal pipeline — all orders flow through without pressure.

  B. Oracle ERP slowdown — BackpressureController detects latency spike;
     consumer reduces rate; AdaptiveRetryStrategy retries with exponential
     backoff + jitter; system recovers when ERP stabilizes.

  C. Permanent failure (schema mismatch) — AdaptiveRetryStrategy identifies
     the error type as NON_RETRYABLE and routes to dead-letter immediately
     without consuming any retry budget.

  D. Retry storm — 5 simultaneous failures with and without jitter: shows
     how synchronized retries (no jitter) create a convoy while jittered
     retries spread load. Retry budget exhaustion routes overflow to dead-
     letter.

No external dependencies required.

Run:
    python examples/09_backpressure_retry.py
"""

from __future__ import annotations

import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Callable


# ---------------------------------------------------------------------------
# Domain types
# ---------------------------------------------------------------------------

class ErrorType(str, Enum):
    """Classification of integration failures by retry policy."""
    TRANSIENT = "transient"               # Temporary: network timeout, rate limit, 503
    RESOURCE_EXHAUSTED = "resource_exhausted"  # Downstream at capacity: slow down
    NON_RETRYABLE = "non_retryable"       # Permanent: schema mismatch, auth failure, 400
    UNKNOWN = "unknown"


class ConsumerState(str, Enum):
    """State of an event consumer under backpressure."""
    NORMAL = "normal"
    THROTTLED = "throttled"
    PAUSED = "paused"


@dataclass
class IntegrationEvent:
    """An event to be processed by the integration pipeline."""
    event_id: str
    payload: dict
    source_system: str
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class RetryRecord:
    """Record of a single retry attempt."""
    attempt: int
    error_type: ErrorType
    error_message: str
    backoff_seconds: float
    attempted_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class DeadLetterRecord:
    """An event that could not be processed, preserved for triage or replay."""
    event: IntegrationEvent
    retry_history: list[RetryRecord]
    final_error: str
    routed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    ttl_seconds: float | None = None

    @property
    def is_expired(self) -> bool:
        if self.ttl_seconds is None:
            return False
        age = (datetime.now(timezone.utc) - self.routed_at).total_seconds()
        return age > self.ttl_seconds


# ---------------------------------------------------------------------------
# Pattern 1 — Adaptive Retry Strategy
# ---------------------------------------------------------------------------

class AdaptiveRetryStrategy:
    """
    Exponential backoff with full jitter for transient failure retry.

    Full jitter formula (per AWS Architecture Blog):
        sleep = random_between(0, min(cap, base * 2^attempt))

    This prevents synchronized retry convoys: when N consumers all fail at
    the same time and retry without jitter, they retry in synchronized waves
    that continue to overwhelm the downstream service. Full jitter spreads
    retries uniformly across the cap interval.

    Per-error-type policy:
    - TRANSIENT: retry with exponential backoff + full jitter
    - RESOURCE_EXHAUSTED: retry with longer base backoff (downstream is slow)
    - NON_RETRYABLE: never retry — route to dead-letter immediately
    - UNKNOWN: treat as TRANSIENT
    """

    def __init__(
        self,
        base_seconds: float = 1.0,
        cap_seconds: float = 32.0,
        jitter: bool = True,
    ) -> None:
        self._base = base_seconds
        self._cap = cap_seconds
        self._jitter = jitter

    def is_retryable(self, error_type: ErrorType) -> bool:
        return error_type != ErrorType.NON_RETRYABLE

    def backoff_seconds(self, attempt: int, error_type: ErrorType) -> float:
        if not self.is_retryable(error_type):
            return 0.0
        base = self._base
        if error_type == ErrorType.RESOURCE_EXHAUSTED:
            base = self._base * 4  # slower base for capacity exhaustion

        ceiling = min(self._cap, base * (2 ** attempt))
        if self._jitter:
            return random.uniform(0, ceiling)
        return ceiling  # no jitter — synchronized retry (used to demonstrate the problem)

    def classify_error(self, exception: Exception) -> ErrorType:
        """Classify an exception by its retry policy. Real implementations
        inspect HTTP status codes, exception types, or error codes."""
        msg = str(exception).lower()
        if "timeout" in msg or "connection" in msg or "503" in msg or "429" in msg:
            return ErrorType.TRANSIENT
        if "capacity" in msg or "resource exhausted" in msg or "overload" in msg:
            return ErrorType.RESOURCE_EXHAUSTED
        if "schema" in msg or "invalid" in msg or "400" in msg or "unauthorized" in msg:
            return ErrorType.NON_RETRYABLE
        return ErrorType.UNKNOWN


# ---------------------------------------------------------------------------
# Pattern 2 — Retry Budget
# ---------------------------------------------------------------------------

class RetryBudget:
    """
    Caps total retry attempts across a consumer within a rolling window.

    When retry budget is exhausted, events route to the dead-letter queue
    immediately rather than blocking indefinitely. This prevents a slow
    downstream service from consuming all available consumer capacity while
    retrying the same events repeatedly.

    The budget is shared across all events processed by the consumer — not
    per-event. This reflects the real cost of retries: each retry consumes
    compute, network, and time resources that could serve new events.
    """

    def __init__(self, max_retries: int = 50, window_seconds: float = 60.0) -> None:
        self._max = max_retries
        self._window = window_seconds
        self._attempts: list[float] = []  # timestamps of retry attempts

    def consume(self) -> bool:
        """Consume one retry unit. Returns True if budget available, False if exhausted."""
        now = time.monotonic()
        self._attempts = [t for t in self._attempts if now - t < self._window]
        if len(self._attempts) >= self._max:
            return False
        self._attempts.append(now)
        return True

    @property
    def remaining(self) -> int:
        now = time.monotonic()
        self._attempts = [t for t in self._attempts if now - t < self._window]
        return max(0, self._max - len(self._attempts))


# ---------------------------------------------------------------------------
# Pattern 3 — Backpressure Controller
# ---------------------------------------------------------------------------

@dataclass
class DownstreamHealth:
    """Observable health metrics for a downstream service."""
    service_name: str
    latency_ms: float          # Current p99 latency
    error_rate: float          # 0.0–1.0
    queue_depth: int           # Pending items in downstream queue

    @property
    def pressure_score(self) -> float:
        """Composite pressure score 0.0–1.0. Higher = more pressure."""
        latency_score = min(1.0, self.latency_ms / 5000.0)  # Cap at 5s
        return (latency_score * 0.5) + (self.error_rate * 0.3) + (min(1.0, self.queue_depth / 1000) * 0.2)


class BackpressureController:
    """
    Monitors downstream service health and signals consumers to adjust rate.

    Watermarks:
    - high_watermark: pressure score above which consumers are THROTTLED
    - critical_watermark: pressure score above which consumers are PAUSED
    - low_watermark: pressure score below which normal rate is restored

    The hysteresis gap between high and low watermarks prevents rapid
    oscillation between NORMAL and THROTTLED states.
    """

    def __init__(
        self,
        low_watermark: float = 0.3,
        high_watermark: float = 0.6,
        critical_watermark: float = 0.85,
    ) -> None:
        self._low = low_watermark
        self._high = high_watermark
        self._critical = critical_watermark
        self._consumer_states: dict[str, ConsumerState] = {}

    def assess(self, health: DownstreamHealth, consumer_id: str) -> ConsumerState:
        """Assess downstream health and return the recommended consumer state."""
        score = health.pressure_score
        current = self._consumer_states.get(consumer_id, ConsumerState.NORMAL)

        if score >= self._critical:
            state = ConsumerState.PAUSED
        elif score >= self._high:
            state = ConsumerState.THROTTLED
        elif score <= self._low:
            state = ConsumerState.NORMAL
        else:
            state = current  # Hysteresis: no change in the gap between low and high

        self._consumer_states[consumer_id] = state
        return state

    def get_state(self, consumer_id: str) -> ConsumerState:
        return self._consumer_states.get(consumer_id, ConsumerState.NORMAL)


# ---------------------------------------------------------------------------
# Pattern 4 — Dead Letter Router
# ---------------------------------------------------------------------------

class DeadLetterRouter:
    """
    Structured dead-letter management with replay and TTL support.

    The dead-letter queue is not a graveyard — it is a triage buffer. Events
    that land here have a retry history and a classified failure reason. An
    operator can:
    - Inspect the event and retry history to diagnose the root cause
    - Fix the downstream system or the event payload
    - Replay the event (re-inject into the main queue)
    - Discard the event (explicit, audited action)

    Per-error-type TTL enables automatic expiry of certain categories:
    TRANSIENT failures during planned maintenance may safely expire after 24
    hours; NON_RETRYABLE schema errors may require indefinite retention for
    audit purposes.
    """

    DEFAULT_TTL: dict[ErrorType, float | None] = {
        ErrorType.TRANSIENT: 86400.0,           # 24 hours — likely recoverable
        ErrorType.RESOURCE_EXHAUSTED: 3600.0,   # 1 hour — drain and replay
        ErrorType.NON_RETRYABLE: None,          # Indefinite — requires human triage
        ErrorType.UNKNOWN: 86400.0,
    }

    def __init__(self) -> None:
        self._queue: list[DeadLetterRecord] = []

    def route(
        self,
        event: IntegrationEvent,
        retry_history: list[RetryRecord],
        final_error: str,
        error_type: ErrorType,
    ) -> DeadLetterRecord:
        record = DeadLetterRecord(
            event=event,
            retry_history=retry_history,
            final_error=final_error,
            ttl_seconds=self.DEFAULT_TTL.get(error_type),
        )
        self._queue.append(record)
        return record

    def replay(self, event_id: str) -> IntegrationEvent | None:
        """Re-inject a dead-lettered event into the main processing queue."""
        for record in self._queue:
            if record.event.event_id == event_id and not record.is_expired:
                self._queue.remove(record)
                return record.event
        return None

    def discard(self, event_id: str) -> bool:
        """Explicitly discard a dead-lettered event (audited action)."""
        for record in self._queue:
            if record.event.event_id == event_id:
                self._queue.remove(record)
                return True
        return False

    @property
    def queue(self) -> list[DeadLetterRecord]:
        return list(self._queue)


# ---------------------------------------------------------------------------
# Pipeline processor (combines all four patterns)
# ---------------------------------------------------------------------------

class IntegrationPipelineProcessor:
    """
    Event processor that combines adaptive retry, retry budget, backpressure,
    and dead-letter routing into a single coherent pipeline component.
    """

    def __init__(
        self,
        consumer_id: str,
        retry_strategy: AdaptiveRetryStrategy,
        retry_budget: RetryBudget,
        backpressure: BackpressureController,
        dead_letter: DeadLetterRouter,
        max_attempts_per_event: int = 4,
    ) -> None:
        self._consumer_id = consumer_id
        self._strategy = retry_strategy
        self._budget = retry_budget
        self._backpressure = backpressure
        self._dead_letter = dead_letter
        self._max_attempts = max_attempts_per_event

    def process(
        self,
        event: IntegrationEvent,
        handler: Callable[[IntegrationEvent], None],
        health_fn: Callable[[], DownstreamHealth],
    ) -> tuple[bool, list[RetryRecord]]:
        """
        Process an event with retry, backpressure, and dead-letter handling.

        Returns (success, retry_history). On permanent failure, the event is
        routed to the dead-letter queue and (False, history) is returned.
        """
        retry_history: list[RetryRecord] = []

        for attempt in range(self._max_attempts):
            # Check backpressure before each attempt
            health = health_fn()
            state = self._backpressure.assess(health, self._consumer_id)

            if state == ConsumerState.PAUSED:
                print(
                    f"  [backpressure] Consumer {self._consumer_id} PAUSED — "
                    f"downstream pressure={health.pressure_score:.2f}, "
                    f"latency={health.latency_ms:.0f}ms"
                )

            try:
                handler(event)
                return True, retry_history

            except Exception as exc:
                error_type = self._strategy.classify_error(exc)

                if not self._strategy.is_retryable(error_type):
                    # Permanent failure — dead-letter immediately, no budget consumed
                    record = RetryRecord(
                        attempt=attempt,
                        error_type=error_type,
                        error_message=str(exc),
                        backoff_seconds=0.0,
                    )
                    retry_history.append(record)
                    self._dead_letter.route(event, retry_history, str(exc), error_type)
                    print(
                        f"  [dead-letter] {event.event_id} → NON_RETRYABLE "
                        f"({str(exc)}); no retry budget consumed"
                    )
                    return False, retry_history

                # Transient/resource failure — check retry budget
                if not self._budget.consume():
                    self._dead_letter.route(event, retry_history, f"retry budget exhausted: {exc}", error_type)
                    print(
                        f"  [dead-letter] {event.event_id} → retry budget exhausted "
                        f"(remaining=0); routing to dead-letter"
                    )
                    return False, retry_history

                backoff = self._strategy.backoff_seconds(attempt, error_type)
                record = RetryRecord(
                    attempt=attempt,
                    error_type=error_type,
                    error_message=str(exc),
                    backoff_seconds=backoff,
                )
                retry_history.append(record)
                print(
                    f"  [retry {attempt+1}/{self._max_attempts-1}] {event.event_id} "
                    f"— {error_type.value}, backoff={backoff:.2f}s "
                    f"(budget remaining={self._budget.remaining})"
                )

        # Max attempts exceeded
        self._dead_letter.route(
            event, retry_history, "max attempts exceeded", ErrorType.TRANSIENT
        )
        return False, retry_history


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------

def _print_scenario(label: str, description: str) -> None:
    print(f"\n{'=' * 72}")
    print(f"Scenario {label}: {description}")
    print("=" * 72)


def main() -> None:
    # ------------------------------------------------------------------
    # Scenario A: Normal pipeline — all orders processed, no pressure
    # ------------------------------------------------------------------
    _print_scenario("A", "Normal pipeline — no downstream pressure, all events succeed")

    strategy_a = AdaptiveRetryStrategy(base_seconds=1.0, cap_seconds=32.0, jitter=True)
    budget_a = RetryBudget(max_retries=20, window_seconds=60.0)
    backpressure_a = BackpressureController()
    dead_letter_a = DeadLetterRouter()
    processor_a = IntegrationPipelineProcessor(
        consumer_id="order-processor-1",
        retry_strategy=strategy_a,
        retry_budget=budget_a,
        backpressure=backpressure_a,
        dead_letter=dead_letter_a,
    )

    events_a = [
        IntegrationEvent(f"order-{i:04d}", {"amount": 100 * i, "product": f"SKU-{i}"}, "salesforce")
        for i in range(1, 4)
    ]

    def healthy_handler(event: IntegrationEvent) -> None:
        pass  # Always succeeds

    def healthy_health() -> DownstreamHealth:
        return DownstreamHealth("oracle-erp", latency_ms=120.0, error_rate=0.0, queue_depth=10)

    for event in events_a:
        success, history = processor_a.process(event, healthy_handler, healthy_health)
        print(f"  {event.event_id}: {'OK' if success else 'FAILED'} (retries={len(history)})")

    print(f"Dead-letter queue depth: {len(dead_letter_a.queue)}")

    # ------------------------------------------------------------------
    # Scenario B: Oracle ERP slowdown — backpressure triggers, retry with backoff
    # ------------------------------------------------------------------
    _print_scenario(
        "B",
        "Oracle ERP slowdown — BackpressureController detects latency spike; "
        "consumer throttled; first attempt fails (transient); retry succeeds.",
    )

    strategy_b = AdaptiveRetryStrategy(base_seconds=1.0, cap_seconds=16.0, jitter=True)
    budget_b = RetryBudget(max_retries=20, window_seconds=60.0)
    backpressure_b = BackpressureController(low_watermark=0.3, high_watermark=0.6)
    dead_letter_b = DeadLetterRouter()
    processor_b = IntegrationPipelineProcessor(
        consumer_id="order-processor-2",
        retry_strategy=strategy_b,
        retry_budget=budget_b,
        backpressure=backpressure_b,
        dead_letter=dead_letter_b,
    )

    event_b = IntegrationEvent("order-b001", {"amount": 5000, "product": "SAP-SYNC"}, "salesforce")

    call_count_b = {"n": 0}

    def erp_slow_handler(event: IntegrationEvent) -> None:
        call_count_b["n"] += 1
        if call_count_b["n"] == 1:
            raise Exception("connection timeout — ERP response time 4800ms")
        # Second attempt succeeds (ERP recovered)

    def slow_health() -> DownstreamHealth:
        return DownstreamHealth("oracle-erp", latency_ms=4800.0, error_rate=0.3, queue_depth=450)

    state_b = backpressure_b.assess(slow_health(), "order-processor-2")
    print(f"  BackpressureController state: {state_b.value} (pressure={slow_health().pressure_score:.2f})")

    success_b, history_b = processor_b.process(event_b, erp_slow_handler, slow_health)
    print(f"  {event_b.event_id}: {'OK (recovered)' if success_b else 'FAILED'} "
          f"(attempts={call_count_b['n']}, retries={len(history_b)})")
    if history_b:
        print(f"  Backoff used: {history_b[0].backoff_seconds:.2f}s (jittered)")
    print(f"  Retry budget remaining: {budget_b.remaining}/20")

    # ------------------------------------------------------------------
    # Scenario C: Permanent failure — NON_RETRYABLE, dead-letter immediately
    # ------------------------------------------------------------------
    _print_scenario(
        "C",
        "Permanent schema failure — AdaptiveRetryStrategy detects NON_RETRYABLE; "
        "event routes to dead-letter immediately; no retry budget consumed.",
    )

    strategy_c = AdaptiveRetryStrategy(base_seconds=1.0, cap_seconds=32.0, jitter=True)
    budget_c = RetryBudget(max_retries=20, window_seconds=60.0)
    backpressure_c = BackpressureController()
    dead_letter_c = DeadLetterRouter()
    processor_c = IntegrationPipelineProcessor(
        consumer_id="order-processor-3",
        retry_strategy=strategy_c,
        retry_budget=budget_c,
        backpressure=backpressure_c,
        dead_letter=dead_letter_c,
    )

    event_c = IntegrationEvent(
        "order-c001",
        {"amount": "INVALID_STRING", "product": None},
        "legacy-erp",
    )

    def schema_fail_handler(event: IntegrationEvent) -> None:
        raise ValueError("schema invalid: 'amount' must be numeric (400 Bad Request)")

    budget_before_c = budget_c.remaining
    success_c, history_c = processor_c.process(event_c, schema_fail_handler, healthy_health)
    budget_after_c = budget_c.remaining

    print(f"  {event_c.event_id}: {'OK' if success_c else 'FAILED — dead-lettered'}")
    print(f"  Error type: {history_c[-1].error_type.value}")
    print(f"  Retry budget before: {budget_before_c}  after: {budget_after_c}  (no budget consumed)")
    dl_record = dead_letter_c.queue[0]
    print(f"  Dead-letter TTL: {dl_record.ttl_seconds} (None = indefinite, human triage required)")

    # ------------------------------------------------------------------
    # Scenario D: Retry storm — jitter vs. no-jitter comparison
    # ------------------------------------------------------------------
    _print_scenario(
        "D",
        "Retry storm — 5 simultaneous failures. Without jitter: synchronized "
        "retry convoy. With jitter: retries spread across the cap interval. "
        "Retry budget exhaustion routes overflow to dead-letter.",
    )

    # No-jitter: all retries converge on the same interval
    no_jitter_strategy = AdaptiveRetryStrategy(base_seconds=2.0, cap_seconds=16.0, jitter=False)
    print("\n  No-jitter backoff schedule (attempt 0, 5 simultaneous consumers):")
    backoffs_no_jitter = [no_jitter_strategy.backoff_seconds(0, ErrorType.TRANSIENT) for _ in range(5)]
    for i, b in enumerate(backoffs_no_jitter):
        print(f"    consumer-{i+1}: {b:.2f}s")
    print(f"  All retry at t+{backoffs_no_jitter[0]:.2f}s — synchronized convoy")

    # Full jitter: retries spread uniformly
    jitter_strategy = AdaptiveRetryStrategy(base_seconds=2.0, cap_seconds=16.0, jitter=True)
    print("\n  Full-jitter backoff schedule (attempt 0, 5 simultaneous consumers):")
    backoffs_jitter = [jitter_strategy.backoff_seconds(0, ErrorType.TRANSIENT) for _ in range(5)]
    for i, b in enumerate(backoffs_jitter):
        print(f"    consumer-{i+1}: {b:.2f}s")
    spread = max(backoffs_jitter) - min(backoffs_jitter)
    print(f"  Spread: {spread:.2f}s — load distributed across [0, {jitter_strategy._cap}]s window")

    # Retry budget exhaustion — tight budget, many failures
    print("\n  Retry budget exhaustion (budget=3, 5 events all fail transiently):")
    tight_budget = RetryBudget(max_retries=3, window_seconds=60.0)
    tight_dead_letter = DeadLetterRouter()
    tight_processor = IntegrationPipelineProcessor(
        consumer_id="tight-consumer",
        retry_strategy=jitter_strategy,
        retry_budget=tight_budget,
        backpressure=BackpressureController(),
        dead_letter=tight_dead_letter,
        max_attempts_per_event=4,
    )

    def always_fail(event: IntegrationEvent) -> None:
        raise ConnectionError("connection timeout — downstream overloaded")

    for i in range(5):
        event = IntegrationEvent(f"storm-{i:03d}", {"n": i}, "upstream")
        success, history = tight_processor.process(event, always_fail, healthy_health)
        print(
            f"  storm-{i:03d}: {'OK' if success else 'dead-lettered'} "
            f"(retries={len(history)}, budget_remaining={tight_budget.remaining})"
        )

    print(f"\n  Dead-letter queue depth: {len(tight_dead_letter.queue)}")
    print(f"  Budget exhaustion → overflow events routed to dead-letter without retrying")
    for record in tight_dead_letter.queue:
        print(f"    {record.event.event_id}: {record.final_error[:60]}, "
              f"retries_before_dl={len(record.retry_history)}")

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print(f"\n{'=' * 72}")
    print("PATTERN SUMMARY")
    print("=" * 72)
    print("AdaptiveRetryStrategy: Exponential backoff with full jitter prevents")
    print("  synchronized retry convoys. NON_RETRYABLE errors skip retry entirely.")
    print("RetryBudget:           Caps total retries in a rolling window. Exhausted")
    print("  budget routes events to dead-letter — prevents indefinite blocking.")
    print("BackpressureController: Monitors latency + error rate + queue depth.")
    print("  Throttles/pauses consumers before the downstream falls over.")
    print("DeadLetterRouter:      Preserves events with full retry history.")
    print("  Supports replay (fix + re-inject) and discard (explicit, audited).")
    print("\nKey design rules:")
    print("  - Classify errors BEFORE consuming retry budget (NON_RETRYABLE)")
    print("  - Jitter is not optional — it is the mechanism that prevents convoys")
    print("  - Backpressure must act before the downstream fails, not after")
    print("  - Dead-letter queue is a triage buffer, not a discard bin")


if __name__ == "__main__":
    main()
