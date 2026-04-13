# Implementation Note 10 — Temporal Windowing Patterns for Enterprise Event Streams

## Context

Enterprise integration pipelines process millions of events per day across order management, payment processing, CRM activity logs, and operational telemetry. Raw events are individually meaningful but analytically insufficient: business decisions require aggregated views over time — "total order value in the last 5 minutes," "API calls per billing interval," or "did this user abandon their cart?"

Temporal windowing is the mechanism by which an event stream is partitioned into bounded time segments for aggregation. Choosing the wrong window type or mishandling late arrivals produces silent data quality defects that are extremely difficult to detect downstream.

This note documents three window types, their semantic guarantees, and the late-arrival watermark model used in `12_event_streaming_windows.py`.

---

## Window Types

### 1. Tumbling Windows (non-overlapping)

A tumbling window of size W assigns each event to exactly one window:

```
window_start = floor(event_time / W) * W
window_end   = window_start + W
```

**Guarantee:** No event is counted twice. The total event count across all windows equals the total number of events in the stream.

**Ideal for:** Periodic reporting — per-minute order counts, hourly revenue roll-ups, 5-minute billing intervals. Any metric where "no double-counting" is a correctness requirement.

**Anti-pattern:** Using tumbling windows for rate monitoring introduces aliasing. A burst that straddles a window boundary appears split across two periods, making the burst invisible to any individual window. Use sliding windows for rate monitoring.

### 2. Sliding Windows (overlapping)

A sliding window of size W advancing every S milliseconds assigns each event to floor(W/S) windows (or fewer near stream boundaries):

```
windows containing event_time T:
    { [k*S, k*S + W) | k*S ≤ T < k*S + W }
```

**Guarantee:** Each event appears in exactly `ceil(W/S)` windows. Events at the boundaries of bursts appear in fewer windows.

**Ideal for:** Smoothed rate metrics — "rolling 60-second API call count computed every 15 seconds." The overlap smooths out the aliasing artifact of tumbling windows.

**Cost:** Events are stored in multiple window accumulators simultaneously. Memory usage scales with W/S. For W=60s and S=15s, each event occupies 4 accumulators. High W/S ratios require careful resource budgeting.

### 3. Session Windows (activity-gap-based)

A session window has no fixed duration. It starts on the first event and extends as long as events arrive within the gap threshold (session_gap_ms). The session closes when the gap is exceeded.

**Guarantee:** Each event belongs to exactly one session. Sessions are independent per stream key (each user's sessions are separate).

**Ideal for:** Customer journey analytics, chatbot conversation boundaries, cart abandonment detection. The gap threshold is a business parameter: a 30-minute inactivity gap is standard for web analytics (Google Analytics default); a 2-minute gap may be appropriate for real-time support chat.

**Important distinction:** Session windows are data-driven, not time-driven. Two users with identical event counts but different inter-event timing produce different session structures. Session window state is unbounded until sessions close.

---

## Watermark Model and Late Arrivals

### Why watermarks are necessary

Events in a distributed system do not arrive in event-time order. A payment confirmation event timestamped at 14:00:01 may arrive at the processor at 14:00:35 due to mobile network latency, batch flushing, or downstream processing delays.

Without a mechanism to declare "the stream is now complete up to time T," windows cannot be finalized: the processor would have to wait forever for potentially-late events before computing a result.

**Watermark:** The processor's estimate of the minimum event time that any future event can carry. Defined as:

```
watermark = max(observed event_time) - allowed_lateness_ms
```

A window is finalized (and its result emitted) when `watermark > window_end_ms`.

### Late-arrival policies

An event is "late" when its event_time falls below the current watermark. Four policies handle this case:

| Policy | Behavior | When to use |
|--------|----------|-------------|
| `ACCEPT` | Re-open the window; update its result | Small allowed_lateness_ms; idempotent aggregations |
| `ASSIGN_TO_LATEST` | Move event to the currently open window | Approximate metrics where event-time accuracy is secondary |
| `DROP` | Discard; write to audit log | Strict correctness requirements; late events are statistically negligible |
| `SIDE_OUTPUT` | Route to reconciliation stream | Financial/accounting pipelines where all events must be accounted for, even late ones |

### The reconciliation stream (SIDE_OUTPUT)

In regulated industries (payment processing, trade reporting), every event must produce a traceable outcome. The `SIDE_OUTPUT` policy routes late events to a separate reconciliation stream rather than discarding them. This stream is processed separately — typically with a corrective journal entry pattern — ensuring that late-arriving data is eventually consistent without corrupting the real-time window results.

---

## Aggregation Design

The `WindowAggregator` computes aggregations in a single pass over the events in a window. Key design decision: aggregations are defined as `(AggregationFunction, field_name)` pairs rather than lambda functions.

**Rationale:** Lambda functions are not serializable (cannot be checkpointed to durable storage or shipped across a network). Named function/field pairs can be serialized as JSON and stored in a configuration system, enabling dynamic aggregation reconfiguration without code deployment.

---

## Enterprise Integration Considerations

### Exactly-once vs. at-least-once

The `StreamProcessor` in this module is a reference implementation assuming at-least-once event delivery (the default for Kafka, AWS Kinesis, Google Pub/Sub). For exactly-once semantics:

1. Assign an `event_id` to every event and track it in the window accumulator.
2. On duplicate delivery, check whether the event_id already exists in the accumulator before adding.
3. The `StreamEvent.event_id` field supports this — it is a UUID that producers must assign at message creation, not at ingestion.

### State management in distributed processors

In Apache Flink, Spark Structured Streaming, or AWS Kinesis Data Analytics, the per-window state (`_window_events` dict) maps to a keyed state backend (RocksDB or Gemini). For session windows, the `_session_accumulators` dict maps to a separate keyed state. The patterns in `SessionWindow` and `StreamProcessor` are isomorphic to the Flink `KeyedStream` + `ProcessFunction` + `ValueState` pattern.

### Clock skew between producer and consumer

The watermark model assumes that `allowed_lateness_ms` captures the maximum observed clock skew between producers. In practice, set `allowed_lateness_ms` to the 99th percentile of observed end-to-end latency in your pipeline, not the median. A watermark that is too aggressive (too small) drops events that arrive just barely late; one that is too conservative delays all window results.

---

## Related Patterns in This Repository

- **[01_event_envelope_retry.py](../examples/01_event_envelope_retry.py)** — EventEnvelope with event_time and idempotency key; the natural input type for the StreamProcessor.
- **[02_transactional_outbox.py](../examples/02_transactional_outbox.py)** — At-least-once delivery from the database outbox; produces the late-arrival distribution that the watermark must account for.
- **[10_schema_evolution.py](../examples/10_schema_evolution.py)** — Schema versioning for events consumed by sliding/session windows; window accumulators must tolerate schema evolution across their lifetime.
