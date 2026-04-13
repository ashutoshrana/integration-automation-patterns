# Implementation Note 07 — Backpressure and Retry Storm Prevention: Why Most Integration Pipelines Fail Under Load

**Repo:** `integration-automation-patterns`
**Relates to:** [`examples/09_backpressure_retry.py`](../examples/09_backpressure_retry.py)
**ADR cross-reference:** [ADR-001 (idempotency key design)](adr/001-idempotency-key-design.md), [ADR-002 (saga boundaries)](adr/002-saga-boundaries.md)

---

## The failure mode that looks like a success

Most enterprise integration pipelines are tested against a healthy downstream system. They pass. The saga orchestrates correctly, the outbox flushes reliably, and the CDC events arrive in order. Then the downstream goes slow — not down, just slow — and the pipeline destroys itself.

The sequence is predictable:

1. Consumer polls the queue. Event arrives. Sends to Oracle ERP. Timeout after 30 seconds.
2. Consumer retries immediately (or after 1 second). Times out again.
3. Queue depth grows because the consumer is spending all its time retrying one event.
4. Three minutes later, 50 events are backed up. All consumers are retrying simultaneously.
5. Oracle ERP is now receiving 50x its normal request rate — from a pipeline trying to recover.
6. ERP falls over completely. Now every retry fails instantly (connection refused).
7. The pipeline is in a retry storm: thousands of events retrying as fast as possible, consuming 100% of consumer CPU, producing zero successful deliveries.

This is not a theoretical failure mode. It is the default behavior of every naive integration pipeline. The patterns in this implementation note prevent it.

---

## Error classification: the first decision that matters

Before backoff, before budget, before backpressure — the most important decision is whether to retry at all.

**Transient failures** are temporary. Network timeouts, 503 Service Unavailable, 429 Too Many Requests, connection resets. The underlying system will recover. Retry with backoff.

**Resource exhaustion** is a capacity signal. The downstream is working but overwhelmed. Retry, but with a longer base interval than transient failures — the downstream needs time to drain, not a faster retry rate.

**Permanent failures** are not recoverable by retrying. Schema validation errors, 400 Bad Request with a malformed payload, 401 Unauthorized with a revoked credential. Retrying these wastes time and consumes retry budget without any possibility of success. Route to dead-letter immediately.

The classification logic belongs in the strategy — not in the catch block. Every retry in your codebase that decides whether to retry based on the exception type in a `try/except` block is duplicating this logic inconsistently. `AdaptiveRetryStrategy.classify_error()` is the single point of this decision.

**The budget implication:** For NON_RETRYABLE errors, no retry budget is consumed. The budget tracks retries — and a permanent failure does not need a retry. Consuming budget on a schema mismatch penalizes transient failures that genuinely need the budget.

---

## Full jitter: the mechanism that prevents convoys

Exponential backoff without jitter does not solve the retry storm problem. It delays it.

Consider 100 consumers that all fail at t=0. Without jitter:
- Attempt 1: all retry at t+1s. All fail again (downstream still recovering).
- Attempt 2: all retry at t+2s. All fail again.
- Attempt 3: all retry at t+4s. All fail again.

The consumers are still synchronized. They retry in a wave. Each wave hits the downstream with full load. The downstream cannot recover because each recovery attempt is immediately followed by another full wave.

The full jitter formula (AWS Architecture Blog, 2015):

```
sleep = random_between(0, min(cap, base * 2^attempt))
```

With 100 consumers and cap=16s:
- Attempt 1: retries spread uniformly across [0, 2s]. Downstream receives ~50 req/s instead of 100 req/s.
- Attempt 2: retries spread across [0, 4s].
- Attempt 3: retries spread across [0, 8s].

Each subsequent attempt spreads wider. The downstream receives a smooth, manageable request rate that allows gradual recovery rather than repeated full-load waves.

**The practical implication:** Jitter is not a nice-to-have. It is the mechanism that converts "retry storm" into "graceful recovery." A pipeline without jitter is a pipeline that will eventually destroy its downstream.

---

## Retry budget: protecting the pipeline from its own recovery logic

The retry budget answers a question that per-event retry logic cannot: "how many total retries can this consumer afford right now?"

Per-event retry logic (`max_retries=3` per event) can result in a consumer spending all its capacity retrying 10 stuck events (3 retries × 10 events = 30 retries in progress) while 990 events queue behind them. The per-event limit is correct from the event's perspective. It is wrong from the consumer's perspective.

The retry budget is a shared resource across the consumer, not per-event. When the budget is exhausted, events that would otherwise retry are routed to the dead-letter queue immediately. This ensures:

1. The consumer continues to make progress on new events while the downstream is degraded.
2. The total retry rate is capped, preventing the consumer from becoming a retry amplifier.
3. Stuck events accumulate in the dead-letter queue where they can be triaged rather than blocking live traffic.

**Choosing the budget window:** The window should be longer than the expected downstream recovery time. If Oracle ERP recovers from a slowdown in 60 seconds, a 60-second window allows retries to succeed during recovery. A 10-second window with a high budget might exhaust the budget before recovery, routing recoverable events to dead-letter unnecessarily.

**Choosing the budget size:** Start with `max_retries = N_consumers × expected_retries_per_event`. For 10 consumers each expecting 2 retries per event, a budget of 20 per 60-second window prevents retry amplification while allowing normal recovery behavior.

---

## Backpressure: acting before the downstream falls over

Backpressure is different from retry. Retry is the response to a failure that already happened. Backpressure prevents the downstream from reaching the point of failure.

The distinction matters because a downstream that reaches the point of failure takes longer to recover than one that was throttled before it reached capacity. Every database under 95% query load that you can hold at 70% load instead recovers in seconds rather than minutes.

**Watermark design:** The gap between the low watermark (resume normal) and the high watermark (start throttling) is intentional. Without this hysteresis:

- At t=0: pressure=0.61 → consumer THROTTLED
- At t=1: pressure=0.59 → consumer NORMAL
- At t=2: pressure=0.61 → consumer THROTTLED
- ...

The consumer oscillates between THROTTLED and NORMAL every second. This instability is itself a load on the downstream (the consumer is repeatedly adjusting its rate). The hysteresis gap ensures the consumer stays in THROTTLED until pressure has actually fallen to a safely-below-threshold level.

**The composite pressure score:** A single metric (latency, error rate, queue depth) is insufficient. A downstream might have high latency but zero error rate — it is overloaded but still functional. Or it might have low latency but a rapidly growing queue depth — it is processing fast but falling behind. The composite score weights all three signals:

```
pressure = latency_score × 0.5 + error_rate × 0.3 + queue_depth_score × 0.2
```

The weights reflect the typical failure sequence: latency is the first signal that precedes failure; error rate rises next; queue depth is a lagging indicator. Weighting latency more heavily gives earlier warning.

---

## Dead-letter queue: triage buffer, not graveyard

The dead-letter queue has a reputation it does not deserve: events go there to die. In practice, a well-designed dead-letter queue is an incident response tool.

**What the dead-letter record must contain:**

| Field | Why it matters |
|---|---|
| Original event (complete) | The triage operator needs the exact payload that failed |
| Retry history | Shows which error type caused each retry, and whether the backoff worked |
| Final error | The specific reason the event was routed here |
| TTL | Determines when the event expires automatically vs. requires human action |

**TTL by error type:**
- TRANSIENT: 24 hours. If the downstream recovers within a day, replay and these events process successfully.
- RESOURCE_EXHAUSTED: 1 hour. Drain the queue, fix the capacity issue, replay.
- NON_RETRYABLE: no TTL (indefinite). A schema mismatch requires a human to decide: fix the schema and replay, or discard with an audit record. Either action is explicitly audited.

**Replay vs. discard:** Both are explicit, audited actions. The distinction is: replay re-injects the event into the main queue (for cases where the fix is in the infrastructure); discard removes it with an audit record (for cases where the event itself was the problem). Neither is automatic.

---

## Integration with the saga pattern

The approval workflow and CQRS patterns in this repo address what happens when events succeed. This pattern addresses what happens when they fail. The integration point:

**Before the saga step:** The consumer applies the retry strategy and backpressure check. If the step eventually succeeds (after retries), the saga proceeds normally. The retry history is available if the saga orchestrator needs to log degraded performance.

**If the event reaches dead-letter:** The saga receives no completion signal. The saga's timeout fires. The saga executes its compensation chain (reversal of all completed steps). The dead-letter event is the audit artifact explaining why the saga was compensated.

**What this means for saga timeout design:** The saga timeout must be longer than `(max_retries × max_backoff) + recovery_time`. If the retry strategy uses a cap of 32 seconds and max 4 attempts, the worst-case retry time is ~128 seconds. The saga timeout should be at least 3–5 minutes to avoid compensating sagas that are legitimately recovering.

---

## What this pattern does not cover

- **Distributed rate limiting across multiple instances:** The `RetryBudget` in this example is per-consumer-instance. In a multi-instance deployment, each instance has its own budget. True distributed rate limiting (shared budget across instances) requires a shared state store (Redis INCR with TTL, or a rate-limiting sidecar like Envoy).
- **Circuit breaker state persistence:** The `BackpressureController` holds consumer state in memory. If the consumer restarts, it starts in NORMAL state. For persistent circuit-breaker state across restarts, store the state in the consumer's local KV store.
- **Priority queues:** When the dead-letter queue is large and recovery begins, some events may be more urgent than others. Priority queue routing (replay high-value events first) is outside the scope of this example.
- **Observability integration:** The retry history and dead-letter records in this example are returned as data structures. In production, each retry attempt and dead-letter routing event should emit a metric (for SLA monitoring) and a structured log (for incident correlation).
