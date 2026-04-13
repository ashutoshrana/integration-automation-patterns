# Implementation Note 04 — Observability and Recovery for Enterprise Integration

**Series:** integration-automation-patterns  
**Topic:** Operational metrics, failure classification, and replay-safe recovery  
**Status:** Published 2026-04-13

---

## Why observability is not optional in enterprise integration

An enterprise integration layer moves events between systems that are operated
by different teams, on different release schedules, with different SLA windows.
When something breaks, the question is always: "did the event get processed,
or did it get lost?"

Without structured observability, answering that question requires:
- Checking the source system to see if the event was emitted
- Checking the message broker to see if it was received
- Checking the consumer to see if it was processed
- Checking the target system to see if the change was applied

Each of these checks is a manual investigation across four different systems.
Multiplied across dozens of integration flows, this is an on-call incident
in the making every time a consumer restarts.

Structured observability means: the integration pipeline itself emits enough
signal to answer "what happened to this event?" with a single query.

---

## What to measure

### Event pipeline metrics

| Metric | Description | Alert threshold |
|--------|-------------|-----------------|
| `events.dispatched` | Events sent to broker | Sudden drop (source stopped emitting) |
| `events.processed` | Events successfully processed | Should track dispatched with lag |
| `events.failed` | Events that failed at least once | Rising trend = downstream unhealthy |
| `events.skipped` | Duplicate events that were deduplicated | Spike = broker redelivering heavily |
| `events.dead_lettered` | Events that exhausted retry budget | Any nonzero = requires investigation |
| `events.retry_depth` | Distribution of attempt counts at success | P95 > 2 = downstream instability |

### Consumer health metrics

| Metric | Description | Alert threshold |
|--------|-------------|-----------------|
| `consumer.lag_seconds` | Age of oldest unprocessed event | > 5 minutes for real-time flows |
| `consumer.backlog_size` | Number of unprocessed events | Growing monotonically = consumer stalled |
| `consumer.dlq_depth` | Events in dead letter queue | Any nonzero |
| `consumer.processing_time_p95` | 95th percentile processing time | > 2× baseline |

### Idempotency metrics

| Metric | Description |
|--------|-------------|
| `dedup.store_hits` | Successful deduplication (duplicates caught) |
| `dedup.store_misses` | First-time event_ids (expected) |
| `dedup.store_errors` | Failed deduplication store lookups (unsafe — may allow duplicates) |

A `dedup.store_errors` spike means the deduplication store (Redis, database) is
unavailable. This is a critical failure: the consumer is operating without
idempotency guarantees. Circuit-break and halt processing until the store
recovers.

---

## Failure classification

Not all failures are equal. Treating them the same leads to dead letter queues
full of poison messages that can never succeed, alongside genuinely retriable
failures that are being discarded prematurely.

### Class 1: Transient failure

**Definition:** The event is well-formed; the downstream system is temporarily
unavailable or rate-limiting.

**Examples:**
- Salesforce API rate limit (HTTP 429)
- Downstream database connection timeout
- Network partition between consumer and target system

**Response:** Retry with exponential backoff per `RetryPolicy`. These failures
are expected and normal in a distributed system.

**Retention:** Keep in the processing queue. Retry up to `max_attempts`.

### Class 2: Permanent failure (not retryable)

**Definition:** The event is well-formed but the downstream system has rejected
it definitively — the rejection will not change on retry.

**Examples:**
- HTTP 400 Bad Request (malformed field value)
- Foreign key constraint violation (referenced record does not exist)
- Business logic rejection ("cannot withdraw after the deadline")

**Response:** Do not retry. Route to dead letter queue immediately. Flag for
human review with the rejection reason and HTTP status code.

**Retention:** DLQ with full event payload and rejection details.

### Class 3: Poison message

**Definition:** The event itself is malformed and cannot be deserialized or
validated.

**Examples:**
- Invalid JSON / missing required field
- Incompatible schema version
- Corrupted message body

**Response:** Route to dead letter queue immediately. Retrying a poison message
wastes retry budget and can block the queue if the consumer crashes on
deserialization.

**Retention:** DLQ with raw message body and parse error. Schema team must
investigate the source.

---

## Recovery runbook

### Step 1: Triage the DLQ

When `consumer.dlq_depth` is nonzero:

```bash
# Inspect DLQ messages
aws sqs receive-message --queue-url $DLQ_URL --max-number-of-messages 10 --attribute-names All

# Or for Kafka dead letter topic:
kafkacat -b $BROKER -t $DLQ_TOPIC -C -o beginning -c 10 -f '%T %k %s\n'
```

Classify each message as Class 1, 2, or 3. Group by failure reason.

### Step 2: Fix the root cause

| Class | Likely cause | Fix |
|-------|-------------|-----|
| 1 (transient) | Consumer restarted before retry window | Re-queue for retry |
| 2 (permanent) | Source data quality issue | Fix source data; re-emit event |
| 3 (poison) | Schema mismatch between source and consumer | Deploy schema fix; purge poison messages |

### Step 3: Replay Class 1 failures

Class 1 failures are safe to replay — the event is valid and the downstream
is now healthy. Because consumers are idempotent (event_id deduplication),
replaying a previously processed event is safe.

```python
# In production: re-enqueue to processing queue, not DLQ
# The consumer's ProcessedEventStore will skip already-processed events
for event in dlq_class1_events:
    assert event.event_id  # must have idempotency key
    queue.send(event)
    # Consumer will check ProcessedEventStore before processing
```

**Never replay Class 2 or 3 without fixing the root cause first.**
Replaying a permanent failure will immediately re-dead-letter it.
Replaying a poison message will re-crash the consumer.

### Step 4: Verify replay

After replay:
1. `events.dead_lettered` should not increase (no new DLQ entries)
2. `events.processed` should increase (successful processing)
3. `events.skipped` may increase if some replayed events were already processed
4. Source system records should show the expected state change

---

## Structured event logging

Every `EventEnvelope` has a built-in audit log entry via `to_audit_line()`.
Log every event at the point of outcome, not just on failure:

```python
# On success
envelope.mark_processed()
logger.info("event.processed", extra={"event": envelope.to_audit_line()})

# On failure
envelope.mark_failed()
logger.warning("event.failed", extra={
    "event": envelope.to_audit_line(),
    "failure_class": classify_failure(error),
    "retry_wait_seconds": envelope.next_retry_wait_seconds(),
})

# On DLQ
logger.error("event.dead_lettered", extra={
    "event": envelope.to_audit_line(),
    "dlq_reason": str(last_error),
})
```

The `to_audit_line()` output includes: `event_id`, `event_type`, `source_system`,
`status`, `attempt_count`, `correlation_id`, and `dispatched_at`. This is enough
to reconstruct the event's history without reading the original message payload.

---

## The cost of missing observability

A real incident pattern in enterprise integration:

1. Finance team reports: "SAP accounts are not being updated with Salesforce changes."
2. Integration team checks: Salesforce webhooks are firing (confirmed in SF Event Log).
3. Integration team checks: MuleSoft flow shows no errors in the last 24h.
4. Integration team checks: SAP shows no recent inbound changes.
5. Root cause (found 3 hours later): A consumer pod restarted during a Kafka rebalance.
   Events were redelivered. The consumer processed them, but the deduplication check
   ran *after* the SAP write — so some were double-applied and some were skipped.

With `dedup.store_hits`, `dedup.store_errors`, and `events.skipped` metrics,
this would have been visible as a `dedup.store_errors` spike at the time of the
pod restart. Investigation time: 10 minutes instead of 3 hours.

---

## See also

- `src/integration_automation_patterns/event_envelope.py` — `EventEnvelope`, `RetryPolicy`, `DeliveryStatus`
- `examples/05_idempotent_crm_update.py` — idempotent CRM update with DLQ path
- ADR 001 — idempotency key design
- Implementation Note 02 — idempotency in event processing
