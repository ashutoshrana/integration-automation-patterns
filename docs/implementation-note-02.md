# Implementation Note 02 — Idempotency in Enterprise Event Processing

**Series:** integration-automation-patterns  
**Topic:** Why retry-safe design requires structure, not discipline  
**Status:** Published 2026-04-11

---

## The problem

Enterprise message brokers — Kafka, SQS, Azure Service Bus, GCP Pub/Sub,
RabbitMQ, IBM MQ — provide **at-least-once delivery**. This is not an
implementation detail. It is the delivery guarantee these systems offer.

At-least-once delivery means: under normal operating conditions, a message
will be delivered. If something goes wrong during acknowledgment (consumer
crash, network timeout, rebalance), the broker will redeliver the message.
The same message may arrive more than once.

If the consumer processing logic is not idempotent, duplicate delivery causes
duplicate side effects:

- A student re-enrolled twice in the same course
- An invoice generated twice for the same transaction
- A notification sent twice to the same recipient
- A record updated twice, with the second update using stale state from the
  first delivery as input

The standard advice is: "make your consumers idempotent." What this advice
often leaves out is how to do that in a consistent, auditable way across a
distributed integration layer with dozens of consumer points.

---

## Why "discipline" is not sufficient

The naive approach is to document that developers should check for duplicate
messages and handle them appropriately. In practice:

- Different teams implement different deduplication strategies using different
  identifiers (message timestamp, payload hash, external record ID).
- Some consumers deduplicate; others do not. The integration works until a
  retry happens at the one consumer that skipped deduplication.
- Without a canonical idempotency key, it is not possible to ask "was this
  event processed exactly once?" without auditing every consumer independently.
- Under incident response at 2am, the question "did this fire twice because
  of a bug or because the broker redelivered?" takes hours to answer if there
  is no structured event identity in the audit trail.

Idempotency implemented as a convention fails at the seams between teams.
Idempotency implemented as structure — in the event envelope itself — holds.

---

## The EventEnvelope pattern

`EventEnvelope` encodes three elements of the reliability contract in a single
object:

```python
from integration_automation_patterns.event_envelope import (
    EventEnvelope,
    RetryPolicy,
)
import uuid

envelope = EventEnvelope(
    event_id=str(uuid.uuid4()),  # canonical idempotency key
    event_type="enrollment.status.changed",
    payload={"student_id": "stu-123", "new_status": "enrolled"},
    source_system="sis",
    retry_policy=RetryPolicy(max_attempts=3, backoff_seconds=30, exponential=True),
)
```

### The idempotency key

`event_id` is the canonical deduplication handle. Every downstream consumer
that needs idempotent processing uses `event_id` as its deduplication key:

```sql
INSERT INTO enrollment_events (event_id, student_id, status, processed_at)
VALUES (:event_id, :student_id, :status, NOW())
ON CONFLICT (event_id) DO NOTHING;
```

The `ON CONFLICT DO NOTHING` clause is the complete idempotency implementation
for this consumer. No custom deduplication logic. No coordination between
consumers. The key is canonical; the constraint is local.

### The retry policy

`RetryPolicy` expresses the bounded retry contract: how many times to attempt
redelivery, and with what backoff, before marking the event as failed.

```python
# Fixed backoff: retry up to 3 times, 30 seconds between attempts
RetryPolicy(max_attempts=3, backoff_seconds=30, exponential=False)

# Exponential backoff: 30s, 60s, 120s (capped at 300s)
RetryPolicy(max_attempts=3, backoff_seconds=30, exponential=True, cap_seconds=300)
```

The retry policy is part of the event, not the consumer. This means:

- The producer can express different reliability requirements for different
  event types (a critical financial event vs. a non-critical cache invalidation).
- The retry behavior is visible in the event audit trail, not buried in
  consumer configuration.

### The delivery status lifecycle

`DeliveryStatus` tracks the event's position in the delivery lifecycle:

```
PENDING → DISPATCHED → ACKNOWLEDGED → PROCESSED
                              └→ RETRYING → PROCESSED
                              └→ RETRYING → FAILED
                    └→ SKIPPED
```

Terminal states: `PROCESSED`, `FAILED`, `SKIPPED`.

Attempting to transition a terminal state raises `ValueError`, preventing
processing loops from reprocessing events that have already reached a final
state.

---

## Audit trail

Every status transition produces an audit log line:

```python
line = envelope.audit_line()
# {
#   "event_id": "a1b2c3...",
#   "event_type": "enrollment.status.changed",
#   "source_system": "sis",
#   "status": "PROCESSED",
#   "attempt": 1,
#   "timestamp": "2026-04-11T14:23:01.456789"
# }
```

The `event_id` appears in every audit line, making it possible to reconstruct
the full delivery history of a single event across all processing steps:

```
event_id=a1b2c3  status=DISPATCHED  attempt=1  ts=14:23:00
event_id=a1b2c3  status=RETRYING    attempt=1  ts=14:23:31
event_id=a1b2c3  status=ACKNOWLEDGED attempt=2 ts=14:24:01
event_id=a1b2c3  status=PROCESSED   attempt=2  ts=14:24:02
```

Under incident response, this trace answers the question "was this processed
once or twice?" in a single log query.

---

## Five rules for idempotent event processing

### 1. Generate event_id at the producer, not the broker

Broker-generated message IDs exist, but they change on redelivery in some
brokers (SQS changes `MessageId` for requeued messages; Kafka does not
regenerate `offset`-based identifiers). Use a UUID4 generated by the
producer at event creation time. This ID is stable across all delivery
attempts.

### 2. Propagate event_id across all derived events

If Event A triggers the creation of Event B, Event B's audit trail should
reference Event A's `event_id` (e.g., as `parent_event_id` in the payload).
This makes it possible to reconstruct causal chains across systems.

### 3. The deduplication constraint belongs at the consumer

The canonical deduplication implementation is: `INSERT ... ON CONFLICT
(event_id) DO NOTHING`. This is a consumer-side database constraint, not a
producer-side guarantee. The producer cannot guarantee exactly-once delivery;
the consumer implements exactly-once *processing* via idempotent writes.

### 4. Distinguish SKIPPED from FAILED

A `FAILED` event is one that could not be processed after exhausting retries —
it needs investigation, possibly a dead-letter queue, and human review.

A `SKIPPED` event is one that arrived but was intentionally not processed —
for example, a duplicate that was detected by the idempotency key constraint,
or an event that arrived out of order and was superseded by a later event.

Treating `SKIPPED` and `FAILED` as the same status conflates processing
errors with expected duplicate handling. Use the `mark_skipped(reason=...)`
method to record why an event was not processed.

### 5. Bound your retries

Unbounded retry loops — retrying forever on failure — can cause processing
backlogs, cascade failures, and resource exhaustion. `RetryPolicy` enforces
an explicit bound: after `max_attempts` retries, the event transitions to
`FAILED`. This makes the retry behavior predictable and allows the system to
surface failures to the right monitoring or alerting channel instead of
silently retrying indefinitely.

---

## Common failure modes

**Using message timestamp as deduplication key**  
Timestamps are not unique when multiple events are generated in the same
millisecond, and they change when a message is requeued in brokers that update
timestamp metadata on retry. Use UUID4 `event_id` instead.

**Deduplication at the broker layer only**  
Some brokers offer exactly-once delivery semantics (Kafka transactions, SQS
FIFO with content-based deduplication). These are not portable across brokers
and have performance trade-offs. Consumer-side idempotency with `event_id`
works the same way regardless of which broker is in use.

**No status tracking across retries**  
Without `DeliveryStatus`, a consumer cannot distinguish "this event failed
and I should retry" from "this event was already processed successfully and I
should not retry." The status lifecycle prevents double-processing.

**Treating all failures as retriable**  
Some failures are transient (network timeout — retry). Others are permanent
(malformed payload — retrying will always fail). `RetryPolicy` bounds the
retry count; callers should distinguish retriable vs. non-retriable errors
before incrementing `attempt_count`.

---

## See also

- `event_envelope.py` — `EventEnvelope`, `RetryPolicy`, `DeliveryStatus`
- `sync_boundary.py` — system-of-record authority boundaries
- `docs/implementation-note-01.md` — Event-driven integration reliability patterns
- `docs/adr/001-idempotency-key-design.md` — ADR for event_id design
- `docs/adr/002-explicit-authority-model.md` — ADR for field-level authority
