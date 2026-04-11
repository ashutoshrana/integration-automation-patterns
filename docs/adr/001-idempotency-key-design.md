# ADR 001 — Event ID as structural idempotency key, not a caller convention

**Status:** Accepted  
**Date:** 2026-04-11  
**Context:** integration-automation-patterns / event_envelope module

---

## Context

Enterprise integration systems deliver events between platforms (CRM, ERP,
ITSM, workflow engines) using message brokers that provide at-least-once
delivery guarantees. Under at-least-once delivery, a consumer may receive the
same event more than once — due to broker retries, consumer restarts, or
network failures during acknowledgment.

If the consumer's processing logic is not idempotent, duplicate delivery causes
duplicate side effects: duplicate records, double-charged transactions,
repeated notifications, or corrupted aggregate state.

There are two approaches to achieving idempotent processing:

1. **Caller convention:** Document that callers should check for duplicate
   events themselves, using whatever identifier they have available. Leave
   the deduplication implementation to each integration point.
2. **Structural key:** Require a unique, immutable `event_id` on every
   `EventEnvelope`, and use this key as the canonical deduplication handle
   across all consumers.

---

## Decision

`EventEnvelope` requires an `event_id` at construction time. The field is
immutable after construction. All audit log entries produced by the envelope
include the `event_id`.

The `event_id` is the canonical idempotency key for the event. Consumers should
use it as the deduplication key in their idempotent write operations (e.g.,
upsert with `ON CONFLICT (event_id) DO NOTHING`).

---

## Rationale

### Structural key: consistent, auditable, discoverable

- Every envelope carries its idempotency key in a known field. Consumers do
  not need to agree on which field to use — there is only one.
- The `event_id` appears in every audit log line, making it possible to trace
  a single event through all processing steps, retries, and delivery attempts.
- New consumers do not need to invent their own deduplication strategy. They
  inherit the pattern from the envelope structure.
- At-least-once delivery is a property of the broker, not an edge case. The
  structural key encodes the assumption that duplicates will occur and that
  handling them correctly is a first-class concern, not an afterthought.

### Caller convention: not reliable at scale

- With distributed teams and multiple integration points, "check for duplicates
  yourself" leads to inconsistent implementations. Some consumers deduplicate;
  others do not.
- Without a canonical key, different consumers may use different fields
  (message timestamp, payload hash, external record ID) as their deduplication
  key. These can collide or drift.
- Audit trails fragmented across consumers make incident diagnosis difficult:
  it is not possible to ask "was this event processed exactly once?" without
  querying every consumer independently.

### Integration with RetryPolicy

`EventEnvelope` pairs the structural idempotency key with a bounded `RetryPolicy`.
Together they express the complete reliability contract for an event:

- `event_id` — "this event is unique; detect duplicates with this key"
- `RetryPolicy` — "if processing fails, retry at most N times with this backoff"
- `DeliveryStatus` — "this is the current state in the delivery lifecycle"

Separating these would leave each concern to be implemented independently and
inconsistently across integration points.

---

## Consequences

### Accepted trade-offs

- Callers must supply an `event_id`. A UUID4 generated at event creation is
  the standard choice. The module does not auto-generate the ID to ensure
  the caller owns the idempotency key lifecycle (e.g., to re-use an ID from
  an upstream system's message identifier if appropriate).
- The `event_id` uniqueness guarantee is enforced by the caller, not by the
  module. If a caller reuses an `event_id` for a genuinely different event,
  the idempotency guarantee breaks. Callers should treat `event_id` as
  immutable and non-reusable.
- Consumer-side deduplication (database upsert, idempotency table) is still
  required. The `event_id` is the key; the consumer provides the lock.

### Alternatives considered

**Auto-generate event_id inside EventEnvelope:** Simpler for callers. Loses
the ability to use an upstream system's existing message ID as the idempotency
key. Makes it harder to correlate the envelope ID with a CRM or ERP record ID.
Rejected.

**Use payload hash as idempotency key:** Payload hashes can collide when two
distinct events have identical payloads (e.g., two status-update events that
happen to have the same field values). Does not distinguish "same event,
delivered twice" from "two different events with identical state." Rejected.

---

## Related

- `event_envelope.py` — `EventEnvelope`, `RetryPolicy`, `DeliveryStatus`
- ADR 002 — Explicit field-level authority model for sync boundaries
- `docs/implementation-note-01.md` — Event-driven integration reliability patterns
