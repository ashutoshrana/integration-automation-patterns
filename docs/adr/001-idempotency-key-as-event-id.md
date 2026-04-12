# ADR-001: Use Event ID as the Canonical Idempotency Key

**Status:** Accepted  
**Date:** 2026-04-12  
**Deciders:** Ashutosh Rana

## Context

Enterprise integration pipelines routinely re-deliver messages: network retries, message broker at-least-once semantics, webhook replay on 5xx responses, and manual re-processing of failed batches all cause the same logical event to arrive multiple times at a consumer. The system must deduplicate these re-deliveries without relying on the upstream sender to track delivery state.

Two strategies for idempotency keys were considered:

1. **Content hash** — hash the event payload; if two messages have the same payload, treat them as the same event
2. **Event ID** — require every event to carry a stable, producer-assigned unique identifier; the consumer deduplicates on this ID

A third option — relying on message broker sequence numbers (Kafka offsets, SQS MessageId) — was also considered.

## Decision

Use a producer-assigned `event_id` (UUID v4 or equivalent) as the canonical idempotency key. Content hashing is a fallback for legacy events without an `event_id`. Broker-assigned IDs are not used as idempotency keys.

## Rationale

**Why event ID over content hash:**
- Content hashes break on semantically-identical but byte-different payloads (timestamp fields, floating-point serialization differences, field reordering). Two webhook deliveries of the same logical order update may differ in the `delivery_attempt` field or timestamp, producing different hashes even though the business event is identical.
- Hashing requires loading the full payload into memory before deduplication, which is expensive for large document or file-attached events.
- An `event_id` makes the producer responsible for assigning identity, which is semantically correct: the producer knows when it is resending the same event, not the consumer.

**Why not broker-assigned IDs:**
- Kafka offsets and SQS MessageId are infrastructure identifiers for a specific delivery, not identifiers for the business event. The same business event re-processed from a dead-letter queue or re-played from a snapshot gets a new broker ID. Using broker IDs as idempotency keys means re-processing is never safe, which defeats the purpose.
- This library targets application-layer idempotency, not broker-layer exactly-once delivery (which requires broker-specific configuration, e.g. Kafka transactional producers).

## Consequences

**Positive:**
- The deduplication check is a single key lookup (`seen_ids[event_id]`), O(1), with no payload parsing required before the check.
- Idempotency is portable across brokers: the same `event_id` deduplicates correctly whether the event arrives via Kafka, SQS, webhook, or batch file replay.
- Audit logs can correlate the original event to all re-delivery attempts via `event_id`.

**Negative:**
- Requires producers to generate and preserve `event_id` across retries. Legacy systems that generate a new UUID per delivery attempt (rather than per business event) will bypass deduplication. This must be fixed in the producer, not the consumer.
- The `InMemoryIdempotencyStore` used in tests and lightweight deployments does not persist across process restarts. Production deployments must use Redis or a database-backed store, which is the consumer's responsibility.

**Neutral:**
- The `IdempotencyStore` interface is intentionally minimal (`contains(event_id)`, `mark_seen(event_id)`) to keep the contract simple and allow any backing store. The library ships only the in-memory reference implementation.
