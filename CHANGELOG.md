# Changelog

All notable changes to this project are documented in this file.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
Versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [0.5.3] — 2026-04-13

### Added — Observability, Recovery Runbook, and Orchestration Boundary ADR

**`docs/implementation-note-04.md`** — "Observability and Recovery for Enterprise Integration":
- Event pipeline metrics: `events.dispatched/processed/failed/skipped/dead_lettered`, consumer lag
- Failure classification: Class 1 (transient), Class 2 (permanent), Class 3 (poison)
- Step-by-step recovery runbook: triage DLQ, fix root cause, safe Class 1 replay
- Deduplication observability: `dedup.store_hits/misses/errors`
- Structured event logging pattern using `to_audit_line()`
- Real incident case study showing the cost of missing observability
- Closes #3.

**`docs/adr/003-orchestration-vs-integration-boundaries.md`** — "Workflow Orchestration
Boundaries: What belongs in orchestration vs integration":
- Hard boundary: integration owns transport (retry, dedup, broker format); orchestration
  owns business process (step sequence, approval checkpoints, compensation, escalation)
- Test for boundary: "if I replace the broker, does this code change?"
- What does NOT belong in the integration layer (business approval logic, SLA tracking)
- Domain event interface between layers: `CRMUpdateApplied` vs `ProcessApproved`
- Rejected alternatives: stateful integration handlers, orchestration-aware envelopes
- Closes #2.

---

## [0.5.2] — 2026-04-13

### Added — Idempotent CRM Update Example

**`examples/05_idempotent_crm_update.py`** — end-to-end idempotent event flow
for a Salesforce Lead Converted → SAP S/4HANA Contact Sync:
- `ProcessedEventStore` — simulates Redis SETNX / DB unique constraint deduplication
- Scenario A: first delivery, 2 transient failures, succeeds on attempt 3
- Scenario B: broker redelivers same event_id → SKIPPED (update not re-applied)
- Scenario C: new event with different event_id → applied on first attempt
- Scenario D: persistent failure → dead-lettered after retry budget exhausted
- Audit trail: `CRMUpdateResult` per event with outcome + attempt count
- Five structural idempotency rules with rationale
- Closes #1.

---

## [0.5.1] — 2026-04-13

### Added — CRM-ERP Sync Boundary Example and Implementation Note

**`examples/04_crm_erp_sync_boundary.py`** — Runnable end-to-end demo of the `SyncBoundary`
pattern applied to a Salesforce ↔ SAP S/4HANA account synchronization scenario:
- Field authority map (SF owns segment/email, SAP owns billing/credit limit, legal name is shared)
- Change detection distinguishing clean updates (one system changed) from true conflicts (both changed)
- Three resolution strategies: authority-wins, last-writer-wins, manual-review
- Shared fields (`legal_entity_name`, `vat_registration_number`) always escalate to manual review
- `allow_system_b_override=True` surfacing unexpected writes from non-authoritative systems
- Excluded fields (`sf_lead_score`, `sap_cost_center`) never touched by sync engine
- Closes #5.

**`docs/implementation-note-03.md`** — "Sync Boundaries in CRM-ERP Integration":
why explicit field authority prevents data corruption in bi-directional sync.
Covers the silent-overwrite failure mode, update-loop failure mode, last-sync-value
conflict detection (vs naive timestamp comparison), and when each resolution
strategy is appropriate.

---

## [0.5.0] — 2026-04-13

### Added — Multi-Broker Envelope Adapters

**AWS SQS/SNS** (`sqs_envelope.py` — `SQSEventEnvelope`):
- Standard queues and FIFO queues (`message_group_id` / `message_deduplication_id`).
- `to_send_params()`: produces a dict directly passable to `boto3` `sqs.send_message(**params)`.
- `from_sqs_message()`: deserialises from SQS message dict; transparently unwraps SNS fan-out
  notifications (``{"Type": "Notification", ...}`` wrapper).
- `mark_dead_letter(reason)`, `is_fifo()`, `approximate_receive_count` tracking.
- Closes #8.

**Azure Service Bus** (`azure_servicebus_envelope.py` — `AzureServiceBusEnvelope`):
- Sessions (ordered, grouped delivery per `session_id` — analogous to Kafka partition keys).
- `to_service_bus_message()`: produces a dict for `azure-servicebus` `ServiceBusMessage(**params)`.
- `from_service_bus_message()`: accepts SDK object or test-friendly dict; handles multi-frame body.
- Scheduled delivery (`enqueue_time_utc`), correlation ID, `mark_dead_letter(reason, description)`.
- `is_session_enabled()`, `is_scheduled()`.

**GCP Pub/Sub** (`gcp_pubsub_envelope.py` — `GCPPubSubEnvelope`):
- Ordering keys for per-key sequential delivery.
- `to_publish_data()` / `to_publish_attributes()`: separate data bytes + string attributes for
  `publisher.publish(topic, data=..., **attrs)`.
- `from_pubsub_message()`: pull subscription (SDK object or dict).
- `from_push_payload()`: push subscription HTTP body (base64 decode + SNS-style unwrap).
- `delivery_attempt` tracking, `exceeds_delivery_attempts(n)` for manual DLQ routing.
- Partially closes #4.

**RabbitMQ AMQP** (`rabbitmq_envelope.py` — `RabbitMQEnvelope`):
- Exchange / routing key / topic exchange routing.
- `to_amqp_body()` / `to_amqp_properties()`: body bytes + pika `BasicProperties`-compatible dict.
- `from_amqp_message()`: accepts pika `BasicProperties` or dict; parses `x-death` header for
  DLX death count tracking.
- Per-message TTL (`expiration`), priority, request/reply (`reply_to`, `correlation_id`).
- `mark_dead_letter(reason)`, `is_dead_lettered()`, `redelivered` flag.
- Closes #6.

### Changed
- `kafka_envelope.py`: `to_producer_record()` now uses `ensure_ascii=False` so non-ASCII
  characters (e.g. names with diacritics) are preserved verbatim in Kafka message bytes.
- `__init__.py`: now exports all modules — `KafkaEventEnvelope`, `SQSEventEnvelope`,
  `AzureServiceBusEnvelope`, `GCPPubSubEnvelope`, `RabbitMQEnvelope`, `CDCEvent`,
  `CDCOperation`, `CDCSourceMetadata`, `CircuitBreaker`, `CircuitOpenError`, `CircuitState`,
  `SagaOrchestrator`, `SagaStep`, `SagaResult`.

### Tests
- 65 new tests in `tests/test_broker_envelopes.py` (create, serialise, deserialise, roundtrip,
  DLQ, SNS-unwrap, push-payload, x-death count for all four adapters).
- Total: **171 passing**.

---

## [0.4.1] — 2026-04-13

### Added
- `rate_limiter.py`: `SlidingWindowRateLimiter` — thread-safe sliding-window rate limiter for enterprise CRM/ERP API rate windows (Salesforce 24h, HubSpot 10s, etc.). Tracks request timestamps in a rolling deque; enforces hard count limits within configurable windows. Supports blocking/non-blocking `acquire()`, `async_acquire()`, `try_acquire()`, `utilization()` (returns 0.0–1.0 utilization fraction), and `requests_in_window()`. Closes #21.
- `__init__.py`: exports `SlidingWindowRateLimiter`

---

## [0.4.0] — 2026-04-12

### Added
- `rate_limiter.py`: `TokenBucketRateLimiter` — thread-safe token-bucket rate limiter for webhook delivery, API call bursting, and event-driven integration rate management. Supports both blocking and non-blocking `acquire()`, plus async `async_acquire()` for FastAPI/asyncio environments. `RateLimitExceeded` exception with `tokens_available` and `cost` attributes.
- `[pydantic]` optional dependency: `pydantic>=2.12.0` for Pydantic v2 model validation in webhook payloads
- `[all]` extra combining kafka, fastapi, and pydantic dependencies

### Changed
- Bumped ecosystem compatibility pins:
  - `fastapi`: `>=0.100.0` → `>=0.130.0` (FastAPI 0.135.3 current; Server-Sent Events + streaming JSON Lines support)
  - `httpx`: `>=0.24.0` → `>=0.27.0`
- `pyproject.toml`: version bumped to 0.4.0

---

## [Unreleased]

### Added
- `http/fastapi_router.py`: `WebhookRouter` — FastAPI `APIRouter` factory for HMAC-verified, idempotency-safe webhook endpoints. Supports async and sync handler callables. Install with `integration-automation-patterns[fastapi]`
- `outbox.py`: `AsyncOutboxProcessor` — async variant that accepts coroutine or sync callables for all I/O operations; for FastAPI, aiohttp, and asyncio-based applications

---

## [0.3.0] — 2026-04-12

### Added
- Enhanced CI: coverage reporting (Codecov), ruff format check, build-check job, pip cache, concurrency cancellation
- Automation: PR auto-labeler, stale bot, Conventional Commits PR title check, first-contributor welcome bot
- Dependabot weekly pip + Actions updates; CODEOWNERS; SECURITY.md; pre-commit config
- `http/webhook_handler.py`: `IdempotentWebhookReceiver` — HMAC validation, idempotency key extraction, bounded LRU duplicate cache
- ADRs: `docs/adr/001-idempotency-key-as-event-id.md`, `002-explicit-field-authority-model.md`, `003-saga-choreography-vs-orchestration.md`
- README: badges, ASCII pipeline diagram, pattern catalog table, quickstart, BibTeX citation
- GitHub Discussions enabled; 22 standardized labels; milestones v0.3.0 + v1.0.0

---

## [0.2.0] — 2026-04-11

### Added

**Resilience patterns:**
- `circuit_breaker.py` — thread-safe circuit breaker with CLOSED/OPEN/HALF_OPEN state machine:
  - `CircuitBreaker` — configurable failure threshold, recovery timeout, half-open probe
  - `CircuitState` — CLOSED, OPEN, HALF_OPEN with automatic transitions
  - `CircuitBreakerStats` — snapshot of failure count, state, and last failure time
  - `CircuitOpenError` — raised when calls are rejected in OPEN state
  - `.call` decorator for transparent wrapping
- `saga.py` — distributed saga orchestrator for multi-step workflows:
  - `SagaOrchestrator` — forward execution + automatic backward compensation on failure
  - `SagaStep` — named step with `action` and `compensate` callables
  - `SagaResult` — succeeded flag, failed_step name, compensated_steps list, per-step results
  - Fluent `.add_step()` chaining API

**Messaging patterns:**
- `outbox.py` — transactional outbox for at-least-once event delivery:
  - `OutboxRecord` — pending outbox entry with UUID, aggregate_id, event_type, payload
  - `OutboxProcessor` — broker-agnostic relay: fetch_pending → publish → mark_published/mark_failed
  - `process_batch()` returns (published_count, failed_count) tuple
- `kafka_envelope.py` — Kafka-aware event envelope:
  - `KafkaEventEnvelope` — partition key, topic, schema version, DLQ routing
  - `.to_producer_record()` — serialised to `{topic, key, value: bytes, headers}`
  - `.from_consumer_record()` — deserialise from consumed record
  - `.mark_dead_letter(reason)` — immutable copy routed to `<topic>.dlq`
- `webhook_handler.py` — HMAC-SHA256 signed webhook verification:
  - `WebhookHandler` — validates `sha256=<hex>`, `v1=<hex>`, and `t=<ts>,v1=<hex>` formats
  - `WebhookEvent` — typed, verified event with event_id, event_type, payload, raw bytes
  - `WebhookSignatureError` / `WebhookReplayError` — specific error classes

**Database change capture:**
- `cdc_event.py` — Change Data Capture event envelope:
  - `CDCEvent` — typed CDC record with operation, source metadata, before/after snapshots
  - `CDCOperation` — INSERT, UPDATE, DELETE, SNAPSHOT, TRUNCATE
  - `CDCSourceMetadata` — database, schema, table, connector, LSN, server_id
  - `.from_debezium()` — parse standard Debezium envelope format
  - `.changed_fields()` — set of field names that differ between before/after
  - `.to_audit_dict()` — JSON-serialisable audit record

**OSS infrastructure:**
- `CODE_OF_CONDUCT.md` — Contributor Covenant 2.1
- `ECOSYSTEM.md` — integration map across message brokers, CDC connectors, and frameworks
- Issue templates: new-pattern, new-framework-integration

### Changed
- `pyproject.toml` → version `0.2.0`; added `kafka` optional dependency group; expanded keywords

---

## [0.1.0] — 2026-04-11

### Added

**Core modules:**
- `event_envelope.py` — reliable event transport for enterprise integration:
  - `EventEnvelope` — idempotency key, delivery status lifecycle, structured audit logging
  - `DeliveryStatus` — PENDING → DISPATCHED → ACKNOWLEDGED → PROCESSED / RETRYING / FAILED / SKIPPED
  - `RetryPolicy` — bounded retry with fixed or exponential backoff and configurable cap
- `sync_boundary.py` — system-of-record synchronization contracts:
  - `SyncBoundary` — explicit field-level authority assignment between two systems
  - `RecordAuthority` — SYSTEM_A / SYSTEM_B / SHARED / MANUAL ownership model
  - `SyncConflict` — structured representation of detected bi-directional update conflicts

**Documentation:**
- `docs/architecture.md` — layered integration architecture (source → integration → workflow → SOR → observability)
- `docs/implementation-note-01.md` — event-driven integration reliability patterns

**Project infrastructure:**
- `LICENSE` — MIT
- `CITATION.cff` — enables GitHub "Cite this repository" button
- `CONTRIBUTING.md` — contribution guidance
- `GOVERNANCE.md` — project governance model
- `ROADMAP.md` — near-term development direction
- `pyproject.toml` — full build configuration with keywords, classifiers, and optional dependency groups
- GitHub Actions CI: pytest (Python 3.10–3.12), ruff lint, mypy type check
- Issue templates: bug report, feature request
- 42 passing tests covering all public module APIs
