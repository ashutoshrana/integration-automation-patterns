# Changelog

All notable changes to this project are documented in this file.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
Versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
