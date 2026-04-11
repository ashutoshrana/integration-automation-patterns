# integration-automation-patterns

[![CI](https://github.com/ashutoshrana/integration-automation-patterns/actions/workflows/ci.yml/badge.svg)](https://github.com/ashutoshrana/integration-automation-patterns/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)
![PyPI](https://img.shields.io/pypi/v/integration-automation-patterns.svg)](https://www.python.org/downloads/)
[![PyPI](https://img.shields.io/pypi/v/integration-automation-patterns.svg)](https://pypi.org/project/integration-automation-patterns/)

Practical patterns for enterprise integration, workflow orchestration, and system-of-record synchronization in complex operating environments.

## Why this repo exists

Enterprise modernization usually breaks down at the integration layer:
- brittle handoffs between systems
- inconsistent event handling
- weak retry and idempotency models
- workflow logic scattered across tools
- poor visibility into operational state

This repository is a public-safe reference for patterns that help teams build more reliable integration and automation systems. The patterns are platform-agnostic and cloud-agnostic — applicable across any combination of CRM, ERP, ITSM, and custom services, on any cloud environment (AWS, GCP, Azure, OCI) or on-premises.

## Scope

This repo focuses on:
- event-driven integration patterns with explicit retry and idempotency models
- system-of-record synchronization with authority boundaries
- workflow orchestration and escalation boundaries
- observability for automation flows
- public-safe architecture notes for enterprise operations

The patterns do not assume any specific vendor, broker, or cloud platform.

## Modules

**Core reliability:**
- `event_envelope.py` — reliable event transport with explicit delivery status, bounded retry, and audit logging. Works with any broker (Kafka, SQS, Azure Service Bus, GCP Pub/Sub, RabbitMQ).
- `sync_boundary.py` — bi-directional system-of-record sync with field-level authority assignment, conflict detection, and exclusion management.

**Resilience patterns (v0.2.0):**
- `circuit_breaker.py` — thread-safe CLOSED/OPEN/HALF_OPEN state machine with automatic recovery probing and `.call` decorator.
- `saga.py` — distributed saga orchestrator: forward execution with automatic backward compensation on failure, fluent `.add_step()` API.

**Messaging patterns (v0.2.0):**
- `outbox.py` — transactional outbox for at-least-once delivery: write events in the same DB transaction, relay separately.
- `kafka_envelope.py` — Kafka-aware envelope: partition key routing, schema version, DLQ routing, producer/consumer roundtrip serialization.
- `webhook_handler.py` — HMAC-SHA256 webhook verification compatible with GitHub, Stripe, Salesforce, and ServiceNow signature formats.

**Change Data Capture (v0.2.0):**
- `cdc_event.py` — typed CDC event envelope: INSERT/UPDATE/DELETE/SNAPSHOT/TRUNCATE, Debezium format parsing, changed-field diff, audit dict.

## Ecosystem

See [ECOSYSTEM.md](./ECOSYSTEM.md) for full broker, connector, and framework coverage matrix.

## Repository structure

```
src/integration_automation_patterns/
├── event_envelope.py         # Reliable event transport + retry
├── sync_boundary.py          # Bi-directional SOR sync
├── circuit_breaker.py        # CLOSED/OPEN/HALF_OPEN state machine
├── saga.py                   # Distributed saga orchestrator
├── outbox.py                 # Transactional outbox pattern
├── kafka_envelope.py         # Kafka-aware event envelope
├── webhook_handler.py        # HMAC-SHA256 webhook verification
└── cdc_event.py              # Change Data Capture event types
docs/
├── architecture.md
├── implementation-note-01.md
├── implementation-note-02.md
└── adr/
```

## Published notes

- [`docs/implementation-note-01.md`](./docs/implementation-note-01.md) — event-driven integration reliability
- [`docs/implementation-note-02.md`](./docs/implementation-note-02.md) — idempotency in enterprise event processing

## Intended audience

- enterprise architects
- integration engineers
- workflow and automation operators
- platform teams responsible for system-of-record reliability across CRM, ERP, and service platforms

## Citing this work

If you use these patterns in your work, see `CITATION.cff` or use GitHub's "Cite this repository" button above.

---

## Part of the enterprise AI patterns trilogy

| Library | Focus | Regulation |
|---------|-------|-----------|
| [enterprise-rag-patterns](https://github.com/ashutoshrana/enterprise-rag-patterns) | What to retrieve | FERPA identity-scoped RAG |
| [regulated-ai-governance](https://github.com/ashutoshrana/regulated-ai-governance) | What agents may do | FERPA, HIPAA, GLBA policy enforcement |
| **integration-automation-patterns** | How data flows | Event-driven enterprise integration |
