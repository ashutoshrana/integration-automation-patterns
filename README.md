# integration-automation-patterns

[![CI](https://github.com/ashutoshrana/integration-automation-patterns/actions/workflows/ci.yml/badge.svg)](https://github.com/ashutoshrana/integration-automation-patterns/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
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

- `event_envelope.py`
  Reliable event transport with explicit delivery status, bounded retry policy,
  and structured audit logging. Works with any message broker (Kafka, SQS,
  Azure Service Bus, GCP Pub/Sub, RabbitMQ, IBM MQ, and others).

- `sync_boundary.py`
  System-of-record synchronization contracts for bi-directional integration
  between enterprise platforms. Explicit field-level authority assignment,
  conflict detection, and exclusion management. Platform-agnostic.

## Repository structure

- `src/integration_automation_patterns/`
  - `event_envelope.py` — event transport with retry and audit
  - `sync_boundary.py` — bi-directional sync authority boundaries
- `docs/architecture.md`
- `docs/implementation-note-01.md`
- `docs/adr/`
- `examples/event-flow.yaml`
- `CITATION.cff`
- `CONTRIBUTING.md`
- `GOVERNANCE.md`

## Near-term roadmap

- add integration reliability ADRs
- add examples for retry-safe event handling across broker types
- document action logging and audit boundaries
- add workflow orchestration boundary patterns

## Published notes

- implementation note: [`docs/implementation-note-01.md`](./docs/implementation-note-01.md)

## Intended audience

- enterprise architects
- integration engineers
- workflow and automation operators
- platform teams responsible for system-of-record reliability across CRM, ERP, and service platforms

## Citing this work

If you use these patterns in your work, see `CITATION.cff` or use GitHub's "Cite this repository" button above.
