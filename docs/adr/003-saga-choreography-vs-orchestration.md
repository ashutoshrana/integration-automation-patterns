# ADR-003: Saga Orchestration over Choreography for Distributed Transaction Patterns

**Status:** Accepted  
**Date:** 2026-04-12  
**Deciders:** Ashutosh Rana

## Context

Enterprise integrations that span multiple systems-of-record — for example, creating a record in a CRM, provisioning an account in an ERP, then sending a welcome message via a messaging platform — require a strategy for handling partial failures. If step 2 succeeds but step 3 fails, step 2 must be compensated (rolled back). Two well-known saga patterns address this:

**Choreography:** Each service listens on an event bus and reacts to domain events. No central coordinator; services coordinate through events. Example: CRM emits `AccountCreated`, ERP listens and emits `ERPAccountProvisioned`, messaging service listens and sends welcome.

**Orchestration:** A central `SagaOrchestrator` object explicitly invokes each step in sequence and is responsible for running compensations in reverse order if any step fails.

The question is which pattern to encode as the default `SagaOrchestrator` class in this library.

## Decision

Implement an explicit `SagaOrchestrator` class that orchestrates steps imperatively. Do not implement a choreography engine as a first-class abstraction. Document choreography as a deployment topology that consumers can implement on top of the library's idempotency primitives.

## Rationale

**Why orchestration is more teachable:**
- Choreography's failure modes are distributed: a silent event consumer failure leaves the saga in an unknown state with no single point of diagnosis. Orchestration concentrates the failure point — the orchestrator knows exactly which step failed, which compensations ran, and what the final status is.
- In regulated enterprise environments (financial services, healthcare, higher ed), auditability of transaction boundaries is a compliance requirement, not a nice-to-have. An orchestrator produces a single `SagaResult` with `failed_at_step` and `compensations_run` that can be written directly to an audit log. Choreography-based systems require correlating events across multiple services' logs to reconstruct the same picture.

**Why choreography is still valid but out of scope:**
- Choreography excels at loose coupling across independently-deployed services communicating via Kafka or similar. This library targets Python-process-level integration patterns, not multi-service deployment topology. Choreography at the deployment level is an infrastructure concern best handled by the consumer's message broker setup, not by a Python class.
- Adding a choreography engine would require an event bus abstraction, which is a significantly larger API surface than `SagaOrchestrator.execute()`.

**Trade-off acknowledged:**
- Orchestration creates a coupling: the orchestrator must import or reference all step callables. In a pure microservices topology this is inappropriate. But at the Python process level — coordinating calls to external APIs — this coupling is acceptable and makes the code far easier to test (each step is a plain `Callable`).

## Consequences

**Positive:**
- `SagaResult` provides a single, structured record of what happened, suitable for compliance audit logs.
- Each `SagaStep.compensate` callable is explicitly paired with its `action`, making compensation logic impossible to omit by accident (unlike choreography where a missing listener is invisible).
- The orchestrator is easily unit-tested: inject stubs for `action` and `compensate`, assert on `SagaResult`.

**Negative:**
- The `SagaOrchestrator` holds references to all step callables in-process. This is not suitable for long-running sagas that span hours or days with human approval steps — those require a durable workflow engine (Temporal, Camunda) and are explicitly out of scope.
- Compensation is best-effort: if a compensation callable itself throws, the orchestrator records the failure but cannot guarantee system-wide consistency. This is a fundamental property of sagas (not a library bug), but it must be clearly documented.

**Neutral:**
- The status enum (PENDING/RUNNING/COMPLETED/COMPENSATING/FAILED) mirrors the state machine used by Temporal, Camunda, and AWS Step Functions, making it easier for consumers to migrate to a durable workflow engine if they outgrow the in-process orchestrator.
