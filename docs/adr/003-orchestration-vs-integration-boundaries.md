# ADR 003 — Workflow Orchestration Boundaries: What belongs in orchestration vs integration

**Status:** Accepted  
**Date:** 2026-04-13  
**Context:** integration-automation-patterns / orchestration and workflow modules

---

## Context

Enterprise automation combines two concerns that are often conflated:

**Integration** — moving data and events between systems. Transport-level concerns:
- Which systems are the source and target of a change
- How messages are delivered (retry policy, at-least-once delivery, deduplication)
- What format the message takes (envelope structure, schema)

**Orchestration** — coordinating multi-step business processes. Workflow-level concerns:
- What sequence of actions constitutes a business process
- Who needs to approve or review before the next step runs
- What happens when a step fails (compensation, escalation, hold)
- What constitutes a "complete" workflow instance

The failure mode is: embedding orchestration logic in integration code, or
embedding integration mechanics (retry, deduplication) in orchestration logic.

---

## Decision

Maintain a hard boundary between orchestration and integration:

1. **Integration layer** (this repo) manages transport concerns. It knows:
   - The event envelope structure and lifecycle
   - Retry policy and backoff
   - Deduplication via idempotency key
   - Dead letter routing
   - Broker-specific envelope formats (Kafka, SQS, Service Bus, etc.)

2. **Orchestration layer** (application code, Camunda, Temporal, Airflow, etc.)
   manages business process concerns. It knows:
   - What events trigger a process
   - The sequence and dependencies of steps
   - Approval and escalation checkpoints
   - Compensation logic for failed steps (sagas)
   - The terminal states of a workflow instance

The integration layer does not know whether an event is part of a workflow.
The orchestration layer does not know how events are delivered.

---

## Rationale

### Why separating these concerns matters

**State multiplicity:** A workflow instance has state ("in progress", "pending approval",
"completed"). A transport event has state ("dispatched", "processed", "failed").
These are different state machines. Conflating them creates objects that are in two
states simultaneously — a workflow that is "in progress" but whose last event is "failed"
— which is ambiguous.

**Retry semantics differ:** At the integration layer, retry means "attempt to deliver
this message again." At the orchestration layer, retry means "re-execute this workflow
step." These are different operations with different idempotency requirements. An
integration retry should be transparent to the orchestration layer.

**Failure handling differs:** An integration failure (transient downstream unavailability)
should not fail a workflow step — it should be retried at the transport layer until it
succeeds or exhausts its retry budget, at which point the orchestration layer is
notified. A workflow step failure (business logic rejection) should not trigger transport
retries — it should trigger the orchestration layer's compensation or escalation path.

### The test for the boundary

> "If I replace the message broker with a different broker, does this code change?"

- If yes: it belongs in the integration layer.
- If no: it belongs in the orchestration layer.

---

## What belongs where

### Integration layer (this repo)

```
EventEnvelope           — message identity, delivery status, retry state
RetryPolicy             — max_attempts, backoff, max_backoff
DeliveryStatus          — PENDING → DISPATCHED → ACKNOWLEDGED → PROCESSED | FAILED
KafkaEventEnvelope      — Kafka-specific headers, partition key
SQSEventEnvelope        — SQS message attributes, FIFO deduplication ID
AzureServiceBusEnvelope — session ID, scheduled delivery time
SyncBoundary            — field authority for bi-directional sync
SyncConflict            — conflict detection and resolution
```

### Orchestration layer (application code / workflow engine)

```
WorkflowState           — current step, input data, compensation actions
ApprovalCheckpoint      — who must approve before step N can run
EscalationPolicy        — conditions under which the process goes to human review
CompensationAction      — what to undo if step N fails after step N-1 succeeded
SagaCoordinator         — tracks multi-step transactions with rollback capability
```

### What does not belong in integration layer

The integration layer must not contain:
- Workflow state (which step are we on)
- Business rule evaluation ("can a student withdraw after the deadline?")
- Approval routing ("route to finance director for amounts > $50k")
- Human escalation logic ("if confidence < 75%, route to advisor")
- SLA tracking ("this must complete within 2 business days")

These belong in the application code or workflow engine. If they appear in the
integration layer, they create coupling between transport concerns and business
logic that makes both harder to test and change independently.

---

## Approval and escalation checkpoints

Approval checkpoints are an orchestration concern, not an integration concern.

**Wrong:**
```python
def handle_lead_converted(envelope: EventEnvelope) -> None:
    # Wrong: business approval logic in transport handler
    if envelope.payload["amount"] > 50_000:
        route_to_finance_director(envelope)
        return
    apply_crm_update(envelope)
```

**Correct:**
```python
# Integration layer: deliver the event, apply transport retry logic
def handle_lead_converted(envelope: EventEnvelope) -> None:
    apply_crm_update(envelope)  # pure transport concern
    emit(CRMUpdateApplied(envelope.event_id, envelope.payload))

# Orchestration layer: decide next step based on workflow state
def on_crm_update_applied(event: CRMUpdateApplied) -> None:
    if event.payload["amount"] > 50_000:
        workflow.escalate("finance_director_review")
    else:
        workflow.advance_to_next_step()
```

The integration handler does not know the amount threshold. The orchestration
handler does not know the retry policy.

---

## Interface between layers

The integration layer communicates with the orchestration layer by emitting
domain events (not by calling orchestration functions directly):

```
Integration layer emits:    CRMUpdateApplied, SyncConflictDetected, DeadLettered
Orchestration layer emits:  ProcessApproved, ProcessEscalated, StepRetryRequested
```

Neither layer directly calls the other's internal functions. This prevents
coupling between transport mechanics and business process state.

---

## Consequences

### Accepted

- Two separate layers to maintain. Teams must know which layer a concern belongs to.
- Integration events are lightweight (envelope only). They do not carry full workflow
  context. The orchestration layer must look up workflow state from its own store.

### Rejected alternatives

**Stateful integration handlers:** Integration handlers that hold workflow state directly
(e.g., a saga coordinator inside a Kafka consumer). This works until the consumer
restarts — at which point workflow state is lost unless it is persisted separately.
Persisting workflow state in the integration layer creates the same coupling we are
avoiding.

**Orchestration-aware envelopes:** Adding workflow fields (step_id, approval_required,
compensation_url) to EventEnvelope. This creates a versioning problem: every change
to the workflow requires a change to the envelope schema and all consumers that parse it.

---

## Related

- `src/integration_automation_patterns/event_envelope.py`
- `src/integration_automation_patterns/saga.py` — saga compensation pattern
- `src/integration_automation_patterns/sync_boundary.py` — bi-directional sync boundary
- ADR 001 — idempotency key design
- Implementation Note 04 — observability and recovery
