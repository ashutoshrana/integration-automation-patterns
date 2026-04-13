# Implementation Note 09: Process Manager Pattern for Distributed Transaction Coordination

## Problem

The Saga pattern (see `05_order_fulfillment_saga.py`) handles **linear** sequences of compensating transactions: each step has exactly one successor and one compensation. But real enterprise workflows are rarely linear. An order fulfillment process might branch based on stock location, pause for manual approval at certain order values, or conditionally skip steps based on customer tier. Sagas express none of this — a saga is a chain, not a graph.

A Process Manager solves the coordination problem that a Saga cannot:

| Concern | Saga | Process Manager |
|---------|------|----------------|
| Step sequencing | Fixed linear chain | Explicit state machine (supports branching) |
| Compensation ordering | Implicit (reverse chain) | Explicit LIFO registry |
| Crash recovery | Restart from beginning | Resume from last checkpoint |
| SLA enforcement | External responsibility | Built-in per-step timeout detection |
| Audit trail | Application-level | Structural, SOX/audit-ready |

## Architecture

```
ProcessManager
    │
    ├── CompensatingTransactionRegistry  ← LIFO stack of completed forward steps
    │   └── CompletedStepRecord          ← step + result + idempotency key
    │
    ├── ProcessCheckpoint                ← durable snapshot for crash recovery
    │   └── (written before each step)
    │
    └── ProcessManagerAuditRecord        ← complete forward + compensation log
```

### State Machine

```
PENDING
  │
  └→ RUNNING
       ├→ COMPLETED        (all steps succeeded)
       └→ COMPENSATING
            ├→ COMPENSATED (all compensations succeeded)
            └→ FAILED      (a compensation step failed — manual intervention required)
```

The transition `RUNNING → COMPENSATING` is triggered by either a step **failure** (action returned `False`) or a step **SLA timeout** (elapsed > `timeout_seconds`). Both paths execute the same LIFO compensation cycle.

## LIFO Compensation: Why Order Matters

When three sequential steps complete — inventory reservation (S0), payment charge (S1), shipment creation (S2) — the business invariants require undoing them in reverse:

```
Forward:       S0 → S1 → S2 (S2 fails or times out)
Compensation:  undo S1 → undo S0   (LIFO: top of stack first)
```

Undoing S0 (inventory) before S1 (payment) would void a payment for inventory that no longer exists — leaving the system in an incoherent state. The LIFO ordering is not a convention; it is a correctness requirement derived from the step dependency graph.

```python
# CompensatingTransactionRegistry pops in LIFO order
while self._stack:
    record = self._stack.pop()           # most recently pushed first
    record.step.compensation(ctx, record.action_result)
```

Each compensation function must be **idempotent** — safe to call more than once. Compensation retries (on compensation failure) must not corrupt state.

## SLA Timeout Detection

The Process Manager measures wall-clock elapsed time around each forward action:

```python
step_start = self._now()
success, result = step.action(context)
elapsed = self._now() - step_start

if elapsed > step.timeout_seconds:
    outcome = StepOutcome.TIMEOUT      # → triggers compensation
```

**Critical design point:** the action may have partially committed side effects before the SLA was detected. In the shipment example, the shipment record was created before the 1 ms SLA was breached. The compensation chain unwinds inventory and payment, but the shipment may require a separate out-of-band cancellation (a `DeadLetterRouter` from `09_backpressure_retry.py` is the natural complement).

The `now_fn` parameter is injectable for deterministic tests:

```python
clock = iter([0.0, 100.0])             # second call returns 100 seconds later
manager = ProcessManager("t", steps, now_fn=lambda: next(clock))
```

## Checkpoint Design

A checkpoint is written **before** each step execution:

```
Step 0: write checkpoint {next_step_index=0}  → execute step 0
Step 1: write checkpoint {next_step_index=1}  → execute step 1
...
```

If the process crashes during step 1's execution, the checkpoint at `next_step_index=1` allows resumption at step 1. Combined with idempotency keys, re-executing step 1 is safe — the downstream system returns the same result for the same key.

**At-most-once semantics require idempotency:** the checkpoint guarantees at-most-once *checkpoint advance*, but step re-execution is possible on resume. Steps must be idempotent, or the system must check whether the step completed before the crash.

```python
# Resume restores the registry from checkpoint snapshot
# so that if the resumed execution fails, LIFO compensation
# correctly covers all steps (including pre-crash ones).
checkpoint = ProcessCheckpoint(
    next_step_index=2,
    registry_snapshot=[
        {"step_id": "s0", "action_result": {...}, ...},
        {"step_id": "s1", "action_result": {...}, ...},
    ],
)
audit = manager.resume_from_checkpoint(checkpoint)
```

The `registry_snapshot` field is a plain list of dicts — serializable to JSON, Postgres, or any document store. Write atomically with your transaction log.

## Audit Record Requirements

The `ProcessManagerAuditRecord` captures:

- **Forward step log:** step ID, name, outcome (SUCCESS/FAILURE/TIMEOUT), idempotency key, action result, duration
- **Compensation log:** step ID, name, outcome, duration
- **Process metadata:** process ID, type, final state, started/finished timestamps, duration

For SOX compliance, the compensation log must be retained separately from the forward log — auditors need to reconstruct exactly which compensating transactions were executed and in what order.

For FAR (US Federal Acquisition Regulation) and ITAR compliance, the process ID and idempotency keys are the correlation handles between the Process Manager log and the downstream service receipts.

## Relationship to Other Patterns

| Pattern | When to use |
|---------|-------------|
| Saga (`05_order_fulfillment_saga.py`) | Fixed linear sequences; no branching; no crash recovery required |
| **Process Manager** | Non-linear workflows; crash recovery required; explicit SLA enforcement |
| Approval Workflow (`08_approval_workflow.py`) | Multi-party human-in-the-loop steps; quorum requirements |
| Schema Evolution (`10_schema_evolution.py`) | Process definition changes across deployed versions |

A production system often composes all four: the Process Manager orchestrates the overall transaction, delegates approval gates to `ApprovalWorkflow`, uses `BackpressureController` to handle downstream pressure during compensation, and uses `SchemaRegistry` to validate the events emitted at each step.

## Testing

Inject `now_fn` to avoid real sleeps in timeout tests:

```python
# Simulates an 8-second elapsed time on the second clock call
t = iter([0.0, 8.0])
manager = ProcessManager("t", [sla_2s_step], now_fn=lambda: next(t))
audit = manager.execute(ctx)
assert audit.timed_out_steps == 1
```

LIFO ordering assertions verify compensation correctness:

```python
assert ctx["order"].index("comp_s1") < ctx["order"].index("comp_s0")
```

Checkpoint roundtrip tests verify crash recovery:

```python
data = checkpoint.serialize()
restored = ProcessCheckpoint.from_dict(data)
assert restored.next_step_index == checkpoint.next_step_index
```
