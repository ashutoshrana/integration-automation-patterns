# Implementation Note 09: Process Manager Pattern for Distributed Transaction Coordination

Example scope: snippets below include application wiring and internal excerpts from
[`examples/11_process_manager.py`](../examples/11_process_manager.py); undefined names are supplied by that example or the application.
They are not standalone package APIs. See the linked module for complete setup.


## Problem

The saga reference (see [`03_saga_compensation.py`](../examples/03_saga_compensation.py))
illustrates compensating steps. A process manager makes step state, checkpoints and
compensation records explicit. More complex production workflows may also need
branching and external approvals; this example executes an ordered step list.

A Process Manager solves the coordination problem that a Saga cannot:

| Concern | Saga | Process Manager |
|---------|------|----------------|
| Step sequencing | Example-specific | Ordered list with explicit execution state |
| Compensation ordering | Implicit (reverse chain) | Explicit LIFO registry |
| Crash recovery | Requires retained state | Resume from a caller-retained checkpoint |
| SLA enforcement | External responsibility | Built-in per-step timeout detection |
| Audit trail | Application-level | Structured application records; caller persists |

## Architecture

```
ProcessManager
    │
    ├── CompensatingTransactionRegistry  ← LIFO stack of completed forward steps
    │   └── CompletedStepRecord          ← step + result + idempotency key
    │
    ├── ProcessCheckpoint                ← serializable snapshot; caller supplies persistence
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
from itertools import count
clock = count(start=0.0, step=100.0)  # enough ticks for all manager clock calls
manager = ProcessManager("t", steps, now_fn=lambda: next(clock))
```

## Checkpoint Design

A checkpoint is assigned to the supplied dictionary **before** each step execution:

```
Step 0: write checkpoint {next_step_index=0}  → execute step 0
Step 1: write checkpoint {next_step_index=1}  → execute step 1
...
```

If the process crashes during step 1's execution, the checkpoint at `next_step_index=1` allows resumption at step 1. Combined with idempotency keys, re-executing step 1 is safe — the downstream system returns the same result for the same key.

**Checkpointing is not an at-most-once guarantee:** the example stores snapshots in an
in-memory dictionary. Persisting and restoring them is the application's responsibility;
a crash can lose the dictionary and step re-execution is possible on resume. Steps must be idempotent, or the system must check whether the step completed before the crash.

```python
# Resume restores the registry from checkpoint snapshot
# so that if the resumed execution fails, LIFO compensation
# correctly covers all steps (including pre-crash ones).
# Load an existing complete checkpoint from the application's persisted snapshot.
checkpoint = ProcessCheckpoint.from_dict(saved_checkpoint_data)
audit = manager.resume_from_checkpoint(checkpoint)
```

The `registry_snapshot` field is a plain list of dicts — serializable to JSON, Postgres, or any document store. Write atomically with your transaction log.

## Audit Record Requirements

The `ProcessManagerAuditRecord` captures:

- **Forward step log:** step ID, name, outcome (SUCCESS/FAILURE/TIMEOUT), idempotency key, action result, duration
- **Compensation log:** step ID, name, outcome, duration
- **Process metadata:** process ID, type, final state, started/finished timestamps, duration

Retain forward and compensation outcomes with separate labels so reviewers can
reconstruct their order. This example does not establish SOX compliance.

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
from itertools import count
t = count(start=0.0, step=8.0)
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
