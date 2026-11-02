# Implementation Note 06 — Approval Workflows in Enterprise Integration: When Human Decision Is a First-Class Event

Example scope: snippets below include application wiring and internal excerpts from
[`examples/08_approval_workflow.py`](../examples/08_approval_workflow.py); undefined names are supplied by that example or the application.
They are not standalone package APIs. See the linked module for complete setup.


**Repo:** `integration-automation-patterns`
**Relates to:** [`examples/08_approval_workflow.py`](../examples/08_approval_workflow.py)
**ADR cross-reference:** [ADR-003 (saga choreography vs. orchestration)](adr/003-saga-choreography-vs-orchestration.md), [ADR-003 (orchestration vs. integration boundaries)](adr/003-orchestration-vs-integration-boundaries.md)

---

## What approval workflows are — and where they fail in practice

An approval workflow is a saga where one or more steps produce no immediate output. A human is the resource. The system emits a request, waits for a decision, and the saga resumes when that decision arrives — or times out and escalates.

This sounds simple. In practice it breaks in several consistent ways:

**Failure mode 1: The approval step is stateless.** The system sends a notification email, stores nothing, and polls a "pending_approvals" table by hand. When the approver takes action, there is no link between the original request context and the decision record. Audit trails are reconstructed manually.

**Failure mode 2: Timeout is an afterthought.** SLA enforcement is added later as a cron job that scans for old records. The escalation path is hard-coded. When the SLA rule changes (48 hours → 24 hours for high-value contracts), the cron job, the database query, the email template, and the escalation target all need to change in different places.

**Failure mode 3: Multi-approver quorum is implicit.** The system sends requests to all approvers and checks `COUNT(approved) >= quorum` in the decision query. But partial decisions (two of three approved) are not a real intermediate state — they have no API representation, no SLA tracking, no audit record. When an approver changes their decision, the behavior is undefined.

**Failure mode 4: The workflow is sequential but the audit trail is flat.** Audit reviewers may need to reconstruct each approval step: who decided, what they saw, when they decided and whether the SLA was met. A flat "approved_by" column loses this context; this example does not establish legal compliance.

The structural fix is the same in all four cases: **make the approval gate a first-class object** with its own identity, SLA clock, escalation policy, and decision record.

---

## The approval gate as a state machine

Every approval gate has three terminal states: `APPROVED`, `REJECTED`, `TIMEOUT`. It has one non-terminal state: `PENDING`. The state transition logic is:

```
PENDING → APPROVED   (approver submits positive decision before SLA deadline)
PENDING → REJECTED   (approver submits negative decision at any time)
PENDING → TIMEOUT  (decision not received before SLA deadline)
TIMEOUT → (escalation handler called)
```

This diagram describes desired workflow outcomes. `ApprovalGate` itself calls a
synchronous decision callback and records whether `decided_at` meets the deadline;
it does not wait for external events or run a timer. `ApprovalWorkflow` handles
`TIMEOUT` outcomes and can call the gate's zero-argument escalation factory.

```python
# Load ApprovalGate from examples/08_approval_workflow.py (not a package export).
gate = ApprovalGate(
    step_name="legal_review",
    approver_id="legal_team",
    sla_hours=48,
    escalation_fn=lambda: ApprovalGate(
        step_name="escalated_legal_review", approver_id="general_counsel", sla_hours=24
    ),
)
```

---

## Quorum gates: representing partial approval as a real state

When a workflow step requires N-of-M approvals (budget approval requiring two of three VPs, contract requiring legal + finance + compliance), the quorum gate must:

1. Track each individual decision independently with its own `ApprovalDecision` record
2. Determine the final outcome (`APPROVED` when quorum is reached, `REJECTED` when enough rejections to make quorum impossible) without waiting for all approvers
3. Short-circuit when quorum is reached or impossible; record remaining approvers as `SKIPPED`
4. Report the final outcome and all individual decisions in a single `ApprovalAuditRecord`

The quorum short-circuit rule for rejection: if `(total_approvers - rejections_so_far) < quorum`, the quorum is mathematically impossible. The gate should return `REJECTED` immediately rather than waiting for the remaining approvers.

For a 2-of-3 quorum:
- After 2 approvals: `APPROVED` (quorum met)
- After 2 rejections: `REJECTED` (3 - 2 = 1 remaining, cannot reach quorum of 2)
- After 1 approval + 1 rejection with 1 pending: wait (1 + 1 = 2, quorum still reachable)

---

## Sequential orchestration: the `ApprovalWorkflow`

An `ApprovalWorkflow` is a sequential saga where each step is either an `ApprovalGate` or a `QuorumApprovalGate`. The orchestrator:

1. Executes gates in order, passing the same `context` object to each
2. Stops immediately on the first `REJECTED` or `TIMEOUT` outcome
3. Collects a decision record for each completed gate
4. Returns a single `ApprovalAuditRecord` covering the entire workflow

This example chooses a stop-on-first-rejection policy: the workflow terminates and records the rejection. A revised request starts a new workflow. This is an application design choice, not a universal SOX or FAR requirement; production rejection and escalation rules must reflect the organization's approved policy and applicable obligations.

For workflows where a rejection at one step should escalate to a higher step (rather than terminate), this is modeled as **a new workflow** with a different gate configuration, not as a bypass flag on the original workflow. This keeps the audit trail clean: the original rejection is retained, and the escalated workflow has its own identity.

---

## The `ApprovalAuditRecord`: suggested review fields

Use a self-contained snapshot when reviewers need to reconstruct approval context.
The following fields are design choices in this example, not a statutory checklist:

| Field | Review purpose |
|---|---|
| `workflow_id` | Correlates the record to the business transaction; must survive application restarts |
| `workflow_name` | Human-readable identifier for the audit reviewer |
| `context` | Full context snapshot at time of workflow execution — recreating this from live data is not permissible |
| `gate_decisions` | Per-step records, ordered; each includes approver, outcome, reason, timestamp, SLA deadline, SLA compliance |
| `final_outcome` | Terminal state of the workflow as a whole |
| `total_sla_met` | Whether the workflow-level SLA was met (not just individual gate SLAs) |
| `initiated_at` / `completed_at` | Full wall-clock duration for process analytics and SLA reporting |

The key constraint: **none of these fields should be computed from live application state at audit time.** The workflow returns an `ApprovalAuditRecord`; the application must persist it and
enforce retention and access controls. The Python object is mutable.

---

## Integration with the saga pattern and transactional outbox

In a full enterprise integration pipeline, an approval workflow is typically embedded inside a saga that also coordinates database writes and external service calls. The integration points:

**Before the approval workflow:** The saga has already validated the request and written a "pending approval" record to the database via the transactional outbox. If the system crashes before the approval workflow starts, the outbox record ensures the approval request is re-emitted.

**During the approval workflow:** No database mutations. The approval gate state is held in memory (or in a dedicated approval workflow state store if the workflow spans multiple Python processes). Approval decisions arrive as external events.

**After the approval workflow:** If the final outcome is `APPROVED`, the saga resumes its next compensatable step. If the final outcome is `REJECTED` or `TIMEOUT`, the saga executes its compensation chain.

A production handoff can use an application-defined event: `ApprovalWorkflowCompletedEvent` containing the `ApprovalAuditRecord`. The saga subscribes to this event type, looks up the correlation ID, and resumes.

---

## When to use quorum vs. sequential approval

**Use sequential gates when:**
- Each approver needs the previous approver's decision context before they can evaluate ("legal approval required before CFO review")
- The approval chain has a strict hierarchy (manager → director → VP — each approver can see what the previous one decided and why)
- A single rejection at any step definitively terminates the request

**Use quorum gates when:**
- Approvers are peers reviewing the same question independently ("any two of three budget committee members")
- Approver availability is uncertain — waiting for a specific individual introduces a single point of failure
- The approval is advisory rather than authoritative — the outcome represents consensus, not a chain of authority

**Avoid mixing both in the same gate:** A "majority of the committee, but the CEO must agree" pattern should be two separate sequential steps — a quorum gate (committee) followed by a single-approver gate (CEO) — not a single compound gate. This keeps each gate's decision record clean and independently auditable.

---

## Testing approval workflows

The deterministic-time injection pattern (`now` parameter on `request_approval`) is the core of approval workflow testing. Without it, tests that verify SLA behavior are either slow (sleep until timeout) or non-deterministic (race conditions against wall clock).

The pattern:

```python
from datetime import datetime, timedelta, timezone
UTC = timezone.utc
# ApprovalDecision and ApprovalOutcome come from the same example module.
# decision_fn returns the actual decision timestamp; now sets the request start.
started = datetime(2026, 1, 1, tzinfo=UTC)
def late_decision(gate, context):
    return ApprovalDecision(
        approver_id=gate.approver_id,
        outcome=ApprovalOutcome.TIMEOUT,
        decided_at=started + timedelta(hours=49),
        reason="No decision within SLA",
    )
decision = gate.request_approval({}, late_decision, now=started)
assert decision.outcome == ApprovalOutcome.TIMEOUT
assert not decision.sla_met
```

`ApprovalOutcome.TIMEOUT` is the gate enum; the workflow-level terminal status is
`WorkflowStatus.TIMED_OUT`.

This keeps the SLA clock logic inside the gate (not in the test) while making it fully exercisable without sleeping.

---

## What this pattern does not cover

- **Persistent approval state across Python restarts:** The `ApprovalGate` holds state in memory. For workflows that span hours or days, a durable state store (Camunda, Temporal, AWS Step Functions) is required. The pattern in this example is the domain model — the state store provides the persistence layer.
- **Approval request UI:** Generating approval request emails, Slack messages, or ServiceNow tickets is outside the gate's responsibility. The gate calls a decision function; the caller is responsible for wiring that function to the actual approval channel.
- **Concurrent approval decisions from the same approver:** The quorum gate does not de-duplicate multiple decisions from the same approver. In production, the approval channel should enforce one-decision-per-approver before calling `request_approvals`.
