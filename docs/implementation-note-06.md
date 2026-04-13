# Implementation Note 06 — Approval Workflows in Enterprise Integration: When Human Decision Is a First-Class Event

**Repo:** `integration-automation-patterns`
**Relates to:** [`examples/08_approval_workflow.py`](../examples/08_approval_workflow.py)
**ADR cross-reference:** [ADR-002 (saga boundaries)](adr/002-saga-boundaries.md), [ADR-003 (orchestration vs. integration boundaries)](adr/003-orchestration-vs-integration-boundaries.md)

---

## What approval workflows are — and where they fail in practice

An approval workflow is a saga where one or more steps produce no immediate output. A human is the resource. The system emits a request, waits for a decision, and the saga resumes when that decision arrives — or times out and escalates.

This sounds simple. In practice it breaks in several consistent ways:

**Failure mode 1: The approval step is stateless.** The system sends a notification email, stores nothing, and polls a "pending_approvals" table by hand. When the approver takes action, there is no link between the original request context and the decision record. Audit trails are reconstructed manually.

**Failure mode 2: Timeout is an afterthought.** SLA enforcement is added later as a cron job that scans for old records. The escalation path is hard-coded. When the SLA rule changes (48 hours → 24 hours for high-value contracts), the cron job, the database query, the email template, and the escalation target all need to change in different places.

**Failure mode 3: Multi-approver quorum is implicit.** The system sends requests to all approvers and checks `COUNT(approved) >= quorum` in the decision query. But partial decisions (two of three approved) are not a real intermediate state — they have no API representation, no SLA tracking, no audit record. When an approver changes their decision, the behavior is undefined.

**Failure mode 4: The workflow is sequential but the audit trail is flat.** SOX Section 404 and FAR 15.406 require that each approval step — who approved, what they were shown, when they decided, whether the SLA was met — is an independently reproducible record. A flat "approved_by" column does not satisfy this requirement.

The structural fix is the same in all four cases: **make the approval gate a first-class object** with its own identity, SLA clock, escalation policy, and decision record.

---

## The approval gate as a state machine

Every approval gate has three terminal states: `APPROVED`, `REJECTED`, `TIMED_OUT`. It has one non-terminal state: `PENDING`. The state transition logic is:

```
PENDING → APPROVED   (approver submits positive decision before SLA deadline)
PENDING → REJECTED   (approver submits negative decision at any time)
PENDING → TIMED_OUT  (decision not received before SLA deadline)
TIMED_OUT → (escalation handler called)
```

The critical design constraint: **the gate must not advance to TIMED_OUT silently.** When timeout occurs, an escalation function is called immediately. The escalation function decides what happens next — it might create a new gate with a different approver, log a compliance event, or raise an exception that terminates the workflow. This keeps timeout handling co-located with the gate definition, not scattered across cron jobs.

```python
gate = ApprovalGate(
    step_name="legal_review",
    approver_id="legal_team",
    sla_hours=48,
    escalation_fn=lambda ctx, decision: (
        send_escalation_alert(ctx, decision, escalate_to="general_counsel")
    ),
)
```

---

## Quorum gates: representing partial approval as a real state

When a workflow step requires N-of-M approvals (budget approval requiring two of three VPs, contract requiring legal + finance + compliance), the quorum gate must:

1. Track each individual decision independently with its own `ApprovalDecision` record
2. Determine the final outcome (`APPROVED` when quorum is reached, `REJECTED` when enough rejections to make quorum impossible) without waiting for all approvers
3. Collect remaining decisions up to SLA deadline even if outcome is already known (for audit completeness)
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
2. Stops immediately on the first `REJECTED` or `TIMED_OUT` outcome
3. Collects a decision record for each completed gate
4. Returns a single `ApprovalAuditRecord` covering the entire workflow

The stop-on-first-rejection rule is the correct default for compliance workflows. SOX and FAR workflows do not "continue and override" a legal review rejection — the workflow terminates, the rejection is recorded, and the initiator must restart with a revised request.

For workflows where a rejection at one step should escalate to a higher step (rather than terminate), this is modeled as **a new workflow** with a different gate configuration, not as a bypass flag on the original workflow. This keeps the audit trail clean: the original rejection is immutable, and the escalated workflow has its own identity.

---

## The `ApprovalAuditRecord`: what SOX and FAR require

SOX Section 404 and FAR Subpart 15.4 both require that approval records be independently reproducible without reference to application state. This means the audit record must be self-contained:

| Field | Why it's required |
|---|---|
| `workflow_id` | Correlates the record to the business transaction; must survive application restarts |
| `workflow_name` | Human-readable identifier for the audit reviewer |
| `context` | Full context snapshot at time of workflow execution — recreating this from live data is not permissible |
| `gate_decisions` | Per-step records, ordered; each includes approver, outcome, reason, timestamp, SLA deadline, SLA compliance |
| `final_outcome` | Terminal state of the workflow as a whole |
| `total_sla_met` | Whether the workflow-level SLA was met (not just individual gate SLAs) |
| `initiated_at` / `completed_at` | Full wall-clock duration for process analytics and SLA reporting |

The key constraint: **none of these fields should be computed from live application state at audit time.** The `ApprovalAuditRecord` is written once when the workflow completes and never modified.

---

## Integration with the saga pattern and transactional outbox

In a full enterprise integration pipeline, an approval workflow is typically embedded inside a saga that also coordinates database writes and external service calls. The integration points:

**Before the approval workflow:** The saga has already validated the request and written a "pending approval" record to the database via the transactional outbox. If the system crashes before the approval workflow starts, the outbox record ensures the approval request is re-emitted.

**During the approval workflow:** No database mutations. The approval gate state is held in memory (or in a dedicated approval workflow state store if the workflow spans multiple JVM instances). Approval decisions arrive as external events.

**After the approval workflow:** If the final outcome is `APPROVED`, the saga resumes its next compensatable step. If the final outcome is `REJECTED` or `TIMED_OUT`, the saga executes its compensation chain.

The handoff between the approval workflow and the saga is an event: `ApprovalWorkflowCompletedEvent` containing the `ApprovalAuditRecord`. The saga subscribes to this event type, looks up the correlation ID, and resumes.

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
# Production use: no 'now' — gate uses datetime.now(UTC)
decision = gate.request_approval(context, approver_fn)

# Test use: inject a time after SLA deadline to trigger timeout
decision = gate.request_approval(
    context,
    approver_fn,
    now=datetime.now(UTC) + timedelta(hours=49),  # past 48hr SLA
)
assert decision.outcome == ApprovalOutcome.TIMED_OUT
assert not decision.sla_met
```

This keeps the SLA clock logic inside the gate (not in the test) while making it fully exercisable without sleeping.

---

## What this pattern does not cover

- **Persistent approval state across JVM restarts:** The `ApprovalGate` holds state in memory. For workflows that span hours or days, a durable state store (Camunda, Temporal, AWS Step Functions) is required. The pattern in this example is the domain model — the state store provides the persistence layer.
- **Approval request UI:** Generating approval request emails, Slack messages, or ServiceNow tickets is outside the gate's responsibility. The gate calls a decision function; the caller is responsible for wiring that function to the actual approval channel.
- **Concurrent approval decisions from the same approver:** The quorum gate does not de-duplicate multiple decisions from the same approver. In production, the approval channel should enforce one-decision-per-approver before calling `request_approvals`.
