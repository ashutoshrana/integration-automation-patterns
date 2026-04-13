"""
08_approval_workflow.py — Multi-step approval workflow with SLA deadlines,
escalation, and quorum decisions.

Demonstrates the enterprise approval workflow pattern for human-in-loop
business processes — a ubiquitous requirement for procurement, contract
management, change management, and financial authorization in regulated enterprises.

    Pattern 1 — ApprovalWorkflow: Orchestrates sequential approval steps.
                Each step has a designated approver, an SLA deadline, and an
                escalation path that fires automatically on timeout.

    Pattern 2 — ApprovalGate: Individual approval step with structured
                outcome tracking (approved/rejected/escalated/timeout).

    Pattern 3 — QuorumApprovalGate: Variant that requires N-of-M approvers.
                Decision is APPROVED if at least ``quorum`` approvers approve.
                Decision is REJECTED if enough approvers reject to make quorum
                mathematically impossible.

    Pattern 4 — ApprovalAuditRecord: Complete audit trail for each workflow
                instance — captures approver, decision, reason, timestamp, and
                SLA compliance status. Required for SOX, FAR, and internal audit.

Scenarios
---------

  A. Happy path — 3-step sequential contract approval:
     Procurement Officer → Legal Counsel → CFO. All three approve.
     Full audit trail with SLA compliance recorded.

  B. Rejection at Step 2 — legal rejects a non-standard contract clause:
     Workflow terminates at Step 2 with REJECTED outcome.
     Remaining steps skipped; termination reason preserved in audit.

  C. Timeout → auto-escalation — procurement manager misses 48h SLA:
     Step 1 timeout fires; request auto-escalates to Deputy Director.
     Deputy approves; workflow continues from Step 2.

  D. Quorum approval — high-value contract ($2.4M) requires 2-of-3 VP sign-off:
     VP_Finance approves, VP_Legal approves (quorum met = 2/3).
     Third approver (VP_Operations) response not needed — workflow proceeds.

No external dependencies required.

Run:
    python examples/08_approval_workflow.py
"""

from __future__ import annotations

import sys
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable
from uuid import uuid4

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------


class ApprovalOutcome(str, Enum):
    """Outcome of an individual approval gate."""

    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    ESCALATED = "ESCALATED"
    TIMEOUT = "TIMEOUT"
    PENDING = "PENDING"
    SKIPPED = "SKIPPED"


class WorkflowStatus(str, Enum):
    """Overall status of an approval workflow instance."""

    IN_PROGRESS = "IN_PROGRESS"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    ESCALATED = "ESCALATED"
    TIMED_OUT = "TIMED_OUT"


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class ApprovalDecision:
    """
    Structured record of a single approver's decision on an approval gate.

    Captures the minimum fields required for SOX and FAR audit compliance:
    who decided, what they decided, when, and why.
    """

    decision_id: str = field(default_factory=lambda: str(uuid4()))
    approver_id: str = ""
    outcome: ApprovalOutcome = ApprovalOutcome.PENDING
    reason: str = ""
    decided_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    sla_deadline: datetime | None = None
    sla_met: bool = True

    @property
    def sla_compliance(self) -> str:
        if self.sla_deadline is None:
            return "N/A"
        return "MET" if self.decided_at <= self.sla_deadline else "MISSED"


@dataclass
class ApprovalAuditRecord:
    """
    Complete audit record for an approval workflow instance.

    Audit records should be retained per the organization's record retention
    policy (typically 7 years for financial approvals per SOX; FAR 4.703 for
    government contracts). Records must be tamper-evident and stored separately
    from the approval workflow engine.
    """

    workflow_id: str
    subject: str
    requestor: str
    requested_amount: float | None
    final_status: WorkflowStatus
    steps: list[dict[str, Any]] = field(default_factory=list)
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: datetime | None = None
    escalation_count: int = 0

    def to_summary(self) -> str:
        duration = (
            f"{(self.completed_at - self.started_at).total_seconds():.1f}s"
            if self.completed_at
            else "ongoing"
        )
        return (
            f"Workflow {self.workflow_id[:8]} | {self.subject[:40]} | "
            f"Status: {self.final_status.value} | Steps: {len(self.steps)} | "
            f"Escalations: {self.escalation_count} | Duration: {duration}"
        )


# ---------------------------------------------------------------------------
# ApprovalGate — single approver step
# ---------------------------------------------------------------------------


class ApprovalGate:
    """
    Single-approver approval gate with SLA deadline and escalation path.

    Design decisions:
    - The ``decision_fn`` is a callable that receives the gate context and
      returns an ``ApprovalDecision``. In production this is an async call to
      an approval service; in tests it is a deterministic stub.
    - Timeout is simulated by the test via a decision with ``outcome=TIMEOUT``.
    - Escalation is modelled as a new ``ApprovalGate`` with a different
      approver, created from the ``escalation_fn`` callable. This keeps the
      escalation path open-ended — escalation can chain indefinitely.

    Args:
        step_name: Human-readable name for this approval step.
        approver_id: Identity of the designated approver.
        sla_hours: Hours within which a decision is expected.
        escalation_fn: Callable returning a new ApprovalGate for escalation.
            If None, timeout results in workflow TIMED_OUT status.
    """

    def __init__(
        self,
        step_name: str,
        approver_id: str,
        sla_hours: int = 48,
        escalation_fn: Callable[[], "ApprovalGate"] | None = None,
    ) -> None:
        self.step_name = step_name
        self.approver_id = approver_id
        self.sla_hours = sla_hours
        self.escalation_fn = escalation_fn
        self._decision: ApprovalDecision | None = None

    def request_approval(
        self,
        context: dict[str, Any],
        decision_fn: Callable[["ApprovalGate", dict[str, Any]], ApprovalDecision],
        now: datetime | None = None,
    ) -> ApprovalDecision:
        """
        Request an approval decision from the designated approver.

        Args:
            context: Workflow context (contract details, requestor info, etc.)
            decision_fn: Callable that produces the approver's decision.
            now: Override for "current time" (for testing).

        Returns:
            The ``ApprovalDecision`` from the approver.
        """
        _now = now or datetime.now(timezone.utc)
        sla_deadline = _now + timedelta(hours=self.sla_hours)

        decision = decision_fn(self, context)
        decision.sla_deadline = sla_deadline
        decision.sla_met = decision.decided_at <= sla_deadline
        self._decision = decision
        return decision

    @property
    def decision(self) -> ApprovalDecision | None:
        return self._decision


# ---------------------------------------------------------------------------
# QuorumApprovalGate — N-of-M approvers
# ---------------------------------------------------------------------------


class QuorumApprovalGate:
    """
    Multi-approver gate that requires ``quorum`` approvals out of ``len(approvers)``.

    The gate is APPROVED as soon as ``quorum`` approvals are received.
    The gate is REJECTED as soon as enough rejections are received that quorum
    is mathematically impossible (i.e., remaining approvers < quorum - approvals_so_far).

    Args:
        step_name: Human-readable name for this approval step.
        approver_ids: List of approver identities to solicit.
        quorum: Minimum number of approvals required.
        sla_hours: Per-approver SLA in hours.
    """

    def __init__(
        self,
        step_name: str,
        approver_ids: list[str],
        quorum: int,
        sla_hours: int = 72,
    ) -> None:
        self.step_name = step_name
        self.approver_ids = approver_ids
        self.quorum = quorum
        self.sla_hours = sla_hours
        self._decisions: list[ApprovalDecision] = []

    def request_approvals(
        self,
        context: dict[str, Any],
        decision_fn: Callable[[str, dict[str, Any]], ApprovalDecision],
        now: datetime | None = None,
    ) -> tuple[ApprovalOutcome, list[ApprovalDecision]]:
        """
        Solicit approvals from all approvers until quorum is met or impossible.

        Args:
            context: Workflow context.
            decision_fn: Callable(approver_id, context) → ApprovalDecision.
            now: Override for current time (for testing).

        Returns:
            Tuple of (overall_outcome, all_decisions_collected).
        """
        _now = now or datetime.now(timezone.utc)
        sla_deadline = _now + timedelta(hours=self.sla_hours)
        approvals = 0
        rejections = 0
        remaining = len(self.approver_ids)

        for approver_id in self.approver_ids:
            decision = decision_fn(approver_id, context)
            decision.sla_deadline = sla_deadline
            decision.sla_met = decision.decided_at <= sla_deadline
            self._decisions.append(decision)
            remaining -= 1

            if decision.outcome == ApprovalOutcome.APPROVED:
                approvals += 1
            elif decision.outcome == ApprovalOutcome.REJECTED:
                rejections += 1

            # Quorum met — no need to wait for remaining approvers
            if approvals >= self.quorum:
                # Mark remaining as SKIPPED
                for skipped_id in self.approver_ids[len(self._decisions):]:
                    skipped = ApprovalDecision(
                        approver_id=skipped_id,
                        outcome=ApprovalOutcome.SKIPPED,
                        reason=f"Quorum ({self.quorum}/{len(self.approver_ids)}) already met",
                    )
                    self._decisions.append(skipped)
                return ApprovalOutcome.APPROVED, self._decisions

            # Quorum impossible — too many rejections
            if rejections > len(self.approver_ids) - self.quorum:
                for skipped_id in self.approver_ids[len(self._decisions):]:
                    skipped = ApprovalDecision(
                        approver_id=skipped_id,
                        outcome=ApprovalOutcome.SKIPPED,
                        reason="Quorum mathematically impossible",
                    )
                    self._decisions.append(skipped)
                return ApprovalOutcome.REJECTED, self._decisions

        # All approvers responded — final tally
        outcome = ApprovalOutcome.APPROVED if approvals >= self.quorum else ApprovalOutcome.REJECTED
        return outcome, self._decisions

    @property
    def decisions(self) -> list[ApprovalDecision]:
        return list(self._decisions)


# ---------------------------------------------------------------------------
# ApprovalWorkflow — orchestrator
# ---------------------------------------------------------------------------


class ApprovalWorkflow:
    """
    Sequential multi-step approval workflow orchestrator.

    Processes ``ApprovalGate`` and ``QuorumApprovalGate`` steps in order.
    Each step's outcome determines whether the workflow continues:
    - APPROVED → proceed to next step
    - REJECTED → terminate with REJECTED status; remaining steps skipped
    - ESCALATED → replace this step with the escalation gate; retry
    - TIMEOUT → if no escalation, terminate with TIMED_OUT status

    Args:
        workflow_id: Unique identifier for this workflow instance.
        subject: Human-readable description of what is being approved.
        requestor: The person/system requesting approval.
        requested_amount: Optional dollar amount (triggers quorum threshold logic).
    """

    def __init__(
        self,
        workflow_id: str,
        subject: str,
        requestor: str,
        requested_amount: float | None = None,
    ) -> None:
        self.workflow_id = workflow_id
        self.subject = subject
        self.requestor = requestor
        self.requested_amount = requested_amount
        self._steps: list[ApprovalGate | QuorumApprovalGate] = []
        self._audit = ApprovalAuditRecord(
            workflow_id=workflow_id,
            subject=subject,
            requestor=requestor,
            requested_amount=requested_amount,
            final_status=WorkflowStatus.IN_PROGRESS,
        )

    def add_step(self, gate: ApprovalGate | QuorumApprovalGate) -> "ApprovalWorkflow":
        self._steps.append(gate)
        return self

    def execute(
        self,
        context: dict[str, Any],
        gate_decision_fn: Callable[[ApprovalGate, dict[str, Any]], ApprovalDecision],
        quorum_decision_fn: Callable[[str, dict[str, Any]], ApprovalDecision],
    ) -> ApprovalAuditRecord:
        """
        Execute all approval steps in sequence.

        Args:
            context: Workflow context passed to each gate.
            gate_decision_fn: Callable for single-approver gates.
            quorum_decision_fn: Callable(approver_id, context) for quorum gates.

        Returns:
            Completed ``ApprovalAuditRecord``.
        """
        for gate in self._steps:
            if isinstance(gate, QuorumApprovalGate):
                outcome, decisions = gate.request_approvals(context, quorum_decision_fn)
                step_record: dict[str, Any] = {
                    "step": gate.step_name,
                    "type": "quorum",
                    "quorum": f"{gate.quorum}/{len(gate.approver_ids)}",
                    "outcome": outcome.value,
                    "decisions": [
                        {
                            "approver": d.approver_id,
                            "outcome": d.outcome.value,
                            "reason": d.reason,
                            "sla": d.sla_compliance,
                        }
                        for d in decisions
                    ],
                }
                self._audit.steps.append(step_record)

                if outcome == ApprovalOutcome.REJECTED:
                    self._audit.final_status = WorkflowStatus.REJECTED
                    self._audit.completed_at = datetime.now(timezone.utc)
                    return self._audit
                # APPROVED → continue

            else:
                # Single-approver gate
                decision = gate.request_approval(context, gate_decision_fn)
                step_record = {
                    "step": gate.step_name,
                    "type": "sequential",
                    "approver": gate.approver_id,
                    "outcome": decision.outcome.value,
                    "reason": decision.reason,
                    "sla": decision.sla_compliance,
                }

                if decision.outcome == ApprovalOutcome.TIMEOUT and gate.escalation_fn:
                    # Escalate: replace gate with escalation gate
                    self._audit.escalation_count += 1
                    escalation_gate = gate.escalation_fn()
                    step_record["escalated_to"] = escalation_gate.approver_id
                    self._audit.steps.append(step_record)

                    escalation_decision = escalation_gate.request_approval(
                        context, gate_decision_fn
                    )
                    esc_record: dict[str, Any] = {
                        "step": f"{gate.step_name} (escalation)",
                        "type": "escalation",
                        "approver": escalation_gate.approver_id,
                        "outcome": escalation_decision.outcome.value,
                        "reason": escalation_decision.reason,
                        "sla": escalation_decision.sla_compliance,
                    }
                    self._audit.steps.append(esc_record)
                    decision = escalation_decision

                elif decision.outcome == ApprovalOutcome.TIMEOUT:
                    step_record["final"] = True
                    self._audit.steps.append(step_record)
                    self._audit.final_status = WorkflowStatus.TIMED_OUT
                    self._audit.completed_at = datetime.now(timezone.utc)
                    return self._audit
                else:
                    self._audit.steps.append(step_record)

                if decision.outcome == ApprovalOutcome.REJECTED:
                    self._audit.final_status = WorkflowStatus.REJECTED
                    self._audit.completed_at = datetime.now(timezone.utc)
                    return self._audit

                # APPROVED or post-escalation APPROVED → continue

        self._audit.final_status = WorkflowStatus.APPROVED
        self._audit.completed_at = datetime.now(timezone.utc)
        return self._audit


# ---------------------------------------------------------------------------
# Main — scenarios
# ---------------------------------------------------------------------------


def main() -> None:
    print("=" * 68)
    print("Enterprise Approval Workflow Patterns")
    print("  Patterns : ApprovalWorkflow | ApprovalGate | QuorumApprovalGate")
    print("  Context  : Government contract procurement approval")
    print("=" * 68)

    # ------------------------------------------------------------------
    # Scenario A — Happy path: 3-step sequential approval
    # ------------------------------------------------------------------
    print("\n--- Scenario A: Happy path — sequential 3-step approval ---")

    def all_approve(gate: ApprovalGate, context: dict) -> ApprovalDecision:
        return ApprovalDecision(
            approver_id=gate.approver_id,
            outcome=ApprovalOutcome.APPROVED,
            reason="Contract terms meet procurement standards.",
        )

    wf_a = (
        ApprovalWorkflow(
            workflow_id=str(uuid4()),
            subject="Contract W81XWH-26-C-0012 — $840K Research Services",
            requestor="contracting_officer_jones",
            requested_amount=840_000,
        )
        .add_step(ApprovalGate("Procurement Review", "procurement_officer", sla_hours=24))
        .add_step(ApprovalGate("Legal Review", "legal_counsel", sla_hours=48))
        .add_step(ApprovalGate("CFO Approval", "cfo_williams", sla_hours=24))
    )
    audit_a = wf_a.execute(
        context={"contract_value": 840_000, "vendor": "Apex Research LLC"},
        gate_decision_fn=all_approve,
        quorum_decision_fn=lambda approver_id, ctx: ApprovalDecision(
            approver_id=approver_id, outcome=ApprovalOutcome.APPROVED
        ),
    )

    print(f"\n  {audit_a.to_summary()}")
    for step in audit_a.steps:
        print(f"    [{step['outcome']:10s}] {step['step']} — approver: {step.get('approver', 'quorum')} — SLA: {step.get('sla', 'N/A')}")

    # ------------------------------------------------------------------
    # Scenario B — Rejection at Step 2
    # ------------------------------------------------------------------
    print("\n--- Scenario B: Step 2 rejection — legal rejects non-standard clause ---")

    def reject_at_legal(gate: ApprovalGate, context: dict) -> ApprovalDecision:
        if gate.approver_id == "legal_counsel":
            return ApprovalDecision(
                approver_id=gate.approver_id,
                outcome=ApprovalOutcome.REJECTED,
                reason=(
                    "Section 12.3 (Limitation of Liability) contains a non-standard "
                    "cap of $50K — must match contract value ($840K). Renegotiate."
                ),
            )
        return ApprovalDecision(
            approver_id=gate.approver_id,
            outcome=ApprovalOutcome.APPROVED,
            reason="Approved — no issues.",
        )

    wf_b = (
        ApprovalWorkflow(
            workflow_id=str(uuid4()),
            subject="Contract W81XWH-26-C-0013 — $840K — Non-standard liability clause",
            requestor="contracting_officer_jones",
            requested_amount=840_000,
        )
        .add_step(ApprovalGate("Procurement Review", "procurement_officer", sla_hours=24))
        .add_step(ApprovalGate("Legal Review", "legal_counsel", sla_hours=48))
        .add_step(ApprovalGate("CFO Approval", "cfo_williams", sla_hours=24))
    )
    audit_b = wf_b.execute(
        context={"contract_value": 840_000, "vendor": "Apex Research LLC"},
        gate_decision_fn=reject_at_legal,
        quorum_decision_fn=lambda approver_id, ctx: ApprovalDecision(
            approver_id=approver_id, outcome=ApprovalOutcome.APPROVED
        ),
    )

    print(f"\n  {audit_b.to_summary()}")
    for step in audit_b.steps:
        print(f"    [{step['outcome']:10s}] {step['step']} — {step.get('reason', '')[:60]}")

    # ------------------------------------------------------------------
    # Scenario C — Timeout → auto-escalation
    # ------------------------------------------------------------------
    print("\n--- Scenario C: Step 1 timeout → auto-escalation to Deputy Director ---")

    def timeout_then_escalation_approves(gate: ApprovalGate, context: dict) -> ApprovalDecision:
        if gate.approver_id == "procurement_officer":
            # Simulate no response within SLA
            return ApprovalDecision(
                approver_id=gate.approver_id,
                outcome=ApprovalOutcome.TIMEOUT,
                reason="No response within 48-hour SLA window.",
            )
        # Escalated approver (Deputy Director) approves
        return ApprovalDecision(
            approver_id=gate.approver_id,
            outcome=ApprovalOutcome.APPROVED,
            reason="Escalation approved — contract is within authority level.",
        )

    def make_escalation_gate() -> ApprovalGate:
        return ApprovalGate(
            "Procurement Review (Escalation)",
            "deputy_director",
            sla_hours=24,
        )

    wf_c = (
        ApprovalWorkflow(
            workflow_id=str(uuid4()),
            subject="Urgent Contract W81XWH-26-C-0014 — $420K Emergency Services",
            requestor="contracting_officer_davis",
            requested_amount=420_000,
        )
        .add_step(
            ApprovalGate(
                "Procurement Review",
                "procurement_officer",
                sla_hours=48,
                escalation_fn=make_escalation_gate,
            )
        )
        .add_step(ApprovalGate("Legal Review", "legal_counsel", sla_hours=48))
        .add_step(ApprovalGate("CFO Approval", "cfo_williams", sla_hours=24))
    )
    audit_c = wf_c.execute(
        context={"contract_value": 420_000, "vendor": "Swift Services Inc."},
        gate_decision_fn=timeout_then_escalation_approves,
        quorum_decision_fn=lambda approver_id, ctx: ApprovalDecision(
            approver_id=approver_id, outcome=ApprovalOutcome.APPROVED
        ),
    )

    print(f"\n  {audit_c.to_summary()}")
    for step in audit_c.steps:
        t = step.get("type", "sequential")
        approver = step.get("approver", "quorum")
        esc = f" [→ escalated to {step.get('escalated_to')}]" if step.get("escalated_to") else ""
        print(f"    [{step['outcome']:10s}] {step['step']}{esc}")

    # ------------------------------------------------------------------
    # Scenario D — Quorum: 2-of-3 VP approval for $2.4M contract
    # ------------------------------------------------------------------
    print("\n--- Scenario D: Quorum approval — 2-of-3 VP sign-off for $2.4M ---")

    vp_responses: dict[str, ApprovalOutcome] = {
        "vp_finance": ApprovalOutcome.APPROVED,
        "vp_legal": ApprovalOutcome.APPROVED,
        "vp_operations": ApprovalOutcome.APPROVED,  # will be SKIPPED — quorum met first
    }

    def quorum_decide(approver_id: str, context: dict) -> ApprovalDecision:
        outcome = vp_responses.get(approver_id, ApprovalOutcome.APPROVED)
        return ApprovalDecision(
            approver_id=approver_id,
            outcome=outcome,
            reason=f"{'Approved' if outcome == ApprovalOutcome.APPROVED else 'Rejected'} by {approver_id}",
        )

    wf_d = (
        ApprovalWorkflow(
            workflow_id=str(uuid4()),
            subject="Strategic Contract W81XWH-26-C-0020 — $2.4M Multi-Year Research",
            requestor="contracting_officer_jones",
            requested_amount=2_400_000,
        )
        .add_step(ApprovalGate("Procurement Review", "procurement_officer", sla_hours=24))
        .add_step(
            QuorumApprovalGate(
                "Executive Approval (2-of-3)",
                approver_ids=["vp_finance", "vp_legal", "vp_operations"],
                quorum=2,
                sla_hours=72,
            )
        )
        .add_step(ApprovalGate("CFO Final Approval", "cfo_williams", sla_hours=24))
    )

    def sequential_approve(gate: ApprovalGate, context: dict) -> ApprovalDecision:
        return ApprovalDecision(
            approver_id=gate.approver_id,
            outcome=ApprovalOutcome.APPROVED,
            reason="Contract approved.",
        )

    audit_d = wf_d.execute(
        context={"contract_value": 2_400_000, "vendor": "Meridian Research Group"},
        gate_decision_fn=sequential_approve,
        quorum_decision_fn=quorum_decide,
    )

    print(f"\n  {audit_d.to_summary()}")
    for step in audit_d.steps:
        if step.get("type") == "quorum":
            print(f"    [{step['outcome']:10s}] {step['step']} (quorum {step['quorum']})")
            for d in step.get("decisions", []):
                print(f"      [{d['outcome']:10s}] {d['approver']} — {d.get('reason', '')[:50]}")
        else:
            print(f"    [{step['outcome']:10s}] {step['step']} — approver: {step.get('approver')}")

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print("\n" + "=" * 68)
    print("Workflow Audit Summary")
    print("=" * 68)
    for audit in [audit_a, audit_b, audit_c, audit_d]:
        status_icon = {
            WorkflowStatus.APPROVED: "APPROVED",
            WorkflowStatus.REJECTED: "REJECTED",
            WorkflowStatus.TIMED_OUT: "TIMEOUT",
        }.get(audit.final_status, audit.final_status.value)
        print(f"  [{status_icon:10s}] {audit.subject[:55]}")
        if audit.escalation_count > 0:
            print(f"              Escalations: {audit.escalation_count}")
    print("=" * 68)


if __name__ == "__main__":
    main()
