"""
11_process_manager.py — Process Manager pattern for long-running distributed
business transactions with explicit state machine, compensating transaction
registry (LIFO), crash-recovery checkpointing, and per-step SLA timeouts.

The Process Manager pattern addresses distributed transaction coordination
beyond what a simple Saga can express. Where a Saga assumes a linear sequence
of steps, a Process Manager:

    - Maintains a full, inspectable state machine
    - Decouples step orchestration from step execution
    - Stores durable checkpoints enabling crash recovery
    - Executes compensations in LIFO order to ensure correct undo semantics
    - Enforces per-step SLA deadlines with automatic compensation triggering

Key components
--------------

  ProcessState          State machine lifecycle: PENDING → RUNNING → COMPLETED
                        or RUNNING → COMPENSATING → FAILED/COMPENSATED

  ProcessStep           Describes a forward action, its corresponding compensation
                        action, an SLA timeout, and an idempotency key.

  CompensatingTransactionRegistry
                        Tracks completed forward steps as a LIFO stack. On
                        process failure, pops and executes each compensation in
                        reverse order — critical for correct undo semantics when
                        earlier steps depend on later ones (e.g. do not deduct
                        inventory before reversing a payment).

  ProcessCheckpoint     Serializable snapshot of process state: completed steps,
                        process context, current step index. Written before each
                        step execution for crash recovery.

  ProcessManager        Orchestrates step execution, checkpoint writes, LIFO
                        compensation, and timeout detection.

  ProcessManagerAuditRecord
                        SOX/audit-compliant complete record of all forward and
                        compensation actions with timestamps and outcomes.

Scenarios
---------

  A. Successful order fulfillment:
     Reserve inventory → Charge payment → Create shipment → Confirm order.
     All four steps succeed. Process reaches COMPLETED.

  B. Payment failure mid-process:
     Reserve inventory succeeds; Charge payment fails.
     LIFO compensation: undo reservation. Process reaches COMPENSATED.

  C. Shipment step SLA timeout:
     Reserve succeeds; Charge succeeds; Shipment step times out (SLA exceeded).
     LIFO compensation: void payment, then unreserve inventory.
     Process reaches COMPENSATED with timeout_reason in audit record.

  D. Crash recovery: rehydrate from checkpoint and resume:
     Simulates a process that completed reservation and payment, crashed,
     then resumes from checkpoint to execute shipment and confirmation.

No external dependencies required.

Run:
    python examples/11_process_manager.py
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional


# ---------------------------------------------------------------------------
# State machine
# ---------------------------------------------------------------------------

class ProcessState(str, Enum):
    """Lifecycle states of a managed distributed process."""
    PENDING = "PENDING"                # Not yet started
    RUNNING = "RUNNING"                # Steps executing
    COMPENSATING = "COMPENSATING"      # Forward failure detected; undoing
    COMPLETED = "COMPLETED"            # All steps succeeded
    COMPENSATED = "COMPENSATED"        # Compensation complete; process failed cleanly
    FAILED = "FAILED"                  # Compensation itself failed (manual intervention)


class StepOutcome(str, Enum):
    """Result of a single step execution or compensation attempt."""
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    TIMEOUT = "TIMEOUT"
    SKIPPED = "SKIPPED"


# ---------------------------------------------------------------------------
# Step definition
# ---------------------------------------------------------------------------

@dataclass
class ProcessStep:
    """
    Defines one forward step of a distributed process and its inverse.

    Attributes
    ----------
    step_id:
        Unique identifier for this step within the process definition.
    name:
        Human-readable step name for audit and observability.
    action:
        Callable that executes the forward action. Receives the process
        context dict and returns (success: bool, result: Any).
    compensation:
        Callable that undoes the forward action. Receives the process
        context dict and the original action result; returns success: bool.
        Must be idempotent — may be called more than once on retry.
    timeout_seconds:
        SLA: maximum wall-clock seconds allowed for the forward action.
        Exceeded → StepOutcome.TIMEOUT → triggers compensation.
    idempotency_key_fn:
        Optional function deriving a per-invocation idempotency key from
        the process context. Callers can use this to deduplicate retries.
    """
    step_id: str
    name: str
    action: Callable[[dict[str, Any]], tuple[bool, Any]]
    compensation: Callable[[dict[str, Any], Any], bool]
    timeout_seconds: float = 30.0
    idempotency_key_fn: Optional[Callable[[dict[str, Any]], str]] = None

    def idempotency_key(self, context: dict[str, Any]) -> str:
        if self.idempotency_key_fn:
            return self.idempotency_key_fn(context)
        return f"{self.step_id}:{context.get('process_id', 'unknown')}"


# ---------------------------------------------------------------------------
# Compensating Transaction Registry (LIFO stack)
# ---------------------------------------------------------------------------

@dataclass
class CompletedStepRecord:
    """Record of a successfully completed forward step, stored for compensation."""
    step: ProcessStep
    action_result: Any
    completed_at: float  # Unix timestamp
    idempotency_key: str


class CompensatingTransactionRegistry:
    """
    LIFO stack of completed forward steps.

    Each successful forward step pushes a record onto the stack. On failure,
    the registry pops and executes compensations in reverse (LIFO) order,
    ensuring correct undo semantics for dependent steps.

    LIFO is critical: if Step 2 (payment) depends on Step 1 (inventory
    reservation), you must undo Step 2 before undoing Step 1 to avoid
    attempting to void a payment for inventory that no longer exists.
    """

    def __init__(self) -> None:
        self._stack: list[CompletedStepRecord] = []

    def push(self, record: CompletedStepRecord) -> None:
        """Register a successfully completed forward step."""
        self._stack.append(record)

    def compensate_all(
        self,
        context: dict[str, Any],
        audit_log: list[dict[str, Any]],
    ) -> bool:
        """
        Execute all compensations in LIFO order.

        Returns True if all compensations succeeded; False if any failed
        (process enters FAILED state requiring manual intervention).
        """
        all_succeeded = True
        while self._stack:
            record = self._stack.pop()
            step = record.step
            start = time.monotonic()
            try:
                succeeded = step.compensation(context, record.action_result)
            except Exception as exc:  # noqa: BLE001
                succeeded = False
                audit_log.append({
                    "type": "compensation_exception",
                    "step_id": step.step_id,
                    "step_name": step.name,
                    "error": str(exc),
                    "timestamp": time.time(),
                })
            duration = time.monotonic() - start
            audit_log.append({
                "type": "compensation",
                "step_id": step.step_id,
                "step_name": step.name,
                "outcome": StepOutcome.SUCCESS.value if succeeded else StepOutcome.FAILURE.value,
                "duration_s": round(duration, 4),
                "timestamp": time.time(),
            })
            if not succeeded:
                all_succeeded = False
        return all_succeeded

    @property
    def depth(self) -> int:
        """Number of completed steps pending compensation."""
        return len(self._stack)

    def to_list(self) -> list[dict[str, Any]]:
        """Serializable representation for checkpoint storage (bottom to top)."""
        return [
            {
                "step_id": r.step.step_id,
                "step_name": r.step.name,
                "action_result": r.action_result,
                "completed_at": r.completed_at,
                "idempotency_key": r.idempotency_key,
            }
            for r in self._stack
        ]


# ---------------------------------------------------------------------------
# Process Checkpoint
# ---------------------------------------------------------------------------

@dataclass
class ProcessCheckpoint:
    """
    Durable snapshot of process execution state for crash recovery.

    Written atomically to persistent storage before each step so that on
    crash, the process manager can rehydrate and resume from the last
    stable state rather than restarting from the beginning.
    """
    process_id: str
    process_type: str
    state: ProcessState
    context: dict[str, Any]
    next_step_index: int
    completed_step_ids: list[str]
    registry_snapshot: list[dict[str, Any]]  # from CompensatingTransactionRegistry.to_list()
    checkpoint_at: float = field(default_factory=time.time)
    checkpoint_seq: int = 0

    def serialize(self) -> dict[str, Any]:
        """Return a JSON-serializable dict for persistent storage."""
        return {
            "process_id": self.process_id,
            "process_type": self.process_type,
            "state": self.state.value,
            "context": self.context,
            "next_step_index": self.next_step_index,
            "completed_step_ids": self.completed_step_ids,
            "registry_snapshot": self.registry_snapshot,
            "checkpoint_at": self.checkpoint_at,
            "checkpoint_seq": self.checkpoint_seq,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ProcessCheckpoint":
        return cls(
            process_id=data["process_id"],
            process_type=data["process_type"],
            state=ProcessState(data["state"]),
            context=data["context"],
            next_step_index=data["next_step_index"],
            completed_step_ids=data["completed_step_ids"],
            registry_snapshot=data["registry_snapshot"],
            checkpoint_at=data["checkpoint_at"],
            checkpoint_seq=data["checkpoint_seq"],
        )


# ---------------------------------------------------------------------------
# Audit record
# ---------------------------------------------------------------------------

@dataclass
class ProcessManagerAuditRecord:
    """
    SOX/audit-compliant complete record of a managed process execution.

    Captures every forward action, compensation, timeout, and state
    transition required for regulatory audit trails and incident post-mortems.
    """
    process_id: str
    process_type: str
    final_state: ProcessState
    started_at: float
    finished_at: float
    duration_s: float
    total_steps: int
    completed_forward_steps: int
    compensation_steps: int
    timed_out_steps: int
    failed_compensation_steps: int
    step_log: list[dict[str, Any]]
    compensation_log: list[dict[str, Any]]
    final_context: dict[str, Any]
    timeout_reason: Optional[str] = None
    failure_reason: Optional[str] = None


# ---------------------------------------------------------------------------
# Process Manager
# ---------------------------------------------------------------------------

class ProcessManager:
    """
    Orchestrates execution of a multi-step distributed process.

    Usage
    -----
    1. Instantiate with a process_type name and a list of ProcessStep objects.
    2. Call execute(context) to run the process from the beginning.
    3. Call resume_from_checkpoint(checkpoint, steps) to resume after a crash.

    The manager writes a checkpoint *before* each step, so crashes during a
    step's execution lead to at-most-once semantics if the step's idempotency
    key prevents double-execution on resume.
    """

    def __init__(
        self,
        process_type: str,
        steps: list[ProcessStep],
        *,
        checkpoint_store: Optional[dict[str, ProcessCheckpoint]] = None,
        now_fn: Callable[[], float] = time.time,
    ) -> None:
        self.process_type = process_type
        self.steps = steps
        self._checkpoint_store = checkpoint_store if checkpoint_store is not None else {}
        self._now = now_fn

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def execute(self, context: dict[str, Any]) -> ProcessManagerAuditRecord:
        """
        Execute the process from the beginning.

        Modifies *context* in-place as steps produce results (e.g. reservation
        IDs, payment receipts). Context is available to all subsequent steps
        and compensations.
        """
        process_id = context.setdefault("process_id", str(uuid.uuid4()))
        return self._run(
            process_id=process_id,
            context=context,
            steps=self.steps,
            start_index=0,
            preloaded_registry=None,
        )

    def resume_from_checkpoint(
        self,
        checkpoint: ProcessCheckpoint,
    ) -> ProcessManagerAuditRecord:
        """
        Resume a process that was interrupted at the given checkpoint.

        The checkpoint's registry_snapshot is used to reconstruct the
        CompensatingTransactionRegistry so that compensation is correct
        if the resumed process subsequently fails.
        """
        # Rehydrate context from checkpoint
        context = dict(checkpoint.context)
        start_index = checkpoint.next_step_index

        return self._run(
            process_id=checkpoint.process_id,
            context=context,
            steps=self.steps,
            start_index=start_index,
            preloaded_registry=checkpoint.registry_snapshot,
        )

    # ------------------------------------------------------------------
    # Internal execution engine
    # ------------------------------------------------------------------

    def _run(
        self,
        process_id: str,
        context: dict[str, Any],
        steps: list[ProcessStep],
        start_index: int,
        preloaded_registry: Optional[list[dict[str, Any]]],
    ) -> ProcessManagerAuditRecord:
        started_at = self._now()
        state = ProcessState.RUNNING
        step_log: list[dict[str, Any]] = []
        compensation_log: list[dict[str, Any]] = []
        registry = CompensatingTransactionRegistry()
        completed_step_ids: list[str] = []
        timeout_reason: Optional[str] = None
        failure_reason: Optional[str] = None
        timed_out = 0

        # Reconstruct registry from checkpoint if resuming
        if preloaded_registry:
            # We cannot call the real action callbacks on pre-completed steps,
            # so we reconstruct the registry using synthetic records that
            # reference the original step objects by step_id.
            step_map = {s.step_id: s for s in steps}
            for rec in preloaded_registry:
                sid = rec["step_id"]
                if sid in step_map:
                    registry.push(CompletedStepRecord(
                        step=step_map[sid],
                        action_result=rec["action_result"],
                        completed_at=rec["completed_at"],
                        idempotency_key=rec["idempotency_key"],
                    ))
                    completed_step_ids.append(sid)

        checkpoint_seq = 0

        for idx in range(start_index, len(steps)):
            step = steps[idx]

            # Write checkpoint before executing step (crash recovery)
            checkpoint_seq += 1
            checkpoint = ProcessCheckpoint(
                process_id=process_id,
                process_type=self.process_type,
                state=ProcessState.RUNNING,
                context=dict(context),
                next_step_index=idx,
                completed_step_ids=list(completed_step_ids),
                registry_snapshot=registry.to_list(),
                checkpoint_at=self._now(),
                checkpoint_seq=checkpoint_seq,
            )
            self._checkpoint_store[process_id] = checkpoint

            idem_key = step.idempotency_key(context)
            step_start = self._now()
            outcome: StepOutcome
            action_result: Any = None

            # Execute forward action with timeout check
            try:
                success, action_result = step.action(context)
                elapsed = self._now() - step_start
                if elapsed > step.timeout_seconds:
                    outcome = StepOutcome.TIMEOUT
                    timeout_reason = (
                        f"Step '{step.name}' exceeded SLA: "
                        f"{elapsed:.2f}s > {step.timeout_seconds}s"
                    )
                    timed_out += 1
                elif success:
                    outcome = StepOutcome.SUCCESS
                else:
                    outcome = StepOutcome.FAILURE
                    failure_reason = f"Step '{step.name}' returned failure"
            except Exception as exc:  # noqa: BLE001
                outcome = StepOutcome.FAILURE
                failure_reason = f"Step '{step.name}' raised: {exc}"
                elapsed = self._now() - step_start

            step_log.append({
                "type": "forward",
                "step_id": step.step_id,
                "step_name": step.name,
                "outcome": outcome.value,
                "idempotency_key": idem_key,
                "action_result": action_result,
                "duration_s": round(self._now() - step_start, 4),
                "timestamp": self._now(),
            })

            if outcome == StepOutcome.SUCCESS:
                # Push to LIFO stack for potential compensation
                registry.push(CompletedStepRecord(
                    step=step,
                    action_result=action_result,
                    completed_at=self._now(),
                    idempotency_key=idem_key,
                ))
                completed_step_ids.append(step.step_id)
            else:
                # Step failed or timed out — compensate all completed steps
                state = ProcessState.COMPENSATING
                all_ok = registry.compensate_all(context, compensation_log)
                state = ProcessState.COMPENSATED if all_ok else ProcessState.FAILED
                break
        else:
            state = ProcessState.COMPLETED

        finished_at = self._now()
        failed_comp = sum(
            1 for e in compensation_log
            if e.get("outcome") == StepOutcome.FAILURE.value
        )

        return ProcessManagerAuditRecord(
            process_id=process_id,
            process_type=self.process_type,
            final_state=state,
            started_at=started_at,
            finished_at=finished_at,
            duration_s=round(finished_at - started_at, 4),
            total_steps=len(steps),
            completed_forward_steps=len(completed_step_ids),
            compensation_steps=len(compensation_log),
            timed_out_steps=timed_out,
            failed_compensation_steps=failed_comp,
            step_log=step_log,
            compensation_log=compensation_log,
            final_context=dict(context),
            timeout_reason=timeout_reason,
            failure_reason=failure_reason,
        )


# ---------------------------------------------------------------------------
# Order Fulfillment Process definition
# ---------------------------------------------------------------------------

def _build_order_fulfillment_steps(
    fail_at_payment: bool = False,
    timeout_at_shipment: bool = False,
    payment_timeout_sla: float = 30.0,
    shipment_timeout_sla: float = 30.0,
    *,
    wall_clock_fn: Callable[[], float] = time.time,
) -> list[ProcessStep]:
    """
    Construct the four-step order fulfillment process.

    Parameters control which scenarios are simulated by injecting
    failures or timeout conditions into specific steps.
    """

    def reserve_inventory(ctx: dict[str, Any]) -> tuple[bool, Any]:
        reservation_id = f"RES-{uuid.uuid4().hex[:8].upper()}"
        ctx["reservation_id"] = reservation_id
        print(f"      → Inventory reserved: {reservation_id}")
        return True, {"reservation_id": reservation_id, "sku": ctx.get("sku"), "qty": ctx.get("qty")}

    def release_inventory(ctx: dict[str, Any], result: Any) -> bool:
        rid = result.get("reservation_id", "?")
        ctx.pop("reservation_id", None)
        print(f"      ↩ Inventory reservation released: {rid}")
        return True

    def charge_payment(ctx: dict[str, Any]) -> tuple[bool, Any]:
        if fail_at_payment:
            print("      → Payment charge FAILED (card declined)")
            return False, None
        payment_id = f"PAY-{uuid.uuid4().hex[:8].upper()}"
        ctx["payment_id"] = payment_id
        print(f"      → Payment charged: {payment_id}")
        return True, {"payment_id": payment_id, "amount_usd": ctx.get("amount_usd")}

    def void_payment(ctx: dict[str, Any], result: Any) -> bool:
        pid = result.get("payment_id", "?") if result else "?"
        ctx.pop("payment_id", None)
        print(f"      ↩ Payment voided: {pid}")
        return True

    def create_shipment(ctx: dict[str, Any]) -> tuple[bool, Any]:
        if timeout_at_shipment:
            # Simulate a slow downstream service that exceeds the SLA.
            # The step itself succeeds, but the ProcessManager measures
            # wall-clock elapsed time and detects the SLA breach.
            time.sleep(0.05)   # 50 ms — will exceed the 0.001s SLA set below
            shipment_id = f"SHIP-{uuid.uuid4().hex[:8].upper()}"
            ctx["shipment_id"] = shipment_id
            print(f"      → Shipment created (but SLA exceeded): {shipment_id}")
            return True, {"shipment_id": shipment_id}
        shipment_id = f"SHIP-{uuid.uuid4().hex[:8].upper()}"
        ctx["shipment_id"] = shipment_id
        print(f"      → Shipment created: {shipment_id}")
        return True, {"shipment_id": shipment_id}

    def cancel_shipment(ctx: dict[str, Any], result: Any) -> bool:
        sid = result.get("shipment_id", "?") if result else "?"
        ctx.pop("shipment_id", None)
        print(f"      ↩ Shipment cancelled: {sid}")
        return True

    def confirm_order(ctx: dict[str, Any]) -> tuple[bool, Any]:
        confirmation_id = f"ORD-{uuid.uuid4().hex[:8].upper()}"
        ctx["confirmation_id"] = confirmation_id
        print(f"      → Order confirmed: {confirmation_id}")
        return True, {"confirmation_id": confirmation_id}

    def unconfirm_order(ctx: dict[str, Any], result: Any) -> bool:
        cid = result.get("confirmation_id", "?") if result else "?"
        ctx.pop("confirmation_id", None)
        print(f"      ↩ Order confirmation cancelled: {cid}")
        return True

    # SLA of 1 ms ensures the 50 ms sleep in create_shipment triggers a timeout
    effective_shipment_sla = 0.001 if timeout_at_shipment else shipment_timeout_sla

    return [
        ProcessStep(
            step_id="step_reserve_inventory",
            name="Reserve Inventory",
            action=reserve_inventory,
            compensation=release_inventory,
            timeout_seconds=30.0,
            idempotency_key_fn=lambda ctx: f"reserve:{ctx.get('order_id')}",
        ),
        ProcessStep(
            step_id="step_charge_payment",
            name="Charge Payment",
            action=charge_payment,
            compensation=void_payment,
            timeout_seconds=payment_timeout_sla,
            idempotency_key_fn=lambda ctx: f"payment:{ctx.get('order_id')}",
        ),
        ProcessStep(
            step_id="step_create_shipment",
            name="Create Shipment",
            action=create_shipment,
            compensation=cancel_shipment,
            timeout_seconds=effective_shipment_sla,
            idempotency_key_fn=lambda ctx: f"shipment:{ctx.get('order_id')}",
        ),
        ProcessStep(
            step_id="step_confirm_order",
            name="Confirm Order",
            action=confirm_order,
            compensation=unconfirm_order,
            timeout_seconds=30.0,
            idempotency_key_fn=lambda ctx: f"confirm:{ctx.get('order_id')}",
        ),
    ]


# ---------------------------------------------------------------------------
# Scenario helpers
# ---------------------------------------------------------------------------

def _print_audit(label: str, audit: ProcessManagerAuditRecord) -> None:
    state_icon = {
        ProcessState.COMPLETED: "✓",
        ProcessState.COMPENSATED: "↩",
        ProcessState.FAILED: "✗",
        ProcessState.RUNNING: "…",
    }.get(audit.final_state, "?")

    print(f"\n{'='*70}")
    print(f"  {label}")
    print(f"{'='*70}")
    print(f"  Process ID   : {audit.process_id[:12]}…")
    print(f"  Final state  : {state_icon}  {audit.final_state.value}")
    print(f"  Forward steps: {audit.completed_forward_steps}/{audit.total_steps} completed")
    print(f"  Compensation : {audit.compensation_steps} steps executed")
    print(f"  Timed-out    : {audit.timed_out_steps} steps")
    print(f"  Duration     : {audit.duration_s:.4f}s")
    if audit.timeout_reason:
        print(f"  Timeout      : {audit.timeout_reason}")
    if audit.failure_reason:
        print(f"  Failure      : {audit.failure_reason}")
    print()
    print("  Step log (forward actions):")
    for entry in audit.step_log:
        icon = "✓" if entry["outcome"] == "SUCCESS" else ("⏱" if entry["outcome"] == "TIMEOUT" else "✗")
        print(f"    {icon}  [{entry['step_id']}] {entry['step_name']} → {entry['outcome']}")
    if audit.compensation_log:
        print()
        print("  Compensation log (LIFO):")
        for entry in audit.compensation_log:
            icon = "✓" if entry.get("outcome") == "SUCCESS" else "✗"
            print(f"    {icon}  [{entry['step_id']}] {entry['step_name']} compensation → {entry.get('outcome', 'EXCEPTION')}")
    print()
    keys = ["reservation_id", "payment_id", "shipment_id", "confirmation_id"]
    final_ctx_summary = {k: audit.final_context.get(k, "(not set)") for k in keys}
    print("  Final context:")
    for k, v in final_ctx_summary.items():
        print(f"    {k:<22} {v}")


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------

def scenario_a_successful_fulfillment() -> None:
    """All four steps succeed. Process reaches COMPLETED."""
    print("\n--- Scenario A: successful 4-step order fulfillment ---")
    steps = _build_order_fulfillment_steps()
    checkpoint_store: dict[str, ProcessCheckpoint] = {}
    manager = ProcessManager("order_fulfillment", steps, checkpoint_store=checkpoint_store)

    context = {
        "order_id": f"ORD-{uuid.uuid4().hex[:8].upper()}",
        "customer_id": "CUST-001",
        "sku": "SKU-WIDGET-42",
        "qty": 3,
        "amount_usd": 89.97,
    }
    audit = manager.execute(context)
    _print_audit("Scenario A — Happy Path: all steps succeed", audit)
    assert audit.final_state == ProcessState.COMPLETED
    assert audit.completed_forward_steps == 4
    assert audit.compensation_steps == 0


def scenario_b_payment_failure_compensation() -> None:
    """Payment fails at step 2. LIFO compensation unreserves inventory."""
    print("\n--- Scenario B: payment failure → LIFO compensation ---")
    steps = _build_order_fulfillment_steps(fail_at_payment=True)
    manager = ProcessManager("order_fulfillment", steps)

    context = {
        "order_id": f"ORD-{uuid.uuid4().hex[:8].upper()}",
        "customer_id": "CUST-002",
        "sku": "SKU-GADGET-7",
        "qty": 1,
        "amount_usd": 249.99,
    }
    audit = manager.execute(context)
    _print_audit("Scenario B — Payment failure: inventory reservation compensated (LIFO)", audit)
    assert audit.final_state == ProcessState.COMPENSATED
    assert audit.completed_forward_steps == 1   # Only reservation succeeded
    assert audit.compensation_steps == 1         # One compensation (unreserve)
    assert "reservation_id" not in audit.final_context


def scenario_c_shipment_sla_timeout() -> None:
    """Shipment step times out. LIFO: cancel payment, then unreserve inventory."""
    print("\n--- Scenario C: shipment SLA timeout → LIFO compensation (2 steps) ---")
    # Set shipment_timeout_sla near-zero to force immediate timeout detection
    steps = _build_order_fulfillment_steps(timeout_at_shipment=True)
    manager = ProcessManager("order_fulfillment", steps)

    context = {
        "order_id": f"ORD-{uuid.uuid4().hex[:8].upper()}",
        "customer_id": "CUST-003",
        "sku": "SKU-HEAVY-BOX",
        "qty": 5,
        "amount_usd": 1299.50,
    }
    audit = manager.execute(context)
    _print_audit("Scenario C — Shipment SLA timeout: void payment + unreserve (LIFO)", audit)
    assert audit.final_state == ProcessState.COMPENSATED
    assert audit.timed_out_steps == 1
    assert audit.compensation_steps == 2  # payment void + inventory release (LIFO)
    assert "payment_id" not in audit.final_context
    assert "reservation_id" not in audit.final_context
    # Note: shipment_id may remain in context — the shipment action completed
    # before the SLA breach was detected; in production, a separate cancellation
    # step would be invoked outside the normal LIFO chain.


def scenario_d_crash_recovery_from_checkpoint() -> None:
    """
    Simulate a crash after payment, then resume from checkpoint.

    Step 1 (reserve) and Step 2 (payment) completed and checkpointed.
    The process 'crashed' before step 3 (shipment). We rehydrate from
    the checkpoint and resume at step 3.
    """
    print("\n--- Scenario D: crash recovery — resume from checkpoint ---")
    checkpoint_store: dict[str, ProcessCheckpoint] = {}
    steps = _build_order_fulfillment_steps()
    manager = ProcessManager("order_fulfillment", steps, checkpoint_store=checkpoint_store)

    process_id = f"ORD-CRASH-{uuid.uuid4().hex[:6].upper()}"
    context: dict[str, Any] = {
        "process_id": process_id,
        "order_id": process_id,
        "customer_id": "CUST-004",
        "sku": "SKU-FRAGILE-99",
        "qty": 2,
        "amount_usd": 599.00,
    }

    # Simulate partial execution: manually build a checkpoint at step 2 complete
    step_map = {s.step_id: s for s in steps}

    # Run forward actions for steps 0 and 1 manually
    print("   [Simulating steps 0+1 completion before crash]")
    reservation_id = f"RES-SIM-{uuid.uuid4().hex[:6].upper()}"
    payment_id = f"PAY-SIM-{uuid.uuid4().hex[:6].upper()}"
    context["reservation_id"] = reservation_id
    context["payment_id"] = payment_id
    print(f"      → Inventory reserved (pre-crash): {reservation_id}")
    print(f"      → Payment charged (pre-crash): {payment_id}")

    # Build a synthetic checkpoint at next_step_index=2 (about to run shipment)
    registry_snapshot = [
        {
            "step_id": "step_reserve_inventory",
            "step_name": "Reserve Inventory",
            "action_result": {"reservation_id": reservation_id, "sku": context["sku"], "qty": context["qty"]},
            "completed_at": time.time(),
            "idempotency_key": f"reserve:{process_id}",
        },
        {
            "step_id": "step_charge_payment",
            "step_name": "Charge Payment",
            "action_result": {"payment_id": payment_id, "amount_usd": context["amount_usd"]},
            "completed_at": time.time(),
            "idempotency_key": f"payment:{process_id}",
        },
    ]
    checkpoint = ProcessCheckpoint(
        process_id=process_id,
        process_type="order_fulfillment",
        state=ProcessState.RUNNING,
        context=dict(context),
        next_step_index=2,   # Resume at step 3 (shipment)
        completed_step_ids=["step_reserve_inventory", "step_charge_payment"],
        registry_snapshot=registry_snapshot,
        checkpoint_seq=2,
    )
    print(f"   [Crash checkpoint written at step_index=2, resuming from there]")

    audit = manager.resume_from_checkpoint(checkpoint)
    _print_audit("Scenario D — Crash recovery: resumed at step 3 (shipment) from checkpoint", audit)
    assert audit.final_state == ProcessState.COMPLETED
    assert audit.completed_forward_steps == 4   # 2 from checkpoint + 2 run in this execution
    assert len(audit.step_log) == 2             # Only steps 3+4 actually executed now


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("Process Manager Pattern — Distributed Transaction Coordination")
    print("LIFO Compensating Transactions · Crash-Recovery Checkpointing · SLA Timeouts")

    scenario_a_successful_fulfillment()
    scenario_b_payment_failure_compensation()
    scenario_c_shipment_sla_timeout()
    scenario_d_crash_recovery_from_checkpoint()

    print("\n" + "="*70)
    print("  All four scenarios complete.")
    print("  Key invariants verified:")
    print("    • LIFO compensation maintains correct undo ordering")
    print("    • Checkpoint enables crash-safe resumption at any step boundary")
    print("    • SLA timeout triggers identical compensation path as step failure")
    print("="*70)
