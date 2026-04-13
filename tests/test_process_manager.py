"""
Tests for 11_process_manager.py

Covers ProcessManager, CompensatingTransactionRegistry, ProcessCheckpoint,
ProcessStep, and ProcessManagerAuditRecord. Verifies LIFO compensation
ordering, SLA timeout detection, checkpoint serialization, and crash recovery.
"""

from __future__ import annotations

import sys
import os
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import importlib.util

_spec = importlib.util.spec_from_file_location(
    "process_manager",
    os.path.join(os.path.dirname(__file__), "..", "examples", "11_process_manager.py"),
)
_mod = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["process_manager"] = _mod  # required for frozen dataclasses on Python 3.14
_spec.loader.exec_module(_mod)  # type: ignore[union-attr]

ProcessState = _mod.ProcessState
StepOutcome = _mod.StepOutcome
ProcessStep = _mod.ProcessStep
CompensatingTransactionRegistry = _mod.CompensatingTransactionRegistry
CompletedStepRecord = _mod.CompletedStepRecord
ProcessCheckpoint = _mod.ProcessCheckpoint
ProcessManager = _mod.ProcessManager


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _always_success(ctx):
    ctx["executed"] = ctx.get("executed", []) + ["fwd"]
    return True, {"result": "ok"}


def _always_fail(ctx):
    return False, None


def _record_compensation(ctx, result):
    ctx["compensated"] = ctx.get("compensated", []) + ["comp"]
    return True


def _fail_compensation(ctx, result):
    return False


def _make_step(step_id: str, *, succeed: bool = True, compensate_ok: bool = True) -> ProcessStep:
    def action(ctx):
        ctx.setdefault("order", []).append(f"fwd_{step_id}")
        return succeed, {"id": step_id}

    def compensation(ctx, result):
        ctx.setdefault("order", []).append(f"comp_{step_id}")
        return compensate_ok

    return ProcessStep(
        step_id=step_id,
        name=f"Step {step_id}",
        action=action,
        compensation=compensation,
        timeout_seconds=30.0,
    )


# ---------------------------------------------------------------------------
# CompensatingTransactionRegistry tests
# ---------------------------------------------------------------------------

class TestCompensatingTransactionRegistry:
    def test_push_and_depth(self):
        reg = CompensatingTransactionRegistry()
        assert reg.depth == 0
        step = _make_step("s1")
        reg.push(CompletedStepRecord(step=step, action_result={"id": "s1"},
                                     completed_at=time.time(), idempotency_key="k1"))
        assert reg.depth == 1

    def test_lifo_compensation_order(self):
        """Compensations execute in reverse order of forward steps."""
        reg = CompensatingTransactionRegistry()
        ctx = {}
        log = []
        for i in range(3):
            step = _make_step(f"s{i}")
            reg.push(CompletedStepRecord(step=step, action_result={"id": f"s{i}"},
                                         completed_at=time.time(), idempotency_key=f"k{i}"))
        reg.compensate_all(ctx, log)
        # LIFO: s2 compensated first, then s1, then s0
        assert ctx["order"] == ["comp_s2", "comp_s1", "comp_s0"]

    def test_compensate_all_returns_true_on_success(self):
        reg = CompensatingTransactionRegistry()
        step = _make_step("s1", compensate_ok=True)
        reg.push(CompletedStepRecord(step=step, action_result={},
                                     completed_at=time.time(), idempotency_key="k1"))
        ok = reg.compensate_all({}, [])
        assert ok is True

    def test_compensate_all_returns_false_on_failure(self):
        reg = CompensatingTransactionRegistry()
        step = _make_step("s1", compensate_ok=False)
        reg.push(CompletedStepRecord(step=step, action_result={},
                                     completed_at=time.time(), idempotency_key="k1"))
        ok = reg.compensate_all({}, [])
        assert ok is False

    def test_compensate_all_empties_stack(self):
        reg = CompensatingTransactionRegistry()
        for i in range(3):
            step = _make_step(f"s{i}")
            reg.push(CompletedStepRecord(step=step, action_result={},
                                         completed_at=time.time(), idempotency_key=f"k{i}"))
        reg.compensate_all({}, [])
        assert reg.depth == 0

    def test_to_list_preserves_insertion_order(self):
        reg = CompensatingTransactionRegistry()
        for i in range(3):
            step = _make_step(f"s{i}")
            reg.push(CompletedStepRecord(step=step, action_result={"id": f"s{i}"},
                                         completed_at=time.time(), idempotency_key=f"k{i}"))
        snapshot = reg.to_list()
        assert [s["step_id"] for s in snapshot] == ["s0", "s1", "s2"]

    def test_compensate_all_on_empty_registry_returns_true(self):
        reg = CompensatingTransactionRegistry()
        assert reg.compensate_all({}, []) is True


# ---------------------------------------------------------------------------
# ProcessCheckpoint tests
# ---------------------------------------------------------------------------

class TestProcessCheckpoint:
    def test_serialize_roundtrip(self):
        cp = ProcessCheckpoint(
            process_id="pid-1",
            process_type="order_fulfillment",
            state=ProcessState.RUNNING,
            context={"order_id": "ORD-1"},
            next_step_index=2,
            completed_step_ids=["s0", "s1"],
            registry_snapshot=[{"step_id": "s0", "action_result": {}, "step_name": "S0",
                                  "completed_at": 1.0, "idempotency_key": "k0"}],
            checkpoint_seq=2,
        )
        data = cp.serialize()
        restored = ProcessCheckpoint.from_dict(data)
        assert restored.process_id == cp.process_id
        assert restored.next_step_index == 2
        assert restored.completed_step_ids == ["s0", "s1"]
        assert restored.state == ProcessState.RUNNING
        assert restored.checkpoint_seq == 2

    def test_serialize_includes_all_required_keys(self):
        cp = ProcessCheckpoint(
            process_id="p", process_type="t", state=ProcessState.RUNNING,
            context={}, next_step_index=0, completed_step_ids=[],
            registry_snapshot=[], checkpoint_seq=0,
        )
        data = cp.serialize()
        for key in ("process_id", "process_type", "state", "context",
                    "next_step_index", "completed_step_ids", "registry_snapshot",
                    "checkpoint_at", "checkpoint_seq"):
            assert key in data


# ---------------------------------------------------------------------------
# ProcessManager tests
# ---------------------------------------------------------------------------

class TestProcessManagerHappyPath:
    def test_all_steps_succeed(self):
        steps = [_make_step("s0"), _make_step("s1"), _make_step("s2")]
        manager = ProcessManager("test", steps)
        ctx = {"process_id": "p1"}
        audit = manager.execute(ctx)
        assert audit.final_state == ProcessState.COMPLETED
        assert audit.completed_forward_steps == 3
        assert audit.compensation_steps == 0

    def test_step_log_has_all_forward_steps(self):
        steps = [_make_step("s0"), _make_step("s1")]
        manager = ProcessManager("test", steps)
        audit = manager.execute({"process_id": "p1"})
        assert len(audit.step_log) == 2
        outcomes = [e["outcome"] for e in audit.step_log]
        assert all(o == StepOutcome.SUCCESS.value for o in outcomes)

    def test_context_modified_by_steps(self):
        def action(ctx):
            ctx["visited"] = True
            return True, {}

        step = ProcessStep("s0", "S0", action, lambda ctx, r: True)
        manager = ProcessManager("test", [step])
        ctx = {"process_id": "p1"}
        audit = manager.execute(ctx)
        assert audit.final_context.get("visited") is True

    def test_checkpoint_written_for_each_step(self):
        checkpoint_store = {}
        steps = [_make_step("s0"), _make_step("s1"), _make_step("s2")]
        manager = ProcessManager("test", steps, checkpoint_store=checkpoint_store)
        ctx = {"process_id": "p-chk"}
        manager.execute(ctx)
        assert "p-chk" in checkpoint_store


class TestProcessManagerFailure:
    def test_failure_triggers_lifo_compensation(self):
        ctx = {"process_id": "pf", "order": []}
        steps = [
            _make_step("s0"),
            _make_step("s1"),
            _make_step("s2", succeed=False),  # fails here
        ]
        manager = ProcessManager("test", steps)
        audit = manager.execute(ctx)
        assert audit.final_state == ProcessState.COMPENSATED
        # s2 never completed (failed), so s1 compensated before s0
        assert "comp_s1" in ctx["order"]
        assert "comp_s0" in ctx["order"]
        assert ctx["order"].index("comp_s1") < ctx["order"].index("comp_s0")

    def test_failure_at_first_step_no_compensation(self):
        steps = [_make_step("s0", succeed=False), _make_step("s1")]
        manager = ProcessManager("test", steps)
        audit = manager.execute({"process_id": "p1"})
        assert audit.final_state == ProcessState.COMPENSATED
        assert audit.compensation_steps == 0

    def test_failed_compensation_leads_to_failed_state(self):
        ctx = {"process_id": "p1"}
        steps = [
            _make_step("s0", compensate_ok=False),
            _make_step("s1", succeed=False),
        ]
        manager = ProcessManager("test", steps)
        audit = manager.execute(ctx)
        assert audit.final_state == ProcessState.FAILED

    def test_completed_forward_steps_reflects_only_succeeded(self):
        steps = [_make_step("s0"), _make_step("s1"), _make_step("s2", succeed=False)]
        manager = ProcessManager("test", steps)
        audit = manager.execute({"process_id": "p1"})
        # s0 and s1 succeeded, s2 failed → 2 completed
        assert audit.completed_forward_steps == 2

    def test_failure_reason_in_audit(self):
        steps = [_make_step("s0", succeed=False)]
        manager = ProcessManager("test", steps)
        audit = manager.execute({"process_id": "p1"})
        assert audit.failure_reason is not None
        assert "s0" in audit.failure_reason or "Step" in audit.failure_reason


class TestProcessManagerTimeout:
    def test_sla_timeout_triggers_compensation(self):
        def slow_action(ctx):
            time.sleep(0.02)  # 20 ms
            return True, {"ok": True}

        slow_step = ProcessStep(
            step_id="slow",
            name="Slow Step",
            action=slow_action,
            compensation=lambda ctx, r: True,
            timeout_seconds=0.001,  # 1 ms SLA — will be exceeded by 20 ms sleep
        )
        fast_step = _make_step("pre")
        manager = ProcessManager("test", [fast_step, slow_step])
        audit = manager.execute({"process_id": "p1"})
        assert audit.final_state == ProcessState.COMPENSATED
        assert audit.timed_out_steps == 1
        assert audit.timeout_reason is not None

    def test_sla_timeout_compensates_previous_steps(self):
        ctx = {"process_id": "p1", "order": []}

        def slow_action(c):
            time.sleep(0.02)
            c["order"].append("fwd_slow")
            return True, {}

        s0 = _make_step("s0")
        slow = ProcessStep("slow", "Slow", slow_action,
                           lambda c, r: c["order"].append("comp_slow") or True,
                           timeout_seconds=0.001)
        manager = ProcessManager("test", [s0, slow])
        audit = manager.execute(ctx)
        # s0 should have been compensated (LIFO)
        assert "comp_s0" in ctx["order"]
        # slow was not pushed to registry (timeout detected after), so not in compensation
        assert audit.compensation_steps == 1

    def test_normal_step_within_sla_does_not_timeout(self):
        step = ProcessStep(
            "s0", "Fast Step",
            lambda ctx: (True, {}),
            lambda ctx, r: True,
            timeout_seconds=10.0,  # generous SLA
        )
        manager = ProcessManager("test", [step])
        audit = manager.execute({"process_id": "p1"})
        assert audit.final_state == ProcessState.COMPLETED
        assert audit.timed_out_steps == 0


class TestProcessManagerCrashRecovery:
    def test_resume_from_checkpoint_completes_remaining_steps(self):
        ctx_order = []

        def action_s2(ctx):
            ctx_order.append("s2")
            return True, {"id": "s2"}

        def action_s3(ctx):
            ctx_order.append("s3")
            return True, {"id": "s3"}

        steps = [
            _make_step("s0"),
            _make_step("s1"),
            ProcessStep("s2", "S2", action_s2, lambda ctx, r: True),
            ProcessStep("s3", "S3", action_s3, lambda ctx, r: True),
        ]
        manager = ProcessManager("test", steps)

        # Simulate crash after s0+s1: build checkpoint at next_step_index=2
        checkpoint = ProcessCheckpoint(
            process_id="pid-resume",
            process_type="test",
            state=ProcessState.RUNNING,
            context={"process_id": "pid-resume"},
            next_step_index=2,
            completed_step_ids=["s0", "s1"],
            registry_snapshot=[
                {"step_id": "s0", "step_name": "s0", "action_result": {}, "completed_at": time.time(), "idempotency_key": "k0"},
                {"step_id": "s1", "step_name": "s1", "action_result": {}, "completed_at": time.time(), "idempotency_key": "k1"},
            ],
            checkpoint_seq=2,
        )
        audit = manager.resume_from_checkpoint(checkpoint)
        assert audit.final_state == ProcessState.COMPLETED
        assert "s2" in ctx_order
        assert "s3" in ctx_order
        # Step log only shows s2 and s3 (s0+s1 were in checkpoint)
        assert len(audit.step_log) == 2

    def test_resume_restores_registry_for_compensation(self):
        """If resumed execution fails, the checkpoint-restored registry compensates correctly."""
        ctx = {"process_id": "pid-comp", "order": []}

        def fail_action(c):
            return False, None

        def record_compensation_s0(c, r):
            c["order"].append("comp_s0")
            return True

        steps = [
            ProcessStep("s0", "S0", lambda c: (True, {}), record_compensation_s0),
            ProcessStep("s1", "S1", fail_action, lambda c, r: True),
        ]
        manager = ProcessManager("test", steps)

        # Simulate crash after s0 was completed
        checkpoint = ProcessCheckpoint(
            process_id="pid-comp",
            process_type="test",
            state=ProcessState.RUNNING,
            context=dict(ctx),
            next_step_index=1,   # Resume at s1
            completed_step_ids=["s0"],
            registry_snapshot=[
                {"step_id": "s0", "step_name": "S0", "action_result": {},
                 "completed_at": time.time(), "idempotency_key": "k0"},
            ],
            checkpoint_seq=1,
        )
        audit = manager.resume_from_checkpoint(checkpoint)
        assert audit.final_state == ProcessState.COMPENSATED
        # s0's compensation should have run
        assert "comp_s0" in ctx["order"]


class TestProcessManagerAuditRecord:
    def test_audit_duration_is_non_negative(self):
        manager = ProcessManager("test", [_make_step("s0")])
        audit = manager.execute({"process_id": "p1"})
        assert audit.duration_s >= 0.0

    def test_audit_process_type(self):
        manager = ProcessManager("my_process", [_make_step("s0")])
        audit = manager.execute({"process_id": "p1"})
        assert audit.process_type == "my_process"

    def test_audit_final_context_is_copy(self):
        ctx = {"process_id": "p1", "data": "initial"}
        manager = ProcessManager("test", [_make_step("s0")])
        audit = manager.execute(ctx)
        ctx["data"] = "mutated"
        # Audit record should not reflect mutation (it's a copy)
        assert audit.final_context.get("data") != "mutated" or audit.final_context.get("data") == "initial"

    def test_idempotency_key_generated(self):
        manager = ProcessManager("test", [_make_step("s0")])
        audit = manager.execute({"process_id": "p1"})
        assert audit.step_log[0]["idempotency_key"]

    def test_empty_step_list_completes_immediately(self):
        manager = ProcessManager("test", [])
        audit = manager.execute({"process_id": "p1"})
        assert audit.final_state == ProcessState.COMPLETED
        assert audit.completed_forward_steps == 0
