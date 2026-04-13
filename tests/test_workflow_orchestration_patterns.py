"""
Tests for 19_workflow_orchestration_patterns.py

Covers:
    - WorkflowStateMachine  (8 tests)
    - WorkflowActivityExecution (7 tests)
    - WorkflowCompensation  (8 tests)
    - WorkflowRetry         (4 tests)
    - WorkflowOrchestrator  (7 tests)
"""

from __future__ import annotations

import importlib.util
import sys
import types
import uuid
from pathlib import Path
from typing import Any

import pytest

# ---------------------------------------------------------------------------
# Module loader
# ---------------------------------------------------------------------------

_MOD_PATH = Path(__file__).parent.parent / "examples" / "19_workflow_orchestration_patterns.py"


def _load_module():
    module_name = "workflow_orchestration_patterns"
    if module_name in sys.modules:
        return sys.modules[module_name]
    spec = importlib.util.spec_from_file_location(module_name, _MOD_PATH)
    mod = types.ModuleType(module_name)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def m():
    return _load_module()


# ---------------------------------------------------------------------------
# Helper factories
# ---------------------------------------------------------------------------


def _succeeding_activity(activity_id: str, output: Any = "ok"):
    """Return a WorkflowActivity that always succeeds."""

    class _Impl:
        def execute(self, input_data: Any):
            mod = _load_module()
            return mod.ActivityResult(
                activity_id=activity_id,
                success=True,
                output=output,
            )

    return _Impl()


def _failing_activity(activity_id: str, error: str = "forced failure"):
    """Return a WorkflowActivity that always fails."""

    class _Impl:
        def execute(self, input_data: Any):
            mod = _load_module()
            return mod.ActivityResult(
                activity_id=activity_id,
                success=False,
                error=error,
            )

    return _Impl()


def _counting_activity(activity_id: str, fail_times: int = 0):
    """Return a WorkflowActivity that counts calls and fails the first *fail_times* calls."""

    class _Impl:
        def __init__(self):
            self.call_count = 0

        def execute(self, input_data: Any):
            mod = _load_module()
            self.call_count += 1
            if self.call_count <= fail_times:
                return mod.ActivityResult(
                    activity_id=activity_id,
                    success=False,
                    error=f"transient error on attempt {self.call_count}",
                )
            return mod.ActivityResult(
                activity_id=activity_id,
                success=True,
                output=f"succeeded on attempt {self.call_count}",
            )

    return _Impl()


def _recording_compensator():
    """Return a CompensatingActivity that records every call and returns True."""

    class _Impl:
        def __init__(self):
            self.calls: list = []

        def compensate(self, original_input: Any, original_output: Any) -> bool:
            self.calls.append((original_input, original_output))
            return True

    return _Impl()


def _failing_compensator():
    """Return a CompensatingActivity that always returns False (compensation fails)."""

    class _Impl:
        def compensate(self, original_input: Any, original_output: Any) -> bool:
            return False

    return _Impl()


def _make_instance(m, steps, input_data=None, timeout_seconds=None, workflow_id=None):
    """Convenience: build a WorkflowInstance ready to execute."""
    definition = m.WorkflowDefinition(
        name="test_workflow",
        steps=steps,
        timeout_seconds=timeout_seconds,
    )
    return m.WorkflowInstance(
        workflow_id=workflow_id or str(uuid.uuid4()),
        definition=definition,
        input_data=input_data,
    )


def _make_step(m, activity_id, activity, compensating_activity=None, max_retries=0, retry_delay_seconds=0.0):
    """Convenience: build a WorkflowStep."""
    return m.WorkflowStep(
        activity_id=activity_id,
        activity=activity,
        compensating_activity=compensating_activity,
        max_retries=max_retries,
        retry_delay_seconds=retry_delay_seconds,
    )


# ===========================================================================
# TestWorkflowStateMachine
# ===========================================================================


class TestWorkflowStateMachine:
    """Tests for state transition enforcement via WorkflowInstance.transition_to()."""

    def test_pending_to_running_valid(self, m):
        """PENDING → RUNNING must succeed without raising."""
        instance = _make_instance(m, steps=[])
        assert instance.state == m.WorkflowState.PENDING
        instance.transition_to(m.WorkflowState.RUNNING)
        assert instance.state == m.WorkflowState.RUNNING

    def test_running_to_completed_valid(self, m):
        """RUNNING → COMPLETED must succeed."""
        instance = _make_instance(m, steps=[])
        instance.transition_to(m.WorkflowState.RUNNING)
        instance.transition_to(m.WorkflowState.COMPLETED)
        assert instance.state == m.WorkflowState.COMPLETED

    def test_running_to_compensating_valid(self, m):
        """RUNNING → COMPENSATING must succeed."""
        instance = _make_instance(m, steps=[])
        instance.transition_to(m.WorkflowState.RUNNING)
        instance.transition_to(m.WorkflowState.COMPENSATING)
        assert instance.state == m.WorkflowState.COMPENSATING

    def test_completed_to_running_invalid(self, m):
        """COMPLETED → RUNNING must raise WorkflowTransitionError (terminal state)."""
        instance = _make_instance(m, steps=[])
        instance.transition_to(m.WorkflowState.RUNNING)
        instance.transition_to(m.WorkflowState.COMPLETED)
        with pytest.raises(m.WorkflowTransitionError):
            instance.transition_to(m.WorkflowState.RUNNING)

    def test_cancelled_to_running_invalid(self, m):
        """CANCELLED → RUNNING must raise WorkflowTransitionError (terminal state)."""
        instance = _make_instance(m, steps=[])
        instance.transition_to(m.WorkflowState.CANCELLED)
        with pytest.raises(m.WorkflowTransitionError):
            instance.transition_to(m.WorkflowState.RUNNING)

    def test_compensated_is_terminal(self, m):
        """COMPENSATED → RUNNING must raise WorkflowTransitionError."""
        instance = _make_instance(m, steps=[])
        instance.transition_to(m.WorkflowState.RUNNING)
        instance.transition_to(m.WorkflowState.COMPENSATING)
        instance.transition_to(m.WorkflowState.COMPENSATED)
        with pytest.raises(m.WorkflowTransitionError):
            instance.transition_to(m.WorkflowState.RUNNING)

    def test_pending_to_cancelled_valid(self, m):
        """PENDING → CANCELLED must succeed (workflow cancelled before starting)."""
        instance = _make_instance(m, steps=[])
        assert instance.state == m.WorkflowState.PENDING
        instance.transition_to(m.WorkflowState.CANCELLED)
        assert instance.state == m.WorkflowState.CANCELLED

    def test_failed_to_compensating_valid(self, m):
        """FAILED → COMPENSATING must succeed (compensation triggered after failure)."""
        instance = _make_instance(m, steps=[])
        instance.transition_to(m.WorkflowState.RUNNING)
        instance.transition_to(m.WorkflowState.FAILED)
        instance.transition_to(m.WorkflowState.COMPENSATING)
        assert instance.state == m.WorkflowState.COMPENSATING


# ===========================================================================
# TestWorkflowActivityExecution
# ===========================================================================


class TestWorkflowActivityExecution:
    """Tests for single and multi-step activity execution within a workflow."""

    def test_single_step_success(self, m):
        """A 1-step workflow whose activity succeeds must reach COMPLETED."""
        step = _make_step(m, "step_a", _succeeding_activity("step_a"))
        instance = _make_instance(m, steps=[step])
        final = instance.execute()
        assert final == m.WorkflowState.COMPLETED

    def test_single_step_failure(self, m):
        """A 1-step workflow whose activity fails must NOT reach COMPLETED."""
        step = _make_step(m, "step_fail", _failing_activity("step_fail"))
        instance = _make_instance(m, steps=[step])
        final = instance.execute()
        assert final in {m.WorkflowState.FAILED, m.WorkflowState.COMPENSATED}

    def test_all_steps_in_history(self, m):
        """A 3-step successful workflow must record exactly 3 history entries."""
        steps = [
            _make_step(m, "s1", _succeeding_activity("s1")),
            _make_step(m, "s2", _succeeding_activity("s2")),
            _make_step(m, "s3", _succeeding_activity("s3")),
        ]
        instance = _make_instance(m, steps=steps)
        instance.execute()
        assert len(instance.get_history()) == 3

    def test_history_records_activity_ids(self, m):
        """History entries must use the activity IDs declared in the steps."""
        steps = [
            _make_step(m, "alpha", _succeeding_activity("alpha")),
            _make_step(m, "beta", _succeeding_activity("beta")),
        ]
        instance = _make_instance(m, steps=steps)
        instance.execute()
        history_ids = [r.activity_id for r in instance.get_history()]
        assert history_ids == ["alpha", "beta"]

    def test_successful_steps_list(self, m):
        """After full completion, successful_steps() must contain all step IDs."""
        ids = ["x", "y", "z"]
        steps = [_make_step(m, aid, _succeeding_activity(aid)) for aid in ids]
        instance = _make_instance(m, steps=steps)
        instance.execute()
        assert instance.successful_steps() == ids

    def test_elapsed_seconds_positive_after_run(self, m):
        """elapsed_seconds() must return a positive value after execution."""
        step = _make_step(m, "timed_step", _succeeding_activity("timed_step"))
        instance = _make_instance(m, steps=[step])
        instance.execute()
        assert instance.elapsed_seconds() > 0.0

    def test_activity_result_output_captured(self, m):
        """The output field from a successful activity must appear in the history."""
        step = _make_step(m, "out_step", _succeeding_activity("out_step", output="my_output"))
        instance = _make_instance(m, steps=[step])
        instance.execute()
        history = instance.get_history()
        assert len(history) == 1
        assert history[0].output == "my_output"


# ===========================================================================
# TestWorkflowCompensation
# ===========================================================================


class TestWorkflowCompensation:
    """Tests for saga compensation logic — reverse rollback on failure."""

    def test_failure_triggers_compensation(self, m):
        """Step 2 failing must trigger compensation of step 1."""
        comp = _recording_compensator()
        steps = [
            _make_step(m, "s1", _succeeding_activity("s1"), compensating_activity=comp),
            _make_step(m, "s2", _failing_activity("s2")),
        ]
        instance = _make_instance(m, steps=steps)
        instance.execute()
        # comp must have been called exactly once
        assert len(comp.calls) == 1

    def test_compensation_runs_in_reverse(self, m):
        """With 3 successful steps before a failure, compensators must fire in reverse order."""
        comp_order: list = []

        class _OrderedComp:
            def __init__(self, label):
                self._label = label

            def compensate(self, original_input, original_output) -> bool:
                comp_order.append(self._label)
                return True

        steps = [
            _make_step(m, "s1", _succeeding_activity("s1"), compensating_activity=_OrderedComp("s1")),
            _make_step(m, "s2", _succeeding_activity("s2"), compensating_activity=_OrderedComp("s2")),
            _make_step(m, "s3", _succeeding_activity("s3"), compensating_activity=_OrderedComp("s3")),
            _make_step(m, "s4", _failing_activity("s4")),
        ]
        instance = _make_instance(m, steps=steps)
        instance.execute()
        # Compensation must run s3 → s2 → s1 (reverse of success order)
        assert comp_order == ["s3", "s2", "s1"]

    def test_compensated_steps_list(self, m):
        """After failure and compensation, compensated_steps() must include compensated IDs."""
        comp1 = _recording_compensator()
        comp2 = _recording_compensator()
        steps = [
            _make_step(m, "a", _succeeding_activity("a"), compensating_activity=comp1),
            _make_step(m, "b", _succeeding_activity("b"), compensating_activity=comp2),
            _make_step(m, "c", _failing_activity("c")),
        ]
        instance = _make_instance(m, steps=steps)
        instance.execute()
        compensated = set(instance.compensated_steps())
        assert "a" in compensated
        assert "b" in compensated

    def test_state_is_compensated_after_full_compensation(self, m):
        """When all compensators run, the final state must be COMPENSATED."""
        comp = _recording_compensator()
        steps = [
            _make_step(m, "ok_step", _succeeding_activity("ok_step"), compensating_activity=comp),
            _make_step(m, "bad_step", _failing_activity("bad_step")),
        ]
        instance = _make_instance(m, steps=steps)
        final = instance.execute()
        assert final == m.WorkflowState.COMPENSATED
        assert instance.state == m.WorkflowState.COMPENSATED

    def test_step_without_compensator_skipped(self, m):
        """A step with no compensating_activity must be skipped silently during rollback."""
        comp = _recording_compensator()
        steps = [
            _make_step(m, "s1", _succeeding_activity("s1"), compensating_activity=comp),
            # s2 has no compensator
            _make_step(m, "s2", _succeeding_activity("s2"), compensating_activity=None),
            _make_step(m, "s3", _failing_activity("s3")),
        ]
        instance = _make_instance(m, steps=steps)
        final = instance.execute()
        # Should not crash; comp for s1 runs once
        assert len(comp.calls) == 1
        assert final in {m.WorkflowState.COMPENSATED, m.WorkflowState.FAILED}

    def test_compensation_failure_marks_failed(self, m):
        """A compensator returning False must not crash the workflow; state must be FAILED or COMPENSATED."""
        steps = [
            _make_step(m, "s1", _succeeding_activity("s1"), compensating_activity=_failing_compensator()),
            _make_step(m, "s2", _failing_activity("s2")),
        ]
        instance = _make_instance(m, steps=steps)
        final = instance.execute()
        # The orchestration must not raise; end state is COMPENSATED (transition still fires) or FAILED
        assert final in {m.WorkflowState.COMPENSATED, m.WorkflowState.FAILED}

    def test_successful_steps_empty_after_compensation(self, m):
        """After compensation, compensated step IDs must not appear in successful_steps()."""
        comp = _recording_compensator()
        steps = [
            _make_step(m, "s1", _succeeding_activity("s1"), compensating_activity=comp),
            _make_step(m, "s2", _failing_activity("s2")),
        ]
        instance = _make_instance(m, steps=steps)
        instance.execute()
        # s1 was compensated, so it must not appear in successful_steps()
        assert "s1" not in instance.successful_steps()

    def test_first_step_fails_no_compensation_needed(self, m):
        """When the very first step fails, there is nothing to compensate — state FAILED."""
        steps = [
            _make_step(m, "s1", _failing_activity("s1")),
        ]
        instance = _make_instance(m, steps=steps)
        final = instance.execute()
        # No prior successful steps → goes straight to FAILED
        assert final == m.WorkflowState.FAILED
        assert instance.compensated_steps() == []


# ===========================================================================
# TestWorkflowRetry
# ===========================================================================


class TestWorkflowRetry:
    """Tests for per-step retry with configurable max_retries."""

    def test_retry_succeeds_on_second_attempt(self, m):
        """Activity fails once then succeeds; max_retries=1 → workflow COMPLETED."""
        activity = _counting_activity("flaky", fail_times=1)
        step = _make_step(m, "flaky", activity, max_retries=1, retry_delay_seconds=0.0)
        instance = _make_instance(m, steps=[step])
        final = instance.execute()
        assert final == m.WorkflowState.COMPLETED
        assert activity.call_count == 2

    def test_retry_exhausted_triggers_compensation(self, m):
        """Activity always fails with max_retries=2 → retries exhausted → FAILED or COMPENSATED."""
        activity = _counting_activity("always_fail", fail_times=999)
        step = _make_step(m, "always_fail", activity, max_retries=2, retry_delay_seconds=0.0)
        instance = _make_instance(m, steps=[step])
        final = instance.execute()
        assert final in {m.WorkflowState.FAILED, m.WorkflowState.COMPENSATED}
        # Should have attempted max_retries+1 = 3 times total
        assert activity.call_count == 3

    def test_no_retry_fails_immediately(self, m):
        """Activity fails, max_retries=0 → only one attempt, workflow FAILED."""
        activity = _counting_activity("no_retry", fail_times=999)
        step = _make_step(m, "no_retry", activity, max_retries=0, retry_delay_seconds=0.0)
        instance = _make_instance(m, steps=[step])
        final = instance.execute()
        assert final in {m.WorkflowState.FAILED, m.WorkflowState.COMPENSATED}
        # Exactly one attempt
        assert activity.call_count == 1

    def test_successful_first_attempt_no_retry_needed(self, m):
        """Activity succeeds first try even with max_retries=3 → COMPLETED after 1 call."""
        activity = _counting_activity("lucky", fail_times=0)
        step = _make_step(m, "lucky", activity, max_retries=3, retry_delay_seconds=0.0)
        instance = _make_instance(m, steps=[step])
        final = instance.execute()
        assert final == m.WorkflowState.COMPLETED
        assert activity.call_count == 1


# ===========================================================================
# TestWorkflowOrchestrator
# ===========================================================================


class TestWorkflowOrchestrator:
    """Tests for WorkflowOrchestrator: lifecycle management and query helpers."""

    def _simple_def(self, m, name="test_wf"):
        """Build a trivial 1-step succeeding workflow definition."""
        return m.WorkflowDefinition(
            name=name,
            steps=[_make_step(m, "s1", _succeeding_activity("s1"))],
        )

    def _failing_def(self, m, name="failing_wf"):
        """Build a 1-step always-failing workflow definition."""
        return m.WorkflowDefinition(
            name=name,
            steps=[_make_step(m, "fail_s1", _failing_activity("fail_s1"))],
        )

    def test_start_returns_workflow_id(self, m):
        """start() must return a non-empty string identifier."""
        orch = m.WorkflowOrchestrator()
        wid = orch.start(self._simple_def(m))
        assert isinstance(wid, str)
        assert len(wid) > 0

    def test_get_status_returns_terminal_state(self, m):
        """After start(), get_status() must return a terminal WorkflowState."""
        orch = m.WorkflowOrchestrator()
        wid = orch.start(self._simple_def(m))
        status = orch.get_status(wid)
        terminal = {
            m.WorkflowState.COMPLETED,
            m.WorkflowState.FAILED,
            m.WorkflowState.COMPENSATED,
        }
        assert status in terminal

    def test_cancel_pending_workflow(self, m):
        """A PENDING workflow registered manually can be cancelled and returns True."""
        orch = m.WorkflowOrchestrator()
        definition = self._simple_def(m)
        wid = str(uuid.uuid4())
        instance = m.WorkflowInstance(
            workflow_id=wid,
            definition=definition,
            input_data=None,
        )
        # Register without executing so it stays PENDING
        orch._instances[wid] = instance
        result = orch.cancel(wid)
        assert result is True
        assert instance.state == m.WorkflowState.CANCELLED

    def test_cancel_already_completed_returns_false(self, m):
        """cancel() on a COMPLETED workflow must return False."""
        orch = m.WorkflowOrchestrator()
        wid = orch.start(self._simple_def(m))
        # Verify it completed
        assert orch.get_status(wid) == m.WorkflowState.COMPLETED
        result = orch.cancel(wid)
        assert result is False

    def test_list_active_empty_after_completion(self, m):
        """After all workflows finish, list_active() must return an empty list."""
        orch = m.WorkflowOrchestrator()
        orch.start(self._simple_def(m))
        orch.start(self._simple_def(m, name="wf2"))
        # Both are synchronous; neither is RUNNING anymore
        assert orch.list_active() == []

    def test_completed_count_increments(self, m):
        """Running 3 successful workflows must bring completed_count() to 3."""
        orch = m.WorkflowOrchestrator()
        for i in range(3):
            orch.start(self._simple_def(m, name=f"wf_{i}"))
        assert orch.completed_count() == 3

    def test_list_by_state_correct(self, m):
        """2 successful + 1 failing workflow → list_by_state(COMPLETED) has exactly 2 entries."""
        orch = m.WorkflowOrchestrator()
        orch.start(self._simple_def(m, name="ok1"))
        orch.start(self._simple_def(m, name="ok2"))
        orch.start(self._failing_def(m, name="bad1"))
        completed = orch.list_by_state(m.WorkflowState.COMPLETED)
        assert len(completed) == 2
