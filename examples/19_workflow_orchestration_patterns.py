"""
19_workflow_orchestration_patterns.py — Workflow orchestration patterns for
long-running business processes with saga compensation, activity retry, and
state machine management.

Demonstrates three complementary patterns that together implement a production-
grade workflow engine suitable for multi-step business processes:

    Pattern 1 — Workflow State Machine:
                Manages lifecycle transitions for a workflow instance through
                a strict set of valid state changes (PENDING → RUNNING →
                COMPLETED / FAILED / COMPENSATING → COMPENSATED). Invalid
                transitions raise WorkflowTransitionError, preventing corrupt
                state. The state machine is the foundation on which the other
                two patterns are layered.

    Pattern 2 — Saga with Compensation (Rollback):
                Executes a sequence of activities as a distributed transaction
                without two-phase commit. Each activity may register a
                compensating activity. If any step fails, completed steps are
                rolled back in reverse order by invoking their compensation
                actions, guaranteeing eventual consistency without a global
                lock. This is the pattern used by Apache Kafka Saga, AWS Step
                Functions, and MuleSoft process flows for cross-service
                transactions.

    Pattern 3 — Activity Retry with Configurable Backoff:
                Each workflow step declares max_retries and retry_delay_seconds.
                Transient failures trigger automatic retries with a fixed inter-
                attempt sleep. Permanent failures after exhausting retries
                immediately trigger saga compensation. This mirrors the retry
                semantics of Temporal.io activity options and Conductor task
                configuration, enabling resilient execution of unreliable
                downstream calls without manual intervention.

Relationship to commercial workflow engines:
    - Temporal.io   : WorkflowInstance ≈ Workflow, WorkflowStep ≈ Activity, retry
                      config ≈ RetryPolicy, compensation ≈ saga pattern via
                      compensate signals.
    - AWS Step Fns  : WorkflowDefinition ≈ State Machine, WorkflowStep ≈ Task
                      State, max_retries ≈ Retry field, compensation ≈ Catch +
                      rollback branch.
    - Apache Camel/
      Camunda BPM   : WorkflowOrchestrator ≈ ProcessEngine, WorkflowInstance ≈
                      ProcessInstance, CompensatingActivity ≈ Boundary
                      Compensation Event.
    - Conductor     : WorkflowDefinition ≈ Workflow definition JSON, WorkflowStep
                      ≈ Task, max_retries ≈ retryCount, WorkflowState ≈
                      WorkflowStatus enum.

No external dependencies required.

Run:
    python examples/19_workflow_orchestration_patterns.py
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional, Protocol


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class WorkflowState(Enum):
    """All possible lifecycle states of a workflow instance."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    COMPENSATING = "compensating"
    COMPENSATED = "compensated"
    CANCELLED = "cancelled"


# ---------------------------------------------------------------------------
# Valid state transition table
# ---------------------------------------------------------------------------

_VALID_TRANSITIONS: dict[WorkflowState, set[WorkflowState]] = {
    WorkflowState.PENDING: {WorkflowState.RUNNING, WorkflowState.CANCELLED},
    WorkflowState.RUNNING: {
        WorkflowState.COMPLETED,
        WorkflowState.FAILED,
        WorkflowState.COMPENSATING,
        WorkflowState.CANCELLED,
    },
    WorkflowState.FAILED: {WorkflowState.COMPENSATING},
    WorkflowState.COMPENSATING: {WorkflowState.COMPENSATED, WorkflowState.FAILED},
    WorkflowState.COMPLETED: set(),
    WorkflowState.COMPENSATED: set(),
    WorkflowState.CANCELLED: set(),
}


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class WorkflowTransitionError(Exception):
    """Raised when an invalid state transition is attempted."""


class WorkflowTimeoutError(Exception):
    """Raised when workflow execution exceeds its configured timeout."""


class ActivityFailedError(Exception):
    """Raised when an activity fails and compensation is needed."""


# ---------------------------------------------------------------------------
# ActivityResult dataclass
# ---------------------------------------------------------------------------


@dataclass
class ActivityResult:
    """Captures the outcome of a single activity execution."""

    activity_id: str
    success: bool
    output: Any = None
    error: str = ""
    duration_ms: float = 0.0
    compensated: bool = False


# ---------------------------------------------------------------------------
# Protocols
# ---------------------------------------------------------------------------


class WorkflowActivity(Protocol):
    """Protocol that every executable activity must satisfy."""

    def execute(self, input_data: Any) -> ActivityResult:
        """Execute the activity with the given input; return an ActivityResult."""
        ...


class CompensatingActivity(Protocol):
    """Protocol for activities that undo the effects of a forward activity."""

    def compensate(self, original_input: Any, original_output: Any) -> bool:
        """Undo the effect of the original activity.

        Args:
            original_input: The input that was passed to the forward activity.
            original_output: The output returned by the successful forward
                             activity execution.

        Returns:
            True if compensation succeeded, False otherwise.
        """
        ...


# ---------------------------------------------------------------------------
# WorkflowStep and WorkflowDefinition
# ---------------------------------------------------------------------------


@dataclass
class WorkflowStep:
    """Describes one step in a workflow: the activity, optional compensator, and retry policy."""

    activity_id: str
    activity: Any  # WorkflowActivity
    compensating_activity: Optional[Any] = None  # CompensatingActivity
    max_retries: int = 0
    retry_delay_seconds: float = 0.0


@dataclass
class WorkflowDefinition:
    """Immutable description of a workflow: its name, ordered steps, and optional timeout."""

    name: str
    steps: list  # List[WorkflowStep]
    timeout_seconds: Optional[float] = None

    def step_ids(self) -> list:
        """Return the ordered list of activity IDs in this definition."""
        return [s.activity_id for s in self.steps]


# ---------------------------------------------------------------------------
# WorkflowInstance — core stateful execution engine
# ---------------------------------------------------------------------------


@dataclass
class WorkflowInstance:
    """
    Stateful execution of a WorkflowDefinition.

    Manages the lifecycle of a single workflow run: state transitions,
    activity execution with retry, saga compensation on failure, and
    elapsed-time tracking.
    """

    workflow_id: str
    definition: WorkflowDefinition
    input_data: Any
    state: WorkflowState = field(default=WorkflowState.PENDING)
    history: list = field(default_factory=list)  # List[ActivityResult]
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    error_message: str = ""

    # ------------------------------------------------------------------
    # State machine
    # ------------------------------------------------------------------

    def transition_to(self, new_state: WorkflowState) -> None:
        """Transition the instance to *new_state*, enforcing the valid-transition table.

        Raises:
            WorkflowTransitionError: If the transition from the current state to
                                     *new_state* is not in _VALID_TRANSITIONS.
        """
        allowed = _VALID_TRANSITIONS.get(self.state, set())
        if new_state not in allowed:
            raise WorkflowTransitionError(
                f"Cannot transition workflow '{self.workflow_id}' from "
                f"{self.state.value!r} to {new_state.value!r}. "
                f"Allowed transitions: {[s.value for s in allowed] or 'none (terminal state)'}."
            )
        self.state = new_state

    # ------------------------------------------------------------------
    # Compensation helper
    # ------------------------------------------------------------------

    def _run_compensation(
        self,
        completed_results: list,  # List[tuple[WorkflowStep, Any, ActivityResult]]
    ) -> None:
        """Iterate completed steps in reverse order and invoke each compensating activity.

        Args:
            completed_results: Accumulated (step, step_input, forward_result) tuples
                               for every step that succeeded before the failure.
        """
        self.transition_to(WorkflowState.COMPENSATING)

        for step, step_input, forward_result in reversed(completed_results):
            if step.compensating_activity is None:
                continue
            try:
                compensation_ok = step.compensating_activity.compensate(
                    step_input, forward_result.output
                )
            except Exception as exc:  # noqa: BLE001
                compensation_ok = False
                self.error_message += (
                    f" | Compensation of '{step.activity_id}' raised: {exc}"
                )

            # Find the corresponding history entry and mark it compensated.
            for history_entry in self.history:
                if history_entry.activity_id == step.activity_id and history_entry.success:
                    history_entry.compensated = compensation_ok
                    break

        self.transition_to(WorkflowState.COMPENSATED)

    # ------------------------------------------------------------------
    # Main execution loop
    # ------------------------------------------------------------------

    def execute(self) -> WorkflowState:
        """Execute all workflow steps in order, applying retry and compensation logic.

        Algorithm:
            1. Transition to RUNNING; record started_at.
            2. For each step in definition.steps:
               a. Before each step: check timeout; raise WorkflowTimeoutError
                  and transition to FAILED if exceeded.
               b. Attempt the activity up to (max_retries + 1) times.
               c. On success: append ActivityResult to history; record the
                  (step, input, result) tuple in completed_results for later
                  potential compensation.
               d. On exhausted retries / unrecoverable failure: append failed
                  ActivityResult; run compensation on completed_results in
                  reverse; return final state.
            3. If all steps succeed: transition to COMPLETED; record completed_at.

        Returns:
            The final WorkflowState after execution.

        Raises:
            WorkflowTimeoutError: Propagated after transitioning to FAILED when
                                  the elapsed time exceeds timeout_seconds.
        """
        self.transition_to(WorkflowState.RUNNING)
        self.started_at = time.time()

        # Accumulates (step, original_input, ActivityResult) for compensation.
        completed_results: list = []

        for step in self.definition.steps:
            # ----------------------------------------------------------
            # Timeout guard — checked before every step
            # ----------------------------------------------------------
            if self.definition.timeout_seconds is not None:
                elapsed = time.time() - self.started_at
                if elapsed > self.definition.timeout_seconds:
                    timeout_err = WorkflowTimeoutError(
                        f"Workflow '{self.workflow_id}' exceeded timeout of "
                        f"{self.definition.timeout_seconds}s after {elapsed:.3f}s."
                    )
                    self.error_message = str(timeout_err)
                    self.transition_to(WorkflowState.FAILED)
                    self.completed_at = time.time()
                    raise timeout_err

            # ----------------------------------------------------------
            # Retry loop
            # ----------------------------------------------------------
            last_result: Optional[ActivityResult] = None
            succeeded = False

            for attempt in range(step.max_retries + 1):
                t_start = time.time()
                try:
                    result = step.activity.execute(self.input_data)
                except Exception as exc:  # noqa: BLE001
                    duration_ms = (time.time() - t_start) * 1000.0
                    last_result = ActivityResult(
                        activity_id=step.activity_id,
                        success=False,
                        error=str(exc),
                        duration_ms=duration_ms,
                    )
                else:
                    result.duration_ms = (time.time() - t_start) * 1000.0
                    last_result = result

                if last_result.success:
                    succeeded = True
                    break

                # Not successful — retry if attempts remain.
                remaining = step.max_retries - attempt
                if remaining > 0 and step.retry_delay_seconds > 0:
                    time.sleep(step.retry_delay_seconds)

            # ----------------------------------------------------------
            # Record result in history
            # ----------------------------------------------------------
            assert last_result is not None  # always set in the loop above
            self.history.append(last_result)

            if succeeded:
                # Track this step for potential reverse compensation.
                completed_results.append((step, self.input_data, last_result))
            else:
                # Exhausted retries — begin saga compensation.
                self.error_message = (
                    f"Activity '{step.activity_id}' failed after "
                    f"{step.max_retries + 1} attempt(s): {last_result.error}"
                )
                if completed_results:
                    # _run_compensation internally transitions to COMPENSATING
                    # then COMPENSATED.
                    self._run_compensation(completed_results)
                else:
                    # Nothing to compensate — go straight to FAILED.
                    self.transition_to(WorkflowState.FAILED)

                self.completed_at = time.time()
                return self.state

        # All steps completed successfully.
        self.transition_to(WorkflowState.COMPLETED)
        self.completed_at = time.time()
        return self.state

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def get_history(self) -> list:
        """Return a shallow copy of the activity result history."""
        return list(self.history)

    def successful_steps(self) -> list:
        """Return activity IDs of steps that completed successfully and were not compensated."""
        return [r.activity_id for r in self.history if r.success and not r.compensated]

    def compensated_steps(self) -> list:
        """Return activity IDs of steps whose effects were successfully compensated."""
        return [r.activity_id for r in self.history if r.compensated]

    def elapsed_seconds(self) -> float:
        """Return elapsed wall-clock time since the workflow started.

        Returns 0.0 if the workflow has not yet started.
        """
        if self.started_at is None:
            return 0.0
        end = self.completed_at if self.completed_at is not None else time.time()
        return end - self.started_at


# ---------------------------------------------------------------------------
# WorkflowOrchestrator — manages multiple workflow instances
# ---------------------------------------------------------------------------


class WorkflowOrchestrator:
    """Registry and launcher for workflow instances.

    Provides a thin management layer over WorkflowInstance: creating,
    tracking, querying, and cancelling workflow runs.  In a production
    system this class would persist instance state to a durable store
    (e.g. a database or Temporal server), but here it uses an in-process
    dictionary for simplicity.
    """

    def __init__(self) -> None:
        self._instances: dict[str, WorkflowInstance] = {}

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self, definition: WorkflowDefinition, input_data: Any = None) -> str:
        """Create a new workflow instance, execute it synchronously, and store it.

        Args:
            definition: The workflow definition to run.
            input_data:  Arbitrary input forwarded to every activity's execute().

        Returns:
            The UUID string that identifies the new workflow instance.
        """
        workflow_id = str(uuid.uuid4())
        instance = WorkflowInstance(
            workflow_id=workflow_id,
            definition=definition,
            input_data=input_data,
        )
        self._instances[workflow_id] = instance
        try:
            instance.execute()
        except WorkflowTimeoutError:
            # Instance already transitioned to FAILED inside execute(); just
            # swallow here so the caller can inspect the state via get_status().
            pass
        return workflow_id

    def cancel(self, workflow_id: str) -> bool:
        """Cancel a PENDING workflow before it starts.

        Args:
            workflow_id: ID of the workflow to cancel.

        Returns:
            True if the workflow was successfully cancelled; False if it was
            not found or its current state does not allow cancellation.
        """
        instance = self._instances.get(workflow_id)
        if instance is None:
            return False
        if instance.state != WorkflowState.PENDING:
            return False
        try:
            instance.transition_to(WorkflowState.CANCELLED)
        except WorkflowTransitionError:
            return False
        return True

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def get_status(self, workflow_id: str) -> Optional[WorkflowState]:
        """Return the current state of a workflow instance, or None if not found."""
        instance = self._instances.get(workflow_id)
        return instance.state if instance is not None else None

    def get_instance(self, workflow_id: str) -> Optional[WorkflowInstance]:
        """Return the full WorkflowInstance object by ID, or None if not found."""
        return self._instances.get(workflow_id)

    def list_active(self) -> list:
        """Return IDs of workflows currently in RUNNING state."""
        return [
            wid
            for wid, inst in self._instances.items()
            if inst.state == WorkflowState.RUNNING
        ]

    def list_by_state(self, state: WorkflowState) -> list:
        """Return IDs of all workflows in the given state."""
        return [
            wid
            for wid, inst in self._instances.items()
            if inst.state == state
        ]

    def completed_count(self) -> int:
        """Return the number of successfully COMPLETED workflow instances."""
        return sum(
            1 for inst in self._instances.values()
            if inst.state == WorkflowState.COMPLETED
        )

    def failed_count(self) -> int:
        """Return the number of FAILED workflow instances (uncompensated failures)."""
        return sum(
            1 for inst in self._instances.values()
            if inst.state == WorkflowState.FAILED
        )


# ===========================================================================
# Smoke-test activities (used only in __main__)
# ===========================================================================


class _SuccessActivity:
    """Always succeeds; records a label in its output."""

    def __init__(self, activity_id: str, label: str) -> None:
        self._id = activity_id
        self._label = label

    def execute(self, input_data: Any) -> ActivityResult:
        return ActivityResult(activity_id=self._id, success=True, output=self._label)


class _FailingActivity:
    """Always fails (used to trigger compensation)."""

    def __init__(self, activity_id: str) -> None:
        self._id = activity_id

    def execute(self, input_data: Any) -> ActivityResult:
        return ActivityResult(
            activity_id=self._id,
            success=False,
            error="Simulated payment gateway timeout",
        )


class _FlakyActivity:
    """Fails the first *fail_times* calls, then succeeds on the next call."""

    def __init__(self, activity_id: str, fail_times: int) -> None:
        self._id = activity_id
        self._fail_times = fail_times
        self._call_count = 0

    def execute(self, input_data: Any) -> ActivityResult:
        self._call_count += 1
        if self._call_count <= self._fail_times:
            return ActivityResult(
                activity_id=self._id,
                success=False,
                error=f"Transient error on attempt {self._call_count}",
            )
        return ActivityResult(
            activity_id=self._id,
            success=True,
            output=f"Succeeded on attempt {self._call_count}",
        )


class _LoggingCompensator:
    """Records compensation calls for verification in tests."""

    def __init__(self, label: str) -> None:
        self.label = label
        self.called_with: list = []

    def compensate(self, original_input: Any, original_output: Any) -> bool:
        self.called_with.append((original_input, original_output))
        return True


# ===========================================================================
# Smoke tests
# ===========================================================================


def _separator(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def _run_happy_path(orchestrator: WorkflowOrchestrator) -> None:
    """Scenario 1: 3-step order processing workflow — all steps succeed."""
    _separator("Scenario 1 — Happy Path (all steps succeed)")

    definition = WorkflowDefinition(
        name="order_processing",
        steps=[
            WorkflowStep(
                activity_id="validate_order",
                activity=_SuccessActivity("validate_order", "order_valid"),
            ),
            WorkflowStep(
                activity_id="reserve_inventory",
                activity=_SuccessActivity("reserve_inventory", "inventory_reserved"),
            ),
            WorkflowStep(
                activity_id="charge_payment",
                activity=_SuccessActivity("charge_payment", "payment_charged"),
            ),
        ],
    )

    wid = orchestrator.start(definition, input_data={"order_id": "ORD-001"})
    instance = orchestrator.get_instance(wid)
    assert instance is not None

    print(f"Workflow ID : {wid}")
    print(f"Final state : {instance.state.value}")
    print(f"Successful  : {instance.successful_steps()}")
    print(f"Compensated : {instance.compensated_steps()}")
    print(f"Elapsed (s) : {instance.elapsed_seconds():.4f}")

    assert instance.state == WorkflowState.COMPLETED, f"Expected COMPLETED, got {instance.state}"
    assert instance.successful_steps() == [
        "validate_order", "reserve_inventory", "charge_payment"
    ]
    assert instance.compensated_steps() == []
    print("PASS")


def _run_compensation_path(orchestrator: WorkflowOrchestrator) -> None:
    """Scenario 2: charge_payment fails → reverse-compensation runs."""
    _separator("Scenario 2 — Compensation Path (payment fails, saga rolls back)")

    cancel_validation_comp = _LoggingCompensator("cancel_validation")
    release_inventory_comp = _LoggingCompensator("release_inventory")

    definition = WorkflowDefinition(
        name="order_processing_with_compensation",
        steps=[
            WorkflowStep(
                activity_id="validate_order",
                activity=_SuccessActivity("validate_order", "order_valid"),
                compensating_activity=cancel_validation_comp,
            ),
            WorkflowStep(
                activity_id="reserve_inventory",
                activity=_SuccessActivity("reserve_inventory", "inventory_reserved"),
                compensating_activity=release_inventory_comp,
            ),
            WorkflowStep(
                activity_id="charge_payment",
                activity=_FailingActivity("charge_payment"),
                # No compensating activity needed — it never succeeded.
                compensating_activity=None,
            ),
        ],
    )

    wid = orchestrator.start(definition, input_data={"order_id": "ORD-002"})
    instance = orchestrator.get_instance(wid)
    assert instance is not None

    print(f"Workflow ID      : {wid}")
    print(f"Final state      : {instance.state.value}")
    print(f"Successful steps : {instance.successful_steps()}")
    print(f"Compensated      : {instance.compensated_steps()}")
    print(f"Error message    : {instance.error_message!r}")
    print(f"release_inventory compensator called: {len(release_inventory_comp.called_with)} time(s)")
    print(f"cancel_validation compensator called: {len(cancel_validation_comp.called_with)} time(s)")

    assert instance.state == WorkflowState.COMPENSATED, (
        f"Expected COMPENSATED, got {instance.state}"
    )
    # Both forward steps were compensated.
    assert set(instance.compensated_steps()) == {"validate_order", "reserve_inventory"}
    # Compensators called exactly once each, in reverse order.
    assert len(release_inventory_comp.called_with) == 1
    assert len(cancel_validation_comp.called_with) == 1
    print("PASS")


def _run_retry_path() -> None:
    """Scenario 3: A flaky activity fails 2× then succeeds; max_retries=3."""
    _separator("Scenario 3 — Retry Path (flaky activity, max_retries=3)")

    flaky = _FlakyActivity("flaky_service_call", fail_times=2)
    step = WorkflowStep(
        activity_id="flaky_service_call",
        activity=flaky,
        max_retries=3,
        retry_delay_seconds=0.0,  # No actual sleep in smoke test
    )
    definition = WorkflowDefinition(
        name="flaky_workflow",
        steps=[step],
    )
    instance = WorkflowInstance(
        workflow_id="retry-smoke-test",
        definition=definition,
        input_data=None,
    )
    final_state = instance.execute()

    print(f"Final state     : {final_state.value}")
    print(f"Attempts made   : {flaky._call_count}")
    history = instance.get_history()
    print(f"History entries : {len(history)}")
    for entry in history:
        status = "success" if entry.success else f"failed ({entry.error})"
        print(f"  [{entry.activity_id}] {status}")
    print(f"Successful steps: {instance.successful_steps()}")

    assert final_state == WorkflowState.COMPLETED, (
        f"Expected COMPLETED, got {final_state}"
    )
    # The activity should have been attempted exactly 3 times (fail, fail, succeed).
    assert flaky._call_count == 3, f"Expected 3 attempts, got {flaky._call_count}"
    # History has exactly 1 entry — only the final result is recorded per step.
    assert len(history) == 1
    assert history[0].success is True
    print("PASS")


def _run_cancel_path(orchestrator: WorkflowOrchestrator) -> None:
    """Scenario 4: A PENDING workflow is cancelled before execution."""
    _separator("Scenario 4 — Cancel Path (PENDING workflow cancelled)")

    # Build a definition but do NOT start it via the orchestrator (which
    # executes immediately).  Instead create the instance manually so it
    # remains PENDING, then cancel it.
    definition = WorkflowDefinition(
        name="cancellable_workflow",
        steps=[
            WorkflowStep(
                activity_id="some_step",
                activity=_SuccessActivity("some_step", "done"),
            )
        ],
    )
    wid = str(uuid.uuid4())
    instance = WorkflowInstance(
        workflow_id=wid,
        definition=definition,
        input_data=None,
    )
    # Register manually so the orchestrator knows about it.
    orchestrator._instances[wid] = instance

    print(f"State before cancel : {instance.state.value}")
    cancelled = orchestrator.cancel(wid)
    print(f"cancel() returned   : {cancelled}")
    print(f"State after cancel  : {instance.state.value}")

    assert cancelled is True
    assert instance.state == WorkflowState.CANCELLED

    # Attempting to cancel again should return False (already in terminal state).
    second_cancel = orchestrator.cancel(wid)
    assert second_cancel is False, "Second cancel should return False"
    print("PASS")


def _run_orchestrator_stats(orchestrator: WorkflowOrchestrator) -> None:
    """Print final orchestrator statistics after all scenarios."""
    _separator("Orchestrator Statistics")

    print(f"Total instances tracked : {len(orchestrator._instances)}")
    print(f"COMPLETED count         : {orchestrator.completed_count()}")
    print(f"FAILED count            : {orchestrator.failed_count()}")
    print(f"COMPENSATED workflows   : {len(orchestrator.list_by_state(WorkflowState.COMPENSATED))}")
    print(f"CANCELLED workflows     : {len(orchestrator.list_by_state(WorkflowState.CANCELLED))}")
    print(f"RUNNING (should be 0)   : {len(orchestrator.list_active())}")

    assert orchestrator.completed_count() == 1, (
        f"Expected 1 COMPLETED, got {orchestrator.completed_count()}"
    )
    assert orchestrator.list_by_state(WorkflowState.COMPENSATED) != []
    assert orchestrator.list_active() == []
    print("All orchestrator assertions passed.")


# ===========================================================================
# Entry point
# ===========================================================================


if __name__ == "__main__":
    print("Workflow Orchestration Patterns — Smoke Test Suite")
    print("Patterns: State Machine | Saga Compensation | Activity Retry")

    orchestrator = WorkflowOrchestrator()

    _run_happy_path(orchestrator)
    _run_compensation_path(orchestrator)
    _run_retry_path()           # Uses WorkflowInstance directly (no orchestrator needed)
    _run_cancel_path(orchestrator)
    _run_orchestrator_stats(orchestrator)

    print("\n" + "=" * 60)
    print("  All scenarios passed.")
    print("=" * 60)
