"""
36_workflow_state_machine.py — Workflow State Machine Patterns

Pure-Python implementation of five workflow and state-machine primitives using
only the standard library (``dataclasses``, ``collections``, ``concurrent.futures``,
``threading``, ``time``, ``typing``).  No external dependencies are required.

    Pattern 1 — StateMachine:
                Generic finite state machine (FSM) with guard conditions and
                entry/exit callbacks.  States are registered with optional
                ``on_enter`` and ``on_exit`` callables.  Transitions are
                registered as ``(from_state, event) → to_state`` with an
                optional boolean *guard* predicate and an optional *action*
                callable executed mid-transition.  :meth:`trigger` fires an
                event: it checks the guard (if any), calls the current state's
                ``on_exit``, calls the action, transitions, and calls the new
                state's ``on_enter``.  Returns ``True`` when the transition
                occurred, ``False`` otherwise.

    Pattern 2 — HierarchicalStateMachine:
                Extends the FSM concept to composite (parent/child) states.
                :meth:`add_child_state` registers a child under a parent.
                :meth:`set_initial_child` designates which child is entered
                automatically when the parent is entered.
                :meth:`enter_state` enters a state and recursively enters its
                configured initial child.
                :meth:`is_in_state` returns ``True`` when the *current* leaf
                state is the given state OR any of its ancestors is the given
                state, enabling "is the machine inside this composite state?"
                queries.

    Pattern 3 — WorkflowEngine:
                Orchestrates multi-step workflows with per-step retry logic and
                optional per-step timeout enforcement.  Each step is a callable
                that receives and optionally mutates a shared *context* dict.
                :meth:`register_step` stores a step with configurable
                ``max_retries`` and ``timeout_ms``.  :meth:`run` executes the
                requested steps in order, passing context through; it retries
                transient failures with exponential back-off and stops on the
                first unrecoverable failure.  Results are returned as a
                :class:`WorkflowResult` dataclass.

    Pattern 4 — ConditionalRouter:
                Routes workflow execution to different step lists based on
                context-driven conditions.  Routes are registered in order;
                :meth:`route` evaluates each condition against the supplied
                context dict and returns the ``target_steps`` of the first
                matching route.  A default route can be registered via
                :meth:`register_default`.  Raises ``ValueError`` when no
                condition matches and no default is set.

    Pattern 5 — ParallelWorkflow:
                Runs named tasks concurrently inside a ``ThreadPoolExecutor``.
                :meth:`add_task` registers a task callable.  :meth:`run`
                executes all tasks in parallel and collects results (or
                exception objects) keyed by task name.  :meth:`run_until_first_success`
                returns the ``(name, result)`` tuple of the first task to
                complete without raising; remaining futures are cancelled and
                a ``RuntimeError`` is raised if every task fails.

No external dependencies required.

Run:
    python examples/36_workflow_state_machine.py
"""

from __future__ import annotations

import time
from collections import defaultdict
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from concurrent.futures import TimeoutError as FuturesTimeoutError
from dataclasses import dataclass
from typing import Any

# ===========================================================================
# Pattern 1 — StateMachine
# ===========================================================================


@dataclass
class _StateConfig:
    """Internal configuration for a registered FSM state."""

    name: str
    on_enter: Callable[[], None] | None = None
    on_exit: Callable[[], None] | None = None


@dataclass
class _TransitionConfig:
    """Internal configuration for a registered FSM transition."""

    from_state: str
    event: str
    to_state: str
    guard: Callable[[], bool] | None = None
    action: Callable[[], None] | None = None


class StateMachine:
    """Generic finite state machine with guard conditions and entry/exit callbacks.

    Example::

        sm = StateMachine()
        sm.add_state("idle")
        sm.add_state("running", on_enter=lambda: print("started"))
        sm.add_state("stopped", on_enter=lambda: print("stopped"))

        sm.add_transition("idle", "start", "running")
        sm.add_transition("running", "stop", "stopped")

        sm.current_state = "idle"   # set initial state directly
        sm.trigger("start")         # → "running"
        sm.trigger("stop")          # → "stopped"
    """

    def __init__(self) -> None:
        self._states: dict[str, _StateConfig] = {}
        # (from_state, event) → _TransitionConfig
        self._transitions: dict[tuple[str, str], _TransitionConfig] = {}
        self._current_state: str | None = None

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def add_state(
        self,
        name: str,
        on_enter: Callable[[], None] | None = None,
        on_exit: Callable[[], None] | None = None,
    ) -> None:
        """Register a state with optional entry and exit callbacks.

        Args:
            name:     Unique state name.
            on_enter: Callable invoked when the machine enters this state.
            on_exit:  Callable invoked when the machine leaves this state.
        """
        self._states[name] = _StateConfig(name=name, on_enter=on_enter, on_exit=on_exit)

    def add_transition(
        self,
        from_state: str,
        event: str,
        to_state: str,
        guard: Callable[[], bool] | None = None,
        action: Callable[[], None] | None = None,
    ) -> None:
        """Register a guarded transition.

        Args:
            from_state: Source state name.
            event:      Event string that triggers this transition.
            to_state:   Destination state name.
            guard:      Optional zero-argument predicate; transition is
                        skipped when it returns ``False``.
            action:     Optional callable executed between exit and entry
                        callbacks during the transition.
        """
        self._transitions[(from_state, event)] = _TransitionConfig(
            from_state=from_state,
            event=event,
            to_state=to_state,
            guard=guard,
            action=action,
        )

    # ------------------------------------------------------------------
    # Runtime
    # ------------------------------------------------------------------

    @property
    def current_state(self) -> str | None:
        """The current state name, or ``None`` if not yet initialised."""
        return self._current_state

    @current_state.setter
    def current_state(self, state: str) -> None:
        """Directly set the current state (bypasses callbacks and guards)."""
        self._current_state = state

    def can_trigger(self, event: str) -> bool:
        """Return ``True`` if *event* has a registered transition from the current state.

        Note: does **not** evaluate the guard predicate.
        """
        return (self._current_state, event) in self._transitions

    def trigger(self, event: str) -> bool:
        """Fire *event* against the current state.

        Steps:
            1. Look up ``(current_state, event)`` transition.
            2. Evaluate guard — abort if ``False``.
            3. Call ``on_exit`` of the current state.
            4. Execute transition action.
            5. Advance ``current_state`` to ``to_state``.
            6. Call ``on_enter`` of the new state.

        Returns:
            ``True`` when the transition occurred; ``False`` when no matching
            transition exists or the guard blocked it.
        """
        key = (self._current_state, event)
        cfg = self._transitions.get(key)
        if cfg is None:
            return False

        # Evaluate guard
        if cfg.guard is not None and not cfg.guard():
            return False

        # on_exit for current state
        current_cfg = self._states.get(self._current_state or "")
        if current_cfg and current_cfg.on_exit:
            current_cfg.on_exit()

        # Transition action
        if cfg.action is not None:
            cfg.action()

        # Advance state
        self._current_state = cfg.to_state

        # on_enter for new state
        new_cfg = self._states.get(cfg.to_state)
        if new_cfg and new_cfg.on_enter:
            new_cfg.on_enter()

        return True


# ===========================================================================
# Pattern 2 — HierarchicalStateMachine
# ===========================================================================


class HierarchicalStateMachine:
    """State machine that supports composite (parent/child) states.

    States can be nested.  When a parent state is entered and an initial child
    is configured, the machine automatically descends into that child (and
    recursively into its initial child, if configured).

    :meth:`is_in_state` checks whether the machine is *within* a given state,
    meaning the queried state equals the current leaf state OR is an ancestor
    of it.

    Example::

        hsm = HierarchicalStateMachine()
        hsm.add_state("active")
        hsm.add_state("active.idle")
        hsm.add_state("active.running")
        hsm.add_child_state("active", "active.idle")
        hsm.add_child_state("active", "active.running")
        hsm.set_initial_child("active", "active.idle")

        hsm.enter_state("active")
        assert hsm.current_state == "active.idle"
        assert hsm.is_in_state("active")   # True — ancestor check
    """

    def __init__(self) -> None:
        self._states: set[str] = set()
        self._children: dict[str, list[str]] = defaultdict(list)
        self._parent: dict[str, str] = {}
        self._initial_child: dict[str, str] = {}
        self._current_state: str | None = None

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def add_state(self, name: str) -> None:
        """Register a state (parent or leaf).

        Args:
            name: Unique state identifier.
        """
        self._states.add(name)

    def add_child_state(self, parent: str, child: str) -> None:
        """Register *child* as a child of *parent*.

        Args:
            parent: Parent state name.
            child:  Child state name (auto-registered if not already).
        """
        self._states.add(parent)
        self._states.add(child)
        if child not in self._children[parent]:
            self._children[parent].append(child)
        self._parent[child] = parent

    def set_initial_child(self, parent: str, child: str) -> None:
        """Designate *child* as the initial child entered when *parent* is entered.

        Args:
            parent: Composite state name.
            child:  Child state to enter automatically.
        """
        self._initial_child[parent] = child

    # ------------------------------------------------------------------
    # Runtime
    # ------------------------------------------------------------------

    @property
    def current_state(self) -> str | None:
        """Current leaf state name."""
        return self._current_state

    def enter_state(self, state: str) -> None:
        """Enter *state*, auto-descending into configured initial children.

        Args:
            state: State to enter.  If an initial child is configured for
                   this state, the machine recursively descends.
        """
        self._current_state = state
        child = self._initial_child.get(state)
        if child is not None:
            self.enter_state(child)

    def is_in_state(self, state: str) -> bool:
        """Return ``True`` if the machine is at or within *state*.

        This resolves to ``True`` when:
        - *state* equals the current leaf state, **or**
        - *state* is an ancestor of the current leaf state.

        Args:
            state: State name to test.
        """
        current = self._current_state
        while current is not None:
            if current == state:
                return True
            current = self._parent.get(current)
        return False


# ===========================================================================
# Pattern 3 — WorkflowEngine
# ===========================================================================


@dataclass
class WorkflowResult:
    """Result of a :class:`WorkflowEngine` run.

    Attributes:
        success:         ``True`` when all requested steps completed.
        completed_steps: Names of steps that finished successfully.
        failed_step:     Name of the step that caused the failure, or ``None``.
        context:         Final context dict after the run.
        error:           Human-readable error description, or ``None``.
    """

    success: bool
    completed_steps: list[str]
    failed_step: str | None
    context: dict[str, Any]
    error: str | None


@dataclass
class _StepConfig:
    """Internal registration record for a workflow step."""

    name: str
    fn: Callable[[dict[str, Any]], None]
    max_retries: int = 3
    timeout_ms: int | None = None


class WorkflowEngine:
    """Orchestrates multi-step workflows with retry and optional timeout.

    Each step is a callable that receives the shared *context* dict and may
    update it in-place.  Failures are retried with exponential back-off up to
    ``max_retries`` attempts.

    Example::

        engine = WorkflowEngine()

        def fetch(ctx):
            ctx["data"] = "fetched"

        def transform(ctx):
            ctx["data"] = ctx["data"].upper()

        engine.register_step("fetch", fetch)
        engine.register_step("transform", transform)

        result = engine.run(["fetch", "transform"], {})
        assert result.success
        assert result.context["data"] == "FETCHED"
    """

    def __init__(self) -> None:
        self._steps: dict[str, _StepConfig] = {}

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register_step(
        self,
        name: str,
        fn: Callable[[dict[str, Any]], None],
        max_retries: int = 3,
        timeout_ms: int | None = None,
    ) -> None:
        """Register a named workflow step.

        Args:
            name:        Unique step identifier.
            fn:          Callable ``(context) -> None`` that may mutate context.
            max_retries: Maximum number of attempts (default 3; 1 = no retries).
            timeout_ms:  Optional per-attempt wall-clock timeout in milliseconds.
                         Exceeded attempts are treated as failures.
        """
        self._steps[name] = _StepConfig(name=name, fn=fn, max_retries=max_retries, timeout_ms=timeout_ms)

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def _run_step_with_timeout(self, cfg: _StepConfig, context: dict[str, Any]) -> None:
        """Execute a single step, enforcing timeout_ms if configured."""
        if cfg.timeout_ms is None:
            cfg.fn(context)
            return

        timeout_sec = cfg.timeout_ms / 1000.0
        exc_holder: list[BaseException] = []

        def _target() -> None:
            try:
                cfg.fn(context)
            except Exception as exc:  # noqa: BLE001
                exc_holder.append(exc)

        thread = ThreadPoolExecutor(max_workers=1)
        fut = thread.submit(_target)
        thread.shutdown(wait=False)
        try:
            fut.result(timeout=timeout_sec)
        except TimeoutError:
            raise TimeoutError(f"Step '{cfg.name}' exceeded timeout of {cfg.timeout_ms}ms")
        if exc_holder:
            raise exc_holder[0]

    def run(self, steps: list[str], context: dict[str, Any]) -> WorkflowResult:
        """Execute *steps* in order, threading *context* through each.

        Retries each step up to ``max_retries`` times on failure, using
        exponential back-off (0.05 s × 2^attempt).  Stops at the first step
        that exhausts all retry attempts.

        Args:
            steps:   Ordered list of step names to execute.
            context: Shared mutable context dict.

        Returns:
            :class:`WorkflowResult` describing the outcome.
        """
        completed: list[str] = []

        for step_name in steps:
            cfg = self._steps.get(step_name)
            if cfg is None:
                return WorkflowResult(
                    success=False,
                    completed_steps=completed,
                    failed_step=step_name,
                    context=context,
                    error=f"Step '{step_name}' is not registered",
                )

            last_exc: Exception | None = None
            for attempt in range(cfg.max_retries):
                try:
                    self._run_step_with_timeout(cfg, context)
                    last_exc = None
                    break
                except Exception as exc:  # noqa: BLE001
                    last_exc = exc
                    if attempt < cfg.max_retries - 1:
                        time.sleep(0.05 * (2**attempt))

            if last_exc is not None:
                return WorkflowResult(
                    success=False,
                    completed_steps=completed,
                    failed_step=step_name,
                    context=context,
                    error=str(last_exc),
                )

            completed.append(step_name)

        return WorkflowResult(
            success=True,
            completed_steps=completed,
            failed_step=None,
            context=context,
            error=None,
        )


# ===========================================================================
# Pattern 4 — ConditionalRouter
# ===========================================================================


@dataclass
class _Route:
    """Internal record for a registered conditional route."""

    name: str
    condition: Callable[[dict[str, Any]], bool]
    target_steps: list[str]


class ConditionalRouter:
    """Routes workflow execution to different step lists based on context conditions.

    Routes are evaluated in registration order; the first matching route wins.
    A default fallback can be registered with :meth:`register_default`.

    Example::

        router = ConditionalRouter()
        router.register_route("fast", lambda ctx: ctx.get("priority") == "high", ["quick_step"])
        router.register_route("normal", lambda ctx: True, ["slow_step"])

        steps = router.route({"priority": "high"})
        assert steps == ["quick_step"]
    """

    def __init__(self) -> None:
        self._routes: list[_Route] = []
        self._default: list[str] | None = None

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register_route(
        self,
        name: str,
        condition: Callable[[dict[str, Any]], bool],
        target_steps: list[str],
    ) -> None:
        """Register a named conditional route.

        Args:
            name:         Unique route identifier.
            condition:    Callable ``(context) -> bool`` evaluated at routing time.
            target_steps: Step list returned when condition is ``True``.
        """
        self._routes.append(_Route(name=name, condition=condition, target_steps=target_steps))

    def register_default(self, target_steps: list[str]) -> None:
        """Set the fallback route used when no condition matches.

        Args:
            target_steps: Step list returned as the default.
        """
        self._default = target_steps

    # ------------------------------------------------------------------
    # Routing
    # ------------------------------------------------------------------

    def route(self, context: dict[str, Any]) -> list[str]:
        """Evaluate registered routes in order and return the first match.

        Args:
            context: Workflow context dict passed to each condition.

        Returns:
            The ``target_steps`` list of the first matching route, or the
            default steps if no route matches and a default is set.

        Raises:
            ValueError: If no route matches and no default is registered.
        """
        for route in self._routes:
            if route.condition(context):
                return route.target_steps

        if self._default is not None:
            return self._default

        raise ValueError("No route matched and no default is registered")


# ===========================================================================
# Pattern 5 — ParallelWorkflow
# ===========================================================================


class ParallelWorkflow:
    """Runs named workflow tasks concurrently via ``ThreadPoolExecutor``.

    Example::

        pw = ParallelWorkflow()
        pw.add_task("a", lambda ctx: ctx["x"] + 1)
        pw.add_task("b", lambda ctx: ctx["x"] * 2)

        results = pw.run({"x": 5})
        assert results["a"] == 6
        assert results["b"] == 10
    """

    def __init__(self) -> None:
        self._tasks: dict[str, Callable[[dict[str, Any]], Any]] = {}
        self._task_order: list[str] = []

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def add_task(self, name: str, fn: Callable[[dict[str, Any]], Any]) -> None:
        """Register a named concurrent task.

        Args:
            name: Unique task identifier.
            fn:   Callable ``(context) -> result`` executed in a thread.
        """
        if name not in self._tasks:
            self._task_order.append(name)
        self._tasks[name] = fn

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def run(
        self,
        context: dict[str, Any],
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        """Run all registered tasks concurrently.

        Args:
            context:         Context dict passed to every task.
            timeout_seconds: Optional wall-clock timeout.  Tasks that have not
                             finished by then are represented as
                             ``TimeoutError`` instances in the result dict.

        Returns:
            Dict mapping each task name to its return value or the exception
            instance that was raised.
        """
        results: dict[str, Any] = {}

        with ThreadPoolExecutor(max_workers=len(self._tasks) or 1) as executor:
            future_to_name: dict[Future[Any], str] = {
                executor.submit(fn, context): name for name, fn in self._tasks.items()
            }

            try:
                for future in as_completed(future_to_name, timeout=timeout_seconds):
                    name = future_to_name[future]
                    exc = future.exception()
                    results[name] = exc if exc is not None else future.result()
            except (TimeoutError, FuturesTimeoutError):
                pass  # unfinished futures handled below

        # Fill in any tasks that did not complete within the timeout
        for name in self._task_order:
            if name not in results:
                results[name] = TimeoutError(f"Task '{name}' did not complete in time")

        return results

    def run_until_first_success(
        self,
        context: dict[str, Any],
    ) -> tuple[str, Any]:
        """Return the name and result of the first task to complete successfully.

        Remaining futures are cancelled after the first success.

        Args:
            context: Context dict passed to every task.

        Returns:
            ``(task_name, result)`` tuple.

        Raises:
            RuntimeError: If every task raises an exception.
        """
        exceptions: dict[str, BaseException] = {}

        with ThreadPoolExecutor(max_workers=len(self._tasks) or 1) as executor:
            future_to_name: dict[Future[Any], str] = {
                executor.submit(fn, context): name for name, fn in self._tasks.items()
            }

            for future in as_completed(future_to_name):
                name = future_to_name[future]
                exc = future.exception()
                if exc is None:
                    # Cancel remaining
                    for f in future_to_name:
                        if f is not future:
                            f.cancel()
                    return name, future.result()
                exceptions[name] = exc

        raise RuntimeError(f"All tasks failed: { {k: str(v) for k, v in exceptions.items()} }")


# ===========================================================================
# Demo
# ===========================================================================

if __name__ == "__main__":
    import sys

    print("=" * 60)
    print("Pattern 1 — StateMachine")
    print("=" * 60)
    log: list[str] = []
    sm = StateMachine()
    sm.add_state("idle", on_enter=lambda: log.append("enter:idle"))
    sm.add_state("running", on_enter=lambda: log.append("enter:running"), on_exit=lambda: log.append("exit:running"))
    sm.add_state("paused", on_enter=lambda: log.append("enter:paused"))
    sm.add_state("stopped", on_enter=lambda: log.append("enter:stopped"))

    flag = {"go": True}
    sm.add_transition("idle", "start", "running", guard=lambda: flag["go"])
    sm.add_transition("running", "pause", "paused")
    sm.add_transition("paused", "resume", "running")
    sm.add_transition("running", "stop", "stopped")

    sm.current_state = "idle"
    print(f"Initial state: {sm.current_state}")
    sm.trigger("start")
    print(f"After 'start': {sm.current_state}")
    sm.trigger("pause")
    print(f"After 'pause': {sm.current_state}")
    sm.trigger("resume")
    print(f"After 'resume': {sm.current_state}")
    # Guard block demo
    flag["go"] = False
    sm2 = StateMachine()
    sm2.add_state("idle")
    sm2.add_state("running")
    sm2.add_transition("idle", "start", "running", guard=lambda: flag["go"])
    sm2.current_state = "idle"
    blocked = not sm2.trigger("start")
    print(f"Guard blocked transition: {blocked}")
    print(f"Callback log: {log}")

    print()
    print("=" * 60)
    print("Pattern 2 — HierarchicalStateMachine")
    print("=" * 60)
    hsm = HierarchicalStateMachine()
    hsm.add_child_state("active", "active.idle")
    hsm.add_child_state("active", "active.working")
    hsm.set_initial_child("active", "active.idle")
    hsm.enter_state("active")
    print(f"Current leaf state: {hsm.current_state}")
    print(f"is_in_state('active'): {hsm.is_in_state('active')}")
    print(f"is_in_state('active.idle'): {hsm.is_in_state('active.idle')}")
    print(f"is_in_state('active.working'): {hsm.is_in_state('active.working')}")

    print()
    print("=" * 60)
    print("Pattern 3 — WorkflowEngine")
    print("=" * 60)

    engine = WorkflowEngine()

    def _fetch(ctx: dict[str, Any]) -> None:
        ctx["data"] = "raw_record"

    def _transform(ctx: dict[str, Any]) -> None:
        ctx["data"] = ctx["data"].upper()

    def _save(ctx: dict[str, Any]) -> None:
        ctx["saved"] = True

    engine.register_step("fetch", _fetch)
    engine.register_step("transform", _transform)
    engine.register_step("save", _save)

    result = engine.run(["fetch", "transform", "save"], {})
    print(f"Success: {result.success}")
    print(f"Completed steps: {result.completed_steps}")
    print(f"Final context: {result.context}")

    # Retry demo
    call_count = {"n": 0}

    def _flaky(ctx: dict[str, Any]) -> None:
        call_count["n"] += 1
        if call_count["n"] < 3:
            raise ValueError("transient error")
        ctx["flaky_done"] = True

    engine2 = WorkflowEngine()
    engine2.register_step("flaky", _flaky, max_retries=3)
    result2 = engine2.run(["flaky"], {})
    print(f"Retry demo — success: {result2.success}, attempts: {call_count['n']}")

    print()
    print("=" * 60)
    print("Pattern 4 — ConditionalRouter")
    print("=" * 60)
    router = ConditionalRouter()
    router.register_route("premium", lambda ctx: ctx.get("tier") == "premium", ["fast_track", "priority_save"])
    router.register_route("standard", lambda ctx: ctx.get("tier") == "standard", ["normal_process"])
    router.register_default(["default_process"])

    for ctx in [{"tier": "premium"}, {"tier": "standard"}, {"tier": "unknown"}]:
        steps = router.route(ctx)
        print(f"tier={ctx['tier']!r} → {steps}")

    print()
    print("=" * 60)
    print("Pattern 5 — ParallelWorkflow")
    print("=" * 60)
    pw = ParallelWorkflow()
    pw.add_task("double", lambda ctx: ctx["x"] * 2)
    pw.add_task("square", lambda ctx: ctx["x"] ** 2)
    pw.add_task("negate", lambda ctx: -ctx["x"])

    results = pw.run({"x": 4})
    for k, v in sorted(results.items()):
        print(f"  {k}: {v}")

    pw2 = ParallelWorkflow()
    pw2.add_task("fail1", lambda ctx: (_ for _ in ()).throw(RuntimeError("err1")))
    pw2.add_task("ok", lambda ctx: "first_success")
    pw2.add_task("fail2", lambda ctx: (_ for _ in ()).throw(RuntimeError("err2")))
    name, val = pw2.run_until_first_success({"x": 1})
    print(f"First success: {name!r} → {val!r}")

    print()
    print("All patterns demonstrated successfully.")
    sys.exit(0)
