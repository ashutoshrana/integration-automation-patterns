"""
Tests for 36_workflow_state_machine.py

Covers all five patterns (StateMachine, HierarchicalStateMachine,
WorkflowEngine, ConditionalRouter, ParallelWorkflow) with 72 test cases
using only the standard library.  No external dependencies required.
"""

from __future__ import annotations

import importlib.util
import sys
import time
import types
from pathlib import Path

import pytest

_MOD_PATH = Path(__file__).parent.parent / "examples" / "36_workflow_state_machine.py"


def _load_module() -> types.ModuleType:
    module_name = "workflow_state_machine_36"
    if module_name in sys.modules:
        return sys.modules[module_name]
    spec = importlib.util.spec_from_file_location(module_name, _MOD_PATH)
    mod = types.ModuleType(module_name)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


@pytest.fixture(scope="module")
def mod() -> types.ModuleType:
    return _load_module()


# ===========================================================================
# StateMachine
# ===========================================================================


class TestStateMachine:
    # --- basic transitions ---

    def test_transition_fires_and_returns_true(self, mod):
        sm = mod.StateMachine()
        sm.add_state("a")
        sm.add_state("b")
        sm.add_transition("a", "go", "b")
        sm.current_state = "a"
        assert sm.trigger("go") is True
        assert sm.current_state == "b"

    def test_unknown_event_returns_false(self, mod):
        sm = mod.StateMachine()
        sm.add_state("a")
        sm.add_state("b")
        sm.add_transition("a", "go", "b")
        sm.current_state = "a"
        assert sm.trigger("unknown") is False
        assert sm.current_state == "a"

    def test_event_from_wrong_state_returns_false(self, mod):
        sm = mod.StateMachine()
        sm.add_state("a")
        sm.add_state("b")
        sm.add_state("c")
        sm.add_transition("a", "go", "b")
        sm.current_state = "c"
        assert sm.trigger("go") is False
        assert sm.current_state == "c"

    def test_chain_multiple_transitions(self, mod):
        sm = mod.StateMachine()
        for s in ("idle", "running", "paused", "stopped"):
            sm.add_state(s)
        sm.add_transition("idle", "start", "running")
        sm.add_transition("running", "pause", "paused")
        sm.add_transition("paused", "resume", "running")
        sm.add_transition("running", "stop", "stopped")
        sm.current_state = "idle"
        sm.trigger("start")
        sm.trigger("pause")
        sm.trigger("resume")
        sm.trigger("stop")
        assert sm.current_state == "stopped"

    # --- guard conditions ---

    def test_guard_allows_transition(self, mod):
        sm = mod.StateMachine()
        sm.add_state("a")
        sm.add_state("b")
        sm.add_transition("a", "go", "b", guard=lambda: True)
        sm.current_state = "a"
        assert sm.trigger("go") is True
        assert sm.current_state == "b"

    def test_guard_blocks_transition(self, mod):
        sm = mod.StateMachine()
        sm.add_state("a")
        sm.add_state("b")
        sm.add_transition("a", "go", "b", guard=lambda: False)
        sm.current_state = "a"
        assert sm.trigger("go") is False
        assert sm.current_state == "a"

    def test_guard_evaluated_dynamically(self, mod):
        allowed = {"v": False}
        sm = mod.StateMachine()
        sm.add_state("a")
        sm.add_state("b")
        sm.add_transition("a", "go", "b", guard=lambda: allowed["v"])
        sm.current_state = "a"
        assert sm.trigger("go") is False
        allowed["v"] = True
        assert sm.trigger("go") is True
        assert sm.current_state == "b"

    # --- on_enter / on_exit callbacks ---

    def test_on_enter_called_when_entering_state(self, mod):
        calls: list[str] = []
        sm = mod.StateMachine()
        sm.add_state("a")
        sm.add_state("b", on_enter=lambda: calls.append("enter_b"))
        sm.add_transition("a", "go", "b")
        sm.current_state = "a"
        sm.trigger("go")
        assert calls == ["enter_b"]

    def test_on_exit_called_when_leaving_state(self, mod):
        calls: list[str] = []
        sm = mod.StateMachine()
        sm.add_state("a", on_exit=lambda: calls.append("exit_a"))
        sm.add_state("b")
        sm.add_transition("a", "go", "b")
        sm.current_state = "a"
        sm.trigger("go")
        assert calls == ["exit_a"]

    def test_on_exit_before_on_enter_order(self, mod):
        order: list[str] = []
        sm = mod.StateMachine()
        sm.add_state("a", on_exit=lambda: order.append("exit_a"))
        sm.add_state("b", on_enter=lambda: order.append("enter_b"))
        sm.add_transition("a", "go", "b")
        sm.current_state = "a"
        sm.trigger("go")
        assert order == ["exit_a", "enter_b"]

    def test_callbacks_not_called_when_guard_blocks(self, mod):
        calls: list[str] = []
        sm = mod.StateMachine()
        sm.add_state("a", on_exit=lambda: calls.append("exit_a"))
        sm.add_state("b", on_enter=lambda: calls.append("enter_b"))
        sm.add_transition("a", "go", "b", guard=lambda: False)
        sm.current_state = "a"
        sm.trigger("go")
        assert calls == []

    # --- action callback ---

    def test_action_called_during_transition(self, mod):
        calls: list[str] = []
        sm = mod.StateMachine()
        sm.add_state("a")
        sm.add_state("b")
        sm.add_transition("a", "go", "b", action=lambda: calls.append("action"))
        sm.current_state = "a"
        sm.trigger("go")
        assert calls == ["action"]

    def test_action_called_between_exit_and_enter(self, mod):
        order: list[str] = []
        sm = mod.StateMachine()
        sm.add_state("a", on_exit=lambda: order.append("exit"))
        sm.add_state("b", on_enter=lambda: order.append("enter"))
        sm.add_transition("a", "go", "b", action=lambda: order.append("action"))
        sm.current_state = "a"
        sm.trigger("go")
        assert order == ["exit", "action", "enter"]

    # --- can_trigger ---

    def test_can_trigger_true_when_transition_registered(self, mod):
        sm = mod.StateMachine()
        sm.add_state("a")
        sm.add_state("b")
        sm.add_transition("a", "go", "b")
        sm.current_state = "a"
        assert sm.can_trigger("go") is True

    def test_can_trigger_false_when_no_transition(self, mod):
        sm = mod.StateMachine()
        sm.add_state("a")
        sm.current_state = "a"
        assert sm.can_trigger("go") is False

    def test_can_trigger_false_in_wrong_state(self, mod):
        sm = mod.StateMachine()
        sm.add_state("a")
        sm.add_state("b")
        sm.add_state("c")
        sm.add_transition("a", "go", "b")
        sm.current_state = "c"
        assert sm.can_trigger("go") is False

    # --- current_state property ---

    def test_current_state_set_directly(self, mod):
        sm = mod.StateMachine()
        sm.add_state("x")
        sm.current_state = "x"
        assert sm.current_state == "x"

    def test_current_state_initially_none(self, mod):
        sm = mod.StateMachine()
        assert sm.current_state is None


# ===========================================================================
# HierarchicalStateMachine
# ===========================================================================


class TestHierarchicalStateMachine:
    # --- add_state / add_child_state ---

    def test_enter_leaf_state(self, mod):
        hsm = mod.HierarchicalStateMachine()
        hsm.add_state("idle")
        hsm.enter_state("idle")
        assert hsm.current_state == "idle"

    def test_enter_parent_without_initial_child_stays_at_parent(self, mod):
        hsm = mod.HierarchicalStateMachine()
        hsm.add_child_state("active", "active.idle")
        hsm.add_child_state("active", "active.running")
        # No initial child configured
        hsm.enter_state("active")
        assert hsm.current_state == "active"

    def test_enter_parent_descends_to_initial_child(self, mod):
        hsm = mod.HierarchicalStateMachine()
        hsm.add_child_state("active", "active.idle")
        hsm.add_child_state("active", "active.running")
        hsm.set_initial_child("active", "active.idle")
        hsm.enter_state("active")
        assert hsm.current_state == "active.idle"

    def test_deep_hierarchy_auto_descent(self, mod):
        hsm = mod.HierarchicalStateMachine()
        hsm.add_child_state("top", "top.mid")
        hsm.add_child_state("top.mid", "top.mid.leaf")
        hsm.set_initial_child("top", "top.mid")
        hsm.set_initial_child("top.mid", "top.mid.leaf")
        hsm.enter_state("top")
        assert hsm.current_state == "top.mid.leaf"

    # --- is_in_state ---

    def test_is_in_state_exact_match(self, mod):
        hsm = mod.HierarchicalStateMachine()
        hsm.add_state("idle")
        hsm.enter_state("idle")
        assert hsm.is_in_state("idle") is True

    def test_is_in_state_ancestor(self, mod):
        hsm = mod.HierarchicalStateMachine()
        hsm.add_child_state("active", "active.idle")
        hsm.set_initial_child("active", "active.idle")
        hsm.enter_state("active")
        assert hsm.is_in_state("active") is True

    def test_is_in_state_sibling_returns_false(self, mod):
        hsm = mod.HierarchicalStateMachine()
        hsm.add_child_state("active", "active.idle")
        hsm.add_child_state("active", "active.running")
        hsm.set_initial_child("active", "active.idle")
        hsm.enter_state("active")
        assert hsm.is_in_state("active.running") is False

    def test_is_in_state_unrelated_state_returns_false(self, mod):
        hsm = mod.HierarchicalStateMachine()
        hsm.add_state("idle")
        hsm.add_state("stopped")
        hsm.enter_state("idle")
        assert hsm.is_in_state("stopped") is False

    def test_is_in_state_none_current_returns_false(self, mod):
        hsm = mod.HierarchicalStateMachine()
        hsm.add_state("idle")
        assert hsm.is_in_state("idle") is False

    def test_is_in_state_grandparent(self, mod):
        hsm = mod.HierarchicalStateMachine()
        hsm.add_child_state("root", "root.mid")
        hsm.add_child_state("root.mid", "root.mid.leaf")
        hsm.set_initial_child("root", "root.mid")
        hsm.set_initial_child("root.mid", "root.mid.leaf")
        hsm.enter_state("root")
        assert hsm.is_in_state("root") is True
        assert hsm.is_in_state("root.mid") is True
        assert hsm.is_in_state("root.mid.leaf") is True


# ===========================================================================
# WorkflowEngine
# ===========================================================================


class TestWorkflowEngine:
    # --- success path ---

    def test_single_step_success(self, mod):
        engine = mod.WorkflowEngine()
        engine.register_step("s1", lambda ctx: ctx.update({"done": True}))
        result = engine.run(["s1"], {})
        assert result.success is True
        assert result.completed_steps == ["s1"]
        assert result.context["done"] is True
        assert result.failed_step is None
        assert result.error is None

    def test_multi_step_context_passed_through(self, mod):
        engine = mod.WorkflowEngine()
        engine.register_step("init", lambda ctx: ctx.update({"val": 1}))
        engine.register_step("double", lambda ctx: ctx.update({"val": ctx["val"] * 2}))
        engine.register_step("inc", lambda ctx: ctx.update({"val": ctx["val"] + 1}))
        result = engine.run(["init", "double", "inc"], {})
        assert result.success is True
        assert result.context["val"] == 3
        assert result.completed_steps == ["init", "double", "inc"]

    def test_empty_steps_returns_success(self, mod):
        engine = mod.WorkflowEngine()
        result = engine.run([], {})
        assert result.success is True
        assert result.completed_steps == []

    # --- failure and retries ---

    def test_always_failing_step_exhausts_retries(self, mod):
        engine = mod.WorkflowEngine()
        calls = {"n": 0}

        def _bad(ctx):
            calls["n"] += 1
            raise ValueError("always fails")

        engine.register_step("bad", _bad, max_retries=3)
        result = engine.run(["bad"], {})
        assert result.success is False
        assert result.failed_step == "bad"
        assert result.error is not None
        assert calls["n"] == 3  # tried exactly max_retries times

    def test_transient_failure_retried_to_success(self, mod):
        engine = mod.WorkflowEngine()
        calls = {"n": 0}

        def _flaky(ctx):
            calls["n"] += 1
            if calls["n"] < 3:
                raise RuntimeError("transient")
            ctx["flaky"] = True

        engine.register_step("flaky", _flaky, max_retries=3)
        result = engine.run(["flaky"], {})
        assert result.success is True
        assert result.context.get("flaky") is True
        assert calls["n"] == 3

    def test_steps_after_failure_not_executed(self, mod):
        engine = mod.WorkflowEngine()
        executed = {"second": False}

        engine.register_step("fail", lambda ctx: (_ for _ in ()).throw(RuntimeError("fail")), max_retries=1)
        engine.register_step("second", lambda ctx: executed.update({"second": True}))

        result = engine.run(["fail", "second"], {})
        assert result.success is False
        assert executed["second"] is False
        assert result.failed_step == "fail"

    def test_completed_steps_accurate_on_partial_failure(self, mod):
        engine = mod.WorkflowEngine()
        engine.register_step("ok1", lambda ctx: ctx.update({"ok1": True}))
        engine.register_step("ok2", lambda ctx: ctx.update({"ok2": True}))
        engine.register_step("bad", lambda ctx: (_ for _ in ()).throw(RuntimeError("bad")), max_retries=1)
        result = engine.run(["ok1", "ok2", "bad"], {})
        assert result.completed_steps == ["ok1", "ok2"]
        assert result.failed_step == "bad"

    def test_unregistered_step_returns_error(self, mod):
        engine = mod.WorkflowEngine()
        result = engine.run(["ghost"], {})
        assert result.success is False
        assert result.failed_step == "ghost"
        assert result.error is not None

    def test_max_retries_one_means_no_retry(self, mod):
        calls = {"n": 0}
        engine = mod.WorkflowEngine()

        def _bad(ctx):
            calls["n"] += 1
            raise ValueError("fail")

        engine.register_step("bad", _bad, max_retries=1)
        result = engine.run(["bad"], {})
        assert result.success is False
        assert calls["n"] == 1

    def test_workflow_result_dataclass_fields(self, mod):
        engine = mod.WorkflowEngine()
        result = engine.run([], {})
        assert hasattr(result, "success")
        assert hasattr(result, "completed_steps")
        assert hasattr(result, "failed_step")
        assert hasattr(result, "context")
        assert hasattr(result, "error")

    # --- timeout ---

    def test_timeout_exceeded_causes_failure(self, mod):
        engine = mod.WorkflowEngine()

        def _slow(ctx):
            time.sleep(0.5)
            ctx["done"] = True

        engine.register_step("slow", _slow, max_retries=1, timeout_ms=50)
        result = engine.run(["slow"], {})
        assert result.success is False
        assert result.failed_step == "slow"

    def test_step_within_timeout_succeeds(self, mod):
        engine = mod.WorkflowEngine()

        def _fast(ctx):
            ctx["fast"] = True

        engine.register_step("fast", _fast, max_retries=1, timeout_ms=2000)
        result = engine.run(["fast"], {})
        assert result.success is True


# ===========================================================================
# ConditionalRouter
# ===========================================================================


class TestConditionalRouter:
    # --- basic routing ---

    def test_first_matching_route_returned(self, mod):
        router = mod.ConditionalRouter()
        router.register_route("r1", lambda ctx: ctx.get("x") == 1, ["step_a"])
        router.register_route("r2", lambda ctx: ctx.get("x") == 2, ["step_b"])
        assert router.route({"x": 1}) == ["step_a"]
        assert router.route({"x": 2}) == ["step_b"]

    def test_first_match_wins_not_best_match(self, mod):
        router = mod.ConditionalRouter()
        router.register_route("always", lambda ctx: True, ["first"])
        router.register_route("also_match", lambda ctx: True, ["second"])
        assert router.route({}) == ["first"]

    def test_no_match_no_default_raises_value_error(self, mod):
        router = mod.ConditionalRouter()
        router.register_route("r1", lambda ctx: False, ["step_a"])
        with pytest.raises(ValueError):
            router.route({})

    def test_default_fallback_used_when_no_match(self, mod):
        router = mod.ConditionalRouter()
        router.register_route("r1", lambda ctx: False, ["step_a"])
        router.register_default(["default_step"])
        assert router.route({}) == ["default_step"]

    def test_default_not_used_when_route_matches(self, mod):
        router = mod.ConditionalRouter()
        router.register_route("r1", lambda ctx: True, ["step_a"])
        router.register_default(["default_step"])
        assert router.route({}) == ["step_a"]

    def test_empty_router_no_default_raises(self, mod):
        router = mod.ConditionalRouter()
        with pytest.raises(ValueError):
            router.route({})

    def test_empty_router_with_default_returns_default(self, mod):
        router = mod.ConditionalRouter()
        router.register_default(["only"])
        assert router.route({}) == ["only"]

    def test_condition_receives_context(self, mod):
        router = mod.ConditionalRouter()
        router.register_route("check", lambda ctx: ctx.get("flag") is True, ["flag_steps"])
        router.register_default(["other"])
        assert router.route({"flag": True}) == ["flag_steps"]
        assert router.route({"flag": False}) == ["other"]

    def test_multiple_step_list_returned(self, mod):
        router = mod.ConditionalRouter()
        router.register_route("multi", lambda ctx: True, ["s1", "s2", "s3"])
        assert router.route({}) == ["s1", "s2", "s3"]

    def test_route_empty_target_steps(self, mod):
        router = mod.ConditionalRouter()
        router.register_route("empty", lambda ctx: True, [])
        assert router.route({}) == []


# ===========================================================================
# ParallelWorkflow
# ===========================================================================


class TestParallelWorkflow:
    # --- run (all complete) ---

    def test_all_tasks_run_and_return_results(self, mod):
        pw = mod.ParallelWorkflow()
        pw.add_task("double", lambda ctx: ctx["x"] * 2)
        pw.add_task("square", lambda ctx: ctx["x"] ** 2)
        results = pw.run({"x": 3})
        assert results["double"] == 6
        assert results["square"] == 9

    def test_failing_task_result_is_exception(self, mod):
        pw = mod.ParallelWorkflow()
        pw.add_task("ok", lambda ctx: 42)
        pw.add_task("bad", lambda ctx: (_ for _ in ()).throw(RuntimeError("boom")))
        results = pw.run({})
        assert results["ok"] == 42
        assert isinstance(results["bad"], RuntimeError)

    def test_context_passed_to_all_tasks(self, mod):
        pw = mod.ParallelWorkflow()
        pw.add_task("a", lambda ctx: ctx["key"])
        pw.add_task("b", lambda ctx: ctx["key"] + "_b")
        results = pw.run({"key": "val"})
        assert results["a"] == "val"
        assert results["b"] == "val_b"

    def test_single_task(self, mod):
        pw = mod.ParallelWorkflow()
        pw.add_task("only", lambda ctx: "done")
        results = pw.run({})
        assert results["only"] == "done"

    def test_tasks_run_concurrently(self, mod):
        """Two 100 ms sleeps in parallel should take ~100 ms, not ~200 ms."""
        pw = mod.ParallelWorkflow()
        pw.add_task("t1", lambda ctx: time.sleep(0.1))
        pw.add_task("t2", lambda ctx: time.sleep(0.1))
        start = time.monotonic()
        pw.run({})
        elapsed = time.monotonic() - start
        assert elapsed < 0.18  # well under 200 ms serial execution

    # --- run_until_first_success ---

    def test_first_success_returned(self, mod):
        pw = mod.ParallelWorkflow()
        pw.add_task("ok", lambda ctx: "winner")
        name, val = pw.run_until_first_success({})
        assert name == "ok"
        assert val == "winner"

    def test_all_fail_raises_runtime_error(self, mod):
        pw = mod.ParallelWorkflow()
        pw.add_task("f1", lambda ctx: (_ for _ in ()).throw(RuntimeError("e1")))
        pw.add_task("f2", lambda ctx: (_ for _ in ()).throw(RuntimeError("e2")))
        with pytest.raises(RuntimeError):
            pw.run_until_first_success({})

    def test_first_success_among_failures(self, mod):
        pw = mod.ParallelWorkflow()
        pw.add_task("bad", lambda ctx: (_ for _ in ()).throw(RuntimeError("bad")))
        pw.add_task("good", lambda ctx: "good_result")
        name, val = pw.run_until_first_success({})
        assert name == "good"
        assert val == "good_result"

    def test_run_result_keys_match_registered_tasks(self, mod):
        pw = mod.ParallelWorkflow()
        pw.add_task("x", lambda ctx: 1)
        pw.add_task("y", lambda ctx: 2)
        pw.add_task("z", lambda ctx: 3)
        results = pw.run({})
        assert set(results.keys()) == {"x", "y", "z"}

    # --- timeout ---

    def test_run_timeout_fills_remaining_tasks(self, mod):
        pw = mod.ParallelWorkflow()
        pw.add_task("fast", lambda ctx: "ok")
        pw.add_task("slow", lambda ctx: (time.sleep(5), "never")[1])
        results = pw.run({}, timeout_seconds=0.1)
        # "fast" should complete; "slow" should be a TimeoutError
        assert results["fast"] == "ok"
        assert isinstance(results["slow"], TimeoutError)

    # --- thread safety ---

    def test_multiple_parallel_runs_independent(self, mod):
        """Ensure two separate ParallelWorkflow instances don't share state."""
        pw1 = mod.ParallelWorkflow()
        pw2 = mod.ParallelWorkflow()
        pw1.add_task("a", lambda ctx: "pw1")
        pw2.add_task("a", lambda ctx: "pw2")
        r1 = pw1.run({})
        r2 = pw2.run({})
        assert r1["a"] == "pw1"
        assert r2["a"] == "pw2"


# ===========================================================================
# Additional edge-case tests
# ===========================================================================


class TestStateMachineEdgeCases:
    def test_same_event_registered_twice_last_wins(self, mod):
        """Re-registering a transition for the same (state, event) replaces it."""
        sm = mod.StateMachine()
        sm.add_state("a")
        sm.add_state("b")
        sm.add_state("c")
        sm.add_transition("a", "go", "b")
        sm.add_transition("a", "go", "c")  # overwrite
        sm.current_state = "a"
        sm.trigger("go")
        assert sm.current_state == "c"

    def test_trigger_with_no_initial_state_returns_false(self, mod):
        sm = mod.StateMachine()
        sm.add_state("a")
        sm.add_state("b")
        sm.add_transition("a", "go", "b")
        # current_state is None — no transition registered for (None, "go")
        assert sm.trigger("go") is False

    def test_self_loop_transition(self, mod):
        calls: list[str] = []
        sm = mod.StateMachine()
        sm.add_state("a", on_enter=lambda: calls.append("enter"), on_exit=lambda: calls.append("exit"))
        sm.add_transition("a", "loop", "a")
        sm.current_state = "a"
        result = sm.trigger("loop")
        assert result is True
        assert sm.current_state == "a"
        assert "exit" in calls
        assert "enter" in calls


class TestWorkflowEngineEdgeCases:
    def test_context_mutations_visible_to_subsequent_steps(self, mod):
        engine = mod.WorkflowEngine()
        engine.register_step("s1", lambda ctx: ctx.update({"seq": [1]}))
        engine.register_step("s2", lambda ctx: ctx["seq"].append(2))
        engine.register_step("s3", lambda ctx: ctx["seq"].append(3))
        result = engine.run(["s1", "s2", "s3"], {})
        assert result.context["seq"] == [1, 2, 3]

    def test_context_unchanged_after_failure(self, mod):
        engine = mod.WorkflowEngine()
        engine.register_step("good", lambda ctx: ctx.update({"before": True}))
        engine.register_step("bad", lambda ctx: (_ for _ in ()).throw(RuntimeError("boom")), max_retries=1)
        result = engine.run(["good", "bad"], {})
        assert result.context.get("before") is True


class TestConditionalRouterEdgeCases:
    def test_route_name_not_required_to_be_unique(self, mod):
        """Duplicate names are allowed; order still determines priority."""
        router = mod.ConditionalRouter()
        router.register_route("dup", lambda ctx: ctx.get("v") == 1, ["path_1"])
        router.register_route("dup", lambda ctx: ctx.get("v") == 2, ["path_2"])
        assert router.route({"v": 1}) == ["path_1"]
        assert router.route({"v": 2}) == ["path_2"]

    def test_overwrite_default_uses_last_set(self, mod):
        router = mod.ConditionalRouter()
        router.register_default(["first_default"])
        router.register_default(["second_default"])
        assert router.route({}) == ["second_default"]


class TestHierarchicalStateMachineEdgeCases:
    def test_add_child_auto_registers_both_states(self, mod):
        hsm = mod.HierarchicalStateMachine()
        hsm.add_child_state("parent", "child")
        hsm.enter_state("child")
        assert hsm.current_state == "child"

    def test_enter_state_directly_bypasses_initial_child(self, mod):
        hsm = mod.HierarchicalStateMachine()
        hsm.add_child_state("active", "active.idle")
        hsm.add_child_state("active", "active.running")
        hsm.set_initial_child("active", "active.idle")
        hsm.enter_state("active.running")  # direct enter bypasses initial child
        assert hsm.current_state == "active.running"
