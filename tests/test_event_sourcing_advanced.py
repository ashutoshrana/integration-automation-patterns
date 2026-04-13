"""
Tests for 35_event_sourcing_advanced.py

Covers all five patterns (EventProjector, EventUpcaster, EventReplay,
CausationChain, TemporalQuery) with 70 test cases using only the standard
library.  No external dependencies required.
"""

from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

import pytest

_MOD_PATH = Path(__file__).parent.parent / "examples" / "35_event_sourcing_advanced.py"


def _load_module() -> types.ModuleType:
    module_name = "event_sourcing_advanced_35"
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
# EventProjector
# ===========================================================================


class TestEventProjector:
    # --- register + project ---

    def test_single_handler_applied(self, mod):
        proj = mod.EventProjector()
        proj.register("Created", lambda s, e: {**s, "name": e["name"]})
        state = proj.project([{"event_type": "Created", "name": "Alice"}])
        assert state["name"] == "Alice"

    def test_multiple_handlers_applied_in_order(self, mod):
        proj = mod.EventProjector()
        proj.register("Created", lambda s, e: {**s, "value": 0})
        proj.register("Incremented", lambda s, e: {**s, "value": s["value"] + 1})
        state = proj.project(
            [
                {"event_type": "Created"},
                {"event_type": "Incremented"},
                {"event_type": "Incremented"},
            ]
        )
        assert state["value"] == 2

    def test_unknown_event_type_is_skipped(self, mod):
        proj = mod.EventProjector()
        proj.register("Known", lambda s, e: {**s, "hit": True})
        state = proj.project([{"event_type": "Unknown"}, {"event_type": "Known"}])
        assert state.get("hit") is True

    def test_only_unknown_events_returns_empty_state(self, mod):
        proj = mod.EventProjector()
        state = proj.project([{"event_type": "Ghost"}, {"event_type": "Phantom"}])
        assert state == {}

    def test_empty_event_list_returns_empty_state(self, mod):
        proj = mod.EventProjector()
        proj.register("Created", lambda s, e: {**s, "x": 1})
        state = proj.project([])
        assert state == {}

    def test_state_accumulates_across_multiple_handlers(self, mod):
        proj = mod.EventProjector()
        proj.register("A", lambda s, e: {**s, "a": 1})
        proj.register("B", lambda s, e: {**s, "b": 2})
        proj.register("C", lambda s, e: {**s, "c": 3})
        state = proj.project(
            [
                {"event_type": "A"},
                {"event_type": "B"},
                {"event_type": "C"},
            ]
        )
        assert state == {"a": 1, "b": 2, "c": 3}

    def test_handler_can_overwrite_existing_key(self, mod):
        proj = mod.EventProjector()
        proj.register("Set", lambda s, e: {**s, "x": e["val"]})
        state = proj.project(
            [
                {"event_type": "Set", "val": 10},
                {"event_type": "Set", "val": 20},
            ]
        )
        assert state["x"] == 20

    def test_reset_clears_state(self, mod):
        proj = mod.EventProjector()
        proj.register("Tick", lambda s, e: {**s, "count": s.get("count", 0) + 1})
        proj.project([{"event_type": "Tick"}, {"event_type": "Tick"}])
        proj.reset()
        assert proj._state == {}

    def test_project_after_reset_starts_fresh(self, mod):
        proj = mod.EventProjector()
        proj.register("Init", lambda s, e: {**s, "ready": True})
        proj.project([{"event_type": "Init"}])
        proj.reset()
        state = proj.project([{"event_type": "Init"}])
        assert state == {"ready": True}

    def test_handler_receives_current_state(self, mod):
        received = []
        proj = mod.EventProjector()
        proj.register("A", lambda s, e: {**s, "a": 1})

        def spy(s, e):
            received.append(dict(s))
            return {**s, "b": 2}

        proj.register("B", spy)
        proj.project([{"event_type": "A"}, {"event_type": "B"}])
        assert received[0].get("a") == 1

    def test_project_returns_same_dict_as_internal_state(self, mod):
        proj = mod.EventProjector()
        proj.register("X", lambda s, e: {**s, "x": True})
        result = proj.project([{"event_type": "X"}])
        assert result is proj._state

    def test_multiple_reset_calls_are_idempotent(self, mod):
        proj = mod.EventProjector()
        proj.reset()
        proj.reset()
        assert proj._state == {}


# ===========================================================================
# EventUpcaster
# ===========================================================================


class TestEventUpcaster:
    # --- single migration ---

    def test_single_migration_applied(self, mod):
        uc = mod.EventUpcaster()

        def v1_v2(e):
            e["schema_version"] = 2
            e["migrated"] = True
            return e

        uc.register_migration(1, 2, v1_v2)
        result = uc.upcast({"schema_version": 1, "data": "x"})
        assert result["schema_version"] == 2
        assert result["migrated"] is True

    def test_chained_migrations_applied_in_order(self, mod):
        uc = mod.EventUpcaster()
        uc.register_migration(1, 2, lambda e: {**e, "schema_version": 2, "step": e.get("step", []) + ["1→2"]})
        uc.register_migration(2, 3, lambda e: {**e, "schema_version": 3, "step": e.get("step", []) + ["2→3"]})
        result = uc.upcast({"schema_version": 1, "step": []})
        assert result["schema_version"] == 3
        assert result["step"] == ["1→2", "2→3"]

    def test_no_migration_needed_for_current_version(self, mod):
        uc = mod.EventUpcaster()
        uc.register_migration(1, 2, lambda e: {**e, "schema_version": 2})
        event = {"schema_version": 2, "name": "Bob"}
        result = uc.upcast(event)
        assert result["schema_version"] == 2
        assert result["name"] == "Bob"

    def test_upcast_event_with_no_migrations_registered(self, mod):
        uc = mod.EventUpcaster()
        event = {"schema_version": 5, "data": "raw"}
        result = uc.upcast(event)
        assert result == event

    def test_current_version_returns_max_target(self, mod):
        uc = mod.EventUpcaster()
        uc.register_migration(1, 2, lambda e: e)
        uc.register_migration(2, 3, lambda e: e)
        uc.register_migration(3, 5, lambda e: e)
        assert uc.current_version == 5

    def test_current_version_zero_when_no_migrations(self, mod):
        uc = mod.EventUpcaster()
        assert uc.current_version == 0

    def test_migration_preserves_unrelated_fields(self, mod):
        uc = mod.EventUpcaster()
        uc.register_migration(1, 2, lambda e: {**e, "schema_version": 2})
        result = uc.upcast({"schema_version": 1, "payload": {"key": "val"}, "id": "abc"})
        assert result["payload"] == {"key": "val"}
        assert result["id"] == "abc"

    def test_three_hop_migration_chain(self, mod):
        uc = mod.EventUpcaster()
        for frm, to in [(1, 2), (2, 3), (3, 4)]:
            fto = to

            def mk(fto=fto):
                return lambda e: {**e, "schema_version": fto}

            uc.register_migration(frm, fto, mk())
        result = uc.upcast({"schema_version": 1})
        assert result["schema_version"] == 4

    def test_upcast_event_missing_schema_version_field(self, mod):
        uc = mod.EventUpcaster()
        uc.register_migration(0, 1, lambda e: {**e, "schema_version": 1, "upgraded": True})
        result = uc.upcast({"data": "no version"})
        assert result["schema_version"] == 1
        assert result["upgraded"] is True


# ===========================================================================
# EventReplay
# ===========================================================================


class TestEventReplay:
    def _make_log(self):
        return [
            {"sequence_id": 1, "aggregate_id": "order-1", "event_type": "OrderPlaced"},
            {"sequence_id": 2, "aggregate_id": "order-2", "event_type": "OrderPlaced"},
            {"sequence_id": 3, "aggregate_id": "order-1", "event_type": "OrderShipped"},
            {"sequence_id": 4, "aggregate_id": "order-3", "event_type": "OrderPlaced"},
            {"sequence_id": 5, "aggregate_id": "order-2", "event_type": "OrderShipped"},
        ]

    # --- since ---

    def test_since_includes_exact_boundary(self, mod):
        replay = mod.EventReplay(self._make_log())
        result = replay.since(3)
        assert [e["sequence_id"] for e in result] == [3, 4, 5]

    def test_since_1_returns_all(self, mod):
        replay = mod.EventReplay(self._make_log())
        assert len(replay.since(1)) == 5

    def test_since_beyond_max_returns_empty(self, mod):
        replay = mod.EventReplay(self._make_log())
        assert replay.since(100) == []

    def test_since_returns_correct_count(self, mod):
        replay = mod.EventReplay(self._make_log())
        assert len(replay.since(4)) == 2

    # --- by_aggregate ---

    def test_by_aggregate_correct_count(self, mod):
        replay = mod.EventReplay(self._make_log())
        assert len(replay.by_aggregate("order-1")) == 2

    def test_by_aggregate_all_match(self, mod):
        replay = mod.EventReplay(self._make_log())
        results = replay.by_aggregate("order-2")
        assert all(e["aggregate_id"] == "order-2" for e in results)

    def test_by_aggregate_unknown_returns_empty(self, mod):
        replay = mod.EventReplay(self._make_log())
        assert replay.by_aggregate("order-99") == []

    def test_by_aggregate_single_event(self, mod):
        replay = mod.EventReplay(self._make_log())
        assert len(replay.by_aggregate("order-3")) == 1

    # --- by_type ---

    def test_by_type_count(self, mod):
        replay = mod.EventReplay(self._make_log())
        assert len(replay.by_type("OrderPlaced")) == 3

    def test_by_type_all_match(self, mod):
        replay = mod.EventReplay(self._make_log())
        results = replay.by_type("OrderShipped")
        assert all(e["event_type"] == "OrderShipped" for e in results)

    def test_by_type_unknown_returns_empty(self, mod):
        replay = mod.EventReplay(self._make_log())
        assert replay.by_type("OrderCancelled") == []

    # --- between ---

    def test_between_inclusive_both_ends(self, mod):
        replay = mod.EventReplay(self._make_log())
        result = replay.between(2, 4)
        assert [e["sequence_id"] for e in result] == [2, 3, 4]

    def test_between_single_event(self, mod):
        replay = mod.EventReplay(self._make_log())
        result = replay.between(3, 3)
        assert len(result) == 1
        assert result[0]["sequence_id"] == 3

    def test_between_out_of_range_returns_empty(self, mod):
        replay = mod.EventReplay(self._make_log())
        assert replay.between(10, 20) == []

    def test_between_full_range(self, mod):
        replay = mod.EventReplay(self._make_log())
        assert len(replay.between(1, 5)) == 5

    def test_empty_log_all_methods_return_empty(self, mod):
        replay = mod.EventReplay([])
        assert replay.since(1) == []
        assert replay.by_aggregate("x") == []
        assert replay.by_type("y") == []
        assert replay.between(1, 5) == []


# ===========================================================================
# CausationChain
# ===========================================================================


class TestCausationChain:
    # --- record + caused_by ---

    def test_root_event_has_no_causation(self, mod):
        cc = mod.CausationChain()
        cc.record("root")
        assert cc.caused_by("root") is None

    def test_caused_by_returns_parent(self, mod):
        cc = mod.CausationChain()
        cc.record("evt-1")
        cc.record("evt-2", causation_id="evt-1")
        assert cc.caused_by("evt-2") == "evt-1"

    def test_caused_by_unknown_event_returns_none(self, mod):
        cc = mod.CausationChain()
        assert cc.caused_by("nonexistent") is None

    # --- chain (linear) ---

    def test_chain_single_root_event(self, mod):
        cc = mod.CausationChain()
        cc.record("root")
        assert cc.chain("root") == ["root"]

    def test_chain_linear_three_hops(self, mod):
        cc = mod.CausationChain()
        cc.record("a")
        cc.record("b", causation_id="a")
        cc.record("c", causation_id="b")
        assert cc.chain("c") == ["a", "b", "c"]

    def test_chain_unknown_event_returns_empty(self, mod):
        cc = mod.CausationChain()
        assert cc.chain("ghost") == []

    def test_chain_returns_root_to_leaf_order(self, mod):
        cc = mod.CausationChain()
        cc.record("evt-1")
        cc.record("evt-2", causation_id="evt-1")
        cc.record("evt-3", causation_id="evt-2")
        cc.record("evt-4", causation_id="evt-3")
        path = cc.chain("evt-4")
        assert path[0] == "evt-1"
        assert path[-1] == "evt-4"
        assert len(path) == 4

    # --- chain (branched) ---

    def test_chain_branched_two_leaves_same_root(self, mod):
        cc = mod.CausationChain()
        cc.record("root")
        cc.record("branch-a", causation_id="root")
        cc.record("branch-b", causation_id="root")
        cc.record("leaf-a", causation_id="branch-a")
        cc.record("leaf-b", causation_id="branch-b")
        chain_a = cc.chain("leaf-a")
        chain_b = cc.chain("leaf-b")
        assert chain_a == ["root", "branch-a", "leaf-a"]
        assert chain_b == ["root", "branch-b", "leaf-b"]

    def test_chain_includes_intermediate_nodes(self, mod):
        cc = mod.CausationChain()
        cc.record("cmd")
        cc.record("evt-a", causation_id="cmd")
        cc.record("evt-b", causation_id="evt-a")
        path = cc.chain("evt-b")
        assert "evt-a" in path

    # --- correlation_group ---

    def test_correlation_group_all_members_returned(self, mod):
        cc = mod.CausationChain()
        cc.record("e1", correlation_id="saga-X")
        cc.record("e2", correlation_id="saga-X")
        cc.record("e3", correlation_id="saga-X")
        cc.record("e4", correlation_id="saga-Y")
        group = cc.correlation_group("saga-X")
        assert set(group) == {"e1", "e2", "e3"}

    def test_correlation_group_unknown_id_returns_empty(self, mod):
        cc = mod.CausationChain()
        assert cc.correlation_group("missing") == []

    def test_correlation_group_does_not_include_other_saga(self, mod):
        cc = mod.CausationChain()
        cc.record("e1", correlation_id="saga-A")
        cc.record("e2", correlation_id="saga-B")
        assert cc.correlation_group("saga-A") == ["e1"]

    def test_event_without_correlation_id_not_in_any_group(self, mod):
        cc = mod.CausationChain()
        cc.record("orphan")
        # No groups should be created for events with no correlation_id
        assert cc.correlation_group("None") == []

    def test_correlation_group_preserves_insertion_order(self, mod):
        cc = mod.CausationChain()
        for i in range(5):
            cc.record(f"evt-{i}", correlation_id="corr")
        group = cc.correlation_group("corr")
        assert group == [f"evt-{i}" for i in range(5)]


# ===========================================================================
# TemporalQuery
# ===========================================================================


class TestTemporalQuery:
    def _make_events(self):
        return [
            {"event_type": "AccountOpened", "timestamp": 1000.0, "balance": 500},
            {"event_type": "Deposited", "timestamp": 2000.0, "amount": 100},
            {"event_type": "Withdrawn", "timestamp": 3000.0, "amount": 50},
            {"event_type": "Deposited", "timestamp": 4000.0, "amount": 200},
        ]

    def _make_projector(self, mod):
        proj = mod.EventProjector()
        proj.register("AccountOpened", lambda s, e: {**s, "balance": e["balance"]})
        proj.register("Deposited", lambda s, e: {**s, "balance": s.get("balance", 0) + e["amount"]})
        proj.register("Withdrawn", lambda s, e: {**s, "balance": s.get("balance", 0) - e["amount"]})
        return proj

    # --- state_at ---

    def test_state_at_before_all_events_returns_empty(self, mod):
        tq = mod.TemporalQuery(self._make_events())
        proj = self._make_projector(mod)
        state = tq.state_at(500.0, proj)
        assert state == {}

    def test_state_at_after_first_event(self, mod):
        tq = mod.TemporalQuery(self._make_events())
        proj = self._make_projector(mod)
        state = tq.state_at(1000.0, proj)
        assert state["balance"] == 500

    def test_state_at_after_second_event(self, mod):
        tq = mod.TemporalQuery(self._make_events())
        proj = self._make_projector(mod)
        state = tq.state_at(2500.0, proj)
        assert state["balance"] == 600

    def test_state_at_resets_projector_between_calls(self, mod):
        tq = mod.TemporalQuery(self._make_events())
        proj = self._make_projector(mod)
        _ = tq.state_at(4000.0, proj)
        # Second call should reset and recompute — not accumulate further
        state2 = tq.state_at(2000.0, proj)
        assert state2["balance"] == 600

    def test_state_at_final_timestamp_includes_all_events(self, mod):
        tq = mod.TemporalQuery(self._make_events())
        proj = self._make_projector(mod)
        state = tq.state_at(9999.0, proj)
        # 500 + 100 - 50 + 200
        assert state["balance"] == 750

    # --- events_in_window ---

    def test_events_in_window_exact_boundary(self, mod):
        tq = mod.TemporalQuery(self._make_events())
        result = tq.events_in_window(2000.0, 3000.0)
        assert len(result) == 2

    def test_events_in_window_no_events_in_range(self, mod):
        tq = mod.TemporalQuery(self._make_events())
        result = tq.events_in_window(5000.0, 6000.0)
        assert result == []

    def test_events_in_window_single_event(self, mod):
        tq = mod.TemporalQuery(self._make_events())
        result = tq.events_in_window(3000.0, 3000.0)
        assert len(result) == 1
        assert result[0]["event_type"] == "Withdrawn"

    def test_events_in_window_all_events(self, mod):
        tq = mod.TemporalQuery(self._make_events())
        result = tq.events_in_window(0.0, 99999.0)
        assert len(result) == 4

    # --- latest_before ---

    def test_latest_before_returns_most_recent(self, mod):
        tq = mod.TemporalQuery(self._make_events())
        event = tq.latest_before(3500.0)
        assert event is not None
        assert event["event_type"] == "Withdrawn"

    def test_latest_before_no_event_returns_none(self, mod):
        tq = mod.TemporalQuery(self._make_events())
        assert tq.latest_before(999.0) is None

    def test_latest_before_exclusive_boundary(self, mod):
        tq = mod.TemporalQuery(self._make_events())
        # timestamp=1000 is NOT strictly before 1000
        assert tq.latest_before(1000.0) is None

    def test_latest_before_all_events_in_past(self, mod):
        tq = mod.TemporalQuery(self._make_events())
        event = tq.latest_before(99999.0)
        assert event is not None
        assert event["timestamp"] == 4000.0

    def test_empty_log_latest_before_returns_none(self, mod):
        tq = mod.TemporalQuery([])
        assert tq.latest_before(9999.0) is None

    def test_empty_log_events_in_window_returns_empty(self, mod):
        tq = mod.TemporalQuery([])
        assert tq.events_in_window(0.0, 9999.0) == []


# ===========================================================================
# Integration tests — combine patterns
# ===========================================================================


class TestIntegration:
    def test_projector_plus_replay_subset(self, mod):
        """Replay a filtered subset of events and project them."""
        log = [
            {"sequence_id": 1, "aggregate_id": "acct-1", "event_type": "Opened", "timestamp": 100.0},
            {"sequence_id": 2, "aggregate_id": "acct-1", "event_type": "Deposited", "amount": 50, "timestamp": 200.0},
            {"sequence_id": 3, "aggregate_id": "acct-2", "event_type": "Opened", "timestamp": 300.0},
            {"sequence_id": 4, "aggregate_id": "acct-1", "event_type": "Deposited", "amount": 25, "timestamp": 400.0},
        ]
        replay = mod.EventReplay(log)
        acct1_events = replay.by_aggregate("acct-1")

        proj = mod.EventProjector()
        proj.register("Opened", lambda s, e: {**s, "balance": 0})
        proj.register("Deposited", lambda s, e: {**s, "balance": s.get("balance", 0) + e["amount"]})
        state = proj.project(acct1_events)
        assert state["balance"] == 75

    def test_temporal_query_with_replay_since(self, mod):
        """Use TemporalQuery + EventReplay together."""
        log = [
            {"sequence_id": 1, "aggregate_id": "x", "event_type": "Init", "timestamp": 500.0, "val": 10},
            {"sequence_id": 2, "aggregate_id": "x", "event_type": "Update", "timestamp": 1500.0, "val": 20},
            {"sequence_id": 3, "aggregate_id": "x", "event_type": "Update", "timestamp": 2500.0, "val": 30},
        ]
        tq = mod.TemporalQuery(log)
        proj = mod.EventProjector()
        proj.register("Init", lambda s, e: {"val": e["val"]})
        proj.register("Update", lambda s, e: {"val": e["val"]})

        state = tq.state_at(1500.0, proj)
        assert state["val"] == 20

    def test_causation_chain_with_correlation_and_replay(self, mod):
        """Record causation chain entries alongside an event replay."""
        log = [
            {"sequence_id": 1, "aggregate_id": "cart", "event_type": "CartCreated"},
            {"sequence_id": 2, "aggregate_id": "cart", "event_type": "ItemAdded"},
            {"sequence_id": 3, "aggregate_id": "cart", "event_type": "OrderPlaced"},
        ]
        replay = mod.EventReplay(log)
        cart_events = replay.by_aggregate("cart")
        assert len(cart_events) == 3

        cc = mod.CausationChain()
        cc.record("CartCreated", correlation_id="session-1")
        cc.record("ItemAdded", causation_id="CartCreated", correlation_id="session-1")
        cc.record("OrderPlaced", causation_id="ItemAdded", correlation_id="session-1")

        path = cc.chain("OrderPlaced")
        assert path == ["CartCreated", "ItemAdded", "OrderPlaced"]
        assert len(cc.correlation_group("session-1")) == 3

    def test_upcaster_then_project(self, mod):
        """Upcast events before projecting them."""
        uc = mod.EventUpcaster()
        uc.register_migration(1, 2, lambda e: {**e, "schema_version": 2, "amount": e.pop("value", 0)})

        raw_events = [
            {"event_type": "Deposited", "schema_version": 1, "value": 100},
            {"event_type": "Deposited", "schema_version": 1, "value": 200},
            {"event_type": "Deposited", "schema_version": 2, "amount": 50},
        ]
        upcasted = [uc.upcast(e) for e in raw_events]
        assert all(e["schema_version"] == 2 for e in upcasted)

        proj = mod.EventProjector()
        proj.register("Deposited", lambda s, e: {**s, "total": s.get("total", 0) + e["amount"]})
        state = proj.project(upcasted)
        assert state["total"] == 350

    def test_full_pipeline_replay_upcast_project_causation(self, mod):
        """Full pipeline: filter by aggregate, upcast, project, build causation chain."""
        log = [
            {
                "sequence_id": 1,
                "aggregate_id": "inv-1",
                "event_type": "InvoiceCreated",
                "schema_version": 1,
                "customer": "Acme",
                "timestamp": 1000.0,
            },
            {
                "sequence_id": 2,
                "aggregate_id": "inv-1",
                "event_type": "LineItemAdded",
                "schema_version": 1,
                "qty": 5,
                "timestamp": 2000.0,
            },
            {
                "sequence_id": 3,
                "aggregate_id": "inv-2",
                "event_type": "InvoiceCreated",
                "schema_version": 1,
                "customer": "Beta",
                "timestamp": 3000.0,
            },
        ]

        replay = mod.EventReplay(log)
        inv1_events = replay.by_aggregate("inv-1")

        uc = mod.EventUpcaster()
        uc.register_migration(1, 2, lambda e: {**e, "schema_version": 2, "v2": True})
        upcasted = [uc.upcast(e) for e in inv1_events]
        assert all(e["schema_version"] == 2 for e in upcasted)

        proj = mod.EventProjector()
        proj.register("InvoiceCreated", lambda s, e: {**s, "customer": e["customer"]})
        proj.register("LineItemAdded", lambda s, e: {**s, "qty": s.get("qty", 0) + e["qty"]})
        state = proj.project(upcasted)
        assert state["customer"] == "Acme"
        assert state["qty"] == 5

        cc = mod.CausationChain()
        cc.record("InvoiceCreated", correlation_id="flow-1")
        cc.record("LineItemAdded", causation_id="InvoiceCreated", correlation_id="flow-1")
        assert cc.chain("LineItemAdded") == ["InvoiceCreated", "LineItemAdded"]
