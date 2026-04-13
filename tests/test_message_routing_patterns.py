"""
Tests for 18_message_routing_patterns.py

Four Enterprise Integration Patterns for message routing:
  1. ContentBasedRouter — predicate-based channel routing
  2. MessageSplitter — one-to-many message splitting with correlation
  3. MessageAggregator — many-to-one aggregation with completion predicate
  4. MessageFilter — accept/reject with optional dead-letter channel
"""

import importlib.util
import sys
import threading
import time
import types
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Module loading
# ---------------------------------------------------------------------------

_MOD_PATH = (
    Path(__file__).parent.parent / "examples" / "18_message_routing_patterns.py"
)


def _load_module():
    module_name = "message_routing_patterns"
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
# Helper
# ---------------------------------------------------------------------------


def _msg(m, payload="test", **kwargs):
    return m.Message(payload=payload, **kwargs)


# ---------------------------------------------------------------------------
# ContentBasedRouter
# ---------------------------------------------------------------------------


class TestContentBasedRouter:

    def test_routes_to_matching_channel(self, m):
        router = m.ContentBasedRouter()
        router.add_route("orders", lambda msg: isinstance(msg.payload, dict) and msg.payload.get("type") == "order")
        router.add_route("payments", lambda msg: isinstance(msg.payload, dict) and msg.payload.get("type") == "payment")
        msg = _msg(m, payload={"type": "order", "id": 1})
        assert router.route(msg) == "orders"

    def test_default_channel_for_unmatched(self, m):
        router = m.ContentBasedRouter(default_channel="unmatched")
        router.add_route("orders", lambda msg: False)
        msg = _msg(m, payload="anything")
        assert router.route(msg) == "unmatched"

    def test_first_match_wins(self, m):
        router = m.ContentBasedRouter()
        router.add_route("first", lambda msg: True)
        router.add_route("second", lambda msg: True)
        msg = _msg(m)
        assert router.route(msg) == "first"

    def test_route_all_groups_by_channel(self, m):
        router = m.ContentBasedRouter()
        router.add_route("high", lambda msg: msg.payload == "high")
        router.add_route("low", lambda msg: msg.payload == "low")
        messages = [_msg(m, payload="high"), _msg(m, payload="low"), _msg(m, payload="high")]
        grouped = router.route_all(messages)
        assert len(grouped["high"]) == 2
        assert len(grouped["low"]) == 1

    def test_route_all_empty_list(self, m):
        router = m.ContentBasedRouter()
        result = router.route_all([])
        assert result == {} or all(len(v) == 0 for v in result.values())

    def test_custom_default_channel(self, m):
        router = m.ContentBasedRouter(default_channel="dlq")
        msg = _msg(m)
        assert router.route(msg) == "dlq"

    def test_multiple_routes_registered_correct_routing(self, m):
        router = m.ContentBasedRouter()
        for i in range(5):
            ch = f"channel_{i}"
            idx = i  # capture
            router.add_route(ch, lambda msg, n=idx: msg.payload == n)
        for i in range(5):
            msg = _msg(m, payload=i)
            assert router.route(msg) == f"channel_{i}"


# ---------------------------------------------------------------------------
# MessageSplitter
# ---------------------------------------------------------------------------


class TestMessageSplitter:

    def test_splits_into_correct_count(self, m):
        splitter = m.MessageSplitter(split_fn=lambda msg: list(msg.payload))
        msg = _msg(m, payload=[1, 2, 3])
        parts = splitter.split(msg)
        assert len(parts) == 3

    def test_each_split_has_correlation_id(self, m):
        splitter = m.MessageSplitter(split_fn=lambda msg: list(msg.payload))
        msg = _msg(m, payload=[1, 2, 3])
        parts = splitter.split(msg)
        for p in parts:
            assert p.correlation_id is not None

    def test_correlation_id_same_for_all_parts(self, m):
        splitter = m.MessageSplitter(split_fn=lambda msg: list(msg.payload))
        msg = _msg(m, payload=["a", "b", "c"])
        parts = splitter.split(msg)
        correlation_ids = {p.correlation_id for p in parts}
        assert len(correlation_ids) == 1

    def test_sequence_numbers_assigned(self, m):
        splitter = m.MessageSplitter(split_fn=lambda msg: list(msg.payload))
        msg = _msg(m, payload=["x", "y"])
        parts = splitter.split(msg)
        sequences = sorted(p.sequence for p in parts)
        assert sequences == [0, 1]

    def test_total_parts_set(self, m):
        splitter = m.MessageSplitter(split_fn=lambda msg: list(msg.payload))
        msg = _msg(m, payload=list(range(4)))
        parts = splitter.split(msg)
        for p in parts:
            assert p.total_parts == 4

    def test_parent_id_set_to_original(self, m):
        splitter = m.MessageSplitter(split_fn=lambda msg: list(msg.payload))
        msg = _msg(m, payload=[1, 2])
        original_id = msg.message_id
        parts = splitter.split(msg)
        for p in parts:
            assert p.parent_id == original_id


# ---------------------------------------------------------------------------
# MessageAggregator
# ---------------------------------------------------------------------------


class TestMessageAggregator:

    def test_returns_none_when_incomplete(self, m):
        def all_3(msgs): return len(msgs) >= 3
        def merge(msgs): return sum(msg.payload for msg in msgs)
        agg = m.MessageAggregator(
            completion_predicate=all_3,
            aggregation_strategy=merge,
        )
        msg = _msg(m, payload=1, correlation_id="c1")
        result = agg.receive(msg)
        assert result is None

    def test_completes_when_predicate_met(self, m):
        def all_3(msgs): return len(msgs) >= 3
        def merge(msgs): return [msg.payload for msg in msgs]
        agg = m.MessageAggregator(
            completion_predicate=all_3,
            aggregation_strategy=merge,
        )
        result = None
        for i in range(3):
            result = agg.receive(_msg(m, payload=i, correlation_id="cid"))
        assert result is not None

    def test_multiple_correlation_groups_independent(self, m):
        def all_2(msgs): return len(msgs) >= 2
        def merge(msgs): return [msg.payload for msg in msgs]
        agg = m.MessageAggregator(completion_predicate=all_2, aggregation_strategy=merge)

        agg.receive(_msg(m, payload="a1", correlation_id="group-A"))
        agg.receive(_msg(m, payload="b1", correlation_id="group-B"))

        result_a = agg.receive(_msg(m, payload="a2", correlation_id="group-A"))
        assert result_a is not None

        # Group B not yet complete
        assert "group-B" in agg.pending_groups()

    def test_pending_groups_returns_incomplete(self, m):
        def all_3(msgs): return len(msgs) >= 3
        def merge(msgs): return msgs
        agg = m.MessageAggregator(completion_predicate=all_3, aggregation_strategy=merge)
        agg.receive(_msg(m, payload=1, correlation_id="pending"))
        assert "pending" in agg.pending_groups()

    def test_flush_group_returns_partial_messages(self, m):
        def all_3(msgs): return len(msgs) >= 3
        def merge(msgs): return msgs
        agg = m.MessageAggregator(completion_predicate=all_3, aggregation_strategy=merge)
        agg.receive(_msg(m, payload=1, correlation_id="flush-me"))
        flushed = agg.flush_group("flush-me")
        assert flushed is not None
        assert len(flushed) == 1

    def test_flush_nonexistent_group_returns_none(self, m):
        def all_2(msgs): return len(msgs) >= 2
        def merge(msgs): return msgs
        agg = m.MessageAggregator(completion_predicate=all_2, aggregation_strategy=merge)
        result = agg.flush_group("does-not-exist")
        assert result is None

    def test_timeout_raises_aggregation_timeout(self, m):
        def never_complete(msgs): return False
        def merge(msgs): return msgs
        agg = m.MessageAggregator(
            completion_predicate=never_complete,
            aggregation_strategy=merge,
            max_wait_seconds=0.001,
        )
        agg.receive(_msg(m, payload=1, correlation_id="timeout-group"))
        time.sleep(0.05)  # Exceed timeout
        with pytest.raises(m.AggregationTimeout):
            agg.receive_with_timeout(_msg(m, payload=2, correlation_id="timeout-group"))


# ---------------------------------------------------------------------------
# MessageFilter
# ---------------------------------------------------------------------------


class TestMessageFilter:

    def test_passes_matching_message(self, m):
        f = m.MessageFilter(predicate=lambda msg: msg.payload == "keep")
        msg = _msg(m, payload="keep")
        result = f.filter(msg)
        assert result is not None
        assert result.payload == "keep"

    def test_filters_non_matching_returns_none(self, m):
        f = m.MessageFilter(predicate=lambda msg: msg.payload == "keep")
        msg = _msg(m, payload="drop")
        result = f.filter(msg)
        assert result is None

    def test_dead_letter_channel_set_on_filtered(self, m):
        f = m.MessageFilter(
            predicate=lambda msg: msg.payload == "pass",
            dead_letter_channel="dlq",
        )
        msg = _msg(m, payload="fail")
        result = f.filter(msg)
        assert result is not None
        assert result.channel == "dlq"

    def test_filter_all_separates_accepted_and_rejected(self, m):
        f = m.MessageFilter(predicate=lambda msg: msg.payload > 5)
        messages = [_msg(m, payload=i) for i in range(10)]
        accepted, rejected = f.filter_all(messages)
        assert all(msg.payload > 5 for msg in accepted)
        assert len(accepted) + len(rejected) == 10

    def test_accepted_count_increments(self, m):
        f = m.MessageFilter(predicate=lambda msg: True)
        for _ in range(3):
            f.filter(_msg(m))
        assert f.accepted_count == 3

    def test_rejected_count_increments(self, m):
        f = m.MessageFilter(predicate=lambda msg: False)
        for _ in range(4):
            f.filter(_msg(m))
        assert f.rejected_count == 4

    def test_reset_stats_clears_counts(self, m):
        f = m.MessageFilter(predicate=lambda msg: True)
        for _ in range(5):
            f.filter(_msg(m))
        f.reset_stats()
        assert f.accepted_count == 0
        assert f.rejected_count == 0
