"""
Tests for 30_message_broker_patterns.py

Covers all five patterns with 50+ test cases using only the standard library.
No pytest-asyncio or external broker libraries required.
"""

from __future__ import annotations

import importlib.util
import queue
import sys
import threading
import time
import types
from pathlib import Path

import pytest

_MOD_PATH = Path(__file__).parent.parent / "examples" / "30_message_broker_patterns.py"


def _load_module():
    module_name = "message_broker_patterns_30"
    if module_name in sys.modules:
        return sys.modules[module_name]
    spec = importlib.util.spec_from_file_location(module_name, _MOD_PATH)
    mod = types.ModuleType(module_name)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def mod():
    return _load_module()


# ===========================================================================
# Pattern 1 — MessageQueue
# ===========================================================================


class TestMessageQueue:
    # --- basic enqueue/dequeue ---

    def test_enqueue_and_dequeue_single_message(self, mod):
        mq = mod.MessageQueue()
        mq.enqueue({"key": "value"})
        msg = mq.dequeue(timeout=1.0)
        assert msg == {"key": "value"}

    def test_dequeue_returns_none_on_empty_with_timeout(self, mod):
        mq = mod.MessageQueue()
        result = mq.dequeue(timeout=0.05)
        assert result is None

    def test_fifo_order_equal_priority(self, mod):
        mq = mod.MessageQueue()
        mq.enqueue({"n": 1})
        mq.enqueue({"n": 2})
        mq.enqueue({"n": 3})
        assert mq.dequeue(timeout=1.0) == {"n": 1}
        assert mq.dequeue(timeout=1.0) == {"n": 2}
        assert mq.dequeue(timeout=1.0) == {"n": 3}

    # --- priority ordering ---

    def test_higher_priority_dequeued_first(self, mod):
        mq = mod.MessageQueue()
        mq.enqueue({"body": "low"}, priority=0)
        mq.enqueue({"body": "high"}, priority=10)
        msg = mq.dequeue(timeout=1.0)
        assert msg is not None
        assert msg["body"] == "high"

    def test_priority_ordering_multiple_levels(self, mod):
        mq = mod.MessageQueue()
        mq.enqueue({"level": "medium"}, priority=5)
        mq.enqueue({"level": "low"}, priority=1)
        mq.enqueue({"level": "high"}, priority=100)
        mq.enqueue({"level": "critical"}, priority=1000)

        order = [mq.dequeue(timeout=1.0)["level"] for _ in range(4)]
        assert order == ["critical", "high", "medium", "low"]

    def test_negative_priority_dequeued_last(self, mod):
        mq = mod.MessageQueue()
        mq.enqueue({"body": "normal"}, priority=0)
        mq.enqueue({"body": "below-normal"}, priority=-1)
        first = mq.dequeue(timeout=1.0)
        assert first is not None
        assert first["body"] == "normal"
        second = mq.dequeue(timeout=1.0)
        assert second is not None
        assert second["body"] == "below-normal"

    def test_equal_priority_fifo(self, mod):
        mq = mod.MessageQueue()
        for i in range(5):
            mq.enqueue({"seq": i}, priority=5)
        seqs = [mq.dequeue(timeout=1.0)["seq"] for _ in range(5)]
        assert seqs == [0, 1, 2, 3, 4]

    # --- max_size ---

    def test_max_size_raises_queue_full(self, mod):
        mq = mod.MessageQueue(max_size=2)
        mq.enqueue({"n": 1})
        mq.enqueue({"n": 2})
        with pytest.raises(queue.Full):
            mq.enqueue({"n": 3})

    def test_unbounded_queue_accepts_many_messages(self, mod):
        mq = mod.MessageQueue()  # max_size=0 means unbounded
        for i in range(1000):
            mq.enqueue({"n": i})
        assert mq.size == 1000

    # --- peek ---

    def test_peek_returns_next_without_removing(self, mod):
        mq = mod.MessageQueue()
        mq.enqueue({"body": "peek-me"})
        peeked = mq.peek()
        assert peeked is not None
        assert peeked["body"] == "peek-me"
        assert mq.size == 1  # still in queue

    def test_peek_empty_queue_returns_none(self, mod):
        mq = mod.MessageQueue()
        assert mq.peek() is None

    def test_peek_returns_highest_priority(self, mod):
        mq = mod.MessageQueue()
        mq.enqueue({"body": "low"}, priority=0)
        mq.enqueue({"body": "high"}, priority=99)
        peeked = mq.peek()
        assert peeked is not None
        assert peeked["body"] == "high"

    # --- size and is_empty ---

    def test_size_reflects_enqueue_count(self, mod):
        mq = mod.MessageQueue()
        assert mq.size == 0
        mq.enqueue({"n": 1})
        assert mq.size == 1
        mq.enqueue({"n": 2})
        assert mq.size == 2

    def test_size_decrements_after_dequeue(self, mod):
        mq = mod.MessageQueue()
        mq.enqueue({"n": 1})
        mq.dequeue(timeout=1.0)
        assert mq.size == 0

    def test_is_empty_true_on_new_queue(self, mod):
        mq = mod.MessageQueue()
        assert mq.is_empty is True

    def test_is_empty_false_after_enqueue(self, mod):
        mq = mod.MessageQueue()
        mq.enqueue({"n": 1})
        assert mq.is_empty is False

    def test_is_empty_true_after_draining(self, mod):
        mq = mod.MessageQueue()
        mq.enqueue({"n": 1})
        mq.dequeue(timeout=1.0)
        assert mq.is_empty is True

    # --- thread-safe concurrent enqueue ---

    def test_thread_safe_concurrent_enqueue(self, mod):
        mq = mod.MessageQueue()
        errors = []

        def producer(start, count):
            try:
                for i in range(start, start + count):
                    mq.enqueue({"n": i})
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=producer, args=(t * 50, 50)) for t in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
        assert mq.size == 500

    def test_thread_safe_concurrent_enqueue_dequeue(self, mod):
        mq = mod.MessageQueue()
        produced = []
        consumed = []
        errors = []

        def producer():
            try:
                for i in range(100):
                    mq.enqueue({"n": i})
                    produced.append(i)
            except Exception as exc:
                errors.append(exc)

        def consumer():
            try:
                drained = 0
                while drained < 100:
                    msg = mq.dequeue(timeout=2.0)
                    if msg is not None:
                        consumed.append(msg["n"])
                        drained += 1
            except Exception as exc:
                errors.append(exc)

        p = threading.Thread(target=producer)
        c = threading.Thread(target=consumer)
        p.start()
        c.start()
        p.join()
        c.join()

        assert errors == []
        assert len(consumed) == 100


# ===========================================================================
# Pattern 2 — TopicExchange
# ===========================================================================


class TestTopicExchange:
    # --- exact match ---

    def test_exact_match_delivers_message(self, mod):
        ex = mod.TopicExchange()
        q = mod.MessageQueue()
        ex.bind("orders.created", q)
        ex.publish("orders.created", {"id": 1})
        assert q.size == 1

    def test_non_matching_topic_not_delivered(self, mod):
        ex = mod.TopicExchange()
        q = mod.MessageQueue()
        ex.bind("orders.created", q)
        ex.publish("orders.updated", {"id": 2})
        assert q.size == 0

    # --- * single-word wildcard ---

    def test_star_matches_single_word(self, mod):
        ex = mod.TopicExchange()
        q = mod.MessageQueue()
        ex.bind("orders.*", q)
        ex.publish("orders.created", {"id": 1})
        assert q.size == 1

    def test_star_does_not_match_two_words(self, mod):
        ex = mod.TopicExchange()
        q = mod.MessageQueue()
        ex.bind("orders.*", q)
        ex.publish("orders.created.eu", {"id": 2})
        assert q.size == 0

    def test_star_does_not_match_zero_words(self, mod):
        ex = mod.TopicExchange()
        q = mod.MessageQueue()
        ex.bind("orders.*", q)
        ex.publish("orders", {"id": 3})
        assert q.size == 0

    def test_star_in_middle_matches_single_word(self, mod):
        ex = mod.TopicExchange()
        q = mod.MessageQueue()
        ex.bind("orders.*.eu", q)
        ex.publish("orders.created.eu", {"id": 4})
        assert q.size == 1

    def test_star_in_middle_no_match_on_two_words(self, mod):
        ex = mod.TopicExchange()
        q = mod.MessageQueue()
        ex.bind("orders.*.eu", q)
        ex.publish("orders.created.processed.eu", {"id": 5})
        assert q.size == 0

    # --- # zero-or-more wildcard ---

    def test_hash_matches_zero_words(self, mod):
        ex = mod.TopicExchange()
        q = mod.MessageQueue()
        ex.bind("orders.#", q)
        ex.publish("orders", {"id": 1})
        assert q.size == 1

    def test_hash_matches_one_word(self, mod):
        ex = mod.TopicExchange()
        q = mod.MessageQueue()
        ex.bind("orders.#", q)
        ex.publish("orders.created", {"id": 2})
        assert q.size == 1

    def test_hash_matches_multiple_words(self, mod):
        ex = mod.TopicExchange()
        q = mod.MessageQueue()
        ex.bind("orders.#", q)
        ex.publish("orders.created.processed.eu", {"id": 3})
        assert q.size == 1

    def test_hash_alone_matches_all(self, mod):
        ex = mod.TopicExchange()
        q = mod.MessageQueue()
        ex.bind("#", q)
        for topic in ["a", "a.b", "a.b.c", "x.y.z.w"]:
            ex.publish(topic, {"t": topic})
        assert q.size == 4

    # --- multiple bindings ---

    def test_multiple_queues_receive_on_match(self, mod):
        ex = mod.TopicExchange()
        q1, q2 = mod.MessageQueue(), mod.MessageQueue()
        ex.bind("events.*", q1)
        ex.bind("events.#", q2)
        ex.publish("events.login", {"uid": 42})
        assert q1.size == 1
        assert q2.size == 1

    def test_non_matching_queue_not_received(self, mod):
        ex = mod.TopicExchange()
        q1, q2 = mod.MessageQueue(), mod.MessageQueue()
        ex.bind("events.*", q1)
        ex.bind("alerts.#", q2)
        ex.publish("events.login", {"uid": 42})
        assert q1.size == 1
        assert q2.size == 0

    # --- binding_count ---

    def test_binding_count_starts_at_zero(self, mod):
        ex = mod.TopicExchange()
        assert ex.binding_count == 0

    def test_binding_count_increments(self, mod):
        ex = mod.TopicExchange()
        q = mod.MessageQueue()
        ex.bind("a.*", q)
        ex.bind("b.#", q)
        assert ex.binding_count == 2

    # --- message copy independence ---

    def test_published_message_is_copied(self, mod):
        ex = mod.TopicExchange()
        q1, q2 = mod.MessageQueue(), mod.MessageQueue()
        ex.bind("t.#", q1)
        ex.bind("t.#", q2)
        original = {"mutable": [1, 2, 3]}
        ex.publish("t.x", original)
        m1 = q1.dequeue(timeout=1.0)
        m2 = q2.dequeue(timeout=1.0)
        assert m1 is not m2
        assert m1 == m2 == original


# ===========================================================================
# Pattern 3 — FanoutExchange
# ===========================================================================


class TestFanoutExchange:
    # --- all subscribers receive ---

    def test_all_bound_queues_receive_message(self, mod):
        ex = mod.FanoutExchange()
        q1, q2, q3 = mod.MessageQueue(), mod.MessageQueue(), mod.MessageQueue()
        ex.bind(q1)
        ex.bind(q2)
        ex.bind(q3)
        ex.publish({"event": "tick"})
        assert q1.size == 1
        assert q2.size == 1
        assert q3.size == 1

    def test_each_subscriber_receives_independent_copy(self, mod):
        ex = mod.FanoutExchange()
        q1, q2 = mod.MessageQueue(), mod.MessageQueue()
        ex.bind(q1)
        ex.bind(q2)
        ex.publish({"data": [1, 2, 3]})
        m1 = q1.dequeue(timeout=1.0)
        m2 = q2.dequeue(timeout=1.0)
        assert m1 is not m2
        assert m1 == m2

    # --- unbind ---

    def test_unbind_stops_delivery(self, mod):
        ex = mod.FanoutExchange()
        q1, q2 = mod.MessageQueue(), mod.MessageQueue()
        ex.bind(q1)
        ex.bind(q2)
        ex.publish({"n": 1})
        ex.unbind(q2)
        ex.publish({"n": 2})
        assert q1.size == 2
        assert q2.size == 1  # only received the first message

    def test_unbind_non_subscriber_no_error(self, mod):
        ex = mod.FanoutExchange()
        q = mod.MessageQueue()
        ex.unbind(q)  # should not raise

    def test_double_bind_does_not_duplicate(self, mod):
        ex = mod.FanoutExchange()
        q = mod.MessageQueue()
        ex.bind(q)
        ex.bind(q)  # second bind should be a no-op
        ex.publish({"n": 1})
        assert q.size == 1

    # --- empty fanout no error ---

    def test_empty_fanout_publish_no_error(self, mod):
        ex = mod.FanoutExchange()
        ex.publish({"event": "ghost"})  # no subscribers — should not raise

    # --- subscriber_count ---

    def test_subscriber_count_starts_at_zero(self, mod):
        ex = mod.FanoutExchange()
        assert ex.subscriber_count == 0

    def test_subscriber_count_increments_on_bind(self, mod):
        ex = mod.FanoutExchange()
        q1, q2 = mod.MessageQueue(), mod.MessageQueue()
        ex.bind(q1)
        ex.bind(q2)
        assert ex.subscriber_count == 2

    def test_subscriber_count_decrements_on_unbind(self, mod):
        ex = mod.FanoutExchange()
        q1, q2 = mod.MessageQueue(), mod.MessageQueue()
        ex.bind(q1)
        ex.bind(q2)
        ex.unbind(q1)
        assert ex.subscriber_count == 1


# ===========================================================================
# Pattern 4 — MessageBatch
# ===========================================================================


class TestMessageBatch:
    # --- add below batch_size returns None ---

    def test_add_below_batch_size_returns_none(self, mod):
        batch = mod.MessageBatch(batch_size=3)
        assert batch.add({"n": 1}) is None
        assert batch.add({"n": 2}) is None

    # --- add at batch_size triggers auto-flush ---

    def test_add_at_batch_size_returns_batch(self, mod):
        batch = mod.MessageBatch(batch_size=3)
        batch.add({"n": 1})
        batch.add({"n": 2})
        result = batch.add({"n": 3})
        assert result is not None
        assert len(result) == 3

    def test_auto_flushed_batch_contains_all_messages(self, mod):
        batch = mod.MessageBatch(batch_size=2)
        batch.add({"n": 1})
        result = batch.add({"n": 2})
        assert result is not None
        assert result[0] == {"n": 1}
        assert result[1] == {"n": 2}

    def test_multiple_auto_flushes(self, mod):
        batch = mod.MessageBatch(batch_size=2)
        r1 = None
        r2 = None
        for i in range(4):
            r = batch.add({"n": i})
            if i == 1:
                r1 = r
            if i == 3:
                r2 = r
        assert r1 is not None and len(r1) == 2
        assert r2 is not None and len(r2) == 2

    # --- flush partial ---

    def test_flush_returns_partial_batch(self, mod):
        batch = mod.MessageBatch(batch_size=10)
        batch.add({"n": 1})
        batch.add({"n": 2})
        result = batch.flush()
        assert len(result) == 2

    def test_flush_resets_pending_to_zero(self, mod):
        batch = mod.MessageBatch(batch_size=10)
        batch.add({"n": 1})
        batch.flush()
        assert batch.pending_count == 0

    def test_flush_empty_batch_returns_empty_list(self, mod):
        batch = mod.MessageBatch(batch_size=5)
        result = batch.flush()
        assert result == []

    # --- total_flushed accumulates ---

    def test_total_flushed_accumulates_across_auto_flushes(self, mod):
        batch = mod.MessageBatch(batch_size=2)
        for i in range(6):
            batch.add({"n": i})
        assert batch.total_flushed == 6

    def test_total_flushed_includes_partial_flush(self, mod):
        batch = mod.MessageBatch(batch_size=5)
        batch.add({"n": 1})
        batch.add({"n": 2})
        batch.flush()
        assert batch.total_flushed == 2

    def test_total_flushed_cumulative_across_mixed(self, mod):
        batch = mod.MessageBatch(batch_size=3)
        batch.add({"n": 1})
        batch.add({"n": 2})
        batch.add({"n": 3})  # auto-flush → 3
        batch.add({"n": 4})
        batch.flush()  # manual flush → 1 more
        assert batch.total_flushed == 4

    # --- pending_count ---

    def test_pending_count_starts_at_zero(self, mod):
        batch = mod.MessageBatch(batch_size=5)
        assert batch.pending_count == 0

    def test_pending_count_increments_on_add(self, mod):
        batch = mod.MessageBatch(batch_size=5)
        batch.add({"n": 1})
        assert batch.pending_count == 1
        batch.add({"n": 2})
        assert batch.pending_count == 2

    def test_pending_count_resets_after_auto_flush(self, mod):
        batch = mod.MessageBatch(batch_size=2)
        batch.add({"n": 1})
        batch.add({"n": 2})  # triggers auto-flush
        assert batch.pending_count == 0

    def test_batch_size_one_immediately_flushes(self, mod):
        batch = mod.MessageBatch(batch_size=1)
        result = batch.add({"n": 42})
        assert result is not None and len(result) == 1
        assert batch.pending_count == 0


# ===========================================================================
# Pattern 5 — DeduplicationFilter
# ===========================================================================


class TestDeduplicationFilter:
    # --- new message → True ---

    def test_new_message_returns_true(self, mod):
        flt = mod.DeduplicationFilter()
        assert flt.process({"message_id": "msg-1"}) is True

    def test_multiple_new_messages_all_true(self, mod):
        flt = mod.DeduplicationFilter()
        for i in range(5):
            assert flt.process({"message_id": f"msg-{i}"}) is True

    # --- duplicate → False ---

    def test_duplicate_message_returns_false(self, mod):
        flt = mod.DeduplicationFilter()
        flt.process({"message_id": "abc"})
        assert flt.process({"message_id": "abc"}) is False

    def test_repeated_duplicate_always_false(self, mod):
        flt = mod.DeduplicationFilter()
        flt.process({"message_id": "dup"})
        for _ in range(5):
            assert flt.process({"message_id": "dup"}) is False

    # --- is_duplicate ---

    def test_is_duplicate_false_for_new_id(self, mod):
        flt = mod.DeduplicationFilter()
        assert flt.is_duplicate("fresh-id") is False

    def test_is_duplicate_true_for_seen_id(self, mod):
        flt = mod.DeduplicationFilter()
        flt.is_duplicate("seen-id")
        assert flt.is_duplicate("seen-id") is True

    # --- window expiry ---

    def test_expired_entry_treated_as_new(self, mod):
        flt = mod.DeduplicationFilter(window_seconds=0.05)
        flt.process({"message_id": "exp-msg"})
        time.sleep(0.15)
        result = flt.process({"message_id": "exp-msg"})
        assert result is True

    def test_non_expired_entry_still_duplicate(self, mod):
        flt = mod.DeduplicationFilter(window_seconds=60.0)
        flt.process({"message_id": "live-msg"})
        result = flt.process({"message_id": "live-msg"})
        assert result is False

    # --- custom id_field ---

    def test_custom_id_field(self, mod):
        flt = mod.DeduplicationFilter()
        assert flt.process({"uid": "u-001", "data": "x"}, id_field="uid") is True
        assert flt.process({"uid": "u-001", "data": "x"}, id_field="uid") is False

    # --- seen_count and duplicate_count ---

    def test_seen_count_reflects_unique_messages(self, mod):
        flt = mod.DeduplicationFilter()
        for i in range(5):
            flt.process({"message_id": f"id-{i}"})
        assert flt.seen_count == 5

    def test_seen_count_does_not_increment_on_duplicate(self, mod):
        flt = mod.DeduplicationFilter()
        flt.process({"message_id": "x"})
        flt.process({"message_id": "x"})
        assert flt.seen_count == 1

    def test_duplicate_count_increments_on_each_duplicate(self, mod):
        flt = mod.DeduplicationFilter()
        flt.process({"message_id": "y"})
        for _ in range(3):
            flt.process({"message_id": "y"})
        assert flt.duplicate_count == 3

    def test_duplicate_count_starts_at_zero(self, mod):
        flt = mod.DeduplicationFilter()
        assert flt.duplicate_count == 0

    def test_seen_count_starts_at_zero(self, mod):
        flt = mod.DeduplicationFilter()
        assert flt.seen_count == 0

    # --- thread-safe concurrent processing ---

    def test_thread_safe_concurrent_processing(self, mod):
        flt = mod.DeduplicationFilter(window_seconds=60.0)
        results = []
        errors = []

        # 10 threads each try to process the same 5 unique IDs
        # Each ID should be accepted exactly once across all threads
        def worker(thread_id):
            try:
                for i in range(5):
                    r = flt.process({"message_id": f"shared-{i}"})
                    results.append(r)
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(t,)) for t in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
        # 5 unique IDs — each accepted exactly once
        assert flt.seen_count == 5
        # 10 threads × 5 IDs = 50 total; 5 unique → 45 duplicates
        assert flt.duplicate_count == 45

    def test_thread_safe_no_race_on_is_duplicate(self, mod):
        flt = mod.DeduplicationFilter(window_seconds=60.0)
        errors = []

        def check_many(prefix):
            try:
                for i in range(100):
                    flt.is_duplicate(f"{prefix}-{i}")
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=check_many, args=(f"t{t}",)) for t in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
