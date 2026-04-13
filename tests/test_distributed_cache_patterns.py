"""Tests for 13_distributed_cache_patterns.py — Distributed Cache Patterns"""
from __future__ import annotations

import importlib.util
import sys
import time
import types
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Module loading
# ---------------------------------------------------------------------------


def _load_module(name: str):
    examples_dir = Path(__file__).parent.parent / "examples"
    spec = importlib.util.spec_from_file_location(
        name, examples_dir / "13_distributed_cache_patterns.py"
    )
    mod = types.ModuleType(name)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def m():
    return _load_module("distributed_cache")


# ---------------------------------------------------------------------------
# TestInMemoryStore
# ---------------------------------------------------------------------------


class TestInMemoryStore:
    def test_set_and_get(self, m):
        store = m.InMemoryStore("node-0")
        store.set("k1", {"v": 1}, ttl_ms=60_000)
        assert store.get("k1") == {"v": 1}

    def test_miss_returns_none(self, m):
        store = m.InMemoryStore("node-0")
        assert store.get("nonexistent") is None

    def test_hit_increments_counter(self, m):
        store = m.InMemoryStore("node-0")
        store.set("k2", "value", ttl_ms=60_000)
        store.get("k2")
        assert store.hit_count == 1

    def test_miss_increments_counter(self, m):
        store = m.InMemoryStore("node-0")
        store.get("missing")
        assert store.miss_count == 1

    def test_delete_removes_entry(self, m):
        store = m.InMemoryStore("node-0")
        store.set("k3", "data", ttl_ms=60_000)
        assert store.delete("k3") is True
        assert store.get("k3") is None

    def test_delete_nonexistent_returns_false(self, m):
        store = m.InMemoryStore("node-0")
        assert store.delete("ghost") is False

    def test_expired_entry_returns_none(self, m):
        store = m.InMemoryStore("node-0")
        # TTL of 1ms — should expire essentially immediately
        store.set("exp", "val", ttl_ms=1)
        time.sleep(0.01)  # 10ms
        assert store.get("exp") is None

    def test_keys_excludes_expired(self, m):
        store = m.InMemoryStore("node-0")
        store.set("alive", "val", ttl_ms=60_000)
        store.set("dead", "val", ttl_ms=1)
        time.sleep(0.01)
        keys = store.keys()
        assert "alive" in keys
        assert "dead" not in keys


# ---------------------------------------------------------------------------
# TestCacheAsidePattern
# ---------------------------------------------------------------------------


class TestCacheAsidePattern:
    def _setup(self, m):
        store = m.BackingDataStore()
        store.seed({"user:100": {"id": 100, "name": "Alice"}})
        cache = m.InMemoryStore("node-0")
        lock_mgr = m.DistributedLockManager("lock-0")
        pattern = m.CacheAsidePattern(cache, store, lock_mgr, default_ttl_ms=30_000)
        return pattern, cache, store

    def test_cache_miss_loads_from_store(self, m):
        pattern, cache, store = self._setup(m)
        val = pattern.get("user:100")
        assert val == {"id": 100, "name": "Alice"}
        assert store.read_count == 1

    def test_cache_hit_avoids_store(self, m):
        pattern, cache, store = self._setup(m)
        pattern.get("user:100")   # populates cache
        store.read_count = 0      # reset counter
        val = pattern.get("user:100")
        assert val == {"id": 100, "name": "Alice"}
        assert store.read_count == 0

    def test_missing_key_returns_none(self, m):
        pattern, cache, store = self._setup(m)
        val = pattern.get("user:999")
        assert val is None

    def test_custom_loader_used_on_miss(self, m):
        pattern, cache, store = self._setup(m)
        calls = []
        def loader(k):
            calls.append(k)
            return {"custom": True}
        val = pattern.get("special:1", loader=loader)
        assert val == {"custom": True}
        assert "special:1" in calls

    def test_invalidate_evicts_cache(self, m):
        pattern, cache, store = self._setup(m)
        pattern.get("user:100")   # populate
        assert cache.exists("user:100")
        pattern.invalidate("user:100")
        assert not cache.exists("user:100")

    def test_jitter_ttl_applied(self, m):
        store = m.BackingDataStore()
        store.seed({"k": "v"})
        cache = m.InMemoryStore("node-0")
        lock_mgr = m.DistributedLockManager("lock-0")
        ttl_strategy = m.TTLInvalidationStrategy(base_ttl_ms=10_000, jitter_factor=0.3)
        pattern = m.CacheAsidePattern(
            cache, store, lock_mgr,
            default_ttl_ms=10_000,
            ttl_strategy=ttl_strategy,
        )
        pattern.get("k")
        ttl = cache.ttl_ms("k")
        # TTL should be within ±30% of 10000ms
        assert 7_000 <= ttl <= 13_000


# ---------------------------------------------------------------------------
# TestWriteThroughCache
# ---------------------------------------------------------------------------


class TestWriteThroughCache:
    def test_put_writes_both_cache_and_store(self, m):
        cache = m.InMemoryStore("node-0")
        store = m.BackingDataStore()
        wt = m.WriteThroughCache(cache, store, default_ttl_ms=30_000)
        wt.put("order:1", {"total": 100.00})
        assert cache.get("order:1") is not None
        assert store.get("order:1") is not None
        assert store.write_count == 1

    def test_get_returns_from_cache(self, m):
        cache = m.InMemoryStore("node-0")
        store = m.BackingDataStore()
        wt = m.WriteThroughCache(cache, store, default_ttl_ms=30_000)
        wt.put("order:2", {"total": 200.00})
        store.read_count = 0
        val = wt.get("order:2")
        assert val == {"total": 200.00}
        assert store.read_count == 0

    def test_get_falls_back_to_store_on_cache_miss(self, m):
        cache = m.InMemoryStore("node-0")
        store = m.BackingDataStore()
        store.seed({"order:3": {"total": 300.00}})
        wt = m.WriteThroughCache(cache, store, default_ttl_ms=30_000)
        val = wt.get("order:3")
        assert val == {"total": 300.00}
        assert store.read_count == 1
        # Now in cache for future reads
        assert cache.exists("order:3")

    def test_delete_removes_from_both(self, m):
        cache = m.InMemoryStore("node-0")
        store = m.BackingDataStore()
        wt = m.WriteThroughCache(cache, store, default_ttl_ms=30_000)
        wt.put("order:4", {"total": 400.00})
        wt.delete("order:4")
        assert cache.get("order:4") is None
        assert store.get("order:4") is None

    def test_custom_ttl_honored(self, m):
        cache = m.InMemoryStore("node-0")
        store = m.BackingDataStore()
        wt = m.WriteThroughCache(cache, store, default_ttl_ms=60_000)
        wt.put("order:5", {"total": 500.00}, ttl_ms=5_000)
        ttl = cache.ttl_ms("order:5")
        assert ttl <= 5_000


# ---------------------------------------------------------------------------
# TestWriteBehindCache
# ---------------------------------------------------------------------------


class TestWriteBehindCache:
    def test_write_updates_cache_immediately(self, m):
        cache = m.InMemoryStore("node-0")
        store = m.BackingDataStore()
        wb = m.WriteBehindCache(cache, store, default_ttl_ms=30_000)
        wb.put("event:1", {"seq": 1})
        assert cache.get("event:1") == {"seq": 1}
        assert store.write_count == 0  # not written yet

    def test_flush_drains_buffer_to_store(self, m):
        cache = m.InMemoryStore("node-0")
        store = m.BackingDataStore()
        wb = m.WriteBehindCache(cache, store, default_ttl_ms=30_000)
        for i in range(5):
            wb.put(f"ev:{i}", {"i": i})
        assert wb.pending_count() == 5
        flushed = wb.flush()
        assert flushed == 5
        assert store.write_count == 5
        assert wb.pending_count() == 0

    def test_buffer_deduplicates_same_key(self, m):
        cache = m.InMemoryStore("node-0")
        store = m.BackingDataStore()
        wb = m.WriteBehindCache(cache, store, default_ttl_ms=30_000)
        wb.put("k", "v1")
        wb.put("k", "v2")
        wb.put("k", "v3")
        assert wb.pending_count() == 1  # dict deduplicates
        wb.flush()
        assert store.get("k") == "v3"  # latest value wins

    def test_get_returns_buffered_value(self, m):
        cache = m.InMemoryStore("node-0")
        store = m.BackingDataStore()
        wb = m.WriteBehindCache(cache, store, default_ttl_ms=30_000)
        wb.put("session:x", {"token": "abc"})
        val = wb.get("session:x")
        assert val == {"token": "abc"}

    def test_delete_removes_from_buffer_and_cache(self, m):
        cache = m.InMemoryStore("node-0")
        store = m.BackingDataStore()
        wb = m.WriteBehindCache(cache, store, default_ttl_ms=30_000)
        wb.put("temp", "data")
        assert wb.pending_count() == 1
        wb.delete("temp")
        assert wb.pending_count() == 0
        assert cache.get("temp") is None

    def test_max_buffer_triggers_auto_flush(self, m):
        cache = m.InMemoryStore("node-0")
        store = m.BackingDataStore()
        wb = m.WriteBehindCache(cache, store, max_buffer_size=3, default_ttl_ms=30_000)
        for i in range(3):
            wb.put(f"auto:{i}", i)
        # Buffer should have been auto-flushed when reaching max_buffer_size
        assert store.write_count > 0


# ---------------------------------------------------------------------------
# TestTTLInvalidationStrategy
# ---------------------------------------------------------------------------


class TestTTLInvalidationStrategy:
    def test_jitter_spreads_ttls(self, m):
        strategy = m.TTLInvalidationStrategy(base_ttl_ms=10_000, jitter_factor=0.2)
        ttls = [strategy.compute_ttl_ms() for _ in range(20)]
        assert min(ttls) >= 8_000
        assert max(ttls) <= 12_000
        # Values should vary (extremely unlikely to all be identical)
        assert len(set(round(t) for t in ttls)) > 1

    def test_base_override_respected(self, m):
        strategy = m.TTLInvalidationStrategy(base_ttl_ms=10_000, jitter_factor=0.0)
        ttl = strategy.compute_ttl_ms(base_override=20_000)
        assert ttl == pytest.approx(20_000, abs=1)

    def test_dependency_registration(self, m):
        strategy = m.TTLInvalidationStrategy()
        strategy.register_dependency("user:1", "user:1:orders")
        strategy.register_dependency("user:1", "user:1:cart")
        deps = strategy.get_dependent_keys("user:1")
        assert "user:1:orders" in deps
        assert "user:1:cart" in deps

    def test_transitive_dependency_resolution(self, m):
        strategy = m.TTLInvalidationStrategy()
        strategy.register_dependency("A", "B")
        strategy.register_dependency("B", "C")
        strategy.register_dependency("C", "D")
        deps = strategy.get_dependent_keys("A")
        assert "B" in deps
        assert "C" in deps
        assert "D" in deps

    def test_no_dependents_returns_empty(self, m):
        strategy = m.TTLInvalidationStrategy()
        deps = strategy.get_dependent_keys("isolated_key")
        assert len(deps) == 0

    def test_sliding_window_renewal_detection(self, m):
        store = m.InMemoryStore("node-0")
        store.set("hot", "v", ttl_ms=5_000)
        strategy = m.TTLInvalidationStrategy(
            base_ttl_ms=10_000,
            sliding_window_ms=8_000,
        )
        entry = store._data["hot"]
        # TTL remaining (≈5000ms) < sliding_window_ms (8000ms) → should renew
        assert strategy.should_renew_on_read(entry) is True

    def test_no_sliding_window_disabled(self, m):
        store = m.InMemoryStore("node-0")
        store.set("cold", "v", ttl_ms=5_000)
        strategy = m.TTLInvalidationStrategy(sliding_window_ms=0)
        entry = store._data["cold"]
        assert strategy.should_renew_on_read(entry) is False


# ---------------------------------------------------------------------------
# TestDistributedLockManager
# ---------------------------------------------------------------------------


class TestDistributedLockManager:
    def test_acquire_returns_token(self, m):
        mgr = m.DistributedLockManager("mgr-0")
        token = mgr.acquire("lock:1", holder_id="w1", ttl_ms=5_000)
        assert token is not None
        assert token.holder_id == "w1"
        assert token.is_valid
        mgr.release(token)

    def test_second_acquire_fails_while_held(self, m):
        mgr = m.DistributedLockManager("mgr-0")
        t1 = mgr.acquire("lock:2", holder_id="w1", ttl_ms=5_000)
        t2 = mgr.acquire("lock:2", holder_id="w2", ttl_ms=5_000)
        assert t1 is not None
        assert t2 is None
        mgr.release(t1)

    def test_release_allows_reacquire(self, m):
        mgr = m.DistributedLockManager("mgr-0")
        t1 = mgr.acquire("lock:3", holder_id="w1", ttl_ms=5_000)
        mgr.release(t1)
        t2 = mgr.acquire("lock:3", holder_id="w2", ttl_ms=5_000)
        assert t2 is not None
        mgr.release(t2)

    def test_fencing_token_sequence_increments(self, m):
        mgr = m.DistributedLockManager("mgr-0")
        t1 = mgr.acquire("lock:4", holder_id="w1", ttl_ms=5_000)
        seq1 = t1.sequence
        mgr.release(t1)
        t2 = mgr.acquire("lock:4", holder_id="w2", ttl_ms=5_000)
        assert t2.sequence > seq1
        mgr.release(t2)

    def test_validate_token_valid_while_held(self, m):
        mgr = m.DistributedLockManager("mgr-0")
        t = mgr.acquire("lock:5", holder_id="w1", ttl_ms=5_000)
        assert mgr.validate_token(t) is True
        mgr.release(t)

    def test_validate_token_invalid_after_release(self, m):
        mgr = m.DistributedLockManager("mgr-0")
        t = mgr.acquire("lock:6", holder_id="w1", ttl_ms=5_000)
        mgr.release(t)
        assert mgr.validate_token(t) is False

    def test_is_locked(self, m):
        mgr = m.DistributedLockManager("mgr-0")
        assert not mgr.is_locked("lock:7")
        t = mgr.acquire("lock:7", holder_id="w1", ttl_ms=5_000)
        assert mgr.is_locked("lock:7")
        mgr.release(t)
        assert not mgr.is_locked("lock:7")

    def test_different_keys_independent(self, m):
        mgr = m.DistributedLockManager("mgr-0")
        t1 = mgr.acquire("key:A", holder_id="w1", ttl_ms=5_000)
        t2 = mgr.acquire("key:B", holder_id="w2", ttl_ms=5_000)
        assert t1 is not None
        assert t2 is not None
        mgr.release(t1)
        mgr.release(t2)


# ---------------------------------------------------------------------------
# TestCacheInvalidationBus
# ---------------------------------------------------------------------------


class TestCacheInvalidationBus:
    def test_publish_delivers_to_subscribers(self, m):
        bus = m.CacheInvalidationBus("bus-0")
        received = []
        bus.subscribe(lambda evt: received.append(evt))
        bus.publish("product:1", m.InvalidationReason.EXPLICIT_DELETE, "node-a")
        assert len(received) == 1
        assert received[0].key == "product:1"

    def test_published_count_tracked(self, m):
        bus = m.CacheInvalidationBus("bus-0")
        bus.publish("k1", m.InvalidationReason.TTL_EXPIRED, "node-a")
        bus.publish("k2", m.InvalidationReason.TTL_EXPIRED, "node-a")
        assert bus.published_count() == 2

    def test_invalidating_cache_evicts_on_bus_event(self, m):
        bus = m.CacheInvalidationBus("bus-0")
        node_a = m.InvalidatingCache("node-a", bus)
        node_b = m.InvalidatingCache("node-b", bus)
        # Seed both nodes
        node_a.set("product:X", "old", ttl_ms=60_000)
        node_b.set("product:X", "old", ttl_ms=60_000)
        # Node A invalidates and publishes
        node_a.invalidate_and_publish("product:X")
        # Node B should have evicted
        assert node_b.get("product:X") is None

    def test_source_node_does_not_self_evict(self, m):
        bus = m.CacheInvalidationBus("bus-0")
        node_a = m.InvalidatingCache("node-a", bus)
        node_b = m.InvalidatingCache("node-b", bus)
        node_a.set("config:1", "v", ttl_ms=60_000)
        node_b.set("config:1", "v", ttl_ms=60_000)
        node_a.set("config:1", "v_new", ttl_ms=60_000)
        node_a.invalidate_and_publish("config:1", m.InvalidationReason.EXPLICIT_DELETE)
        # After publish, node_a set the new value — node_a should not self-evict
        # (it deleted config:1 locally before publishing, so it's None, but not via bus eviction)
        # Node B should be evicted
        assert node_b.get("config:1") is None

    def test_dependency_key_propagated_in_event(self, m):
        bus = m.CacheInvalidationBus("bus-0")
        received = []
        bus.subscribe(lambda e: received.append(e))
        bus.publish(
            "user:1:orders",
            m.InvalidationReason.DEPENDENCY_CHANGED,
            "node-a",
            dependency_key="user:1",
        )
        assert received[0].dependency_key == "user:1"
        assert received[0].reason == m.InvalidationReason.DEPENDENCY_CHANGED


# ---------------------------------------------------------------------------
# TestCacheWarmupStrategy
# ---------------------------------------------------------------------------


class TestCacheWarmupStrategy:
    def test_warmup_from_records(self, m):
        cache = m.InMemoryStore("node-0")
        store = m.BackingDataStore()
        warmup = m.CacheWarmupStrategy(cache, store, default_ttl_ms=60_000)
        records = [
            m.WarmupRecord(key="product:1", value={"name": "A"}, priority=100),
            m.WarmupRecord(key="product:2", value={"name": "B"}, priority=50),
        ]
        loaded = warmup.warmup_from_records(records)
        assert loaded == 2
        assert cache.get("product:1") is not None
        assert cache.get("product:2") is not None

    def test_priority_order_preserved(self, m):
        cache = m.InMemoryStore("node-0")
        store = m.BackingDataStore()
        warmup = m.CacheWarmupStrategy(cache, store, max_warmup_keys=1, default_ttl_ms=60_000)
        records = [
            m.WarmupRecord(key="low", value="L", priority=1),
            m.WarmupRecord(key="high", value="H", priority=100),
        ]
        warmup.warmup_from_records(records)
        # Only 1 key loaded — should be the high-priority one
        assert cache.get("high") is not None
        assert cache.get("low") is None

    def test_warmup_from_key_list(self, m):
        cache = m.InMemoryStore("node-0")
        store = m.BackingDataStore()
        store.seed({"a": 1, "b": 2, "c": 3})
        warmup = m.CacheWarmupStrategy(cache, store, default_ttl_ms=60_000)
        loaded = warmup.warmup_from_key_list(["a", "b"])
        assert loaded == 2
        assert cache.get("a") == 1
        assert cache.get("b") == 2

    def test_warmup_from_store_prefix(self, m):
        cache = m.InMemoryStore("node-0")
        store = m.BackingDataStore()
        store.seed({
            "config:timeout": 30,
            "config:retries": 3,
            "user:001": "alice",
        })
        warmup = m.CacheWarmupStrategy(cache, store, default_ttl_ms=60_000)
        loaded = warmup.warmup_from_store_prefix("config:")
        assert loaded == 2
        assert cache.get("config:timeout") == 30
        assert cache.get("config:retries") == 3
        assert cache.get("user:001") is None

    def test_max_warmup_keys_respected(self, m):
        cache = m.InMemoryStore("node-0")
        store = m.BackingDataStore()
        store.seed({f"k:{i}": i for i in range(20)})
        warmup = m.CacheWarmupStrategy(cache, store, max_warmup_keys=5, default_ttl_ms=60_000)
        loaded = warmup.warmup_from_key_list([f"k:{i}" for i in range(20)])
        assert loaded == 5
        assert warmup.loaded_count == 5

    def test_loaded_count_accumulates(self, m):
        cache = m.InMemoryStore("node-0")
        store = m.BackingDataStore()
        store.seed({"x": 1, "y": 2})
        warmup = m.CacheWarmupStrategy(cache, store, default_ttl_ms=60_000)
        warmup.warmup_from_key_list(["x"])
        warmup.warmup_from_key_list(["y"])
        assert warmup.loaded_count == 2


# ---------------------------------------------------------------------------
# TestDemos (smoke tests)
# ---------------------------------------------------------------------------


class TestDemos:
    def test_demo_cache_aside(self, m):
        m.demo_cache_aside_stampede_protection()

    def test_demo_write_through(self, m):
        m.demo_write_through_cache()

    def test_demo_write_behind(self, m):
        m.demo_write_behind_cache()

    def test_demo_ttl(self, m):
        m.demo_ttl_invalidation_strategy()

    def test_demo_distributed_lock(self, m):
        m.demo_distributed_lock()

    def test_demo_invalidation_bus(self, m):
        m.demo_cache_invalidation_bus()

    def test_demo_warmup(self, m):
        m.demo_cache_warmup()
