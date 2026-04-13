"""
Tests for 31_service_discovery_patterns.py

Covers all five patterns (ServiceInstance, ServiceRegistry, LoadBalancer,
HealthChecker, ServiceMesh) with 55 test cases using only the standard library.
No external dependencies required.
"""

from __future__ import annotations

import importlib.util
import sys
import threading
import time
import types
from pathlib import Path
from unittest.mock import patch

import pytest

_MOD_PATH = (
    Path(__file__).parent.parent / "examples" / "31_service_discovery_patterns.py"
)


def _load_module():
    module_name = "service_discovery_patterns_31"
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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_instance(mod, service_name="svc", instance_id="i-1",
                   host="127.0.0.1", port=8080, weight=1,
                   health_status="HEALTHY"):
    """Convenience factory for ServiceInstance."""
    inst = mod.ServiceInstance(
        service_name=service_name,
        instance_id=instance_id,
        host=host,
        port=port,
        weight=weight,
    )
    inst.health_status = health_status
    return inst


# ===========================================================================
# ServiceInstance
# ===========================================================================


class TestServiceInstance:

    def test_required_fields_set_correctly(self, mod):
        inst = mod.ServiceInstance("svc", "i-1", "10.0.0.1", 9000)
        assert inst.service_name == "svc"
        assert inst.instance_id == "i-1"
        assert inst.host == "10.0.0.1"
        assert inst.port == 9000

    def test_default_health_status_is_healthy(self, mod):
        inst = mod.ServiceInstance("svc", "i-1", "localhost", 8080)
        assert inst.health_status == "HEALTHY"

    def test_default_weight_is_one(self, mod):
        inst = mod.ServiceInstance("svc", "i-1", "localhost", 8080)
        assert inst.weight == 1

    def test_default_metadata_is_empty_dict(self, mod):
        inst = mod.ServiceInstance("svc", "i-1", "localhost", 8080)
        assert inst.metadata == {}

    def test_default_metadata_not_shared_between_instances(self, mod):
        a = mod.ServiceInstance("svc", "i-1", "localhost", 8080)
        b = mod.ServiceInstance("svc", "i-2", "localhost", 8081)
        a.metadata["key"] = "value"
        assert "key" not in b.metadata

    def test_last_heartbeat_is_recent_float(self, mod):
        before = time.monotonic()
        inst = mod.ServiceInstance("svc", "i-1", "localhost", 8080)
        after = time.monotonic()
        assert before <= inst.last_heartbeat <= after

    def test_custom_fields_accepted(self, mod):
        inst = mod.ServiceInstance(
            service_name="payment",
            instance_id="p-99",
            host="192.168.1.1",
            port=443,
            metadata={"region": "eu-west-1"},
            health_status="UNHEALTHY",
            weight=5,
        )
        assert inst.weight == 5
        assert inst.health_status == "UNHEALTHY"
        assert inst.metadata["region"] == "eu-west-1"


# ===========================================================================
# ServiceRegistry
# ===========================================================================


class TestServiceRegistry:

    # --- register ---

    def test_register_single_instance(self, mod):
        registry = mod.ServiceRegistry()
        inst = _make_instance(mod)
        registry.register(inst)
        assert len(registry.get_instances("svc")) == 1

    def test_register_multiple_instances_same_service(self, mod):
        registry = mod.ServiceRegistry()
        for i in range(3):
            registry.register(_make_instance(mod, instance_id=f"i-{i}"))
        assert len(registry.get_instances("svc")) == 3

    def test_register_replaces_existing_key(self, mod):
        registry = mod.ServiceRegistry()
        inst_v1 = _make_instance(mod, port=8080)
        inst_v2 = _make_instance(mod, port=9090)
        registry.register(inst_v1)
        registry.register(inst_v2)
        result = registry.get_instances("svc")
        assert len(result) == 1
        assert result[0].port == 9090

    # --- deregister ---

    def test_deregister_removes_instance(self, mod):
        registry = mod.ServiceRegistry()
        registry.register(_make_instance(mod))
        registry.deregister("svc", "i-1")
        assert registry.get_instances("svc") == []

    def test_deregister_nonexistent_raises_key_error(self, mod):
        registry = mod.ServiceRegistry()
        with pytest.raises(KeyError):
            registry.deregister("svc", "ghost")

    def test_deregister_only_removes_target(self, mod):
        registry = mod.ServiceRegistry()
        registry.register(_make_instance(mod, instance_id="i-1"))
        registry.register(_make_instance(mod, instance_id="i-2"))
        registry.deregister("svc", "i-1")
        remaining = registry.get_instances("svc")
        assert len(remaining) == 1
        assert remaining[0].instance_id == "i-2"

    # --- get_instances (HEALTHY only) ---

    def test_get_instances_returns_only_healthy(self, mod):
        registry = mod.ServiceRegistry()
        registry.register(_make_instance(mod, instance_id="healthy", health_status="HEALTHY"))
        registry.register(_make_instance(mod, instance_id="sick", health_status="UNHEALTHY"))
        result = registry.get_instances("svc")
        assert len(result) == 1
        assert result[0].instance_id == "healthy"

    def test_get_instances_empty_when_all_unhealthy(self, mod):
        registry = mod.ServiceRegistry()
        inst = _make_instance(mod)
        inst.health_status = "UNHEALTHY"
        registry.register(inst)
        assert registry.get_instances("svc") == []

    def test_get_instances_unknown_service_returns_empty(self, mod):
        registry = mod.ServiceRegistry()
        assert registry.get_instances("no-such-service") == []

    def test_get_instances_does_not_return_other_services(self, mod):
        registry = mod.ServiceRegistry()
        registry.register(_make_instance(mod, service_name="svc-a", instance_id="a-1"))
        registry.register(_make_instance(mod, service_name="svc-b", instance_id="b-1"))
        result = registry.get_instances("svc-a")
        assert len(result) == 1
        assert result[0].service_name == "svc-a"

    # --- heartbeat ---

    def test_heartbeat_updates_last_heartbeat(self, mod):
        registry = mod.ServiceRegistry()
        inst = _make_instance(mod)
        inst.last_heartbeat = time.monotonic() - 100.0   # old
        registry.register(inst)
        before = inst.last_heartbeat
        registry.heartbeat("svc", "i-1")
        assert inst.last_heartbeat > before

    def test_heartbeat_marks_instance_healthy(self, mod):
        registry = mod.ServiceRegistry()
        inst = _make_instance(mod, health_status="UNHEALTHY")
        registry.register(inst)
        registry.heartbeat("svc", "i-1")
        assert inst.health_status == "HEALTHY"

    def test_heartbeat_nonexistent_raises_key_error(self, mod):
        registry = mod.ServiceRegistry()
        with pytest.raises(KeyError):
            registry.heartbeat("svc", "ghost")

    # --- evict_stale ---

    def test_evict_stale_marks_old_instance_unhealthy(self, mod):
        registry = mod.ServiceRegistry()
        inst = _make_instance(mod)
        inst.last_heartbeat = time.monotonic() - 60.0   # older than 30s TTL
        registry.register(inst)
        count = registry.evict_stale(ttl_seconds=30.0)
        assert count == 1
        assert inst.health_status == "UNHEALTHY"

    def test_evict_stale_leaves_fresh_instance_healthy(self, mod):
        registry = mod.ServiceRegistry()
        inst = _make_instance(mod)   # last_heartbeat = now
        registry.register(inst)
        count = registry.evict_stale(ttl_seconds=30.0)
        assert count == 0
        assert inst.health_status == "HEALTHY"

    def test_evict_stale_returns_count(self, mod):
        registry = mod.ServiceRegistry()
        for i in range(3):
            inst = _make_instance(mod, instance_id=f"i-{i}")
            inst.last_heartbeat = time.monotonic() - 100.0
            registry.register(inst)
        count = registry.evict_stale(ttl_seconds=30.0)
        assert count == 3

    def test_evict_stale_already_unhealthy_not_double_counted(self, mod):
        """Instances already UNHEALTHY before eviction should not be counted."""
        registry = mod.ServiceRegistry()
        inst = _make_instance(mod, health_status="UNHEALTHY")
        inst.last_heartbeat = time.monotonic() - 100.0
        registry.register(inst)
        count = registry.evict_stale(ttl_seconds=30.0)
        assert count == 0

    def test_evict_stale_instance_not_removed_from_registry(self, mod):
        """Evicted instances remain in the store but are UNHEALTHY."""
        registry = mod.ServiceRegistry()
        inst = _make_instance(mod)
        inst.last_heartbeat = time.monotonic() - 100.0
        registry.register(inst)
        registry.evict_stale(ttl_seconds=30.0)
        all_svcs = registry.all_services()
        assert "svc" in all_svcs
        assert len(all_svcs["svc"]) == 1

    # --- all_services ---

    def test_all_services_returns_all_statuses(self, mod):
        registry = mod.ServiceRegistry()
        registry.register(_make_instance(mod, instance_id="h", health_status="HEALTHY"))
        registry.register(_make_instance(mod, instance_id="u", health_status="UNHEALTHY"))
        all_svcs = registry.all_services()
        assert len(all_svcs["svc"]) == 2

    def test_all_services_groups_by_service_name(self, mod):
        registry = mod.ServiceRegistry()
        registry.register(_make_instance(mod, service_name="alpha", instance_id="a"))
        registry.register(_make_instance(mod, service_name="beta", instance_id="b"))
        all_svcs = registry.all_services()
        assert set(all_svcs.keys()) == {"alpha", "beta"}

    def test_all_services_empty_registry(self, mod):
        registry = mod.ServiceRegistry()
        assert registry.all_services() == {}

    # --- thread safety ---

    def test_register_thread_safe(self, mod):
        registry = mod.ServiceRegistry()
        errors = []

        def worker(start):
            try:
                for i in range(start, start + 20):
                    registry.register(
                        _make_instance(mod, service_name="tsvc", instance_id=f"t-{i}")
                    )
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(t * 20,)) for t in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
        assert len(registry.get_instances("tsvc")) == 100


# ===========================================================================
# LoadBalancer
# ===========================================================================


class TestLoadBalancer:

    # --- round_robin ---

    def test_round_robin_cycles_in_order(self, mod):
        lb = mod.LoadBalancer(strategy="round_robin")
        instances = [_make_instance(mod, instance_id=f"i-{i}") for i in range(3)]
        selections = [lb.select(instances).instance_id for _ in range(6)]
        assert selections == ["i-0", "i-1", "i-2", "i-0", "i-1", "i-2"]

    def test_round_robin_single_instance_always_same(self, mod):
        lb = mod.LoadBalancer(strategy="round_robin")
        inst = _make_instance(mod)
        for _ in range(5):
            assert lb.select([inst]) is inst

    def test_round_robin_index_starts_at_zero(self, mod):
        lb = mod.LoadBalancer(strategy="round_robin")
        assert lb._index == 0

    def test_round_robin_index_increments(self, mod):
        lb = mod.LoadBalancer(strategy="round_robin")
        instances = [_make_instance(mod, instance_id=f"i-{i}") for i in range(3)]
        lb.select(instances)
        assert lb._index == 1
        lb.select(instances)
        assert lb._index == 2

    def test_round_robin_wraps_around(self, mod):
        lb = mod.LoadBalancer(strategy="round_robin")
        instances = [_make_instance(mod, instance_id=f"i-{i}") for i in range(2)]
        ids = [lb.select(instances).instance_id for _ in range(4)]
        assert ids == ["i-0", "i-1", "i-0", "i-1"]

    # --- random ---

    def test_random_returns_instance_from_list(self, mod):
        lb = mod.LoadBalancer(strategy="random")
        instances = [_make_instance(mod, instance_id=f"i-{i}") for i in range(5)]
        for _ in range(20):
            chosen = lb.select(instances)
            assert chosen in instances

    def test_random_uses_random_choice(self, mod):
        """Verify random strategy delegates to random.choice."""
        lb = mod.LoadBalancer(strategy="random")
        instances = [_make_instance(mod, instance_id="only")]
        mod_path = "service_discovery_patterns_31.random.choice"
        with patch(mod_path, return_value=instances[0]) as mock_choice:
            result = lb.select(instances)
            mock_choice.assert_called_once_with(instances)
        assert result.instance_id == "only"

    # --- weighted ---

    def test_weighted_returns_instance_from_list(self, mod):
        lb = mod.LoadBalancer(strategy="weighted")
        instances = [
            _make_instance(mod, instance_id="a", weight=5),
            _make_instance(mod, instance_id="b", weight=5),
        ]
        for _ in range(20):
            assert lb.select(instances) in instances

    def test_weighted_higher_weight_chosen_more_often(self, mod):
        lb = mod.LoadBalancer(strategy="weighted")
        instances = [
            _make_instance(mod, instance_id="heavy", weight=9),
            _make_instance(mod, instance_id="light", weight=1),
        ]
        counter = {"heavy": 0, "light": 0}
        for _ in range(1000):
            counter[lb.select(instances).instance_id] += 1
        # heavy should win ~90% of the time; allow ±8% slack
        assert counter["heavy"] > 800, f"Expected >800 heavy selections, got {counter['heavy']}"

    def test_weighted_single_instance_always_returned(self, mod):
        lb = mod.LoadBalancer(strategy="weighted")
        inst = _make_instance(mod, weight=42)
        for _ in range(5):
            assert lb.select([inst]) is inst

    # --- empty list raises ValueError ---

    def test_select_raises_on_empty_list_round_robin(self, mod):
        lb = mod.LoadBalancer(strategy="round_robin")
        with pytest.raises(ValueError):
            lb.select([])

    def test_select_raises_on_empty_list_random(self, mod):
        lb = mod.LoadBalancer(strategy="random")
        with pytest.raises(ValueError):
            lb.select([])

    def test_select_raises_on_empty_list_weighted(self, mod):
        lb = mod.LoadBalancer(strategy="weighted")
        with pytest.raises(ValueError):
            lb.select([])

    # --- invalid strategy ---

    def test_invalid_strategy_raises_value_error(self, mod):
        with pytest.raises(ValueError):
            mod.LoadBalancer(strategy="unicorn")

    # --- strategy property ---

    def test_strategy_property_returns_configured_strategy(self, mod):
        lb = mod.LoadBalancer(strategy="weighted")
        assert lb.strategy == "weighted"


# ===========================================================================
# HealthChecker
# ===========================================================================


class TestHealthChecker:

    def test_zero_failure_rate_always_passes(self, mod):
        checker = mod.HealthChecker(failure_rate=0.0)
        inst = _make_instance(mod)
        for _ in range(20):
            assert checker.check(inst) is True

    def test_full_failure_rate_always_fails(self, mod):
        checker = mod.HealthChecker(failure_rate=1.0)
        inst = _make_instance(mod)
        for _ in range(20):
            assert checker.check(inst) is False

    def test_failure_rate_property(self, mod):
        checker = mod.HealthChecker(failure_rate=0.3)
        assert checker.failure_rate == 0.3

    def test_invalid_failure_rate_negative(self, mod):
        with pytest.raises(ValueError):
            mod.HealthChecker(failure_rate=-0.1)

    def test_invalid_failure_rate_above_one(self, mod):
        with pytest.raises(ValueError):
            mod.HealthChecker(failure_rate=1.1)

    def test_check_all_returns_dict_keyed_by_instance_id(self, mod):
        checker = mod.HealthChecker(failure_rate=0.0)
        instances = [_make_instance(mod, instance_id=f"i-{i}") for i in range(3)]
        results = checker.check_all(instances)
        assert set(results.keys()) == {"i-0", "i-1", "i-2"}

    def test_check_all_passes_when_failure_rate_zero(self, mod):
        checker = mod.HealthChecker(failure_rate=0.0)
        instances = [_make_instance(mod, instance_id=f"i-{i}") for i in range(5)]
        results = checker.check_all(instances)
        assert all(results.values())

    def test_check_all_fails_when_failure_rate_one(self, mod):
        checker = mod.HealthChecker(failure_rate=1.0)
        instances = [_make_instance(mod, instance_id=f"i-{i}") for i in range(5)]
        results = checker.check_all(instances)
        assert not any(results.values())

    def test_check_all_empty_list_returns_empty_dict(self, mod):
        checker = mod.HealthChecker()
        assert checker.check_all([]) == {}


# ===========================================================================
# ServiceMesh
# ===========================================================================


class TestServiceMesh:

    def _build_mesh(self, mod, failure_rate=0.0, strategy="round_robin"):
        registry = mod.ServiceRegistry()
        balancer = mod.LoadBalancer(strategy=strategy)
        checker = mod.HealthChecker(failure_rate=failure_rate)
        mesh = mod.ServiceMesh(registry, balancer, checker)
        return registry, balancer, checker, mesh

    # --- route happy path ---

    def test_route_returns_service_instance(self, mod):
        registry, _, _, mesh = self._build_mesh(mod)
        registry.register(_make_instance(mod))
        result = mesh.route("svc")
        assert isinstance(result, mod.ServiceInstance)

    def test_route_selects_from_healthy_only(self, mod):
        registry, _, _, mesh = self._build_mesh(mod)
        registry.register(_make_instance(mod, instance_id="h", health_status="HEALTHY"))
        registry.register(_make_instance(mod, instance_id="u", health_status="UNHEALTHY"))
        # Run 10 times — should never return the unhealthy instance
        for _ in range(10):
            assert mesh.route("svc").instance_id == "h"

    def test_route_round_robins_across_instances(self, mod):
        registry, _, _, mesh = self._build_mesh(mod, strategy="round_robin")
        for i in range(3):
            registry.register(_make_instance(mod, instance_id=f"i-{i}"))
        ids = [mesh.route("svc").instance_id for _ in range(6)]
        assert ids == ["i-0", "i-1", "i-2", "i-0", "i-1", "i-2"]

    # --- route raises when no healthy instances ---

    def test_route_raises_runtime_error_no_instances(self, mod):
        _, _, _, mesh = self._build_mesh(mod)
        with pytest.raises(RuntimeError):
            mesh.route("no-such-service")

    def test_route_raises_runtime_error_all_fail_health_check(self, mod):
        registry, _, _, mesh = self._build_mesh(mod, failure_rate=1.0)
        registry.register(_make_instance(mod))
        with pytest.raises(RuntimeError):
            mesh.route("svc")

    def test_route_raises_after_all_instances_become_unhealthy(self, mod):
        registry, _, _, mesh = self._build_mesh(mod)
        inst = _make_instance(mod)
        registry.register(inst)
        inst.health_status = "UNHEALTHY"
        with pytest.raises(RuntimeError):
            mesh.route("svc")

    # --- rebalance removes failed instances ---

    def test_rebalance_removes_all_when_failure_rate_one(self, mod):
        registry, _, _, mesh = self._build_mesh(mod, failure_rate=1.0)
        for i in range(3):
            registry.register(_make_instance(mod, instance_id=f"i-{i}"))
        removed = mesh.rebalance("svc")
        assert removed == 3
        assert registry.all_services().get("svc", []) == []

    def test_rebalance_removes_none_when_failure_rate_zero(self, mod):
        registry, _, _, mesh = self._build_mesh(mod, failure_rate=0.0)
        for i in range(3):
            registry.register(_make_instance(mod, instance_id=f"i-{i}"))
        removed = mesh.rebalance("svc")
        assert removed == 0
        assert len(registry.all_services()["svc"]) == 3

    def test_rebalance_unknown_service_returns_zero(self, mod):
        _, _, _, mesh = self._build_mesh(mod)
        assert mesh.rebalance("ghost-service") == 0

    def test_rebalance_returns_count_removed(self, mod):
        registry, _, _, mesh = self._build_mesh(mod, failure_rate=1.0)
        for i in range(5):
            registry.register(_make_instance(mod, instance_id=f"i-{i}"))
        count = mesh.rebalance("svc")
        assert count == 5

    # --- register_and_check ---

    def test_register_and_check_returns_true_on_pass(self, mod):
        registry, _, _, mesh = self._build_mesh(mod, failure_rate=0.0)
        inst = _make_instance(mod)
        result = mesh.register_and_check(inst)
        assert result is True
        assert len(registry.get_instances("svc")) == 1

    def test_register_and_check_returns_false_on_fail(self, mod):
        registry, _, _, mesh = self._build_mesh(mod, failure_rate=1.0)
        inst = _make_instance(mod)
        result = mesh.register_and_check(inst)
        assert result is False

    def test_register_and_check_deregisters_on_fail(self, mod):
        registry, _, _, mesh = self._build_mesh(mod, failure_rate=1.0)
        inst = _make_instance(mod)
        mesh.register_and_check(inst)
        # Instance should be removed from registry
        assert registry.all_services().get("svc", []) == []

    def test_register_and_check_keeps_instance_on_pass(self, mod):
        registry, _, _, mesh = self._build_mesh(mod, failure_rate=0.0)
        inst = _make_instance(mod)
        mesh.register_and_check(inst)
        assert len(registry.get_instances("svc")) == 1
