"""
31_service_discovery_patterns.py — Service Discovery and Registry Patterns

Pure-Python simulation of service-discovery primitives using only the standard
library (``dataclasses``, ``threading``, ``time``, ``random``, ``collections``,
``enum``).  No external service-mesh or registry libraries are required.

    Pattern 1 — ServiceInstance (data model):
                Immutable-ish dataclass representing a single running instance
                of a named service.  Carries connection details (host, port),
                arbitrary metadata, a health_status string, a last_heartbeat
                timestamp, and an integer weight used by the weighted
                load-balancer strategy.

    Pattern 2 — ServiceRegistry (in-process service catalog):
                Thread-safe registry that maps (service_name, instance_id)
                keys to ServiceInstance objects.  Supports registration,
                deregistration, heartbeat renewal, and TTL-based eviction of
                stale instances.

    Pattern 3 — LoadBalancer (client-side load distribution):
                Distributes requests across a list of ServiceInstance objects
                using one of three strategies: round-robin (deterministic
                cycling), random (uniform random selection), or weighted
                (probabilistic selection proportional to instance.weight).

    Pattern 4 — HealthChecker (simulated health probes):
                Models active health probing by returning a pass/fail result
                for individual instances with a configurable failure rate.
                Supports bulk checks across instance lists.

    Pattern 5 — ServiceMesh (composite orchestrator):
                Combines ServiceRegistry + LoadBalancer + HealthChecker into a
                single routing layer.  Provides high-level operations: route a
                request to a healthy instance, rebalance by evicting instances
                that fail health checks, and atomically register-then-check a
                new instance.

No external dependencies required.

Run:
    python examples/31_service_discovery_patterns.py
"""

from __future__ import annotations

import random
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional


# ===========================================================================
# Pattern 1 — ServiceInstance
# ===========================================================================


@dataclass
class ServiceInstance:
    """Data model for a single running service instance.

    Carries everything a client needs to connect to the instance plus
    metadata used by the registry (heartbeat timestamp, health status) and
    by the load-balancer (weight).

    Attributes:
        service_name:   Logical service name (e.g. ``"payment-service"``).
        instance_id:    Unique identifier within the service (e.g. ``"i-abc123"``).
        host:           Hostname or IP address.
        port:           TCP port the instance listens on.
        metadata:       Arbitrary key-value metadata (region, version, …).
        health_status:  Current health — ``"HEALTHY"`` or ``"UNHEALTHY"``.
        last_heartbeat: Monotonic timestamp of the most recent heartbeat.
        weight:         Positive integer weight for weighted load-balancing.

    Example::

        inst = ServiceInstance(
            service_name="payment-service",
            instance_id="i-001",
            host="10.0.0.1",
            port=8080,
        )
        print(inst.health_status)   # "HEALTHY"
        print(inst.weight)          # 1
    """

    service_name: str
    instance_id: str
    host: str
    port: int
    metadata: dict = field(default_factory=dict)
    health_status: str = "HEALTHY"
    last_heartbeat: float = field(default_factory=time.monotonic)
    weight: int = 1


# ===========================================================================
# Pattern 2 — ServiceRegistry
# ===========================================================================


class ServiceRegistry:
    """Thread-safe in-process service registry.

    Instances are keyed by ``(service_name, instance_id)``.  The registry
    is the single source of truth for which instances are currently alive;
    the :meth:`evict_stale` method marks instances whose heartbeat has not
    been renewed within a configurable TTL as ``UNHEALTHY``.

    Example::

        registry = ServiceRegistry()
        registry.register(ServiceInstance("svc", "i-1", "localhost", 8080))
        instances = registry.get_instances("svc")   # [<ServiceInstance …>]
        registry.heartbeat("svc", "i-1")
        evicted = registry.evict_stale(ttl_seconds=30.0)
    """

    def __init__(self) -> None:
        # Primary store: (service_name, instance_id) → ServiceInstance
        self._store: Dict[tuple, ServiceInstance] = {}
        self._lock: threading.Lock = threading.Lock()

    # ------------------------------------------------------------------
    # Mutations
    # ------------------------------------------------------------------

    def register(self, instance: ServiceInstance) -> None:
        """Add *instance* to the registry.

        If an entry with the same ``(service_name, instance_id)`` already
        exists it is silently replaced.

        Args:
            instance: The :class:`ServiceInstance` to register.
        """
        key = (instance.service_name, instance.instance_id)
        with self._lock:
            self._store[key] = instance

    def deregister(self, service_name: str, instance_id: str) -> None:
        """Remove the instance identified by *(service_name, instance_id)*.

        Args:
            service_name: Logical service name.
            instance_id:  Instance identifier.

        Raises:
            KeyError: If no matching instance is registered.
        """
        key = (service_name, instance_id)
        with self._lock:
            if key not in self._store:
                raise KeyError(
                    f"No instance registered for ({service_name!r}, {instance_id!r})"
                )
            del self._store[key]

    def heartbeat(self, service_name: str, instance_id: str) -> None:
        """Renew the heartbeat for *(service_name, instance_id)*.

        Updates ``last_heartbeat`` to the current monotonic time and sets
        ``health_status`` to ``"HEALTHY"``.

        Args:
            service_name: Logical service name.
            instance_id:  Instance identifier.

        Raises:
            KeyError: If no matching instance is registered.
        """
        key = (service_name, instance_id)
        with self._lock:
            if key not in self._store:
                raise KeyError(
                    f"No instance registered for ({service_name!r}, {instance_id!r})"
                )
            inst = self._store[key]
            inst.last_heartbeat = time.monotonic()
            inst.health_status = "HEALTHY"

    def evict_stale(self, ttl_seconds: float = 30.0) -> int:
        """Mark instances whose heartbeat is older than *ttl_seconds* as UNHEALTHY.

        Instances are *not* removed from the registry — their
        ``health_status`` is set to ``"UNHEALTHY"`` so that
        :meth:`get_instances` will exclude them from routing.

        Args:
            ttl_seconds: Maximum age (in seconds) of a valid heartbeat.

        Returns:
            The number of instances that were newly marked as UNHEALTHY.
        """
        now = time.monotonic()
        evicted = 0
        with self._lock:
            for inst in self._store.values():
                if (now - inst.last_heartbeat) > ttl_seconds:
                    if inst.health_status == "HEALTHY":
                        inst.health_status = "UNHEALTHY"
                        evicted += 1
        return evicted

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    def get_instances(self, service_name: str) -> List[ServiceInstance]:
        """Return all HEALTHY instances for *service_name*.

        Args:
            service_name: Logical service name to query.

        Returns:
            List of :class:`ServiceInstance` objects with
            ``health_status == "HEALTHY"``; may be empty.
        """
        with self._lock:
            return [
                inst
                for (svc, _iid), inst in self._store.items()
                if svc == service_name and inst.health_status == "HEALTHY"
            ]

    def all_services(self) -> Dict[str, List[ServiceInstance]]:
        """Return every registered instance grouped by service name.

        Includes instances of all health statuses.

        Returns:
            Dict mapping ``service_name`` → list of
            :class:`ServiceInstance` objects (all statuses).
        """
        result: Dict[str, List[ServiceInstance]] = {}
        with self._lock:
            for (svc, _iid), inst in self._store.items():
                result.setdefault(svc, []).append(inst)
        return result


# ===========================================================================
# Pattern 3 — LoadBalancer
# ===========================================================================


class LoadBalancer:
    """Client-side load balancer for a list of :class:`ServiceInstance` objects.

    Supports three selection strategies:

    * ``"round_robin"`` — cycles through instances in order using an
      internal counter.  Thread-safe cycling via :class:`threading.Lock`.
    * ``"random"``      — selects a uniformly random instance on each call.
    * ``"weighted"``    — selects an instance with probability proportional
      to ``instance.weight``; higher-weight instances are chosen more often.

    Example::

        lb = LoadBalancer(strategy="round_robin")
        chosen = lb.select([inst_a, inst_b, inst_c])
    """

    def __init__(self, strategy: str = "round_robin") -> None:
        if strategy not in ("round_robin", "random", "weighted"):
            raise ValueError(
                f"Unknown strategy {strategy!r}; "
                "must be 'round_robin', 'random', or 'weighted'"
            )
        self._strategy = strategy
        self._index: int = 0
        self._lock: threading.Lock = threading.Lock()

    # ------------------------------------------------------------------
    # Selection
    # ------------------------------------------------------------------

    def select(self, instances: List[ServiceInstance]) -> ServiceInstance:
        """Pick one instance from *instances* according to the configured strategy.

        Args:
            instances: Non-empty list of :class:`ServiceInstance` candidates.

        Returns:
            The selected :class:`ServiceInstance`.

        Raises:
            ValueError: If *instances* is empty.
        """
        if not instances:
            raise ValueError("Cannot select from an empty instance list")

        if self._strategy == "round_robin":
            return self._round_robin(instances)
        if self._strategy == "random":
            return self._random(instances)
        # weighted
        return self._weighted(instances)

    # ------------------------------------------------------------------
    # Strategy implementations
    # ------------------------------------------------------------------

    def _round_robin(self, instances: List[ServiceInstance]) -> ServiceInstance:
        """Cycle through *instances* in index order."""
        with self._lock:
            idx = self._index % len(instances)
            self._index += 1
        return instances[idx]

    @staticmethod
    def _random(instances: List[ServiceInstance]) -> ServiceInstance:
        """Uniformly random selection."""
        return random.choice(instances)

    @staticmethod
    def _weighted(instances: List[ServiceInstance]) -> ServiceInstance:
        """Weighted random selection proportional to ``instance.weight``."""
        total = sum(inst.weight for inst in instances)
        r = random.uniform(0, total)
        cumulative = 0.0
        for inst in instances:
            cumulative += inst.weight
            if r <= cumulative:
                return inst
        # Fallback (floating-point edge case)
        return instances[-1]

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def strategy(self) -> str:
        """The active load-balancing strategy name."""
        return self._strategy


# ===========================================================================
# Pattern 4 — HealthChecker
# ===========================================================================


class HealthChecker:
    """Simulates active health probing with a configurable failure rate.

    In production systems a health checker would make real HTTP/TCP probes.
    Here a random number is drawn per check; if it falls below
    *failure_rate* the check fails, modelling transient or systematic
    faults.

    Example::

        checker = HealthChecker(failure_rate=0.0)
        assert checker.check(instance) is True   # always passes

        flaky = HealthChecker(failure_rate=1.0)
        assert flaky.check(instance) is False    # always fails
    """

    def __init__(self, failure_rate: float = 0.0) -> None:
        if not 0.0 <= failure_rate <= 1.0:
            raise ValueError(
                f"failure_rate must be in [0.0, 1.0]; got {failure_rate}"
            )
        self._failure_rate = failure_rate

    # ------------------------------------------------------------------
    # Checks
    # ------------------------------------------------------------------

    def check(self, instance: ServiceInstance) -> bool:
        """Run a health probe on *instance*.

        Args:
            instance: The :class:`ServiceInstance` to check.

        Returns:
            ``False`` with probability *failure_rate*, ``True`` otherwise.
        """
        return random.random() >= self._failure_rate

    def check_all(
        self, instances: List[ServiceInstance]
    ) -> Dict[str, bool]:
        """Run health probes on every instance in *instances*.

        Args:
            instances: List of :class:`ServiceInstance` objects to probe.

        Returns:
            Dict mapping ``instance_id`` → health check result (bool).
        """
        return {inst.instance_id: self.check(inst) for inst in instances}

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def failure_rate(self) -> float:
        """Configured probability of a simulated health check failure."""
        return self._failure_rate


# ===========================================================================
# Pattern 5 — ServiceMesh
# ===========================================================================


class ServiceMesh:
    """Composite orchestrator combining registry, load balancer, and health checker.

    :class:`ServiceMesh` provides high-level routing semantics on top of the
    three lower-level primitives:

    * :meth:`route` — find a healthy instance for *service_name* and select
      one via the load balancer.
    * :meth:`rebalance` — probe all registered instances for a service and
      deregister any that fail the health check.
    * :meth:`register_and_check` — register an instance and immediately run
      a health check; if the check fails the instance is deregistered.

    Example::

        mesh = ServiceMesh(
            registry=ServiceRegistry(),
            balancer=LoadBalancer(strategy="round_robin"),
            checker=HealthChecker(failure_rate=0.0),
        )
        mesh.register_and_check(instance)
        chosen = mesh.route("payment-service")
    """

    def __init__(
        self,
        registry: ServiceRegistry,
        balancer: LoadBalancer,
        checker: HealthChecker,
    ) -> None:
        self._registry = registry
        self._balancer = balancer
        self._checker = checker

    # ------------------------------------------------------------------
    # Routing
    # ------------------------------------------------------------------

    def route(self, service_name: str) -> ServiceInstance:
        """Return a healthy instance of *service_name* chosen by the load balancer.

        Retrieves HEALTHY instances from the registry, runs a health check
        on each one, and passes those that pass to the load balancer.

        Args:
            service_name: Logical name of the service to route to.

        Returns:
            A :class:`ServiceInstance` selected by the load balancer.

        Raises:
            RuntimeError: If no healthy instances pass the health check.
        """
        candidates = self._registry.get_instances(service_name)
        healthy = [inst for inst in candidates if self._checker.check(inst)]
        if not healthy:
            raise RuntimeError(
                f"No healthy instances available for service {service_name!r}"
            )
        return self._balancer.select(healthy)

    # ------------------------------------------------------------------
    # Rebalancing
    # ------------------------------------------------------------------

    def rebalance(self, service_name: str) -> int:
        """Deregister all instances of *service_name* that fail the health check.

        Runs :meth:`HealthChecker.check_all` on every registered instance
        (regardless of current health_status) and deregisters any that
        return ``False``.

        Args:
            service_name: Logical name of the service to rebalance.

        Returns:
            The number of instances deregistered.
        """
        all_instances = self._registry.all_services().get(service_name, [])
        results = self._checker.check_all(all_instances)
        removed = 0
        for inst in all_instances:
            if not results.get(inst.instance_id, True):
                try:
                    self._registry.deregister(service_name, inst.instance_id)
                    removed += 1
                except KeyError:
                    pass  # Already removed by a concurrent call
        return removed

    # ------------------------------------------------------------------
    # Registration with immediate health check
    # ------------------------------------------------------------------

    def register_and_check(self, instance: ServiceInstance) -> bool:
        """Register *instance* and immediately run a health check.

        If the health check passes the instance remains registered and
        ``True`` is returned.  If it fails the instance is deregistered
        before returning ``False``.

        Args:
            instance: The :class:`ServiceInstance` to register and probe.

        Returns:
            ``True`` if the instance passed the health check;
            ``False`` if it failed (and was deregistered).
        """
        self._registry.register(instance)
        if self._checker.check(instance):
            return True
        # Health check failed — roll back the registration
        try:
            self._registry.deregister(instance.service_name, instance.instance_id)
        except KeyError:
            pass
        return False


# ===========================================================================
# Smoke-test helpers (used only in __main__)
# ===========================================================================


def _separator(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def _run_registration_demo() -> None:
    """Scenario 1: Register 3 instances and round-robin route across them."""
    _separator("Scenario 1 — Registration + Round-Robin Routing (6 selections)")

    registry = ServiceRegistry()
    balancer = LoadBalancer(strategy="round_robin")
    checker = HealthChecker(failure_rate=0.0)  # never fails
    mesh = ServiceMesh(registry, balancer, checker)

    for i in range(1, 4):
        inst = ServiceInstance(
            service_name="payment-service",
            instance_id=f"i-00{i}",
            host=f"10.0.0.{i}",
            port=8080 + i,
            metadata={"region": "us-east-1"},
        )
        registry.register(inst)
        print(f"Registered : {inst.instance_id}  {inst.host}:{inst.port}")

    print()
    seen = []
    for n in range(1, 7):
        chosen = mesh.route("payment-service")
        print(f"  Selection {n}: {chosen.instance_id}  ({chosen.host}:{chosen.port})")
        seen.append(chosen.instance_id)

    # Round-robin should cycle: i-001, i-002, i-003, i-001, i-002, i-003
    expected_cycle = ["i-001", "i-002", "i-003"] * 2
    assert seen == expected_cycle, f"Expected {expected_cycle}, got {seen}"
    print("Round-robin cycle verified.")
    print("PASS")


def _run_stale_eviction_demo() -> None:
    """Scenario 2: Simulate a stale instance and evict it."""
    _separator("Scenario 2 — Stale Instance Eviction (TTL-based)")

    registry = ServiceRegistry()

    for i in range(1, 4):
        registry.register(
            ServiceInstance(
                service_name="payment-service",
                instance_id=f"i-00{i}",
                host=f"10.0.0.{i}",
                port=8080 + i,
            )
        )

    all_before = registry.get_instances("payment-service")
    print(f"Healthy instances before eviction: {len(all_before)}")
    assert len(all_before) == 3

    # Simulate i-002 going silent: backdate its heartbeat by 60 seconds
    stale_key = ("payment-service", "i-002")
    registry._store[stale_key].last_heartbeat = time.monotonic() - 60.0
    print("Backdated heartbeat for i-002 by 60 s (TTL = 30 s)")

    evicted = registry.evict_stale(ttl_seconds=30.0)
    print(f"Evicted count : {evicted} (expected 1)")
    assert evicted == 1

    all_after = registry.get_instances("payment-service")
    print(f"Healthy instances after eviction : {len(all_after)} (expected 2)")
    assert len(all_after) == 2

    remaining_ids = {inst.instance_id for inst in all_after}
    assert "i-002" not in remaining_ids, "i-002 should be UNHEALTHY"
    print("i-002 correctly marked UNHEALTHY.")
    print("PASS")


def _run_weighted_selection_demo() -> None:
    """Scenario 3: Weighted load balancing with varying weights."""
    _separator("Scenario 3 — Weighted Load Balancing")

    instances = [
        ServiceInstance("api", "heavy", "10.0.1.1", 8081, weight=8),
        ServiceInstance("api", "medium", "10.0.1.2", 8082, weight=2),
    ]

    balancer = LoadBalancer(strategy="weighted")
    counter: Dict[str, int] = {"heavy": 0, "medium": 0}
    total = 1000
    for _ in range(total):
        chosen = balancer.select(instances)
        counter[chosen.instance_id] += 1

    heavy_pct = counter["heavy"] / total * 100
    medium_pct = counter["medium"] / total * 100
    print(f"heavy  (weight=8): {counter['heavy']:4d} / {total}  ({heavy_pct:.1f}%)")
    print(f"medium (weight=2): {counter['medium']:4d} / {total}  ({medium_pct:.1f}%)")

    # With 1000 samples, expect heavy ≈ 80% ± 5%
    assert 70 <= heavy_pct <= 90, (
        f"Expected heavy ~80%, got {heavy_pct:.1f}%"
    )
    print("Weighted distribution within expected range.")
    print("PASS")


def _run_rebalance_demo() -> None:
    """Scenario 4: ServiceMesh.rebalance removes unhealthy instances."""
    _separator("Scenario 4 — ServiceMesh Rebalance")

    registry = ServiceRegistry()
    balancer = LoadBalancer(strategy="random")
    checker = HealthChecker(failure_rate=1.0)  # all checks fail
    mesh = ServiceMesh(registry, balancer, checker)

    for i in range(1, 4):
        registry.register(
            ServiceInstance("cache-service", f"c-{i}", f"10.1.0.{i}", 6379)
        )

    before = registry.all_services().get("cache-service", [])
    print(f"Instances before rebalance: {len(before)}")
    assert len(before) == 3

    removed = mesh.rebalance("cache-service")
    print(f"Removed by rebalance      : {removed} (expected 3, failure_rate=1.0)")
    assert removed == 3

    after = registry.all_services().get("cache-service", [])
    print(f"Instances after rebalance : {len(after)} (expected 0)")
    assert len(after) == 0
    print("PASS")


# ===========================================================================
# Entry point
# ===========================================================================


if __name__ == "__main__":
    print("Service Discovery and Registry Patterns — Smoke Test Suite")
    print(
        "Patterns: ServiceInstance | ServiceRegistry | LoadBalancer "
        "| HealthChecker | ServiceMesh"
    )

    _run_registration_demo()
    _run_stale_eviction_demo()
    _run_weighted_selection_demo()
    _run_rebalance_demo()

    print("\n" + "=" * 60)
    print("  All scenarios passed.")
    print("=" * 60)
