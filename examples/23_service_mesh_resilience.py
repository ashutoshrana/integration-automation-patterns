"""
23_service_mesh_resilience.py — Service-mesh resilience patterns for protecting
microservices from cascading failures, resource exhaustion, and thundering-herd
retry storms.

Demonstrates four complementary patterns that together implement the resilience
layer found inside every production service mesh (Istio, Linkerd, Envoy Proxy,
AWS App Mesh) as well as standalone resilience libraries such as Resilience4j,
Netflix Hystrix, and Polly (.NET):

    Pattern 1 — Bulkhead:
                Limits the number of concurrent calls allowed to reach a
                downstream service to at most max_concurrent, while optionally
                queuing up to max_queue_size additional callers.  Any caller
                that cannot be served within timeout_seconds receives a
                BulkheadRejected exception, protecting the calling service from
                thread-pool exhaustion caused by a slow or unresponsive
                dependency.  The pattern is named after the watertight
                compartments ("bulkheads") of a ship: a flood in one compartment
                does not sink the vessel.  Bulkhead isolation is a first-class
                feature in Netflix Hystrix (threadPoolProperties), Resilience4j
                BulkheadRegistry, and Istio's connectionPool.tcp.maxConnections
                setting per DestinationRule.  The implementation uses a
                threading.Semaphore to gate concurrent slots and a separate
                threading.RLock + integer counter to gate the waiting queue,
                giving O(1) admission decisions with no heap allocation per call.

    Pattern 2 — Timeout Hierarchy:
                Enforces a three-tier deadline budget: an innermost operation
                timeout (e.g. 200 ms per individual I/O call), a mid-tier
                service timeout (e.g. 1 000 ms for the full service call
                including retries), and an outermost request timeout (e.g.
                3 000 ms for the entire end-to-end user request).  Each tier
                is independently configurable; a violation at any tier raises
                TimeoutExceeded identifying which tier expired and what the
                budget was.  This mirrors Envoy Proxy's three-level timeout
                model (routeTimeout → requestTimeout → idleTimeout), Istio
                VirtualService timeout fields, and gRPC's deadline propagation
                mechanism.  The implementation wraps every callable in a
                concurrent.futures.ThreadPoolExecutor submit → Future.result
                pattern, which is the standard Python approach to imposing
                wall-clock limits on arbitrary synchronous callables without
                modifying their code.

    Pattern 3 — Health Check Aggregator:
                Runs registered health-check probes (each a zero-argument
                callable returning bool) concurrently, measures probe latency,
                and aggregates individual ServiceHealth records into an
                AggregateHealth view.  Aggregation logic follows Kubernetes
                readiness-probe semantics: all services healthy → HEALTHY; any
                service unhealthy → UNHEALTHY overall; otherwise → DEGRADED.
                is_ready() returns True if the aggregate is HEALTHY or DEGRADED,
                matching Kubernetes' decision to continue routing traffic when
                some (but not all) backends are degraded.  This pattern is
                implemented natively in Spring Boot Actuator
                (CompositeHealthContributor), Micrometer health registry, and
                Istio's OutlierDetection which ejects endpoints that fail health
                checks from the load-balancing pool.  Latency above a configurable
                threshold (default 500 ms) downgrades an otherwise-healthy
                service to DEGRADED, reflecting the principle that a service
                responding slowly is nearly as harmful as one that is down.

    Pattern 4 — Retry with Jitter:
                Retries a failing callable up to max_attempts times using
                exponential back-off (delay = base_delay_ms × 2^attempt).
                When jitter=True the delay is drawn uniformly from
                [0, min(max_delay_ms, exponential_cap)], implementing the
                "full jitter" strategy recommended by AWS ("Exponential Backoff
                and Jitter", 2015) to eliminate the thundering-herd problem
                that pure exponential back-off exhibits when many clients fail
                simultaneously.  When jitter=False the delay is the
                deterministic exponential cap (clamped to max_delay_ms),
                which is useful for tests that need predictable timing.
                After exhausting all attempts RetryExhausted is raised carrying
                the attempt count and the last exception for observability.
                The same algorithm is used by Resilience4j RetryRegistry,
                Envoy's retry_policy with random jitter, and Spring Retry's
                ExponentialRandomBackOffPolicy.

Relationship to service-mesh and resilience platforms:

    - Istio / Envoy     : Bulkhead ≈ DestinationRule connectionPool settings;
                          Timeout Hierarchy ≈ VirtualService timeout + per-route
                          timeout + idle_timeout; Retry ≈ VirtualService retryPolicy
                          with retryOn + perTryTimeout + retryRemoteStatuses.
    - Resilience4j      : Bulkhead ≈ BulkheadRegistry (semaphore-based) and
                          ThreadPoolBulkheadRegistry (thread-pool-based);
                          Retry ≈ RetryRegistry with exponential backoff decorator.
    - Netflix Hystrix   : Bulkhead ≈ threadPoolProperties (coreSize + queueSizeRejectionThreshold);
                          Timeout ≈ execution.isolation.thread.timeoutInMilliseconds.
    - AWS App Mesh      : Bulkhead ≈ VirtualNode listener connectionPool;
                          Retry ≈ Route retryPolicy with maxRetries + perRetryTimeout.
    - Kubernetes        : HealthCheckAggregator ≈ readinessProbe + livenessProbe
                          with successThreshold / failureThreshold; AggregateHealth
                          ≈ ComponentStatus in kube-apiserver /healthz breakdown.
    - Spring Boot       : HealthCheckAggregator ≈ CompositeHealthContributor +
                          HealthEndpoint; ServiceHealth ≈ Health.Builder result.

Usage (quick demo):

    python 23_service_mesh_resilience.py
"""

from __future__ import annotations

import concurrent.futures
import math
import random
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional


# ---------------------------------------------------------------------------
# Pattern 1 — Bulkhead
# ---------------------------------------------------------------------------


@dataclass
class BulkheadConfig:
    """Configuration for the Bulkhead pattern.

    Attributes:
        max_concurrent:   Maximum number of calls executing at the same time.
        max_queue_size:   Maximum additional callers allowed to wait for a slot.
        timeout_seconds:  Maximum time (wall-clock) a caller will wait to
                          acquire a concurrent slot before BulkheadRejected is
                          raised.
    """

    max_concurrent: int
    max_queue_size: int
    timeout_seconds: float


class BulkheadRejected(Exception):
    """Raised when a call cannot be admitted — both the concurrent slots and
    the waiting queue are full, or the wait timed out."""


class Bulkhead:
    """Thread-safe bulkhead that limits concurrent access to a downstream resource.

    Implementation note:
        Two controls protect the resource:
          1. ``_semaphore`` (threading.Semaphore with *max_concurrent* permits)
             gates actual execution.
          2. ``_queue_lock`` + ``_queued`` counter gate *waiting* callers so the
             total number of in-flight + waiting callers never exceeds
             max_concurrent + max_queue_size.
        A caller first increments ``_queued`` (rejecting immediately if the
        queue is full), then acquires ``_semaphore`` with a timeout, then
        decrements ``_queued``.  This avoids spawning threads for rejection
        decisions while remaining safe under concurrent use.
    """

    def __init__(self, name: str, config: BulkheadConfig) -> None:
        self.name = name
        self._config = config
        self._semaphore = threading.Semaphore(config.max_concurrent)
        self._queue_lock = threading.RLock()
        self._queued: int = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def execute(self, fn: Callable, *args: Any, **kwargs: Any) -> Any:
        """Execute *fn* under bulkhead protection.

        The call is admitted immediately if a concurrent slot is free.
        Otherwise it joins the waiting queue (up to *max_queue_size*) and
        blocks until a slot becomes available or *timeout_seconds* elapses.

        Raises:
            BulkheadRejected: When both slots and queue are full, or when the
                              caller times out waiting for a slot.
        """
        # --- fast path: try to acquire a slot immediately (no queuing needed) ---
        if self._semaphore.acquire(blocking=False):
            try:
                return fn(*args, **kwargs)
            finally:
                self._semaphore.release()

        # --- no slot available: check whether the caller can join the queue ---
        with self._queue_lock:
            if self._queued >= self._config.max_queue_size:
                raise BulkheadRejected(
                    f"Bulkhead '{self.name}' rejected call: "
                    f"queue full ({self._config.max_queue_size} waiting)"
                )
            self._queued += 1

        try:
            # --- wait for a concurrent slot with timeout ---
            acquired = self._semaphore.acquire(timeout=self._config.timeout_seconds)
            if not acquired:
                raise BulkheadRejected(
                    f"Bulkhead '{self.name}' timed out after "
                    f"{self._config.timeout_seconds}s waiting for a slot"
                )
        finally:
            # Leave the queue regardless of outcome
            with self._queue_lock:
                self._queued -= 1

        # --- execute the call while holding the slot ---
        try:
            return fn(*args, **kwargs)
        finally:
            self._semaphore.release()

    # ------------------------------------------------------------------
    # Observability
    # ------------------------------------------------------------------

    @property
    def available_slots(self) -> int:
        """Approximate number of free concurrent slots (not thread-safe snapshot)."""
        # Semaphore internal counter — valid for observability only
        return self._semaphore._value  # type: ignore[attr-defined]

    @property
    def queued_count(self) -> int:
        """Approximate number of callers currently waiting in the queue."""
        with self._queue_lock:
            return self._queued


# ---------------------------------------------------------------------------
# Pattern 2 — Timeout Hierarchy
# ---------------------------------------------------------------------------


@dataclass
class TimeoutConfig:
    """Three-tier timeout budget for a single user-visible request.

    Attributes:
        operation_timeout_ms: Per-operation deadline (innermost tier, e.g. 200 ms).
        service_timeout_ms:   Per-service-call deadline including retries (e.g. 1 000 ms).
        request_timeout_ms:   End-to-end user-request budget (outermost, e.g. 3 000 ms).
    """

    operation_timeout_ms: int
    service_timeout_ms: int
    request_timeout_ms: int


class TimeoutExceeded(Exception):
    """Raised when a timeout tier fires before the callable completes.

    Attributes:
        tier:      Which tier expired: ``'operation'``, ``'service'``, or ``'request'``.
        budget_ms: The configured budget (milliseconds) for that tier.
    """

    def __init__(self, tier: str, budget_ms: int) -> None:
        self.tier = tier
        self.budget_ms = budget_ms
        super().__init__(
            f"Timeout exceeded at '{tier}' tier ({budget_ms} ms)"
        )


class TimeoutHierarchy:
    """Enforces a three-tier timeout hierarchy using a shared thread-pool.

    A single ``ThreadPoolExecutor`` (max_workers=4) is reused across calls to
    avoid spawning a new thread per invocation.  The executor is a daemon pool
    so it does not prevent interpreter shutdown.
    """

    def __init__(self, config: TimeoutConfig) -> None:
        self._config = config
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=4, thread_name_prefix="timeout-hierarchy"
        )

    # ------------------------------------------------------------------
    # Tier methods
    # ------------------------------------------------------------------

    def execute_operation(self, fn: Callable, *args: Any, **kwargs: Any) -> Any:
        """Run *fn* enforcing the operation-level timeout.

        Raises:
            TimeoutExceeded: With tier='operation' if *fn* does not complete
                             within ``operation_timeout_ms`` milliseconds.
        """
        return self._run(fn, args, kwargs, "operation", self._config.operation_timeout_ms)

    def execute_service_call(self, fn: Callable, *args: Any, **kwargs: Any) -> Any:
        """Run *fn* enforcing the service-level timeout.

        Raises:
            TimeoutExceeded: With tier='service' if *fn* does not complete
                             within ``service_timeout_ms`` milliseconds.
        """
        return self._run(fn, args, kwargs, "service", self._config.service_timeout_ms)

    def execute_request(self, fn: Callable, *args: Any, **kwargs: Any) -> Any:
        """Run *fn* enforcing the request-level timeout.

        Raises:
            TimeoutExceeded: With tier='request' if *fn* does not complete
                             within ``request_timeout_ms`` milliseconds.
        """
        return self._run(fn, args, kwargs, "request", self._config.request_timeout_ms)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _run(
        self,
        fn: Callable,
        args: tuple,
        kwargs: dict,
        tier: str,
        budget_ms: int,
    ) -> Any:
        future = self._executor.submit(fn, *args, **kwargs)
        try:
            return future.result(timeout=budget_ms / 1000.0)
        except concurrent.futures.TimeoutError:
            future.cancel()
            raise TimeoutExceeded(tier, budget_ms)


# ---------------------------------------------------------------------------
# Pattern 3 — Health Check Aggregator
# ---------------------------------------------------------------------------


class HealthStatus(Enum):
    """Overall health state of a service or aggregate view."""

    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    UNHEALTHY = "UNHEALTHY"


@dataclass
class ServiceHealth:
    """Health record for a single downstream service.

    Attributes:
        service_name:  Logical name of the service.
        status:        Observed health status after running the probe.
        latency_ms:    Wall-clock time taken to run the probe (milliseconds).
        last_checked:  UTC timestamp when the probe was run.
        error_message: Exception message if the probe raised; otherwise None.
    """

    service_name: str
    status: HealthStatus
    latency_ms: float
    last_checked: datetime
    error_message: Optional[str] = None


@dataclass
class AggregateHealth:
    """Aggregated view across all registered health checks.

    Attributes:
        overall_status:  Composite status: HEALTHY / DEGRADED / UNHEALTHY.
        services:        Ordered list of individual ServiceHealth records.
        healthy_count:   Number of services in HEALTHY state.
        degraded_count:  Number of services in DEGRADED state.
        unhealthy_count: Number of services in UNHEALTHY state.
        checked_at:      UTC timestamp when aggregation was computed.
    """

    overall_status: HealthStatus
    services: list
    healthy_count: int
    degraded_count: int
    unhealthy_count: int
    checked_at: datetime

    def is_ready(self) -> bool:
        """Return True if the aggregate is ready to serve traffic.

        The system is considered ready unless the overall status is UNHEALTHY,
        matching Kubernetes readiness-probe semantics where a DEGRADED backend
        continues to receive traffic while remediation is underway.
        """
        return self.overall_status != HealthStatus.UNHEALTHY


class HealthCheckAggregator:
    """Runs registered health probes and aggregates their results.

    Each probe is a zero-argument callable that returns ``True`` (healthy) or
    ``False`` / raises an exception (unhealthy).  Probes execute with a 2-second
    wall-clock timeout enforced via a dedicated thread pool.

    Latency aggregation rule:
        If a probe returns ``True`` but the round-trip latency exceeds
        ``latency_threshold_ms`` the service is classified as DEGRADED rather
        than HEALTHY, reflecting the principle that excessive latency is a
        degraded service state.

    Overall aggregation rule:
        - At least one UNHEALTHY → overall UNHEALTHY
        - All HEALTHY → overall HEALTHY
        - Any other mix (some HEALTHY, some DEGRADED) → overall DEGRADED
    """

    _PROBE_TIMEOUT_S: float = 2.0

    def __init__(self, latency_threshold_ms: float = 500.0) -> None:
        self._latency_threshold_ms = latency_threshold_ms
        self._checks: dict[str, Callable[[], bool]] = {}

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register(self, service_name: str, check_fn: Callable[[], bool]) -> None:
        """Register a health-check probe for *service_name*.

        If a probe for the same service name was previously registered it is
        silently replaced, enabling dynamic re-registration.
        """
        self._checks[service_name] = check_fn

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def check_all(self) -> AggregateHealth:
        """Run every registered probe and return an AggregateHealth view.

        Probes execute sequentially.  The order matches insertion order of
        ``register`` calls (Python 3.7+ dict ordering guarantee).
        """
        results: list[ServiceHealth] = [
            self.check_service(name) for name in self._checks
        ]

        healthy = sum(1 for r in results if r.status == HealthStatus.HEALTHY)
        degraded = sum(1 for r in results if r.status == HealthStatus.DEGRADED)
        unhealthy = sum(1 for r in results if r.status == HealthStatus.UNHEALTHY)

        if unhealthy > 0:
            overall = HealthStatus.UNHEALTHY
        elif degraded > 0:
            overall = HealthStatus.DEGRADED
        else:
            overall = HealthStatus.HEALTHY

        return AggregateHealth(
            overall_status=overall,
            services=results,
            healthy_count=healthy,
            degraded_count=degraded,
            unhealthy_count=unhealthy,
            checked_at=datetime.utcnow(),
        )

    def check_service(self, service_name: str) -> ServiceHealth:
        """Run the probe for a single service and return its ServiceHealth.

        Catches all exceptions from the probe; an exception is treated as
        UNHEALTHY and its message is recorded in ``error_message``.
        """
        check_fn = self._checks[service_name]
        start = time.monotonic()
        error_msg: Optional[str] = None
        passed = False

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as exe:
            future = exe.submit(check_fn)
            try:
                passed = bool(future.result(timeout=self._PROBE_TIMEOUT_S))
            except concurrent.futures.TimeoutError:
                error_msg = f"Health probe timed out after {self._PROBE_TIMEOUT_S}s"
                passed = False
            except Exception as exc:  # noqa: BLE001
                error_msg = str(exc)
                passed = False

        latency_ms = (time.monotonic() - start) * 1000.0

        if not passed:
            status = HealthStatus.UNHEALTHY
        elif latency_ms > self._latency_threshold_ms:
            status = HealthStatus.DEGRADED
        else:
            status = HealthStatus.HEALTHY

        return ServiceHealth(
            service_name=service_name,
            status=status,
            latency_ms=latency_ms,
            last_checked=datetime.utcnow(),
            error_message=error_msg,
        )


# ---------------------------------------------------------------------------
# Pattern 4 — Retry with Jitter
# ---------------------------------------------------------------------------


@dataclass
class RetryConfig:
    """Configuration for the exponential back-off retry policy.

    Attributes:
        max_attempts:  Total number of attempts (1 = no retries; 3 = two retries
                       after the initial attempt).
        base_delay_ms: Base delay for the exponential formula (milliseconds).
        max_delay_ms:  Upper bound on computed delay (milliseconds).
        jitter:        When True use full-jitter (random in [0, cap]) to
                       prevent thundering-herd storms; when False use the
                       deterministic cap value.
    """

    max_attempts: int
    base_delay_ms: int
    max_delay_ms: int
    jitter: bool = True


class RetryExhausted(Exception):
    """Raised after all retry attempts have been exhausted.

    Attributes:
        attempts:    Total number of attempts made.
        last_error:  The exception raised by the final attempt.
    """

    def __init__(self, attempts: int, last_error: Exception) -> None:
        self.attempts = attempts
        self.last_error = last_error
        super().__init__(
            f"All {attempts} attempt(s) failed. "
            f"Last error: {type(last_error).__name__}: {last_error}"
        )


class RetryWithJitter:
    """Retries a callable using exponential back-off with optional full jitter.

    Delay formula (attempt index is 0-based after each failure):
        cap = min(max_delay_ms, base_delay_ms * 2^attempt)
        delay = random(0, cap)   # jitter=True  (full jitter)
        delay = cap              # jitter=False (deterministic)

    The first attempt is made immediately (attempt=0 → delay applied *before*
    the next attempt, not before the first).
    """

    def __init__(self, config: RetryConfig) -> None:
        self._config = config

    def execute(self, fn: Callable, *args: Any, **kwargs: Any) -> Any:
        """Execute *fn*, retrying on any exception up to *max_attempts* total.

        Raises:
            RetryExhausted: After all attempts have failed, carrying the last
                            exception for root-cause analysis.
        """
        last_exc: Exception = RuntimeError("No attempts made")

        for attempt in range(self._config.max_attempts):
            try:
                return fn(*args, **kwargs)
            except Exception as exc:  # noqa: BLE001
                last_exc = exc

                # No sleep after the final attempt
                if attempt < self._config.max_attempts - 1:
                    delay_ms = self._compute_delay(attempt)
                    time.sleep(delay_ms / 1000.0)

        raise RetryExhausted(self._config.max_attempts, last_exc)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _compute_delay(self, attempt: int) -> float:
        """Return the sleep duration in milliseconds for *attempt* (0-indexed)."""
        cap = min(
            self._config.max_delay_ms,
            self._config.base_delay_ms * math.pow(2, attempt),
        )
        if self._config.jitter:
            return random.uniform(0, cap)
        return cap


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------

def _demo_bulkhead() -> None:
    print("\n=== Pattern 1: Bulkhead ===")
    config = BulkheadConfig(max_concurrent=2, max_queue_size=1, timeout_seconds=0.5)
    bh = Bulkhead("payment-service", config)

    results = []
    barrier = threading.Barrier(4)

    def slow_call(idx: int) -> str:
        barrier.wait()          # all threads start together
        try:
            return bh.execute(lambda: (time.sleep(0.2), f"ok-{idx}")[1])
        except BulkheadRejected as exc:
            return f"rejected-{idx}: {exc}"

    threads = [threading.Thread(target=lambda i=i: results.append(slow_call(i)))
               for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    for r in sorted(results):
        print(f"  {r}")


def _demo_timeout_hierarchy() -> None:
    print("\n=== Pattern 2: Timeout Hierarchy ===")
    config = TimeoutConfig(
        operation_timeout_ms=100,
        service_timeout_ms=300,
        request_timeout_ms=1000,
    )
    th = TimeoutHierarchy(config)

    # Fast operation — succeeds
    result = th.execute_operation(lambda: "fast-result")
    print(f"  Fast operation: {result}")

    # Slow operation — times out at operation tier
    try:
        th.execute_operation(lambda: time.sleep(1))
    except TimeoutExceeded as exc:
        print(f"  Caught TimeoutExceeded: tier={exc.tier}, budget={exc.budget_ms}ms")

    # Slow service call — times out at service tier
    try:
        th.execute_service_call(lambda: time.sleep(1))
    except TimeoutExceeded as exc:
        print(f"  Caught TimeoutExceeded: tier={exc.tier}, budget={exc.budget_ms}ms")


def _demo_health_check_aggregator() -> None:
    print("\n=== Pattern 3: Health Check Aggregator ===")
    agg = HealthCheckAggregator(latency_threshold_ms=200.0)

    agg.register("auth-service",     lambda: True)
    agg.register("payment-service",  lambda: True)
    agg.register("inventory-service", lambda: (_ for _ in ()).throw(
        ConnectionError("db timeout")))  # type: ignore[misc]

    health = agg.check_all()
    print(f"  Overall: {health.overall_status.value}")
    print(f"  Healthy: {health.healthy_count}, Degraded: {health.degraded_count}, "
          f"Unhealthy: {health.unhealthy_count}")
    print(f"  Ready to serve: {health.is_ready()}")
    for svc in health.services:
        msg = f" ({svc.error_message})" if svc.error_message else ""
        print(f"    {svc.service_name}: {svc.status.value} ({svc.latency_ms:.1f}ms){msg}")


def _demo_retry_with_jitter() -> None:
    print("\n=== Pattern 4: Retry with Jitter ===")
    config = RetryConfig(
        max_attempts=4,
        base_delay_ms=50,
        max_delay_ms=400,
        jitter=True,
    )
    retry = RetryWithJitter(config)

    # Succeeds on 3rd attempt
    attempt_counter = {"n": 0}

    def flaky() -> str:
        attempt_counter["n"] += 1
        if attempt_counter["n"] < 3:
            raise IOError(f"transient error (attempt {attempt_counter['n']})")
        return "success"

    result = retry.execute(flaky)
    print(f"  Result after {attempt_counter['n']} attempts: {result}")

    # Exhausts all retries
    try:
        retry.execute(lambda: (_ for _ in ()).throw(RuntimeError("always fails")))  # type: ignore[misc]
    except RetryExhausted as exc:
        print(f"  RetryExhausted after {exc.attempts} attempts: {exc.last_error}")


if __name__ == "__main__":
    _demo_bulkhead()
    _demo_timeout_hierarchy()
    _demo_health_check_aggregator()
    _demo_retry_with_jitter()
    print("\nAll demos complete.")
