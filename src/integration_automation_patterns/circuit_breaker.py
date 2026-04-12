"""
circuit_breaker.py — Circuit Breaker pattern for enterprise integration resilience.

Prevents cascading failures when an external system (CRM, ERP, payment gateway)
is unavailable or degraded.  Wraps any callable and tracks success/failure counts
to transition between CLOSED → OPEN → HALF_OPEN states.

State machine:
  CLOSED   — normal operation; calls pass through; failures increment counter
  OPEN     — calls blocked immediately; set when failure_threshold exceeded
  HALF_OPEN — one probe call allowed; success → CLOSED, failure → OPEN

Usage::

    breaker = CircuitBreaker(name="salesforce", failure_threshold=5, recovery_timeout=60)

    @breaker.call
    def sync_account(account_id: str) -> dict:
        return crm_client.get_account(account_id)

    try:
        result = sync_account("001XX000003GYk1")
    except CircuitOpenError:
        # Circuit is open — return cached/default value or enqueue for retry
        result = fallback_cache.get(account_id)
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from threading import Lock
from typing import Any, TypeVar

F = TypeVar("F", bound=Callable[..., Any])


class CircuitState(Enum):
    """State of the circuit breaker."""

    CLOSED = "closed"
    """Normal operation.  Calls pass through."""

    OPEN = "open"
    """Circuit tripped.  Calls are blocked and raise ``CircuitOpenError``."""

    HALF_OPEN = "half_open"
    """Recovery probe.  One call is allowed through to test the downstream system."""


class CircuitOpenError(RuntimeError):
    """Raised when a call is attempted while the circuit is OPEN."""

    def __init__(self, name: str, retry_after: float) -> None:
        self.name = name
        self.retry_after = retry_after
        super().__init__(f"Circuit '{name}' is OPEN. Retry after {retry_after:.1f}s.")


@dataclass
class CircuitBreakerStats:
    """Snapshot of circuit breaker metrics."""

    name: str
    state: CircuitState
    failure_count: int
    success_count: int
    last_failure_time: float | None
    last_state_change_time: float


@dataclass
class CircuitBreaker:
    """
    Thread-safe circuit breaker for enterprise integration calls.

    Protects against cascading failures when downstream systems degrade.
    Compatible with any Python callable — wrap with ``@breaker.call`` or
    call ``breaker.execute(fn, *args, **kwargs)`` directly.

    Args:
        name: Human-readable identifier for logging and metrics.
        failure_threshold: Number of consecutive failures before tripping OPEN.
        recovery_timeout: Seconds to wait in OPEN state before probing HALF_OPEN.
        success_threshold: Consecutive successes in HALF_OPEN to return to CLOSED.
        expected_exceptions: Exception types that count as failures.
            Defaults to ``(Exception,)`` — all exceptions.

    Example::

        breaker = CircuitBreaker(
            name="erp_inventory",
            failure_threshold=3,
            recovery_timeout=30,
        )

        try:
            result = breaker.execute(erp_client.get_inventory, sku="ABC-001")
        except CircuitOpenError as e:
            return {"cached": True, "retry_after": e.retry_after}
    """

    name: str
    failure_threshold: int = 5
    recovery_timeout: float = 60.0
    success_threshold: int = 1
    expected_exceptions: tuple[type[BaseException], ...] = field(default_factory=lambda: (Exception,))

    _state: CircuitState = field(default=CircuitState.CLOSED, init=False, repr=False)
    _failure_count: int = field(default=0, init=False, repr=False)
    _success_count: int = field(default=0, init=False, repr=False)
    _last_failure_time: float | None = field(default=None, init=False, repr=False)
    _last_state_change: float = field(default_factory=time.monotonic, init=False, repr=False)
    _lock: Lock = field(default_factory=Lock, init=False, repr=False)

    @property
    def state(self) -> CircuitState:
        """Current circuit state (may transition OPEN → HALF_OPEN on read)."""
        with self._lock:
            return self._check_recovery()

    def execute(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """
        Execute *fn* with circuit breaker protection.

        Args:
            fn: Callable to protect.
            *args: Positional arguments forwarded to *fn*.
            **kwargs: Keyword arguments forwarded to *fn*.

        Returns:
            The return value of *fn*.

        Raises:
            CircuitOpenError: If the circuit is OPEN.
            Exception: Any exception raised by *fn* (also recorded as a failure).
        """
        with self._lock:
            current_state = self._check_recovery()

            if current_state == CircuitState.OPEN:
                retry_after = self.recovery_timeout - (time.monotonic() - (self._last_failure_time or 0))
                raise CircuitOpenError(self.name, max(0.0, retry_after))

        try:
            result = fn(*args, **kwargs)
            self._on_success()
            return result
        except self.expected_exceptions as exc:
            self._on_failure()
            raise exc

    def call(self, fn: F) -> F:
        """Decorator that wraps *fn* with this circuit breaker."""

        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return self.execute(fn, *args, **kwargs)

        wrapper.__name__ = fn.__name__
        wrapper.__doc__ = fn.__doc__
        return wrapper  # type: ignore[return-value]

    def reset(self) -> None:
        """Manually reset the circuit to CLOSED state."""
        with self._lock:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._success_count = 0
            self._last_failure_time = None
            self._last_state_change = time.monotonic()

    def stats(self) -> CircuitBreakerStats:
        """Return a snapshot of current metrics."""
        with self._lock:
            return CircuitBreakerStats(
                name=self.name,
                state=self._state,
                failure_count=self._failure_count,
                success_count=self._success_count,
                last_failure_time=self._last_failure_time,
                last_state_change_time=self._last_state_change,
            )

    # ------------------------------------------------------------------
    # Internal helpers (must be called with _lock held)
    # ------------------------------------------------------------------

    def _check_recovery(self) -> CircuitState:
        """Transition OPEN → HALF_OPEN if recovery_timeout has elapsed."""
        if self._state == CircuitState.OPEN:
            elapsed = time.monotonic() - (self._last_failure_time or 0)
            if elapsed >= self.recovery_timeout:
                self._state = CircuitState.HALF_OPEN
                self._success_count = 0
                self._last_state_change = time.monotonic()
        return self._state

    def _on_success(self) -> None:
        with self._lock:
            self._success_count += 1
            if self._state == CircuitState.HALF_OPEN and self._success_count >= self.success_threshold:
                self._state = CircuitState.CLOSED
                self._failure_count = 0
                self._last_state_change = time.monotonic()
            elif self._state == CircuitState.CLOSED:
                self._failure_count = 0

    def _on_failure(self) -> None:
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.monotonic()
            if self._state in (CircuitState.CLOSED, CircuitState.HALF_OPEN):
                if self._failure_count >= self.failure_threshold:
                    self._state = CircuitState.OPEN
                    self._last_state_change = time.monotonic()
