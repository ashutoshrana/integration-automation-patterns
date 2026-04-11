"""
saga.py — Saga pattern for distributed transaction management.

Coordinates multi-step operations across independent services (CRM, ERP, billing,
fulfilment) where a two-phase commit is unavailable.  Each step has a corresponding
compensation action that reverses it if a later step fails.

Pattern: forward sequence (step 1→2→3) + backward compensation (step 3→2→1) on failure.

Usage::

    saga = SagaOrchestrator("order-placement")
    saga.add_step(
        name="reserve_inventory",
        action=lambda: inventory.reserve(sku, qty),
        compensate=lambda: inventory.release(sku, qty),
    )
    saga.add_step(
        name="charge_payment",
        action=lambda: payment.charge(card_token, amount),
        compensate=lambda: payment.refund(charge_id),
    )
    saga.add_step(
        name="create_shipment",
        action=lambda: shipping.create(order_id),
        compensate=lambda: shipping.cancel(shipment_id),
    )

    result = saga.execute()
    if not result.succeeded:
        print(f"Saga failed at step '{result.failed_step}': {result.error}")
        print(f"Compensated steps: {result.compensated_steps}")
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class SagaStep:
    """
    A single step in a saga, with its forward action and compensation.

    Args:
        name: Identifier for this step (used in logging and result reporting).
        action: Callable that performs the forward operation. Should return any
            result value (stored in ``SagaResult.step_results``).
        compensate: Callable that reverses the forward action. Called only if
            a later step fails.  Should be idempotent where possible.
    """

    name: str
    action: Callable[[], Any]
    compensate: Callable[[], Any]


@dataclass
class SagaResult:
    """
    Outcome of a saga execution.

    Attributes:
        succeeded: True if all steps completed successfully.
        failed_step: Name of the step that raised an exception, or ``None``.
        error: The exception raised, or ``None``.
        completed_steps: Names of steps that ran their forward action successfully.
        compensated_steps: Names of steps whose compensation ran.
        step_results: Dict mapping step name → return value of its action.
    """

    succeeded: bool
    failed_step: str | None = None
    error: Exception | None = None
    completed_steps: list[str] = field(default_factory=list)
    compensated_steps: list[str] = field(default_factory=list)
    step_results: dict[str, Any] = field(default_factory=dict)


class SagaOrchestrator:
    """
    Executes a sequence of ``SagaStep`` objects with automatic compensation.

    Steps are executed in the order they were added.  If any step raises an
    exception, all previously completed steps are compensated in reverse order.

    Compensation failures are logged but do not suppress the original failure.
    The saga design assumes compensations are best-effort and idempotent.

    Example::

        saga = SagaOrchestrator("create-user-account")
        saga.add_step(
            "provision_auth",
            action=lambda: auth_service.create(email),
            compensate=lambda: auth_service.delete(email),
        )
        saga.add_step(
            "create_profile",
            action=lambda: profile_service.create(user_id),
            compensate=lambda: profile_service.delete(user_id),
        )
        result = saga.execute()
    """

    def __init__(self, name: str) -> None:
        """
        Args:
            name: Logical name of this saga (used in log messages).
        """
        self.name = name
        self._steps: list[SagaStep] = []

    def add_step(
        self,
        name: str,
        action: Callable[[], Any],
        compensate: Callable[[], Any],
    ) -> SagaOrchestrator:
        """
        Append a step to this saga.

        Args:
            name: Step identifier.
            action: Forward operation callable.
            compensate: Compensation callable (runs on rollback).

        Returns:
            ``self`` for method chaining.
        """
        self._steps.append(SagaStep(name=name, action=action, compensate=compensate))
        return self

    def execute(self) -> SagaResult:
        """
        Execute all steps in order, compensating on failure.

        Returns:
            A ``SagaResult`` describing the outcome.
        """
        result = SagaResult(succeeded=False)
        logger.info("[SAGA] Starting saga=%s steps=%d", self.name, len(self._steps))

        for step in self._steps:
            try:
                logger.debug("[SAGA] Executing step=%s saga=%s", step.name, self.name)
                step_result = step.action()
                result.completed_steps.append(step.name)
                result.step_results[step.name] = step_result
                logger.debug("[SAGA] Step succeeded step=%s saga=%s", step.name, self.name)
            except Exception as exc:
                result.failed_step = step.name
                result.error = exc
                logger.error("[SAGA] Step failed step=%s saga=%s error=%s", step.name, self.name, exc)
                self._compensate(result)
                return result

        result.succeeded = True
        logger.info("[SAGA] Saga succeeded saga=%s steps_completed=%d", self.name, len(result.completed_steps))
        return result

    def _compensate(self, result: SagaResult) -> None:
        """Run compensations in reverse order for all completed steps."""
        for step_name in reversed(result.completed_steps):
            step = next(s for s in self._steps if s.name == step_name)
            try:
                logger.info("[SAGA] Compensating step=%s saga=%s", step_name, self.name)
                step.compensate()
                result.compensated_steps.append(step_name)
                logger.info("[SAGA] Compensation succeeded step=%s saga=%s", step_name, self.name)
            except Exception as comp_exc:
                # Log but continue — compensation failures are non-fatal
                logger.error(
                    "[SAGA] Compensation failed step=%s saga=%s error=%s",
                    step_name,
                    self.name,
                    comp_exc,
                )
