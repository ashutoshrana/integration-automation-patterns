"""
03_saga_compensation.py — SagaOrchestrator with automatic compensation.

Demonstrates a 3-step order-placement saga:
  1. reserve_inventory  — succeeds
  2. charge_payment     — fails (simulated)
  3. create_shipment    — never runs (not reached)

When step 2 fails, the orchestrator compensates all completed steps in
reverse order (step 1 only, since step 2 never completed).

Run:
    python examples/03_saga_compensation.py
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from integration_automation_patterns.saga import SagaOrchestrator


def main() -> None:
    print("=" * 62)
    print("Saga Orchestrator Demo — Compensation on Failure")
    print("=" * 62)

    # Track what happens at each step so we can print a clear log
    execution_log: list[str] = []

    # --- Step implementations ---

    def reserve_inventory():
        execution_log.append("  [ACTION]      reserve_inventory — reserved 3x WIDGET-A")
        return {"reservation_id": "res-001", "sku": "WIDGET-A", "qty": 3}

    def compensate_reserve_inventory():
        execution_log.append("  [COMPENSATE]  reserve_inventory — released reservation res-001")

    def charge_payment():
        execution_log.append("  [ACTION]      charge_payment — FAILED (card declined)")
        raise RuntimeError("Payment gateway timeout: card_token=tok_abc declined")

    def compensate_charge_payment():
        # This should never run — the step that failed is NOT compensated
        execution_log.append("  [COMPENSATE]  charge_payment — refunded charge (should not run)")

    def create_shipment():
        execution_log.append("  [ACTION]      create_shipment — (should never run)")
        return {"shipment_id": "ship-999"}

    def compensate_create_shipment():
        execution_log.append("  [COMPENSATE]  create_shipment — (should never run)")

    # --- Build and run saga ---
    saga = SagaOrchestrator("order-placement")
    saga.add_step("reserve_inventory", reserve_inventory, compensate_reserve_inventory)
    saga.add_step("charge_payment", charge_payment, compensate_charge_payment)
    saga.add_step("create_shipment", create_shipment, compensate_create_shipment)

    print("\nExecuting saga 'order-placement' (3 steps)...")
    print()

    result = saga.execute()

    # Print the captured step-by-step log
    print("Step-by-step execution log:")
    print("-" * 62)
    for entry in execution_log:
        print(entry)
    print("-" * 62)

    # Summary
    print()
    print(f"Saga succeeded:       {result.succeeded}")
    print(f"Failed step:          {result.failed_step!r}")
    print(f"Error:                {result.error}")
    print(f"Completed steps:      {result.completed_steps}")
    print(f"Compensated steps:    {result.compensated_steps}")
    print()
    print("Observations:")
    print("  - reserve_inventory ran its ACTION and its COMPENSATION (rollback)")
    print("  - charge_payment raised an error — its compensation did NOT run")
    print("    (a failed step is not compensated; only completed steps are)")
    print("  - create_shipment was never reached")

    # Assertions to prove the expected outcome
    assert not result.succeeded
    assert result.failed_step == "charge_payment"
    assert result.completed_steps == ["reserve_inventory"]
    assert result.compensated_steps == ["reserve_inventory"]

    print("\nAll assertions passed.")
    print("\nDone.")


if __name__ == "__main__":
    main()
