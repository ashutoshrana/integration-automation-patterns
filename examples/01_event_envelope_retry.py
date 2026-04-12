"""
01_event_envelope_retry.py — EventEnvelope with RetryPolicy and exponential backoff.

Demonstrates how an EventEnvelope tracks delivery attempts, calculates
exponential backoff delays, and transitions through DeliveryStatus states.
Simulates 3 delivery attempts: two failures, then success.

Run:
    python examples/01_event_envelope_retry.py
"""

import sys
import os
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from integration_automation_patterns.event_envelope import (
    DeliveryStatus,
    EventEnvelope,
    RetryPolicy,
)


def simulate_delivery(envelope: EventEnvelope, attempt_num: int) -> bool:
    """Return True on the 3rd attempt (zero-indexed attempt 2), False otherwise."""
    return attempt_num >= 2


def main() -> None:
    print("=" * 62)
    print("EventEnvelope Retry Demo — Exponential Backoff")
    print("=" * 62)

    policy = RetryPolicy(
        max_attempts=4,
        backoff_seconds=5,
        exponential=True,
        max_backoff_seconds=120,
    )

    envelope = EventEnvelope(
        event_id=str(uuid.uuid4()),
        event_type="enrollment.student.updated",
        source_system="crm-salesforce",
        payload={"student_id": "stu_007", "status": "enrolled", "term": "2026-FA"},
        correlation_id="req-abc-123",
        retry_policy=policy,
    )

    print(f"\nEvent ID:    {envelope.event_id}")
    print(f"Event type:  {envelope.event_type}")
    print(f"Source:      {envelope.source_system}")
    print(f"Payload:     {envelope.payload}")
    print(f"Max attempts: {policy.max_attempts}")
    print(f"Base backoff: {policy.backoff_seconds}s (exponential=True)")
    print()

    # Pre-compute the backoff schedule for display
    print("Backoff schedule:")
    for i in range(policy.max_attempts - 1):
        wait = policy.wait_seconds_for_attempt(i)
        print(f"  After attempt {i + 1}: wait {wait}s before retry")

    print()
    print(f"{'Attempt':<10} {'Action':<22} {'Status':<14} {'Next wait'}")
    print("-" * 62)

    total_wait = 0
    for attempt_num in range(policy.max_attempts):
        # Mark as dispatched (increments attempt_count)
        envelope.mark_dispatched()
        success = simulate_delivery(envelope, attempt_num)

        if success:
            envelope.mark_acknowledged()
            envelope.mark_processed()
            wait_label = "—"
        else:
            envelope.mark_failed()
            wait_secs = envelope.next_retry_wait_seconds()
            total_wait += wait_secs
            wait_label = f"{wait_secs}s" if wait_secs else "—"

        action = "SUCCESS" if success else "FAILED"
        print(f"  #{envelope.attempt_count:<7} {action:<22} {envelope.status.value:<14} {wait_label}")

        if envelope.is_terminal():
            break

    print()
    print(f"Final status:    {envelope.status.value}")
    print(f"Total attempts:  {envelope.attempt_count}")
    print(f"Total wait time: {total_wait}s ({total_wait // 60}m {total_wait % 60}s)")
    print(f"Is terminal:     {envelope.is_terminal()}")
    print()
    print("Audit log entry:")
    print(" ", envelope.to_audit_line())
    print("\nDone.")


if __name__ == "__main__":
    main()
