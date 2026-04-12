"""
02_transactional_outbox.py — Transactional Outbox pattern with in-memory storage.

Demonstrates OutboxProcessor with plain Python lists acting as a fake database.
Shows how 5 OutboxRecords are created, processed in a batch, and how idempotency
is enforced: processing the same records a second time does not re-publish them.

Run:
    python examples/02_transactional_outbox.py
"""

import sys
import os
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from integration_automation_patterns.outbox import OutboxProcessor, OutboxRecord


def main() -> None:
    print("=" * 62)
    print("Transactional Outbox Demo — In-Memory Storage")
    print("=" * 62)

    # --- In-memory "database" ---
    # pending: records waiting to be published
    # published_ids: tracks which record_ids have been marked published
    pending: list[OutboxRecord] = []
    published_ids: set[str] = set()
    published_events: list[dict] = []  # broker's received messages

    # --- Populate outbox with 5 records ---
    events = [
        ("account-001", "AccountCreated", {"account_id": "001", "name": "Alice"}),
        ("account-002", "AccountCreated", {"account_id": "002", "name": "Bob"}),
        ("order-101", "OrderPlaced", {"order_id": "101", "sku": "WIDGET-A", "qty": 3}),
        ("order-102", "OrderPlaced", {"order_id": "102", "sku": "GADGET-B", "qty": 1}),
        ("payment-501", "PaymentProcessed", {"payment_id": "501", "amount": 49.99}),
    ]

    for agg_id, event_type, payload in events:
        record = OutboxRecord.create(
            aggregate_id=agg_id,
            event_type=event_type,
            payload=payload,
        )
        pending.append(record)

    print(f"\nOutbox populated with {len(pending)} records.")
    print("\nInitial outbox state:")
    print(f"  {'record_id':<38} {'aggregate_id':<14} {'event_type':<22} published")
    print("  " + "-" * 80)
    for r in pending:
        status = "Yes" if r.record_id in published_ids else "No"
        print(f"  {r.record_id:<38} {r.aggregate_id:<14} {r.event_type:<22} {status}")

    # --- Wire up the processor with in-memory callbacks ---

    def fetch_pending(batch_size: int) -> list[OutboxRecord]:
        """Return records that have not yet been marked published."""
        return [r for r in pending if r.record_id not in published_ids][:batch_size]

    def publish(event: dict) -> None:
        """Fake broker: store the event."""
        published_events.append(event)

    def mark_published(record_id: str) -> None:
        """Mark a record as published in our fake DB."""
        published_ids.add(record_id)
        for r in pending:
            if r.record_id == record_id:
                r.published_at = datetime.now(timezone.utc)
                break

    processor = OutboxProcessor(
        fetch_pending=fetch_pending,
        publish=publish,
        mark_published=mark_published,
    )

    # --- First batch ---
    print("\n--- Batch 1 (first pass) ---")
    pub1, fail1 = processor.process_batch(batch_size=10)
    print(f"Published: {pub1}  Failed: {fail1}")

    # --- Idempotency check: second pass on the same records ---
    print("\n--- Batch 2 (idempotency check — same records, should publish 0) ---")
    pub2, fail2 = processor.process_batch(batch_size=10)
    print(f"Published: {pub2}  Failed: {fail2}")

    assert pub2 == 0, "Idempotency broken: records published twice!"
    assert fail2 == 0, "Unexpected failures on second pass"

    # --- Final state ---
    print("\nFinal outbox state:")
    print(f"  {'record_id':<38} {'event_type':<22} published_at")
    print("  " + "-" * 80)
    for r in pending:
        ts = r.published_at.strftime("%H:%M:%S.%f") if r.published_at else "pending"
        print(f"  {r.record_id:<38} {r.event_type:<22} {ts}")

    print(f"\nTotal broker messages received: {len(published_events)}")
    print(f"Idempotency verified: second pass published 0 of {len(pending)} records")
    print("\nDone.")


if __name__ == "__main__":
    main()
