"""
05_idempotent_crm_update.py — Idempotent CRM record update with deduplication.

Demonstrates a complete retry-safe event flow for a Salesforce webhook:

  1. Inbound webhook fires when a Lead is converted to a Contact.
  2. The integration publishes an EventEnvelope with a unique event_id.
  3. The downstream CRM-ERP sync handler receives the event.
  4. The handler checks the processed-event store before applying the update.
  5. On duplicate delivery, the event is skipped — the update is NOT re-applied.
  6. On transient failure, the event is retried with exponential backoff.
  7. Every outcome is logged to the audit trail.

Key pattern: idempotency key = event_id, set at source, not at consumer.
This matches ADR 001 (structural idempotency) in this repo.

Run:
    python examples/05_idempotent_crm_update.py
"""

from __future__ import annotations

import os
import sys
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from integration_automation_patterns.event_envelope import (
    EventEnvelope,
    RetryPolicy,
)

# ---------------------------------------------------------------------------
# Simulated processed-event store (in production: Redis SETNX or DB UPSERT)
# ---------------------------------------------------------------------------


class ProcessedEventStore:
    """
    Tracks processed event_ids to enable idempotent consumers.

    In production this is backed by Redis (SETNX with TTL), a database
    unique constraint on event_id, or the message broker's built-in
    exactly-once semantics.

    This in-memory version is used here for illustration.
    """

    def __init__(self) -> None:
        self._processed: dict[str, datetime] = {}

    def is_processed(self, event_id: str) -> bool:
        return event_id in self._processed

    def mark_processed(self, event_id: str) -> None:
        self._processed[event_id] = datetime.now(timezone.utc)

    def count(self) -> int:
        return len(self._processed)


# ---------------------------------------------------------------------------
# Simulated CRM update handler
# ---------------------------------------------------------------------------


@dataclass
class CRMUpdateResult:
    event_id: str
    outcome: str  # "applied" | "skipped_duplicate" | "failed" | "dead_lettered"
    attempt_count: int
    notes: str
    processed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


def apply_crm_contact_update(
    payload: dict[str, Any],
    attempt_num: int,
    fail_attempts: int,
) -> bool:
    """
    Simulate applying a CRM contact update.

    Fails for the first ``fail_attempts`` attempts, then succeeds.
    Models transient downstream unavailability (e.g. Salesforce API rate limit).
    """
    return attempt_num >= fail_attempts


# ---------------------------------------------------------------------------
# Core handler: idempotent CRM event processor
# ---------------------------------------------------------------------------


def process_event(
    envelope: EventEnvelope,
    store: ProcessedEventStore,
    fail_attempts: int = 2,
) -> CRMUpdateResult:
    """
    Process one EventEnvelope idempotently.

    Steps:
      1. Check processed-event store — skip if already processed.
      2. Attempt the CRM update with retries.
      3. On success, mark the event as processed.
      4. On retry exhaustion, move to dead letter.
    """
    # Step 1: Deduplication check
    if store.is_processed(envelope.event_id):
        envelope.mark_skipped()
        return CRMUpdateResult(
            event_id=envelope.event_id,
            outcome="skipped_duplicate",
            attempt_count=0,
            notes="event_id already in processed store — update NOT re-applied",
        )

    # Step 2: Attempt delivery with retries
    for attempt_num in range(envelope.retry_policy.max_attempts):
        envelope.mark_dispatched()
        success = apply_crm_contact_update(
            payload=envelope.payload,
            attempt_num=attempt_num,
            fail_attempts=fail_attempts,
        )

        if success:
            envelope.mark_acknowledged()
            envelope.mark_processed()
            # Step 3: Mark processed — future duplicates will be skipped
            store.mark_processed(envelope.event_id)
            return CRMUpdateResult(
                event_id=envelope.event_id,
                outcome="applied",
                attempt_count=envelope.attempt_count,
                notes=f"contact record updated after {envelope.attempt_count} attempt(s)",
            )
        else:
            envelope.mark_failed()
            if envelope.is_terminal():
                break
            wait = envelope.next_retry_wait_seconds()
            # In production: sleep(wait) before re-enqueueing
            _ = wait  # skip actual sleep in demo

    # Step 4: Dead letter path
    return CRMUpdateResult(
        event_id=envelope.event_id,
        outcome="dead_lettered",
        attempt_count=envelope.attempt_count,
        notes="retry budget exhausted — event routed to DLQ for manual review",
    )


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------


def make_lead_converted_event(event_id: str | None = None) -> EventEnvelope:
    return EventEnvelope(
        event_id=event_id or str(uuid.uuid4()),
        event_type="crm.lead.converted",
        source_system="salesforce-webhook",
        payload={
            "lead_id": "00Q8a000002EwbXEAS",
            "contact_id": "0038a000004QlGkAAK",
            "account_id": "0018a000002PjXoAAK",
            "converted_by": "user:j.chen@acme-corp.com",
            "converted_at": "2026-04-12T14:30:00Z",
            "sync_target": "sap_s4",
        },
        correlation_id="wf-crm-erp-sync-001",
        retry_policy=RetryPolicy(
            max_attempts=4,
            backoff_seconds=10,
            exponential=True,
            max_backoff_seconds=120,
        ),
    )


def main() -> None:
    store = ProcessedEventStore()
    audit_trail: list[CRMUpdateResult] = []

    print("=" * 66)
    print("Idempotent CRM Update — Retry-Safe Event Flow")
    print("Salesforce Lead Converted → SAP S/4HANA Contact Sync")
    print("=" * 66)

    # -----------------------------------------------------------------------
    # Scenario A: First delivery — fails twice, then succeeds
    # -----------------------------------------------------------------------
    print("\n" + "─" * 66)
    print("SCENARIO A: First delivery (2 transient failures, then success)")
    print("─" * 66)

    event_id = str(uuid.uuid4())
    envelope_a = make_lead_converted_event(event_id=event_id)

    print(f"\n  Event ID:  {envelope_a.event_id}")
    print(f"  Type:      {envelope_a.event_type}")
    print(f"  Source:    {envelope_a.source_system}")
    print(f"  Payload:   lead_id={envelope_a.payload['lead_id'][:18]}...")
    print(f"  Max attempts: {envelope_a.retry_policy.max_attempts}")
    print()
    print(f"  {'Attempt':<10} {'Outcome':<20} {'Status'}")
    print("  " + "-" * 50)

    # Manually step through so we can print each attempt
    fail_count = 2
    for attempt_num in range(envelope_a.retry_policy.max_attempts):
        envelope_a.mark_dispatched()
        success = apply_crm_contact_update(envelope_a.payload, attempt_num, fail_count)

        if success:
            envelope_a.mark_acknowledged()
            envelope_a.mark_processed()
            store.mark_processed(envelope_a.event_id)
            print(f"  #{envelope_a.attempt_count:<9} {'SUCCESS':<20} {envelope_a.status.value}")
            result_a = CRMUpdateResult(
                event_id=event_id,
                outcome="applied",
                attempt_count=envelope_a.attempt_count,
                notes=f"contact record synced after {envelope_a.attempt_count} attempt(s)",
            )
            audit_trail.append(result_a)
            break
        else:
            envelope_a.mark_failed()
            wait = envelope_a.next_retry_wait_seconds()
            wait_str = f"retry in {wait}s" if wait else "—"
            print(f"  #{envelope_a.attempt_count:<9} {'FAILED':<20} {envelope_a.status.value}  [{wait_str}]")

    print()
    print(f"  Final status: {envelope_a.status.value}")
    print(f"  Processed store size: {store.count()} event(s)")

    # -----------------------------------------------------------------------
    # Scenario B: Broker redelivers the SAME event (duplicate)
    # -----------------------------------------------------------------------
    print("\n" + "─" * 66)
    print("SCENARIO B: Broker redelivers the same event (duplicate)")
    print("─" * 66)

    # Duplicate delivery: same event_id, delivered again
    envelope_b = make_lead_converted_event(event_id=event_id)

    print(f"\n  Event ID:  {envelope_b.event_id} (same as Scenario A)")
    print(f"  store.is_processed(event_id): {store.is_processed(event_id)}")
    print()

    result_b = process_event(envelope_b, store, fail_attempts=0)
    audit_trail.append(result_b)

    print(f"  Outcome:          {result_b.outcome}")
    print(f"  Attempt count:    {result_b.attempt_count}")
    print(f"  Notes:            {result_b.notes}")
    print(f"  Envelope status:  {envelope_b.status.value}")
    print()
    print("  CRM record was NOT updated again — idempotency preserved.")

    # -----------------------------------------------------------------------
    # Scenario C: Different event (new lead conversion) — always succeeds
    # -----------------------------------------------------------------------
    print("\n" + "─" * 66)
    print("SCENARIO C: New event (different event_id) — succeeds on first attempt")
    print("─" * 66)

    envelope_c = make_lead_converted_event()
    envelope_c.payload["lead_id"] = "00Q8a000003RtcYEAS"  # different lead

    result_c = process_event(envelope_c, store, fail_attempts=0)
    audit_trail.append(result_c)

    print(f"\n  Event ID:   {envelope_c.event_id}")
    print(f"  Outcome:    {result_c.outcome}")
    print(f"  Attempts:   {result_c.attempt_count}")
    print(f"  Notes:      {result_c.notes}")
    print(f"  Store size: {store.count()} event(s)")

    # -----------------------------------------------------------------------
    # Scenario D: Event that exhausts retry budget → dead letter
    # -----------------------------------------------------------------------
    print("\n" + "─" * 66)
    print("SCENARIO D: Persistent failure — retry budget exhausted (DLQ)")
    print("─" * 66)

    envelope_d = make_lead_converted_event()
    result_d = process_event(envelope_d, store, fail_attempts=99)  # never succeeds
    audit_trail.append(result_d)

    print(f"\n  Event ID:    {envelope_d.event_id}")
    print(f"  Outcome:     {result_d.outcome}")
    print(f"  Attempts:    {result_d.attempt_count}")
    print(f"  Notes:       {result_d.notes}")

    # -----------------------------------------------------------------------
    # Audit trail
    # -----------------------------------------------------------------------
    print("\n" + "─" * 66)
    print("AUDIT TRAIL")
    print("─" * 66)
    print()
    for i, r in enumerate(audit_trail, 1):
        print(f"  [{i}] {r.outcome:<24} attempts={r.attempt_count}  event_id={r.event_id[:16]}...")
        print(f"       {r.notes}")
        print()

    # -----------------------------------------------------------------------
    # Design notes
    # -----------------------------------------------------------------------
    print("─" * 66)
    print("KEY IDEMPOTENCY RULES")
    print("─" * 66)
    print(
        """
  1. The idempotency key (event_id) is set at the SOURCE, not the consumer.
     A UUID generated at the Salesforce webhook handler identifies this
     specific conversion event. All downstream consumers use the same key.

  2. Deduplication happens BEFORE business logic runs.
     Check the processed store before attempting the CRM update. Never apply
     the update and then check — the update may have partial side effects.

  3. The processed store is durable.
     Redis SETNX with a 24h TTL, or a DB unique constraint on event_id.
     In-memory stores do not survive restarts and break replay safety.

  4. "Skipped duplicate" is a success outcome.
     The consumer does not return an error for a duplicate — it returns
     SKIPPED and acknowledges the message. Returning an error causes the
     broker to redeliver, creating an infinite loop.

  5. Dead-lettered events require human review, not silent discard.
     After retry exhaustion, the event is routed to a DLQ. An operator
     must investigate the root cause before replaying or discarding it.

  See: docs/adr/001-idempotency-key-design.md for the full rationale.
"""
    )


if __name__ == "__main__":
    main()
