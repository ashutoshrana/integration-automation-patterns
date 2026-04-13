"""
06_end_to_end_integration.py — End-to-end integration: webhook → CDC → saga → outbox.

Demonstrates all four core integration patterns working together in a single
cohesive flow for a Salesforce Contact Created → SAP S/4HANA Customer Master
Record synchronization:

    Step 1 — Webhook receipt (WebhookHandler):
              Salesforce fires a ``contact.created`` webhook signed with HMAC-SHA256.
              The handler verifies the signature and parses the payload.

    Step 2 — Deduplication check (idempotency):
              A ProcessedWebhookStore checks the webhook's event_id before
              proceeding. Replayed webhooks are identified and skipped.

    Step 3 — CDC normalization (CDCEvent):
              The webhook payload is normalized into a CDCEvent (INSERT operation)
              that can be routed to any downstream consumer using standard
              Debezium envelope semantics.

    Step 4 — Saga orchestration (SagaOrchestrator):
              A 4-step saga executes the business workflow:
                4a. Validate contact data (name, email, phone)
                4b. Create SAP S/4HANA customer master record
                4c. Sync back to Salesforce (confirm Account linked)
                4d. Send welcome email via CommHub

    Step 5 — Transactional outbox (OutboxProcessor):
              Each saga step publishes a completion event through the outbox.
              The OutboxProcessor relays pending records to the message broker
              with guaranteed at-least-once delivery semantics.

Scenarios
---------

  A. Happy path — all 4 saga steps complete; outbox relays 4 events.

  B. Compensation — SAP step succeeds, CRM sync fails → SAP record deleted,
     clean saga rollback. Outbox publishes a ``SagaCompensated`` event.

  C. Webhook replay — the same event_id arrives twice (broker redelivery).
     The deduplication store identifies the duplicate and skips processing.

No external dependencies required.

Run:
    python examples/06_end_to_end_integration.py
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import sys
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from integration_automation_patterns.cdc_event import (
    CDCEvent,
)
from integration_automation_patterns.outbox import OutboxProcessor, OutboxRecord
from integration_automation_patterns.saga import SagaOrchestrator
from integration_automation_patterns.webhook_handler import (
    WebhookHandler,
    WebhookSignatureError,
)

# ---------------------------------------------------------------------------
# Shared webhook secret (in production, from Secret Manager / env)
# ---------------------------------------------------------------------------

_WEBHOOK_SECRET = "whsec_sf_contact_created_2026"

# ---------------------------------------------------------------------------
# Simulated processed-webhook store (in production: Redis SETNX or DB UPSERT)
# ---------------------------------------------------------------------------


class ProcessedWebhookStore:
    """Tracks processed webhook event_ids for idempotent reception."""

    def __init__(self) -> None:
        self._seen: dict[str, datetime] = {}

    def is_seen(self, event_id: str) -> bool:
        return event_id in self._seen

    def mark_seen(self, event_id: str) -> None:
        self._seen[event_id] = datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Simulated downstream systems (in production: SAP API, Salesforce API, etc.)
# ---------------------------------------------------------------------------


@dataclass
class SAPCustomerRecord:
    bp_number: str
    first_name: str
    last_name: str
    email: str
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class MockSAPSystem:
    """Simulates SAP S/4HANA Customer Master (Business Partner) API."""

    def __init__(self, *, fail_on_call: int | None = None) -> None:
        self._call_count = 0
        self._fail_on_call = fail_on_call
        self.records: dict[str, SAPCustomerRecord] = {}

    def create_customer(self, first_name: str, last_name: str, email: str) -> SAPCustomerRecord:
        self._call_count += 1
        if self._fail_on_call is not None and self._call_count == self._fail_on_call:
            raise RuntimeError("SAP API timeout: connection pool exhausted")
        bp_number = f"BP{uuid.uuid4().hex[:8].upper()}"
        record = SAPCustomerRecord(bp_number=bp_number, first_name=first_name, last_name=last_name, email=email)
        self.records[bp_number] = record
        return record

    def delete_customer(self, bp_number: str) -> None:
        """Compensation action: remove a created customer master record."""
        self.records.pop(bp_number, None)


class MockCRMSystem:
    """Simulates Salesforce Account link-back confirmation."""

    def __init__(self, *, fail_on_call: int | None = None) -> None:
        self._call_count = 0
        self._fail_on_call = fail_on_call

    def link_account(self, sf_contact_id: str, sap_bp_number: str) -> dict[str, str]:
        self._call_count += 1
        if self._fail_on_call is not None and self._call_count == self._fail_on_call:
            raise RuntimeError("Salesforce API: rate limit exceeded (429)")
        return {"sf_contact_id": sf_contact_id, "sap_bp_number": sap_bp_number, "status": "linked"}


class MockEmailSystem:
    """Simulates CommHub welcome email dispatch."""

    sent: list[dict[str, str]] = []

    def send_welcome(self, email: str, first_name: str) -> dict[str, str]:
        msg = {"to": email, "subject": f"Welcome, {first_name}!", "status": "queued"}
        self.sent.append(msg)
        return msg


# ---------------------------------------------------------------------------
# Transactional outbox store
# ---------------------------------------------------------------------------


class InMemoryOutboxStore:
    """In-memory outbox store. In production: a DB table in the same transaction."""

    def __init__(self) -> None:
        self._records: list[OutboxRecord] = []
        self._by_id: dict[str, OutboxRecord] = {}

    def add(self, event_type: str, aggregate_id: str, payload: dict[str, Any]) -> OutboxRecord:
        record = OutboxRecord(
            aggregate_id=aggregate_id,
            event_type=event_type,
            payload=payload,
        )
        self._records.append(record)
        self._by_id[record.record_id] = record
        return record

    def fetch_pending(self, batch_size: int) -> list[OutboxRecord]:
        return [r for r in self._records if r.published_at is None][:batch_size]

    def mark_published(self, record_id: str) -> None:
        """Called by OutboxProcessor after successful broker publish."""
        if record_id in self._by_id:
            self._by_id[record_id].published_at = datetime.now(timezone.utc)

    def mark_failed(self, record_id: str, error: str, attempt_count: int) -> None:
        """Called by OutboxProcessor on publish failure."""
        if record_id in self._by_id:
            rec = self._by_id[record_id]
            rec.last_error = error
            rec.attempt_count = attempt_count

    def published_count(self) -> int:
        return sum(1 for r in self._records if r.published_at is not None)

    def pending_count(self) -> int:
        return sum(1 for r in self._records if r.published_at is None)


# ---------------------------------------------------------------------------
# Broker mock (in production: Kafka/SQS/Service Bus producer)
# ---------------------------------------------------------------------------


broker_log: list[dict[str, Any]] = []


def publish_to_broker(event: dict[str, Any]) -> None:
    """Mock broker publish — receives the event dict from record.to_event_dict()."""
    broker_log.append(
        {
            "event_type": event["event_type"],
            "aggregate_id": event["aggregate_id"],
            "record_id": event["record_id"],
            "published_at": datetime.now(timezone.utc).isoformat(),
        }
    )


# ---------------------------------------------------------------------------
# Step 1: Simulate inbound Salesforce webhook
# ---------------------------------------------------------------------------


def build_salesforce_webhook_payload(event_id: str, sf_contact_id: str) -> tuple[bytes, str]:
    """
    Return (raw_bytes, signature_header) for a mock Salesforce contact.created webhook.

    The 'id' field is used by WebhookHandler (default id_field='id').
    The 'type' field is the event discriminator (default type_field='type').
    """
    data = {
        "id": event_id,
        "type": "contact.created",
        "sf_contact_id": sf_contact_id,
        "data": {
            "first_name": "Priya",
            "last_name": "Sundaram",
            "email": "priya.sundaram@acmegroup.com",
            "phone": "+1-415-555-0192",
            "account_id": "001Vz000007ABCDEF",
        },
    }
    raw = json.dumps(data).encode("utf-8")
    sig = "sha256=" + hmac.new(_WEBHOOK_SECRET.encode(), raw, hashlib.sha256).hexdigest()
    return raw, sig


# ---------------------------------------------------------------------------
# Step 3: Convert webhook payload to CDCEvent (Debezium envelope format)
# ---------------------------------------------------------------------------


def to_cdc_event(contact_data: dict[str, Any], sf_contact_id: str) -> CDCEvent:
    """
    Normalize a Salesforce Contact payload into a CDCEvent using Debezium envelope format.

    In a real integration, the Salesforce CDC connector (MuleSoft, Debezium,
    or AWS DMS) would produce this envelope automatically.
    """
    debezium_payload = {
        "op": "c",  # INSERT
        "before": None,
        "after": {
            "sf_id": sf_contact_id,
            "first_name": contact_data["first_name"],
            "last_name": contact_data["last_name"],
            "email": contact_data["email"],
            "phone": contact_data["phone"],
            "account_id": contact_data["account_id"],
            "source_system": "salesforce",
        },
        "source": {
            "db": "salesforce_production",
            "schema": "crm",
            "table": "contact",
            "connector": "salesforce-cdc",
            "lsn": "sf-2026-04-13T10:00:00Z",
        },
    }
    return CDCEvent.from_debezium(debezium_payload)


# ---------------------------------------------------------------------------
# Full integration pipeline
# ---------------------------------------------------------------------------


def run_integration_pipeline(
    event_id: str,
    sf_contact_id: str,
    webhook_store: ProcessedWebhookStore,
    sap: MockSAPSystem,
    crm: MockCRMSystem,
    email: MockEmailSystem,
    outbox: InMemoryOutboxStore,
    scenario_label: str,
) -> None:
    print(f"\n  Scenario {scenario_label}: event_id={event_id}, sf_contact_id={sf_contact_id}")

    # ---- Step 1: Receive and verify Salesforce webhook ----
    handler = WebhookHandler(secret=_WEBHOOK_SECRET, tolerance_seconds=None)  # no replay check for demo
    raw_payload, signature = build_salesforce_webhook_payload(event_id, sf_contact_id)
    try:
        webhook_event = handler.parse(raw_payload, signature)
    except WebhookSignatureError as exc:
        print(f"  [STEP 1] SIGNATURE INVALID: {exc}")
        return
    print(f"  [STEP 1] Webhook verified: type={webhook_event.event_type}, id={webhook_event.event_id}")

    # ---- Step 2: Deduplication check ----
    if webhook_store.is_seen(webhook_event.event_id):
        print(f"  [STEP 2] DUPLICATE — event_id={webhook_event.event_id} already processed. Skipped.")
        return
    webhook_store.mark_seen(webhook_event.event_id)
    print("  [STEP 2] Deduplication: new event, processing.")

    # ---- Step 3: Normalize to CDCEvent ----
    contact_data = webhook_event.payload["data"]
    cdc_event = to_cdc_event(contact_data, sf_contact_id)
    after = cdc_event.after or {}
    print(
        f"  [STEP 3] CDC normalized: op={cdc_event.operation.value}, "
        f"table={cdc_event.source.table}, "
        f"changed_fields={sorted(after.keys())[:4]}"
    )

    # ---- Step 4: Saga orchestration ----
    # Mutable state shared between saga steps and their compensations
    saga_state: dict[str, Any] = {"bp_number": None}

    def step_validate() -> dict[str, Any]:
        """4a. Validate contact data before creating downstream records."""
        required = ["first_name", "last_name", "email"]
        missing = [f for f in required if not after.get(f)]
        if missing:
            raise ValueError(f"Contact record missing required fields: {missing}")
        return {"valid": True, "fields_checked": required}

    def step_create_sap() -> SAPCustomerRecord:
        """4b. Create SAP S/4HANA Business Partner (customer master record)."""
        record = sap.create_customer(
            first_name=after["first_name"],
            last_name=after["last_name"],
            email=after["email"],
        )
        saga_state["bp_number"] = record.bp_number  # capture for compensation
        outbox.add(
            event_type="SAPCustomerCreated",
            aggregate_id=sf_contact_id,
            payload={"sf_contact_id": sf_contact_id, "sap_bp_number": record.bp_number},
        )
        return record

    def compensate_create_sap() -> None:
        """Rollback: delete the SAP customer master record if a later step fails."""
        if saga_state.get("bp_number"):
            sap.delete_customer(saga_state["bp_number"])
            outbox.add(
                event_type="SAPCustomerDeleted",
                aggregate_id=sf_contact_id,
                payload={"sap_bp_number": saga_state["bp_number"], "reason": "saga_compensation"},
            )

    def step_sync_crm() -> dict[str, str]:
        """4c. Confirm the SAP BP link back in Salesforce."""
        bp_number = saga_state.get("bp_number") or ""
        link = crm.link_account(sf_contact_id, bp_number)
        outbox.add(
            event_type="CRMAccountLinked",
            aggregate_id=sf_contact_id,
            payload=link,
        )
        return link

    def compensate_sync_crm() -> None:
        """Rollback: log that the CRM link was never established."""
        outbox.add(
            event_type="CRMSyncFailed",
            aggregate_id=sf_contact_id,
            payload={"sf_contact_id": sf_contact_id, "reason": "saga_compensation"},
        )

    def step_send_welcome() -> dict[str, str]:
        """4d. Send welcome email via CommHub."""
        msg = email.send_welcome(after["email"], after["first_name"])
        outbox.add(
            event_type="WelcomeEmailQueued",
            aggregate_id=sf_contact_id,
            payload={"to": after["email"], "status": msg["status"]},
        )
        return msg

    saga = (
        SagaOrchestrator(name="contact_onboarding_saga")
        .add_step("validate_contact", step_validate, lambda: None)
        .add_step("create_sap_customer", step_create_sap, compensate_create_sap)
        .add_step("sync_crm_account", step_sync_crm, compensate_sync_crm)
        .add_step("send_welcome_email", step_send_welcome, lambda: None)
    )

    result = saga.execute()

    if result.succeeded:
        print(f"  [STEP 4] Saga completed: {result.completed_steps}")
    else:
        print(f"  [STEP 4] Saga failed at step '{result.failed_step}': {result.error}")
        print(f"           Compensated steps: {result.compensated_steps}")
        # Publish a saga-level compensation event
        outbox.add(
            event_type="SagaCompensated",
            aggregate_id=sf_contact_id,
            payload={
                "failed_step": result.failed_step,
                "compensated_steps": result.compensated_steps,
                "error": str(result.error),
            },
        )

    # ---- Step 5: Outbox relay ----
    processor = OutboxProcessor(
        fetch_pending=outbox.fetch_pending,
        publish=publish_to_broker,
        mark_published=outbox.mark_published,
        mark_failed=outbox.mark_failed,
    )
    published, failed = processor.process_batch(batch_size=10)
    print(
        f"  [STEP 5] Outbox relay: {published} published, {failed} failed. "
        f"Broker log: {[e['event_type'] for e in broker_log[-published:]]}"
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    print("=" * 70)
    print("End-to-End Integration: Webhook → CDC → Saga → Outbox")
    print("Flow: Salesforce contact.created → SAP S/4HANA Customer Master")
    print("=" * 70)

    outbox = InMemoryOutboxStore()
    webhook_store = ProcessedWebhookStore()
    email_system = MockEmailSystem()

    # ------------------------------------------------------------------
    # Scenario A: Happy path — all 4 saga steps complete
    # ------------------------------------------------------------------
    print("\n── Scenario A: Happy path (all 4 steps succeed) ─────────────────────")
    run_integration_pipeline(
        event_id="evt_sf_001",
        sf_contact_id="003Vz000001ABCDE",
        webhook_store=webhook_store,
        sap=MockSAPSystem(),
        crm=MockCRMSystem(),
        email=email_system,
        outbox=outbox,
        scenario_label="A",
    )

    # ------------------------------------------------------------------
    # Scenario B: Compensation — CRM sync fails, SAP record rolled back
    # ------------------------------------------------------------------
    print("\n── Scenario B: Compensation (CRM sync fails after SAP succeeds) ─────")
    run_integration_pipeline(
        event_id="evt_sf_002",
        sf_contact_id="003Vz000002FGHIJ",
        webhook_store=webhook_store,
        sap=MockSAPSystem(),
        crm=MockCRMSystem(fail_on_call=1),  # CRM API fails on first call
        email=email_system,
        outbox=outbox,
        scenario_label="B",
    )

    # ------------------------------------------------------------------
    # Scenario C: Webhook replay — same event_id rejected
    # ------------------------------------------------------------------
    print("\n── Scenario C: Webhook replay (duplicate event_id rejected) ─────────")
    run_integration_pipeline(
        event_id="evt_sf_001",  # Same event_id as Scenario A
        sf_contact_id="003Vz000001ABCDE",
        webhook_store=webhook_store,
        sap=MockSAPSystem(),
        crm=MockCRMSystem(),
        email=email_system,
        outbox=outbox,
        scenario_label="C",
    )

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print("\n\n── Integration Pipeline Summary ─────────────────────────────────────")
    print(f"  Broker events published: {len(broker_log)}")
    print(f"  Outbox records total:    {outbox.published_count() + outbox.pending_count()}")
    print(f"  Outbox published:        {outbox.published_count()}")
    print(f"  Outbox pending:          {outbox.pending_count()}")
    print("  SAP records created:     (see broker log for SAPCustomerCreated events)")

    event_type_counts: dict[str, int] = {}
    for entry in broker_log:
        t = entry["event_type"]
        event_type_counts[t] = event_type_counts.get(t, 0) + 1
    print("\n  Broker event breakdown:")
    for event_type, count in sorted(event_type_counts.items()):
        print(f"    {event_type:<30s} × {count}")

    print("\n── Integration Pattern Map ──────────────────────────────────────────")
    patterns = [
        ("Step 1", "WebhookHandler", "HMAC-SHA256 signature verification + payload parsing"),
        ("Step 2", "ProcessedWebhookStore", "Idempotent reception — event_id deduplication"),
        ("Step 3", "CDCEvent.from_debezium()", "Canonical event envelope for downstream routing"),
        ("Step 4", "SagaOrchestrator", "Forward execution + automatic compensation on failure"),
        ("Step 5", "OutboxProcessor", "At-least-once broker relay via transactional outbox"),
    ]
    for step, component, description in patterns:
        print(f"  {step}: {component:<25s} {description}")

    print()


if __name__ == "__main__":
    main()
