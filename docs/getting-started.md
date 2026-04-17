# Getting Started — integration-automation-patterns

This guide walks from installation to running a reliable enterprise integration pattern — idempotent event processing, saga compensation, transactional outbox — in under 10 minutes.

---

## 1. Install

```bash
pip install integration-automation-patterns
```

Optional extras:

```bash
pip install 'integration-automation-patterns[kafka]'   # KafkaEventEnvelope
pip install 'integration-automation-patterns[fastapi]' # FastAPI webhook router
pip install 'integration-automation-patterns[pydantic]' # Pydantic v2 validation
```

---

## 2. Core concepts

### Why integration fails

Enterprise systems that span CRM + ERP + messaging fail in four predictable ways:

1. **Duplicate processing** — at-least-once delivery means the same event arrives twice; without idempotency the operation runs twice
2. **Partial transaction failure** — the database write succeeds but the event publish fails; the systems are now inconsistent
3. **Silent data conflicts** — two systems update the same field; one silently overwrites the other
4. **Unrecoverable workflow state** — a multi-step saga fails partway through with no compensation

This library provides structural solutions to each failure mode:

| Problem | Pattern | Class |
|---------|---------|-------|
| Duplicate processing | Idempotent Event | `EventEnvelope` |
| Partial transaction failure | Transactional Outbox | `OutboxPublisher` |
| Silent data conflicts | Field Authority Model | `SyncBoundary` |
| Unrecoverable workflow state | Saga + Compensation | `SagaOrchestrator` |

---

## 3. Idempotent event processing

The `EventEnvelope` assigns a stable `event_id` to every event. Before processing, check the ID against a deduplication store. The same event arriving twice only executes once.

```python
from integration_automation_patterns import EventEnvelope, RetryPolicy, DeliveryStatus

event = EventEnvelope(
    event_id="evt_8f3a1b",          # idempotency key — UUID or CRM record ID
    source="salesforce",
    event_type="contact.updated",
    payload={"email": "alice@example.com", "phone": "+1-555-0100"},
    schema_version="1.0",
    retry_policy=RetryPolicy(max_attempts=3, backoff_base_seconds=2),
)

# Before processing:
if dedup_store.has(event.event_id):
    return  # already processed — safe to discard

# Process:
result = process_contact_update(event.payload)
event.mark_delivered()              # DeliveryStatus.DELIVERED
dedup_store.add(event.event_id)
```

---

## 4. Field authority model

When CRM and ERP both maintain a customer record, which system owns each field? `SyncBoundary` makes this explicit and detectable:

```python
from integration_automation_patterns import SyncBoundary, RecordAuthority

boundary = SyncBoundary(
    authorities={
        "email":  RecordAuthority(owner="salesforce", read_others=["erp"]),
        "phone":  RecordAuthority(owner="erp",        read_others=["salesforce"]),
        "status": RecordAuthority(owner="erp",        read_others=["salesforce"]),
    }
)

conflicts = boundary.detect_conflict(
    incoming_system="salesforce",
    fields={"email": "alice@example.com", "phone": "+1-555-0100"},
)
# conflicts → {"phone": SyncConflict(owner="erp", incoming_system="salesforce")}
# "phone" belongs to ERP; Salesforce cannot overwrite it
```

---

## 5. Transactional outbox

The outbox pattern writes the event to the same database transaction as the domain change. A separate relay process delivers the event to the broker. This makes the write + publish atomic:

```python
from integration_automation_patterns import OutboxPublisher, OutboxRecord

# Inside a database transaction:
with db.transaction():
    db.save(customer_record)
    publisher = OutboxPublisher(db_connection=db)
    publisher.publish(OutboxRecord(
        event_id="evt_9b2c",
        destination="crm.contacts.updated",
        payload=customer_record.to_dict(),
    ))
# Both the record and the outbox entry commit together.
# Relay process polls the outbox table and delivers to Kafka/SQS/RabbitMQ.
```

---

## 6. Saga: distributed transaction with compensation

```python
from integration_automation_patterns import SagaOrchestrator, SagaStep

saga = SagaOrchestrator(steps=[
    SagaStep(
        name="reserve_inventory",
        action=lambda: inventory.reserve(order.items),
        compensation=lambda: inventory.release(order.items),
    ),
    SagaStep(
        name="charge_payment",
        action=lambda: payment.charge(order.total),
        compensation=lambda: payment.refund(order.total),
    ),
    SagaStep(
        name="schedule_fulfillment",
        action=lambda: fulfillment.schedule(order.id),
        compensation=lambda: fulfillment.cancel(order.id),
    ),
])

result = saga.execute()
if not result.success:
    # Compensation runs in reverse order automatically
    print(f"Saga failed at step: {result.failed_step}")
    print(f"Compensated steps: {result.compensated_steps}")
```

---

## 7. Circuit breaker

```python
from integration_automation_patterns import CircuitBreaker, CircuitOpenError

cb = CircuitBreaker(
    failure_threshold=5,
    recovery_timeout_seconds=30,
    half_open_max_calls=3,
)

try:
    result = cb.call(lambda: external_api.fetch(record_id))
except CircuitOpenError:
    # Circuit is OPEN — fallback to cache or queue for retry
    result = cache.get(record_id)
```

---

## 8. Broker-specific envelopes

The library provides broker-specific envelope wrappers that keep your domain event model broker-agnostic:

```python
# Kafka
from integration_automation_patterns import KafkaEventEnvelope
envelope = KafkaEventEnvelope.from_event(event, topic="contacts.updated", partition_key="stu_001")

# SQS
from integration_automation_patterns import SQSEventEnvelope
envelope = SQSEventEnvelope.from_event(event, queue_url="https://sqs.us-east-1.amazonaws.com/...")

# Google Cloud Pub/Sub
from integration_automation_patterns import GCPPubSubEnvelope
envelope = GCPPubSubEnvelope.from_event(event, topic="projects/my-project/topics/contacts")

# Azure Service Bus
from integration_automation_patterns import AzureServiceBusEnvelope
envelope = AzureServiceBusEnvelope.from_event(event, queue_name="contacts-updated")
```

---

## 9. Webhook handler

```python
from integration_automation_patterns import WebhookHandler, WebhookEvent

handler = WebhookHandler(
    signing_secret="wh_secret_abc",          # HMAC-SHA256 verification
    dedup_window_seconds=300,                # replay protection
)

# FastAPI / Flask endpoint:
@app.post("/webhooks/salesforce")
async def handle_webhook(request: Request):
    try:
        event: WebhookEvent = handler.parse(
            payload=await request.body(),
            signature=request.headers.get("X-SFDC-Signature"),
        )
    except WebhookSignatureError:
        return {"error": "invalid signature"}, 401
    except WebhookReplayError:
        return {"status": "duplicate"}, 200  # idempotent — already processed

    process_event(event)
    return {"status": "ok"}
```

---

## 10. Example files

| File | What it shows |
|------|---------------|
| `01_event_envelope_retry.py` | Idempotent event with retry |
| `02_transactional_outbox.py` | Outbox + relay delivery |
| `03_saga_compensation.py` | Multi-step saga rollback |
| `04_crm_erp_sync_boundary.py` | Field authority conflict detection |
| `06_end_to_end_integration.py` | All patterns combined |
| `22_event_sourcing_cqrs_patterns.py` | Aggregate root + event store |
| `42_mcp_security_patterns.py` | MCP tool security (CVE-2025-6514 class) |

---

## 11. Running tests

```bash
pip install pytest
pytest tests/ -v
```

1,865 tests, all passing.

---

## See also

- [API Reference](./api-reference.md)
- [Pattern Reference](./patterns.md)
- [Architecture Decision Records](./adr/)
- [regulated-ai-governance](https://github.com/ashutoshrana/regulated-ai-governance) — governance layer for agentic AI using these integration patterns
